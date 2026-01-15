#!/usr/bin/env python3
"""
Command-line interface for imeteo-radar

Focused on DWD dmax product with simple fetch command.
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple


def create_parser() -> argparse.ArgumentParser:
    """Create command-line argument parser"""
    parser = argparse.ArgumentParser(
        description="Weather radar data processor for DWD", prog="imeteo-radar"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Fetch command - simplified for DWD dmax
    fetch_parser = subparsers.add_parser(
        "fetch", help="Download and process radar data to PNG"
    )
    fetch_parser.add_argument(
        "--source",
        choices=["dwd", "shmu", "chmi", "arso", "omsz"],
        default="dwd",
        help="Radar source (DWD for Germany, SHMU for Slovakia, CHMI for Czechia, ARSO for Slovenia, OMSZ for Hungary)",
    )
    fetch_parser.add_argument(
        "--output", type=Path, help="Output directory (default: /tmp/{country}/)"
    )
    fetch_parser.add_argument(
        "--backload", action="store_true", help="Enable backload of historical data"
    )
    fetch_parser.add_argument("--hours", type=int, help="Number of hours to backload")
    fetch_parser.add_argument(
        "--from",
        dest="from_time",
        type=str,
        help="Start time for backload (YYYY-MM-DD HH:MM)",
    )
    fetch_parser.add_argument(
        "--to",
        dest="to_time",
        type=str,
        help="End time for backload (YYYY-MM-DD HH:MM)",
    )
    fetch_parser.add_argument(
        "--update-extent",
        action="store_true",
        help="Force update extent_index.json file",
    )
    fetch_parser.add_argument(
        "--disable-upload",
        action="store_true",
        help="Disable upload to DigitalOcean Spaces (for local development only)",
    )

    # Extent command - generate extent information only
    extent_parser = subparsers.add_parser(
        "extent", help="Generate extent information JSON"
    )
    extent_parser.add_argument(
        "--source",
        choices=["dwd", "shmu", "chmi", "arso", "omsz", "all"],
        default="all",
        help="Radar source(s) to generate extent for",
    )
    extent_parser.add_argument(
        "--output", type=Path, help="Output directory (default: /tmp/{country}/)"
    )

    # Composite command - merge multiple sources
    composite_parser = subparsers.add_parser(
        "composite", help="Generate composite radar images from multiple sources"
    )
    composite_parser.add_argument(
        "--sources",
        type=str,
        default="dwd,shmu,chmi,omsz,arso",
        help="Comma-separated list of sources to merge (default: dwd,shmu,chmi,omsz,arso)",
    )
    composite_parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp/composite"),
        help="Output directory (default: /tmp/composite/)",
    )
    composite_parser.add_argument(
        "--resolution",
        type=float,
        default=500.0,
        help="Target resolution in meters (default: 500)",
    )
    composite_parser.add_argument(
        "--backload", action="store_true", help="Enable backload of historical data"
    )
    composite_parser.add_argument(
        "--hours", type=int, help="Number of hours to backload"
    )
    composite_parser.add_argument(
        "--from",
        dest="from_time",
        type=str,
        help='Start time for backload (format: "YYYY-MM-DD HH:MM")',
    )
    composite_parser.add_argument(
        "--to",
        dest="to_time",
        type=str,
        help='End time for backload (format: "YYYY-MM-DD HH:MM")',
    )
    composite_parser.add_argument(
        "--update-extent",
        action="store_true",
        help="Force update extent_index.json file",
    )
    composite_parser.add_argument(
        "--no-individual",
        action="store_true",
        help="Skip generating individual source images (only create composite)",
    )
    composite_parser.add_argument(
        "--timestamp-tolerance",
        type=int,
        default=2,
        help="Timestamp matching tolerance in minutes (default: 2)",
    )
    composite_parser.add_argument(
        "--require-arso",
        action="store_true",
        help="Fail if ARSO data cannot be matched (default: fallback to composite without ARSO)",
    )

    # Coverage mask command - generate static coverage masks
    coverage_parser = subparsers.add_parser(
        "coverage-mask", help="Generate static coverage mask PNG files"
    )
    coverage_parser.add_argument(
        "--source",
        choices=["dwd", "shmu", "chmi", "arso", "omsz", "all"],
        default="all",
        help="Radar source to generate mask for (default: all)",
    )
    coverage_parser.add_argument(
        "--composite",
        action="store_true",
        help="Generate composite coverage mask (combining all sources)",
    )
    coverage_parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp"),
        help="Base output directory (default: /tmp/) - masks saved alongside radar data",
    )
    coverage_parser.add_argument(
        "--resolution",
        type=float,
        default=500.0,
        help="Resolution for composite mask in meters (default: 500)",
    )

    return parser


def parse_time_range(
    from_time: Optional[str], to_time: Optional[str], hours: Optional[int]
) -> Tuple[datetime, datetime]:
    """Parse time range from arguments"""
    import pytz

    now = datetime.now(pytz.UTC)

    if from_time and to_time:
        # Parse specific time range
        start = datetime.strptime(from_time, "%Y-%m-%d %H:%M")
        end = datetime.strptime(to_time, "%Y-%m-%d %H:%M")
        # Make timezone aware
        start = pytz.UTC.localize(start)
        end = pytz.UTC.localize(end)
    elif hours:
        # Last N hours
        end = now
        start = now - timedelta(hours=hours)
    else:
        # Just latest
        end = now
        start = now - timedelta(minutes=30)  # Small window for latest

    return start, end


def parse_timestamp_to_datetime(timestamp_str: str, source: str) -> datetime:
    """Parse timestamp string to datetime based on source format

    Args:
        timestamp_str: Timestamp string from the source
        source: Source name (dwd, shmu, chmi, arso, omsz)

    Returns:
        datetime object
    """
    if source == "omsz":
        # OMSZ format: YYYYMMDD_HHMM or YYYYMMDDHHMM
        if "_" in timestamp_str:
            return datetime.strptime(timestamp_str, "%Y%m%d_%H%M")
        else:
            return datetime.strptime(timestamp_str[:12], "%Y%m%d%H%M")
    else:
        # DWD/SHMU/CHMI/ARSO format: YYYYMMDDHHMM00 (14 chars) or YYYYMMDDHHMM (12 chars)
        return datetime.strptime(timestamp_str[:12], "%Y%m%d%H%M")


def generate_extent_info(source, source_name: str, country_dir: str) -> dict:
    """Generate extent information from a radar source"""
    from datetime import datetime

    extent = source.get_extent()

    # Build extent info structure
    extent_info = {
        "name": source_name,
        "country": country_dir.capitalize(),
        "generated": datetime.now().isoformat() + "Z",
        "extent": extent.get("wgs84", {}),
        "projection": extent.get("projection", "unknown"),
        "grid_size": extent.get("grid_size", []),
        "resolution_m": extent.get("resolution_m", []),
    }

    # Add mercator bounds if available
    if "mercator" in extent:
        extent_info["mercator"] = extent["mercator"]

    return extent_info


def save_extent_index(output_dir: Path, extent_info: dict, force: bool = False):
    """Save extent information to JSON file"""
    import json

    extent_file = output_dir / "extent_index.json"

    # Check if file exists and skip if not forced
    if extent_file.exists() and not force:
        return False

    # Create the full structure
    extent_data = {
        "metadata": {
            "title": "Radar Coverage Extent",
            "description": "Geographic extent and projection information for radar data",
            "version": "1.0",
            "generated": extent_info["generated"],
            "coordinate_system": "WGS84 geographic coordinates (EPSG:4326)",
        },
        "source": extent_info,
    }

    # Save to file
    with open(extent_file, "w") as f:
        json.dump(extent_data, f, indent=2)

    print(f"💾 Saved extent information to: {extent_file}")
    return True


def cleanup_old_files(output_dir: Path, max_age_hours: int = 6):
    """
    Clean up PNG files older than max_age_hours

    Args:
        output_dir: Directory containing PNG files
        max_age_hours: Maximum age in hours (default: 6)
    """
    if not output_dir.exists():
        return

    current_time = time.time()
    max_age_seconds = max_age_hours * 3600
    deleted_count = 0

    for png_file in output_dir.glob("*.png"):
        # Skip extent_index.json and other non-PNG files
        if png_file.suffix != ".png":
            continue

        # Check file age
        file_age = current_time - png_file.stat().st_mtime

        if file_age > max_age_seconds:
            try:
                png_file.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"⚠️  Failed to delete {png_file.name}: {e}")

    if deleted_count > 0:
        print(
            f"🗑️  Cleaned up {deleted_count} old PNG files (older than {max_age_hours}h)"
        )


def fetch_command(args) -> int:
    """Handle fetch command for radar data"""

    # Import here to avoid circular imports and speed up CLI startup
    try:
        from .config.sources import get_source_config, get_source_instance
        from .processing.exporter import PNGExporter
        from .utils.spaces_uploader import SpacesUploader, is_spaces_configured

        # Initialize source using centralized registry
        source_config = get_source_config(args.source)
        if not source_config:
            print(f"❌ Unknown source: {args.source}")
            return 1

        source = get_source_instance(args.source)
        product = source_config["product"]
        country_dir = source_config["country"]

        exporter = PNGExporter()

        # Initialize DigitalOcean Spaces uploader
        uploader = None
        upload_enabled = not args.disable_upload

        if upload_enabled:
            if is_spaces_configured():
                try:
                    uploader = SpacesUploader()
                    print("☁️  DigitalOcean Spaces upload enabled")
                except Exception as e:
                    print(f"⚠️  Failed to initialize Spaces uploader: {e}")
                    print("⚠️  Falling back to local-only mode (upload disabled)")
                    upload_enabled = False
            else:
                print(
                    "⚠️  DigitalOcean Spaces not configured (missing environment variables)"
                )
                print("⚠️  Falling back to local-only mode (upload disabled)")
                upload_enabled = False
        else:
            print("📁 Local-only mode (--disable-upload flag used)")

        # Set output directory based on source
        if not args.output:
            output_dir = Path(f"/tmp/{country_dir}/")
        else:
            output_dir = args.output

        # Create output directory if needed
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate and save extent information on first run or if requested
        extent_info = generate_extent_info(source, args.source.upper(), country_dir)
        save_extent_index(
            output_dir, extent_info, force=getattr(args, "update_extent", False)
        )

        print(f"📡 Fetching {args.source.upper()} {product} radar data...")
        print(f"📁 Output directory: {output_dir}")

        if args.backload:
            # Handle backload
            start, end = parse_time_range(args.from_time, args.to_time, args.hours)

            print(
                f"⏰ Backload period: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}"
            )

            # Calculate number of 5-minute intervals
            time_diff = end - start
            intervals = int(time_diff.total_seconds() / 300)  # 5 minutes = 300 seconds

            # Limit to reasonable number
            intervals = min(intervals, 100)  # Max 100 files

            print(f"📥 Downloading up to {intervals} timestamps...")

            # Download data (don't use LATEST for backload)
            if args.source == "dwd":
                files = source.download_latest(
                    count=intervals,
                    products=[product],
                    use_latest=False,
                    start_time=start,
                    end_time=end,
                )
            else:  # SHMU
                files = source.download_latest(
                    count=intervals, products=[product], start_time=start, end_time=end
                )

            if not files:
                print("❌ No data available for the specified period")
                return 1

            print(f"✅ Downloaded {len(files)} files")

            # Process each file to PNG
            processed_count = 0
            for file_info in files:
                try:
                    # Extract timestamp for filename
                    timestamp_str = file_info["timestamp"]
                    # Convert to datetime using source-specific format
                    dt = parse_timestamp_to_datetime(timestamp_str, args.source)
                    unix_timestamp = int(dt.timestamp())
                    filename = f"{unix_timestamp}.png"
                    output_path = output_dir / filename

                    # Process to array
                    radar_data = source.process_to_array(file_info["path"])

                    # Prepare data for PNG export
                    export_data = {
                        "data": radar_data["data"],
                        "timestamp": timestamp_str,
                        "product": product,
                        "source": args.source,
                        "units": "dBZ",
                    }

                    # Export to PNG (using fast method to reduce memory usage)
                    extent = source.get_extent()
                    exporter.export_png_fast(
                        radar_data=export_data,
                        output_path=output_path,
                        extent=extent,
                        colormap_type="reflectivity_shmu",  # Use SHMU colormap for consistency
                    )

                    print(f"💾 Saved: {output_path}")
                    processed_count += 1

                    # Upload to DigitalOcean Spaces if enabled
                    if upload_enabled and uploader:
                        uploader.upload_file(output_path, args.source, filename)

                    # Clean up matplotlib and numpy memory after each file
                    import gc

                    import matplotlib.pyplot as plt

                    plt.close("all")
                    gc.collect()

                except Exception as e:
                    print(f"⚠️  Failed to process {file_info['timestamp']}: {e}")
                    continue

            # Summary
            print(f"✅ Summary: Processed {processed_count} files")

            # Clean up temporary files after backload
            source.cleanup_temp_files()

            # Clean up old PNG files (older than 6 hours)
            cleanup_old_files(output_dir, max_age_hours=6)

        else:
            # Just fetch latest using LATEST endpoint
            print("📥 Downloading latest timestamp...")

            if args.source == "dwd":
                files = source.download_latest(
                    count=1, products=[product], use_latest=True
                )
            else:  # SHMU
                files = source.download_latest(count=1, products=[product])

            if not files:
                print("❌ No data available")
                return 1

            file_info = files[0]

            # Extract timestamp for filename
            timestamp_str = file_info["timestamp"]
            dt = parse_timestamp_to_datetime(timestamp_str, args.source)
            unix_timestamp = int(dt.timestamp())
            filename = f"{unix_timestamp}.png"
            output_path = output_dir / filename

            # Process to array
            radar_data = source.process_to_array(file_info["path"])

            # Prepare data for PNG export
            export_data = {
                "data": radar_data["data"],
                "timestamp": timestamp_str,
                "product": product,
                "source": args.source,
                "units": "dBZ",
            }

            # Export to PNG (using fast method to reduce memory usage)
            extent = source.get_extent()
            exporter.export_png_fast(
                radar_data=export_data,
                output_path=output_path,
                extent=extent,
                colormap_type="reflectivity_shmu",
            )

            print(f"✅ Saved: {output_path}")

            # Upload to DigitalOcean Spaces if enabled
            if upload_enabled and uploader:
                uploader.upload_file(output_path, args.source, filename)

        # Clean up temporary files
        source.cleanup_temp_files()

        # Clean up old PNG files (older than 6 hours)
        cleanup_old_files(output_dir, max_age_hours=6)

        return 0

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please ensure the package is properly installed with: pip install -e .")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        # Try to clean up if source exists
        try:
            if "source" in locals():
                source.cleanup_temp_files()
        except:
            pass
        return 1


def main():
    """Main CLI entry point"""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    try:
        if args.command == "fetch":
            return fetch_command(args)
        elif args.command == "extent":
            return extent_command(args)
        elif args.command == "composite":
            return composite_command(args)
        elif args.command == "coverage-mask":
            return coverage_mask_command(args)
        else:
            print(f"Unknown command: {args.command}")
            return 1

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


def extent_command(args) -> int:
    """Handle extent generation command"""
    try:
        import json

        from .config.sources import (
            get_all_source_names,
            get_source_config,
            get_source_instance,
        )

        # Build list of sources to process
        sources_to_process = []
        source_names = get_all_source_names() if args.source == "all" else [args.source]

        for source_name in source_names:
            config = get_source_config(source_name)
            if config:
                sources_to_process.append(
                    (source_name, get_source_instance(source_name), config["country"])
                )

        combined_extent = {
            "metadata": {
                "title": "Radar Coverage Extents",
                "description": "Geographic extents and projection information for radar data sources",
                "version": "1.0",
                "generated": datetime.now().isoformat() + "Z",
                "coordinate_systems": {
                    "wgs84": "WGS84 geographic coordinates (EPSG:4326)"
                },
            },
            "sources": {},
        }

        for source_name, source_obj, country_dir in sources_to_process:
            print(f"📡 Generating extent for {source_name.upper()}...")

            # Get extent information
            extent_info = generate_extent_info(
                source_obj, source_name.upper(), country_dir
            )

            # Save individual extent file
            if args.output:
                output_dir = args.output
            else:
                output_dir = Path(f"/tmp/{country_dir}")

            output_dir.mkdir(parents=True, exist_ok=True)
            save_extent_index(output_dir, extent_info, force=True)

            # Add to combined structure
            combined_extent["sources"][source_name] = extent_info

        # If processing all sources, save combined file
        if args.source == "all":
            combined_file = Path("/tmp/radar_extent_combined.json")
            with open(combined_file, "w") as f:
                json.dump(combined_extent, f, indent=2)
            print(f"💾 Saved combined extent to: {combined_file}")

        return 0

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


def composite_command(args) -> int:
    """Handle composite generation command"""
    from .cli_composite import composite_command_impl

    return composite_command_impl(args)


def coverage_mask_command(args) -> int:
    """Handle coverage mask generation command"""
    try:
        import os

        from .config.sources import get_all_source_names, get_source_config
        from .processing.coverage_mask import (
            generate_all_coverage_masks,
            generate_composite_coverage_mask,
            generate_source_coverage_mask,
        )

        output_base = str(args.output)

        if args.composite:
            # Generate only composite mask
            result = generate_composite_coverage_mask(
                sources=get_all_source_names(),
                output_dir=os.path.join(output_base, "composite"),
                resolution_m=args.resolution,
            )
            return 0 if result else 1

        elif args.source == "all":
            # Generate all masks (individual + composite)
            results = generate_all_coverage_masks(
                output_base_dir=output_base, resolution_m=args.resolution
            )
            return 0 if results else 1

        else:
            # Generate single source mask
            config = get_source_config(args.source)
            if not config:
                print(f"❌ Unknown source: {args.source}")
                return 1

            folder = config["folder"]
            output_dir = os.path.join(output_base, folder)
            result = generate_source_coverage_mask(args.source, output_dir)
            return 0 if result else 1

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
