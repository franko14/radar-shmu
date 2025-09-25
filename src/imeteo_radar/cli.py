#!/usr/bin/env python3
"""
Command-line interface for imeteo-radar

Focused on DWD dmax product with simple fetch command.
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple


def create_parser() -> argparse.ArgumentParser:
    """Create command-line argument parser"""
    parser = argparse.ArgumentParser(
        description="Weather radar data processor for DWD",
        prog="imeteo-radar"
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Fetch command - simplified for DWD dmax
    fetch_parser = subparsers.add_parser(
        'fetch',
        help='Download and process radar data to PNG'
    )
    fetch_parser.add_argument(
        '--source',
        choices=['dwd', 'shmu'],
        default='dwd',
        help='Radar source (DWD for Germany, SHMU for Slovakia)'
    )
    fetch_parser.add_argument(
        '--output',
        type=Path,
        help='Output directory (default: /tmp/{country}/)'
    )
    fetch_parser.add_argument(
        '--backload',
        action='store_true',
        help='Enable backload of historical data'
    )
    fetch_parser.add_argument(
        '--hours',
        type=int,
        help='Number of hours to backload'
    )
    fetch_parser.add_argument(
        '--from',
        dest='from_time',
        type=str,
        help='Start time for backload (YYYY-MM-DD HH:MM)'
    )
    fetch_parser.add_argument(
        '--to',
        dest='to_time',
        type=str,
        help='End time for backload (YYYY-MM-DD HH:MM)'
    )
    fetch_parser.add_argument(
        '--update-extent',
        action='store_true',
        help='Force update extent_index.json file'
    )

    # Extent command - generate extent information only
    extent_parser = subparsers.add_parser(
        'extent',
        help='Generate extent information JSON'
    )
    extent_parser.add_argument(
        '--source',
        choices=['dwd', 'shmu', 'all'],
        default='all',
        help='Radar source(s) to generate extent for'
    )
    extent_parser.add_argument(
        '--output',
        type=Path,
        help='Output directory (default: /tmp/{country}/)'
    )

    return parser


def parse_time_range(from_time: Optional[str], to_time: Optional[str],
                    hours: Optional[int]) -> Tuple[datetime, datetime]:
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


def generate_extent_info(source, source_name: str, country_dir: str) -> dict:
    """Generate extent information from a radar source"""
    from datetime import datetime

    extent = source.get_extent()

    # Build extent info structure
    extent_info = {
        "name": source_name,
        "country": country_dir.capitalize(),
        "generated": datetime.now().isoformat() + "Z",
        "extent": extent.get('wgs84', {}),
        "projection": extent.get('projection', 'unknown'),
        "grid_size": extent.get('grid_size', []),
        "resolution_m": extent.get('resolution_m', [])
    }

    # Add mercator bounds if available
    if 'mercator' in extent:
        extent_info['mercator'] = extent['mercator']

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
            "coordinate_system": "WGS84 geographic coordinates (EPSG:4326)"
        },
        "source": extent_info
    }

    # Save to file
    with open(extent_file, 'w') as f:
        json.dump(extent_data, f, indent=2)

    print(f"💾 Saved extent information to: {extent_file}")
    return True


def fetch_command(args) -> int:
    """Handle fetch command for radar data"""

    # Import here to avoid circular imports and speed up CLI startup
    try:
        from .sources.dwd import DWDRadarSource
        from .sources.shmu import SHMURadarSource
        from .processing.exporter import PNGExporter

        # Initialize source based on selection
        if args.source == 'dwd':
            source = DWDRadarSource()
            product = 'dmax'
            country_dir = 'germany'
        elif args.source == 'shmu':
            source = SHMURadarSource()
            product = 'zmax'
            country_dir = 'slovakia'
        else:
            print(f"❌ Unknown source: {args.source}")
            return 1

        exporter = PNGExporter()

        # Set output directory based on source
        if not args.output:
            output_dir = Path(f"/tmp/{country_dir}/")
        else:
            output_dir = args.output

        # Create output directory if needed
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate and save extent information on first run or if requested
        extent_info = generate_extent_info(source, args.source.upper(), country_dir)
        save_extent_index(output_dir, extent_info, force=getattr(args, 'update_extent', False))

        print(f"📡 Fetching {args.source.upper()} {product} radar data...")
        print(f"📁 Output directory: {output_dir}")

        if args.backload:
            # Handle backload
            start, end = parse_time_range(args.from_time, args.to_time, args.hours)

            print(f"⏰ Backload period: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}")

            # Calculate number of 5-minute intervals
            time_diff = end - start
            intervals = int(time_diff.total_seconds() / 300)  # 5 minutes = 300 seconds

            # Limit to reasonable number
            intervals = min(intervals, 100)  # Max 100 files

            print(f"📥 Downloading up to {intervals} timestamps...")

            # Download data (don't use LATEST for backload)
            if args.source == 'dwd':
                files = source.download_latest(count=intervals, products=[product], use_latest=False)
            else:  # SHMU
                files = source.download_latest(count=intervals, products=[product])

            if not files:
                print("❌ No data available for the specified period")
                return 1

            print(f"✅ Downloaded {len(files)} files")

            # Process each file to PNG
            for file_info in files:
                try:
                    # Process to array
                    radar_data = source.process_to_array(file_info['path'])

                    # Extract timestamp for filename
                    timestamp_str = file_info['timestamp']
                    # Convert YYYYMMDDHHMM00 to datetime
                    dt = datetime.strptime(timestamp_str[:12], "%Y%m%d%H%M")
                    filename = dt.strftime("%Y-%m-%d_%H%M.png")
                    output_path = output_dir / filename

                    # Prepare data for PNG export
                    export_data = {
                        'data': radar_data['data'],
                        'timestamp': timestamp_str,
                        'product': product,
                        'source': args.source,
                        'units': 'dBZ'
                    }

                    # Export to PNG
                    extent = source.get_extent()
                    exporter.export_png(
                        radar_data=export_data,
                        output_path=output_path,
                        extent=extent,
                        colormap_type='reflectivity_shmu'  # Use SHMU colormap for consistency
                    )

                    print(f"💾 Saved: {output_path}")

                except Exception as e:
                    print(f"⚠️  Failed to process {file_info['timestamp']}: {e}")
                    continue

            # Clean up temporary files after backload
            source.cleanup_temp_files()

        else:
            # Just fetch latest using LATEST endpoint
            print("📥 Downloading latest timestamp...")

            if args.source == 'dwd':
                files = source.download_latest(count=1, products=[product], use_latest=True)
            else:  # SHMU
                files = source.download_latest(count=1, products=[product])

            if not files:
                print("❌ No data available")
                return 1

            file_info = files[0]

            # Process to array
            radar_data = source.process_to_array(file_info['path'])

            # Extract timestamp for filename
            timestamp_str = file_info['timestamp']
            dt = datetime.strptime(timestamp_str[:12], "%Y%m%d%H%M")
            filename = dt.strftime("%Y-%m-%d_%H%M.png")
            output_path = output_dir / filename

            # Prepare data for PNG export
            export_data = {
                'data': radar_data['data'],
                'timestamp': timestamp_str,
                'product': product,
                'source': args.source,
                'units': 'dBZ'
            }

            # Export to PNG
            extent = source.get_extent()
            exporter.export_png(
                radar_data=export_data,
                output_path=output_path,
                extent=extent,
                colormap_type='reflectivity_shmu'
            )

            print(f"✅ Saved: {output_path}")

        # Clean up temporary files
        source.cleanup_temp_files()

        return 0

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please ensure the package is properly installed with: pip install -e .")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        # Try to clean up if source exists
        try:
            if 'source' in locals():
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
        if args.command == 'fetch':
            return fetch_command(args)
        elif args.command == 'extent':
            return extent_command(args)
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
        from .sources.dwd import DWDRadarSource
        from .sources.shmu import SHMURadarSource
        import json

        sources_to_process = []

        if args.source == 'all' or args.source == 'dwd':
            sources_to_process.append(('dwd', DWDRadarSource(), 'germany'))

        if args.source == 'all' or args.source == 'shmu':
            sources_to_process.append(('shmu', SHMURadarSource(), 'slovakia'))

        combined_extent = {
            "metadata": {
                "title": "Radar Coverage Extents",
                "description": "Geographic extents and projection information for radar data sources",
                "version": "1.0",
                "generated": datetime.now().isoformat() + "Z",
                "coordinate_systems": {
                    "wgs84": "WGS84 geographic coordinates (EPSG:4326)"
                }
            },
            "sources": {}
        }

        for source_name, source_obj, country_dir in sources_to_process:
            print(f"📡 Generating extent for {source_name.upper()}...")

            # Get extent information
            extent_info = generate_extent_info(source_obj, source_name.upper(), country_dir)

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
        if args.source == 'all':
            combined_file = Path("/tmp/radar_extent_combined.json")
            with open(combined_file, 'w') as f:
                json.dump(combined_extent, f, indent=2)
            print(f"💾 Saved combined extent to: {combined_file}")

        return 0

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())