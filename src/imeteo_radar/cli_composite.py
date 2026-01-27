#!/usr/bin/env python3
"""
Composite command implementation for CLI

Separated into its own module to keep cli.py manageable.
"""

import gc
from pathlib import Path
from typing import Any


def composite_command_impl(args: Any) -> int:
    """Handle composite generation command"""
    try:
        from .processing.exporter import PNGExporter
        from .sources.arso import ARSORadarSource
        from .sources.chmi import CHMIRadarSource
        from .sources.dwd import DWDRadarSource
        from .sources.imgw import IMGWRadarSource
        from .sources.omsz import OMSZRadarSource
        from .sources.shmu import SHMURadarSource

        # Parse sources list
        source_names = [s.strip() for s in args.sources.split(",")]
        print(f"🎯 Creating composite from sources: {', '.join(source_names).upper()}")

        # Create output directory
        output_dir = args.output
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Output directory: {output_dir}")

        # Initialize sources
        sources = {}
        for source_name in source_names:
            if source_name == "dwd":
                sources["dwd"] = (DWDRadarSource(), "dmax")
            elif source_name == "shmu":
                sources["shmu"] = (SHMURadarSource(), "zmax")
            elif source_name == "chmi":
                sources["chmi"] = (CHMIRadarSource(), "maxz")
            elif source_name == "arso":
                sources["arso"] = (ARSORadarSource(), "zm")
            elif source_name == "omsz":
                sources["omsz"] = (OMSZRadarSource(), "cmax")
            elif source_name == "imgw":
                sources["imgw"] = (IMGWRadarSource(), "cmax")
            else:
                print(f"❌ Unknown source: {source_name}")
                return 1

        # Initialize PNG exporter
        exporter = PNGExporter()

        # Determine what to process
        if args.backload:
            # Backload mode - process historical data
            return _process_backload(args, sources, exporter, output_dir)
        else:
            # Single timestamp mode - process latest available data
            return _process_latest(args, sources, exporter, output_dir)

    except KeyboardInterrupt:
        print("\n❌ Interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1


def _find_common_timestamp_with_tolerance(
    timestamp_groups, sources, tolerance_minutes=2
):
    """Find most recent timestamp where all sources have data within tolerance window

    Args:
        timestamp_groups: Dict mapping timestamp str -> Dict[source_name, file_info]
        sources: Dict of source configurations
        tolerance_minutes: Maximum time difference allowed (default: 2 minutes)

    Returns:
        (common_timestamp, source_files) or (None, None)
    """
    from datetime import datetime, timedelta

    # Parse all timestamps to datetime
    timestamp_datetimes = {}
    for ts_str in timestamp_groups.keys():
        try:
            dt = datetime.strptime(ts_str[:12], "%Y%m%d%H%M")
            timestamp_datetimes[ts_str] = dt
        except ValueError:
            continue

    # Sort by datetime (most recent first)
    sorted_timestamps = sorted(
        timestamp_datetimes.keys(), key=lambda x: timestamp_datetimes[x], reverse=True
    )

    tolerance = timedelta(minutes=tolerance_minutes)

    # For each timestamp, check if all sources have data within tolerance
    for candidate_ts in sorted_timestamps:
        candidate_dt = timestamp_datetimes[candidate_ts]
        sources_in_window = {}

        # Find sources with data in this time window
        for ts_str, ts_dt in timestamp_datetimes.items():
            if abs(ts_dt - candidate_dt) <= tolerance:
                for source_name, file_info in timestamp_groups[ts_str].items():
                    if source_name not in sources_in_window:
                        sources_in_window[source_name] = (ts_str, file_info, ts_dt)
                    else:
                        # Keep the closer timestamp
                        existing_ts, existing_info, existing_dt = sources_in_window[
                            source_name
                        ]
                        if abs(ts_dt - candidate_dt) < abs(existing_dt - candidate_dt):
                            sources_in_window[source_name] = (ts_str, file_info, ts_dt)

        # Check if all sources present
        if len(sources_in_window) == len(sources):
            print(f"✅ Found common time window around {candidate_ts}")
            for src, (ts, _, dt) in sources_in_window.items():
                offset = (dt - candidate_dt).total_seconds() / 60
                print(f"   {src.upper()}: {ts} (offset: {offset:+.1f} min)")

            return candidate_ts, {
                src: info for src, (_, info, _) in sources_in_window.items()
            }

    return None, None


def _process_latest(args, sources, exporter, output_dir):
    """Process latest available data from all sources - MEMORY OPTIMIZED TWO-PASS VERSION

    TWO-PASS ARCHITECTURE for ~75% memory reduction:
    - Pass 1: Extract extents only (no data loading) → Calculate combined extent
    - Pass 2: Process each source sequentially: Load → Export individual → Merge → Delete

    Ensures all sources have data for the SAME timestamp before creating composite.
    Also exports individual source images with their native extents.
    """
    from datetime import datetime

    import pytz

    from .processing.compositor import RadarCompositor

    print("\n🔍 Finding common timestamp across all sources...")

    # Step 1: Download recent timestamps from each source (get more to find overlap)
    all_source_files = {}
    for source_name, (source, product) in sources.items():
        print(f"\n📥 Checking {source_name.upper()} recent timestamps...")
        files = source.download_latest(
            count=4, products=[product]
        )  # Get last 4 to find overlap (reduced from 10 to save memory)

        if files:
            all_source_files[source_name] = files
            timestamps = [f["timestamp"] for f in files[:3]]  # Show first 3
            print(
                f"✅ Found {len(files)} timestamps (recent: {', '.join(timestamps)}...)"
            )
        else:
            print(f"❌ No data from {source_name.upper()}")
            return 1

    # Step 2: Find most recent timestamp that ALL sources have
    print("\n🔍 Finding common timestamp...")

    # Group files by timestamp
    timestamp_groups = {}
    for source_name, files in all_source_files.items():
        for file_info in files:
            timestamp = file_info["timestamp"]
            if timestamp not in timestamp_groups:
                timestamp_groups[timestamp] = {}
            timestamp_groups[timestamp][source_name] = file_info

    # Find most recent timestamp with all sources (with time-window tolerance)
    tolerance = getattr(args, "timestamp_tolerance", 2)
    common_timestamp, source_files = _find_common_timestamp_with_tolerance(
        timestamp_groups, sources, tolerance
    )

    if not common_timestamp:
        # If ARSO is included and no match found, try again without ARSO
        # ARSO only provides latest data, so timing mismatches are common
        require_arso = getattr(args, "require_arso", False)
        if "arso" in sources and len(sources) > 2 and not require_arso:
            print("⚠️  No common timestamp with ARSO, retrying without ARSO...")
            print("   (ARSO only provides single latest timestamp)")

            # Remove ARSO from sources and timestamp groups
            del sources["arso"]
            if "arso" in all_source_files:
                del all_source_files["arso"]

            # Rebuild timestamp groups without ARSO
            timestamp_groups = {}
            for source_name, files in all_source_files.items():
                for file_info in files:
                    timestamp = file_info["timestamp"]
                    if timestamp not in timestamp_groups:
                        timestamp_groups[timestamp] = {}
                    timestamp_groups[timestamp][source_name] = file_info

            # Try matching again
            common_timestamp, source_files = _find_common_timestamp_with_tolerance(
                timestamp_groups, sources, tolerance
            )

            if not common_timestamp:
                print("❌ No common timestamp found even without ARSO")
                print(f"   Timestamp tolerance: {tolerance} minutes")
                print(
                    "\n💡 Try again in a few minutes or increase --timestamp-tolerance"
                )
                return 1
        else:
            print("❌ No common timestamp found across all sources")
            print(f"   Timestamp tolerance: {tolerance} minutes")
            print("\nAvailable timestamps by source:")
            for source_name in sources.keys():
                if source_name in all_source_files and all_source_files[source_name]:
                    timestamps = [
                        f["timestamp"] for f in all_source_files[source_name][:3]
                    ]
                    print(f"   {source_name.upper()}: {', '.join(timestamps)}")
            print("\n💡 Try again in a few minutes or increase --timestamp-tolerance")
            return 1

    # Parse timestamp for filename generation
    dt = datetime.strptime(common_timestamp[:14], "%Y%m%d%H%M%S")
    dt_utc = pytz.UTC.localize(dt)
    unix_timestamp = int(dt_utc.timestamp())

    # Process data from matched sources
    if not source_files:
        print("❌ No source files matched (internal error)")
        return 1

    # ========== PASS 1: EXTRACT EXTENTS ONLY (NO DATA LOADING) ==========
    print("\n📐 Pass 1: Extracting extents only (memory-efficient)...")
    all_extents = []
    source_metadata = {}

    for source_name, file_info in source_files.items():
        source, _product = sources[source_name]
        try:
            # Extract extent WITHOUT loading full data array
            extent_info = source.extract_extent_only(file_info["path"])
            all_extents.append(extent_info["extent"]["wgs84"])
            source_metadata[source_name] = {
                "file_path": file_info["path"],
                "dimensions": extent_info["dimensions"],
            }
            print(f"   ✅ {source_name.upper()}: extent extracted")
        except Exception as e:
            print(f"❌ Failed to extract extent from {source_name}: {e}")
            return 1

    # Calculate combined extent from all sources
    combined_extent = {
        "west": min(ext["west"] for ext in all_extents),
        "east": max(ext["east"] for ext in all_extents),
        "south": min(ext["south"] for ext in all_extents),
        "north": max(ext["north"] for ext in all_extents),
    }
    print(
        f"   📊 Combined extent: {combined_extent['west']:.2f}°E to {combined_extent['east']:.2f}°E, "
        f"{combined_extent['south']:.2f}°N to {combined_extent['north']:.2f}°N"
    )

    # ========== PASS 2: SEQUENTIAL PROCESSING (ONE SOURCE AT A TIME) ==========
    print("\n📡 Pass 2: Processing sources sequentially (memory-optimized)...")

    # Create compositor with pre-computed combined extent
    compositor = RadarCompositor(combined_extent, resolution_m=args.resolution)

    for source_name in source_files:
        source, _product = sources[source_name]
        file_path = source_metadata[source_name]["file_path"]

        try:
            # Load ONE source at a time
            print(f"\n   🔄 Loading {source_name.upper()}...")
            radar_data = source.process_to_array(file_path)

            # Export individual source image if requested
            if not args.no_individual:
                _export_single_source(
                    source_name,
                    radar_data,
                    exporter,
                    unix_timestamp,
                    common_timestamp,
                    args,
                )
                # Free export-related memory before merging
                gc.collect()

            # Merge into compositor
            compositor.add_source(source_name, radar_data)

            # CRITICAL: Release memory immediately after merging
            del radar_data
            gc.collect()

            # Delete temp file
            try:
                Path(file_path).unlink(missing_ok=True)
            except Exception:
                pass

            print(f"   ✅ {source_name.upper()}: processed and merged")

        except Exception as e:
            print(f"❌ Failed to process {source_name}: {e}")
            return 1

    # Get final composite
    print("\n🎨 Finalizing composite...")
    composite = compositor.get_composite()

    # Generate output filename
    filename = f"{unix_timestamp}.png"
    output_path = output_dir / filename

    # Export composite to PNG
    print(f"\n💾 Exporting composite to {filename} (timestamp: {common_timestamp})...")
    radar_data_for_export = {
        "data": composite["data"],
        "timestamp": common_timestamp,
        "product": "composite",
        "source": "composite",
        "units": "dBZ",
    }
    exporter.export_png_fast(
        radar_data=radar_data_for_export,
        output_path=output_path,
        extent={"wgs84": composite["extent"]},
        colormap_type="shmu",
    )

    print(f"✅ Composite saved to {output_path}")
    print(f"   Data timestamp: {common_timestamp} (Unix: {unix_timestamp})")

    # Update extent index if requested
    if args.update_extent or not (output_dir / "extent_index.json").exists():
        # Wrap extent for compatibility
        composite_for_extent = {
            "extent": {"wgs84": composite["extent"]},
            "data": composite["data"],
        }
        _save_extent_index(
            output_dir, composite_for_extent, list(sources.keys()), args.resolution
        )

    # Cleanup to free memory
    compositor.clear_cache()
    del compositor, composite
    gc.collect()

    return 0


def _export_individual_sources(
    sources_data, exporter, unix_timestamp, timestamp_str, args
):
    """Export individual source images with their native extents.

    Each source is exported to its own directory:
    - DWD -> /tmp/germany/
    - SHMU -> /tmp/slovakia/
    - CHMI -> /tmp/czechia/
    - ARSO -> /tmp/slovenia/
    - OMSZ -> /tmp/hungary/

    Args:
        sources_data: List of (source_name, radar_data) tuples
        exporter: PNGExporter instance
        unix_timestamp: Unix timestamp for filename
        timestamp_str: Timestamp string for metadata
        args: CLI arguments
    """
    import json

    # Source name to output directory mapping
    source_dirs = {
        "dwd": Path("/tmp/germany/"),
        "shmu": Path("/tmp/slovakia/"),
        "chmi": Path("/tmp/czechia/"),
        "arso": Path("/tmp/slovenia"),
        "omsz": Path("/tmp/hungary/"),
        "imgw": Path("/tmp/poland/"),
    }

    for source_name, radar_data in sources_data:
        # Get output directory for this source
        source_output_dir = source_dirs.get(source_name)
        if source_output_dir is None:
            print(f"⚠️  Unknown source {source_name}, skipping individual export")
            continue

        # Create output directory
        source_output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        filename = f"{unix_timestamp}.png"
        output_path = source_output_dir / filename

        # Prepare radar data for export
        radar_data_for_export = {
            "data": radar_data["data"],
            "timestamp": timestamp_str,
            "product": radar_data.get("product", "unknown"),
            "source": source_name,
            "units": "dBZ",
            "metadata": radar_data.get("metadata", {}),
        }

        # Export to PNG with native extent
        print(f"   💾 {source_name.upper()} -> {output_path}")
        exporter.export_png_fast(
            radar_data=radar_data_for_export,
            output_path=output_path,
            extent=radar_data["extent"],
            colormap_type="shmu",
        )

        # Save extent_index.json if it doesn't exist
        extent_file = source_output_dir / "extent_index.json"
        if not extent_file.exists() or args.update_extent:
            extent_data = {
                "metadata": {
                    "title": f"{source_name.upper()} Radar Coverage",
                    "description": f"Native extent for {source_name.upper()} radar data",
                    "source": source_name,
                },
                "extent": radar_data["extent"],
            }
            with open(extent_file, "w") as f:
                json.dump(extent_data, f, indent=2)
            print(f"   📐 Saved extent to {extent_file}")


def _export_single_source(
    source_name, radar_data, exporter, unix_timestamp, timestamp_str, args
):
    """Export a single source image - called during sequential processing.

    This function is used in the two-pass architecture to export individual
    source images while processing sources one at a time.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')
        radar_data: Dictionary from source.process_to_array()
        exporter: PNGExporter instance
        unix_timestamp: Unix timestamp for filename
        timestamp_str: Timestamp string for metadata
        args: CLI arguments
    """
    import json

    # Source name to output directory mapping
    source_dirs = {
        "dwd": Path("/tmp/germany/"),
        "shmu": Path("/tmp/slovakia/"),
        "chmi": Path("/tmp/czechia/"),
        "arso": Path("/tmp/slovenia/"),
        "omsz": Path("/tmp/hungary/"),
        "imgw": Path("/tmp/poland/"),
    }

    output_dir = source_dirs.get(source_name)
    if not output_dir:
        return

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename
    output_path = output_dir / f"{unix_timestamp}.png"

    # Prepare radar data for export
    radar_data_for_export = {
        "data": radar_data["data"],
        "timestamp": timestamp_str,
        "product": radar_data.get("metadata", {}).get("product", "unknown"),
        "source": source_name,
        "units": "dBZ",
        "metadata": radar_data.get("metadata", {}),
    }

    # Export to PNG with native extent
    print(f"      💾 {source_name.upper()} -> {output_path}")
    exporter.export_png_fast(
        radar_data=radar_data_for_export,
        output_path=output_path,
        extent=radar_data["extent"],
        colormap_type="shmu",
    )

    # Save extent_index.json if it doesn't exist
    extent_file = output_dir / "extent_index.json"
    if not extent_file.exists() or args.update_extent:
        extent_data = {
            "metadata": {
                "title": f"{source_name.upper()} Radar Coverage",
                "description": f"Native extent for {source_name.upper()} radar data",
                "source": source_name,
            },
            "extent": radar_data["extent"],
        }
        with open(extent_file, "w") as f:
            json.dump(extent_data, f, indent=2)


def _process_backload(args, sources, exporter, output_dir):
    """Process historical data from all sources - MEMORY OPTIMIZED TWO-PASS VERSION

    TWO-PASS ARCHITECTURE for ~75% memory reduction:
    - Pass 1: Extract extents only (no data loading) → Calculate combined extent
    - Pass 2: Process each source sequentially: Load → Export individual → Merge → Delete
    """
    import gc
    from datetime import datetime as dt

    import pytz

    from .cli import parse_time_range
    from .processing.compositor import RadarCompositor

    start, end = parse_time_range(args.from_time, args.to_time, args.hours)
    print(
        f"\n📅 Backload mode: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}"
    )

    # Calculate number of 5-minute intervals
    time_diff = end - start
    intervals = int(time_diff.total_seconds() / 300) + 1  # 300 seconds = 5 minutes
    print(f"   Expected intervals: {intervals}")

    # Remove ARSO from sources for backload mode (no historical data available)
    if "arso" in sources:
        print("\n⚠️  Excluding ARSO from backload mode (no historical data available)")
        print("   ARSO only provides latest data")
        del sources["arso"]

    # Download data from all sources for the time range
    all_source_files = {}
    for source_name, (source, product) in sources.items():
        print(f"\n🌐 Downloading {source_name.upper()} data...")
        files = source.download_latest(
            count=intervals, products=[product], start_time=start, end_time=end
        )
        if files:
            all_source_files[source_name] = files
            print(f"✅ Downloaded {len(files)} files from {source_name.upper()}")
        else:
            print(f"⚠️  No data from {source_name.upper()}")

    if not all_source_files:
        print("❌ No data downloaded from any source")
        return 1

    # Group files by timestamp
    timestamp_groups = {}
    for source_name, files in all_source_files.items():
        for file_info in files:
            timestamp = file_info["timestamp"]
            if timestamp not in timestamp_groups:
                timestamp_groups[timestamp] = {}
            timestamp_groups[timestamp][source_name] = file_info

    print(f"\n📊 Found {len(timestamp_groups)} unique timestamps")

    # Process each timestamp with two-pass architecture
    processed_count = 0
    last_composite = None

    for timestamp in sorted(timestamp_groups.keys()):
        source_files = timestamp_groups[timestamp]

        # Skip if not all sources have data for this timestamp
        if len(source_files) < len(sources):
            missing = set(sources.keys()) - set(source_files.keys())
            print(f"\n⏭️  Skipping {timestamp} (missing: {', '.join(missing).upper()})")
            continue

        print(f"\n📡 Processing {timestamp}...")

        # Generate Unix timestamp for filenames
        dt_obj = dt.strptime(timestamp, "%Y%m%d%H%M%S")
        dt_obj = pytz.UTC.localize(dt_obj)
        unix_timestamp = int(dt_obj.timestamp())

        # ========== PASS 1: EXTRACT EXTENTS ONLY ==========
        print("   📐 Pass 1: Extracting extents...")
        all_extents = []
        source_metadata = {}

        for source_name, file_info in source_files.items():
            source, _product = sources[source_name]
            try:
                extent_info = source.extract_extent_only(file_info["path"])
                all_extents.append(extent_info["extent"]["wgs84"])
                source_metadata[source_name] = {"file_path": file_info["path"]}
            except Exception as e:
                print(f"   ⚠️  Failed to extract extent from {source_name}: {e}")
                continue

        if len(all_extents) < 2:
            print("   ⚠️  Not enough valid extents for composite, skipping")
            continue

        # Calculate combined extent
        combined_extent = {
            "west": min(ext["west"] for ext in all_extents),
            "east": max(ext["east"] for ext in all_extents),
            "south": min(ext["south"] for ext in all_extents),
            "north": max(ext["north"] for ext in all_extents),
        }

        # ========== PASS 2: SEQUENTIAL PROCESSING ==========
        print("   📡 Pass 2: Processing sources sequentially...")
        compositor = RadarCompositor(combined_extent, resolution_m=args.resolution)
        sources_processed = 0

        for source_name in source_metadata:
            source, _product = sources[source_name]
            file_path = source_metadata[source_name]["file_path"]

            try:
                # Load ONE source at a time
                radar_data = source.process_to_array(file_path)

                # Export individual source image if requested
                if not args.no_individual:
                    _export_single_source(
                        source_name,
                        radar_data,
                        exporter,
                        unix_timestamp,
                        timestamp,
                        args,
                    )

                # Merge into compositor
                compositor.add_source(source_name, radar_data)
                sources_processed += 1

                # CRITICAL: Release memory immediately
                del radar_data
                gc.collect()

                # Delete temp file
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception:
                    pass

            except Exception as e:
                print(f"   ⚠️  Failed to process {source_name}: {e}")

        if sources_processed < 2:
            print("   ⚠️  Not enough valid sources for composite, skipping")
            compositor.clear_cache()
            del compositor
            gc.collect()
            continue

        # Get final composite and export
        try:
            composite = compositor.get_composite()

            filename = f"{unix_timestamp}.png"
            output_path = output_dir / filename

            print(f"   💾 Exporting composite to {filename}...")
            radar_data_for_export = {
                "data": composite["data"],
                "timestamp": timestamp,
                "product": "composite",
                "source": "composite",
                "units": "dBZ",
            }
            exporter.export_png_fast(
                radar_data=radar_data_for_export,
                output_path=output_path,
                extent={"wgs84": composite["extent"]},
                colormap_type="shmu",
            )

            processed_count += 1
            last_composite = {
                "extent": {"wgs84": composite["extent"]},
                "data": composite["data"],
            }

            # Cleanup
            compositor.clear_cache()
            del compositor, composite
            gc.collect()

        except Exception as e:
            print(f"   ❌ Failed to create composite: {e}")
            import traceback

            traceback.print_exc()

    print(f"\n✅ Processed {processed_count} composites")

    # Update extent index if requested
    if args.update_extent or processed_count > 0:
        if last_composite is not None:
            _save_extent_index(
                output_dir, last_composite, list(sources.keys()), args.resolution
            )

    return 0


def _save_extent_index(output_dir, composite, source_names, resolution):
    """Save extent index JSON"""
    import json
    from datetime import datetime

    print("\n📐 Generating extent information...")

    extent_data = {
        "metadata": {
            "title": "Composite Radar Coverage",
            "description": f"Combined extent from sources: {', '.join(source_names)}",
            "generated": datetime.now().isoformat() + "Z",
            "sources": source_names,
            "resolution_m": resolution,
        },
        "extent": composite["extent"],
    }

    extent_path = output_dir / "extent_index.json"
    with open(extent_path, "w") as f:
        json.dump(extent_data, f, indent=2)

    print(f"✅ Extent info saved to {extent_path}")
