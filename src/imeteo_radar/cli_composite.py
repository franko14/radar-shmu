#!/usr/bin/env python3
"""
Composite command implementation for CLI

Separated into its own module to keep cli.py manageable.
"""

from pathlib import Path
from typing import Any


def composite_command_impl(args: Any) -> int:
    """Handle composite generation command"""
    try:
        from .sources.dwd import DWDRadarSource
        from .sources.shmu import SHMURadarSource
        from .sources.chmi import CHMIRadarSource
        from .processing.compositor import create_composite
        from .processing.exporter import PNGExporter
        from .cli import parse_time_range
        import json

        # Parse sources list
        source_names = [s.strip() for s in args.sources.split(',')]
        print(f"🎯 Creating composite from sources: {', '.join(source_names).upper()}")

        # Create output directory
        output_dir = args.output
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Output directory: {output_dir}")

        # Initialize sources
        sources = {}
        for source_name in source_names:
            if source_name == 'dwd':
                sources['dwd'] = (DWDRadarSource(), 'dmax')
            elif source_name == 'shmu':
                sources['shmu'] = (SHMURadarSource(), 'zmax')
            elif source_name == 'chmi':
                sources['chmi'] = (CHMIRadarSource(), 'maxz')
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


def _process_latest(args, sources, exporter, output_dir):
    """Process latest available data from all sources

    Ensures all sources have data for the SAME timestamp before creating composite.
    Also exports individual source images with their native extents.
    """
    from .processing.compositor import create_composite
    import json
    from datetime import datetime
    import pytz

    print("\n🔍 Finding common timestamp across all sources...")

    # Step 1: Download recent timestamps from each source (get more to find overlap)
    all_source_files = {}
    for source_name, (source, product) in sources.items():
        print(f"\n📥 Checking {source_name.upper()} recent timestamps...")
        files = source.download_latest(count=10, products=[product])  # Get last 10 to find overlap

        if files:
            all_source_files[source_name] = files
            timestamps = [f['timestamp'] for f in files[:3]]  # Show first 3
            print(f"✅ Found {len(files)} timestamps (recent: {', '.join(timestamps)}...)")
        else:
            print(f"❌ No data from {source_name.upper()}")
            return 1

    # Step 2: Find most recent timestamp that ALL sources have
    print("\n🔍 Finding common timestamp...")

    # Group files by timestamp
    timestamp_groups = {}
    for source_name, files in all_source_files.items():
        for file_info in files:
            timestamp = file_info['timestamp']
            if timestamp not in timestamp_groups:
                timestamp_groups[timestamp] = {}
            timestamp_groups[timestamp][source_name] = file_info

    # Find most recent timestamp with all sources
    common_timestamp = None
    for timestamp in sorted(timestamp_groups.keys(), reverse=True):  # Most recent first
        source_files = timestamp_groups[timestamp]
        if len(source_files) == len(sources):  # All sources have this timestamp
            common_timestamp = timestamp
            print(f"✅ Found common timestamp: {timestamp}")
            break

    if not common_timestamp:
        print(f"❌ No common timestamp found across all sources")
        print(f"   Each source has different timestamps - try again in a few minutes")
        return 1

    # Step 3: Process data for the common timestamp
    print(f"\n📡 Processing data for timestamp: {common_timestamp}...")

    # Parse timestamp for filename generation
    dt = datetime.strptime(common_timestamp[:14], "%Y%m%d%H%M%S")
    dt_utc = pytz.UTC.localize(dt)
    unix_timestamp = int(dt_utc.timestamp())

    sources_data = []
    for source_name, file_info in timestamp_groups[common_timestamp].items():
        source, product = sources[source_name]
        try:
            radar_data = source.process_to_array(file_info['path'])
            sources_data.append((source_name, radar_data))
            print(f"✅ Processed {source_name.upper()}")
        except Exception as e:
            print(f"❌ Failed to process {source_name}: {e}")
            return 1

    # Export individual source images (with native extents)
    if not args.no_individual:
        print("\n📸 Exporting individual source images...")
        _export_individual_sources(
            sources_data, exporter, unix_timestamp, common_timestamp, args
        )

    # Create composite
    print("\n🎨 Creating composite...")
    composite = create_composite(
        sources_data,
        resolution_m=args.resolution
    )

    # Generate output filename
    filename = f"{unix_timestamp}.png"
    output_path = output_dir / filename

    # Export composite to PNG
    print(f"\n💾 Exporting composite to {filename} (timestamp: {common_timestamp})...")
    radar_data_for_export = {
        'data': composite['data'],
        'timestamp': common_timestamp,
        'product': 'composite',
        'source': 'composite',
        'units': 'dBZ'
    }
    exporter.export_png_fast(
        radar_data=radar_data_for_export,
        output_path=output_path,
        extent=composite['extent'],
        colormap_type='shmu'
    )

    print(f"✅ Composite saved to {output_path}")
    print(f"   Data timestamp: {common_timestamp} (Unix: {unix_timestamp})")

    # Update extent index if requested
    if args.update_extent or not (output_dir / 'extent_index.json').exists():
        _save_extent_index(output_dir, composite, list(sources.keys()), args.resolution)

    return 0


def _export_individual_sources(sources_data, exporter, unix_timestamp, timestamp_str, args):
    """Export individual source images with their native extents.

    Each source is exported to its own directory:
    - DWD -> /tmp/germany/
    - SHMU -> /tmp/slovakia/
    - CHMI -> /tmp/czechia/

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
        'dwd': Path('/tmp/germany/'),
        'shmu': Path('/tmp/slovakia/'),
        'chmi': Path('/tmp/czechia/'),
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
            'data': radar_data['data'],
            'timestamp': timestamp_str,
            'product': radar_data.get('product', 'unknown'),
            'source': source_name,
            'units': 'dBZ',
            'metadata': radar_data.get('metadata', {})
        }

        # Export to PNG with native extent
        print(f"   💾 {source_name.upper()} -> {output_path}")
        exporter.export_png_fast(
            radar_data=radar_data_for_export,
            output_path=output_path,
            extent=radar_data['extent'],
            colormap_type='shmu'
        )

        # Save extent_index.json if it doesn't exist
        extent_file = source_output_dir / 'extent_index.json'
        if not extent_file.exists() or args.update_extent:
            extent_data = {
                "metadata": {
                    "title": f"{source_name.upper()} Radar Coverage",
                    "description": f"Native extent for {source_name.upper()} radar data",
                    "source": source_name
                },
                "extent": radar_data['extent']
            }
            with open(extent_file, 'w') as f:
                json.dump(extent_data, f, indent=2)
            print(f"   📐 Saved extent to {extent_file}")


def _process_backload(args, sources, exporter, output_dir):
    """Process historical data from all sources"""
    from .processing.compositor import create_composite
    from .cli import parse_time_range
    from datetime import datetime as dt
    import pytz
    import gc

    start, end = parse_time_range(args.from_time, args.to_time, args.hours)
    print(f"\n📅 Backload mode: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}")

    # Calculate number of 5-minute intervals
    time_diff = end - start
    intervals = int(time_diff.total_seconds() / 300) + 1  # 300 seconds = 5 minutes
    print(f"   Expected intervals: {intervals}")

    # Download data from all sources for the time range
    all_source_files = {}
    for source_name, (source, product) in sources.items():
        print(f"\n🌐 Downloading {source_name.upper()} data...")
        files = source.download_latest(
            count=intervals,
            products=[product],
            start_time=start,
            end_time=end
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
            timestamp = file_info['timestamp']
            if timestamp not in timestamp_groups:
                timestamp_groups[timestamp] = {}
            timestamp_groups[timestamp][source_name] = file_info

    print(f"\n📊 Found {len(timestamp_groups)} unique timestamps")

    # Process each timestamp
    processed_count = 0
    for timestamp in sorted(timestamp_groups.keys()):
        source_files = timestamp_groups[timestamp]

        # Skip if not all sources have data for this timestamp
        if len(source_files) < len(sources):
            missing = set(sources.keys()) - set(source_files.keys())
            print(f"\n⏭️  Skipping {timestamp} (missing: {', '.join(missing).upper()})")
            continue

        print(f"\n📡 Processing {timestamp}...")

        # Process data from each source
        sources_data = []
        for source_name, file_info in source_files.items():
            source, product = sources[source_name]
            try:
                radar_data = source.process_to_array(file_info['path'])
                sources_data.append((source_name, radar_data))
            except Exception as e:
                print(f"⚠️  Failed to process {source_name}: {e}")

        if len(sources_data) < 2:
            print(f"⚠️  Not enough valid sources for composite, skipping")
            continue

        # Generate Unix timestamp for filenames
        dt_obj = dt.strptime(timestamp, "%Y%m%d%H%M%S")
        dt_obj = pytz.UTC.localize(dt_obj)
        unix_timestamp = int(dt_obj.timestamp())

        # Export individual source images (with native extents)
        if not args.no_individual:
            _export_individual_sources(
                sources_data, exporter, unix_timestamp, timestamp, args
            )

        # Create composite
        try:
            composite = create_composite(
                sources_data,
                resolution_m=args.resolution
            )

            filename = f"{unix_timestamp}.png"
            output_path = output_dir / filename

            # Export composite to PNG
            print(f"💾 Exporting composite to {filename} (timestamp: {timestamp})...")
            radar_data_for_export = {
                'data': composite['data'],
                'timestamp': timestamp,
                'product': 'composite',
                'source': 'composite',
                'units': 'dBZ'
            }
            exporter.export_png_fast(
                radar_data=radar_data_for_export,
                output_path=output_path,
                extent=composite['extent'],
                colormap_type='shmu'
            )

            processed_count += 1

            # Cleanup
            del composite, sources_data
            gc.collect()

        except Exception as e:
            print(f"❌ Failed to create composite: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n✅ Processed {processed_count} composites")

    # Update extent index if requested
    if args.update_extent or processed_count > 0:
        # Use last composite for extent info (they should all be the same)
        if 'composite' in locals():
            _save_extent_index(output_dir, composite, list(sources.keys()), args.resolution)

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
            "resolution_m": resolution
        },
        "extent": composite['extent']
    }

    extent_path = output_dir / 'extent_index.json'
    with open(extent_path, 'w') as f:
        json.dump(extent_data, f, indent=2)

    print(f"✅ Extent info saved to {extent_path}")
