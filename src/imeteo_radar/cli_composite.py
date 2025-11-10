#!/usr/bin/env python3
"""
Composite command implementation for CLI

Separated into its own module to keep cli.py manageable.
"""

from pathlib import Path
from datetime import datetime
from typing import Any
import gc


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
    """Process latest available data from all sources"""
    from .processing.compositor import create_composite
    import json
    from datetime import datetime
    import pytz

    print("\n🔍 Downloading latest data from all sources...")

    sources_data = []
    for source_name, (source, product) in sources.items():
        print(f"\n📥 Downloading from {source_name.upper()}...")
        files = source.download_latest(count=1, products=[product])

        if files:
            file_info = files[0]
            print(f"✅ Downloaded: {file_info['timestamp']}")

            # Process to array
            try:
                radar_data = source.process_to_array(file_info['path'])
                sources_data.append((source_name, radar_data))
            except Exception as e:
                print(f"⚠️  Failed to process {source_name}: {e}")
        else:
            print(f"⚠️  No data from {source_name.upper()}")

    if len(sources_data) < 2:
        print("❌ Need at least 2 sources to create composite")
        return 1

    # Create composite
    print("\n🎨 Creating composite...")
    composite = create_composite(
        sources_data,
        resolution_m=args.resolution
    )

    # Generate output filename
    timestamp_str = datetime.now(pytz.UTC).strftime("%Y-%m-%d_%H%M")
    filename = f"{timestamp_str}.png"
    output_path = output_dir / filename

    # Export to PNG
    print(f"\n💾 Exporting to {filename}...")
    exporter.export_png_fast(
        data=composite['data'],
        output_path=str(output_path),
        colormap_type='shmu'
    )

    print(f"✅ Composite saved to {output_path}")

    # Update extent index if requested
    if args.update_extent or not (output_dir / 'extent_index.json').exists():
        _save_extent_index(output_dir, composite, list(sources.keys()), args.resolution)

    return 0


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

        # Create composite
        try:
            composite = create_composite(
                sources_data,
                resolution_m=args.resolution
            )

            # Generate output filename
            dt_obj = dt.strptime(timestamp, "%Y%m%d%H%M%S")
            dt_obj = pytz.UTC.localize(dt_obj)
            filename = dt_obj.strftime("%Y-%m-%d_%H%M") + ".png"
            output_path = output_dir / filename

            # Export to PNG
            print(f"💾 Exporting to {filename}...")
            exporter.export_png_fast(
                data=composite['data'],
                output_path=str(output_path),
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
