#!/usr/bin/env python3
"""
Simple memory profiler for imeteo-radar

Usage:
    python scripts/profile_memory.py --source dwd --backload --hours 1

This will run your fetch command and show:
- Peak memory usage
- Memory usage by function
- Top memory-allocating lines
"""

import sys
import tracemalloc
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def format_size(bytes):
    """Format bytes to human-readable size"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes < 1024.0:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.1f} TB"


def profile_fetch():
    """Profile the fetch command with memory tracking"""
    # Parse arguments (same as CLI)
    parser = argparse.ArgumentParser(description="Memory profile radar fetch")
    parser.add_argument('--source', choices=['dwd', 'shmu'], default='dwd')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--backload', action='store_true')
    parser.add_argument('--hours', type=int)
    parser.add_argument('--from', dest='from_time', type=str)
    parser.add_argument('--to', dest='to_time', type=str)
    parser.add_argument('--update-extent', action='store_true')
    parser.add_argument('--disable-upload', action='store_true', default=True)  # Always disable for profiling

    args = parser.parse_args()

    print("=" * 70)
    print("MEMORY PROFILING - iMeteo Radar")
    print("=" * 70)
    print(f"Source: {args.source}")
    if args.backload:
        print(f"Backload: {args.hours} hours" if args.hours else "Backload: custom range")
    else:
        print("Mode: Latest only")
    print("=" * 70)
    print()

    # Start memory tracking
    tracemalloc.start()

    # Take initial snapshot
    snapshot_start = tracemalloc.take_snapshot()

    try:
        # Import and run fetch command
        from imeteo_radar.cli import fetch_command

        print("🔍 Starting profiled fetch...")
        print()

        # Run the fetch
        result = fetch_command(args)

        # Take final snapshot
        snapshot_end = tracemalloc.take_snapshot()

        # Get current memory and peak
        current, peak = tracemalloc.get_traced_memory()

        print()
        print("=" * 70)
        print("MEMORY PROFILING RESULTS")
        print("=" * 70)
        print(f"Peak memory usage: {format_size(peak)}")
        print(f"Current memory usage: {format_size(current)}")
        print()

        # Show top memory allocations
        print("Top 10 memory allocations:")
        print("-" * 70)

        top_stats = snapshot_end.compare_to(snapshot_start, 'lineno')

        for index, stat in enumerate(top_stats[:10], 1):
            print(f"{index}. {stat}")

        print()
        print("=" * 70)
        print("Top files by memory allocation:")
        print("-" * 70)

        # Group by file
        top_stats_by_file = snapshot_end.compare_to(snapshot_start, 'filename')

        for index, stat in enumerate(top_stats_by_file[:10], 1):
            frame = stat.traceback[0]
            print(f"{index}. {frame.filename}")
            print(f"   Size: {format_size(stat.size)}")
            print(f"   Count: {stat.count} allocations")
            print()

        print("=" * 70)
        print("ANALYSIS")
        print("=" * 70)

        # Simple leak detection
        if current > peak * 0.8:
            print("⚠️  WARNING: Memory not released after processing!")
            print(f"   Current usage ({format_size(current)}) is close to peak ({format_size(peak)})")
            print("   This may indicate a memory leak.")
        else:
            print("✓ Memory appears to be properly released after processing")
            released = peak - current
            print(f"  {format_size(released)} released ({(released/peak*100):.1f}% of peak)")

        print()

        return result

    except Exception as e:
        print(f"\n❌ Error during profiling: {e}")
        import traceback as tb
        tb.print_exc()
        return 1

    finally:
        tracemalloc.stop()


if __name__ == '__main__':
    sys.exit(profile_fetch())
