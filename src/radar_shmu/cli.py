#!/usr/bin/env python3
"""
Command-line interface for radar-shmu

Provides CLI commands for radar data processing operations.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional


def create_parser() -> argparse.ArgumentParser:
    """Create command-line argument parser"""
    parser = argparse.ArgumentParser(
        description="Multi-source radar data processor",
        prog="radar-processor"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Download command
    download_parser = subparsers.add_parser(
        'download', 
        help='Download radar data from sources'
    )
    download_parser.add_argument(
        '--sources', 
        nargs='+', 
        choices=['shmu', 'dwd', 'all'],
        default=['shmu'], 
        help='Radar sources to download from'
    )
    download_parser.add_argument(
        '--count', 
        type=int, 
        default=5,
        help='Number of timestamps to download'
    )
    download_parser.add_argument(
        '--output', 
        type=Path, 
        default=Path('outputs'),
        help='Output directory'
    )
    download_parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Force fresh download, bypass cache for latest data'
    )
    
    # Merge command
    merge_parser = subparsers.add_parser(
        'merge',
        help='Merge radar data from multiple sources'
    )
    merge_parser.add_argument(
        '--sources',
        nargs='+', 
        choices=['shmu', 'dwd'],
        default=['shmu', 'dwd'],
        help='Radar sources to merge'
    )
    merge_parser.add_argument(
        '--output',
        type=Path, 
        default=Path('outputs/merged'),
        help='Output directory for merged data'
    )
    merge_parser.add_argument(
        '--strategy',
        choices=['average', 'max', 'priority'],
        default='average',
        help='Merging strategy'
    )
    merge_parser.add_argument(
        '--time-range',
        type=int,
        default=1,
        help='Hours back from now to process'
    )
    
    # Animate command  
    animate_parser = subparsers.add_parser(
        'animate',
        help='Create animated GIFs from radar data'
    )
    animate_parser.add_argument(
        '--input-dir',
        type=Path,
        required=True,
        help='Directory containing radar images'
    )
    animate_parser.add_argument(
        '--output',
        type=Path,
        default=Path('outputs/animations'),
        help='Output directory for animations'
    )
    animate_parser.add_argument(
        '--fps',
        type=int,
        default=12,
        help='Frames per second'
    )
    
    return parser


def main():
    """Main CLI entry point"""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        if args.command == 'download':
            return download_command(args)
        elif args.command == 'merge':
            return merge_command(args)
        elif args.command == 'animate':
            return animate_command(args)
        else:
            print(f"Unknown command: {args.command}")
            return 1
            
    except KeyboardInterrupt:
        print("\\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


def download_command(args) -> int:
    """Handle download command"""
    print(f"🔄 Download command - Sources: {args.sources}, Count: {args.count}")
    print(f"📁 Output: {args.output}")
    if args.no_cache:
        print("🧹 No-cache mode: Force fresh download")
    
    # Import here to avoid circular imports and speed up CLI startup
    try:
        from .sources.shmu import SHMURadarSource
        from .sources.dwd import DWDRadarSource
        
        # Clear cache if no-cache flag is set
        if args.no_cache:
            import shutil
            from pathlib import Path
            cache_dirs = ['processed', 'storage']
            for cache_dir in cache_dirs:
                cache_path = Path(cache_dir)
                if cache_path.exists():
                    print(f"🧹 Clearing cache: {cache_dir}")
                    shutil.rmtree(cache_path)
        
        sources = {}
        if 'shmu' in args.sources or 'all' in args.sources:
            sources['shmu'] = SHMURadarSource()
        if 'dwd' in args.sources or 'all' in args.sources:
            sources['dwd'] = DWDRadarSource()
        
        # Download from each source
        total_downloaded = 0
        for name, source in sources.items():
            print(f"\\n📡 Downloading from {name.upper()}...")
            files = source.download_latest(count=args.count)
            total_downloaded += len(files)
            print(f"✅ {name.upper()}: {len(files)} files downloaded")
        
        print(f"\\n🎉 Total downloaded: {total_downloaded} files")
        return 0
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please ensure the package is properly installed.")
        return 1


def merge_command(args) -> int:
    """Handle merge command"""
    try:
        from .processing.merged_products import create_merged_products_cli
        
        print(f"🔄 Merge command - Strategy: {args.strategy}")
        print(f"📁 Output: {args.output}")
        
        # Create merged products
        exit_code = create_merged_products_cli(
            sources=args.sources,
            time_range_hours=args.time_range,
            strategies=[args.strategy],
            output_dir=str(args.output)
        )
        
        return exit_code
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Merge command failed: {e}")
        return 1


def animate_command(args) -> int:
    """Handle animate command"""
    print(f"🔄 Animate command - FPS: {args.fps}")
    print(f"📁 Input: {args.input_dir}, Output: {args.output}")
    
    # TODO: Implement animation functionality  
    print("⚠️  Animate command not yet implemented in CLI")
    return 0


if __name__ == "__main__":
    sys.exit(main())