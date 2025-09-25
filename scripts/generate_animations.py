#!/usr/bin/env python3
"""
Generate Radar Animations Script

This script generates GIF animations from radar PNG sequences for all available sources.
It auto-detects available data ranges and creates optimized animations.

Usage:
    python scripts/generate_animations.py [options]

Examples:
    # Generate animations for all sources
    python scripts/generate_animations.py

    # Generate only SHMU animations
    python scripts/generate_animations.py --sources shmu

    # Custom frame rate and output directory
    python scripts/generate_animations.py --fps 15 --output animations/

    # Generate specific product animations
    python scripts/generate_animations.py --sources shmu --products zmax
"""

import sys
import argparse
from pathlib import Path
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.radar_sources.animator import RadarAnimator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate GIF animations from radar PNG sequences",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Generate all animations
  %(prog)s --sources shmu dwd                # Only SHMU and DWD
  %(prog)s --fps 15 --optimize               # Custom frame rate with optimization
  %(prog)s --data-dir custom/path             # Custom data directory
        """
    )
    
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("outputs/production/latest_radar_data"),
        help="Input directory containing radar data (default: %(default)s)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/production/animations"),
        help="Output directory for animations (default: %(default)s)"
    )
    
    parser.add_argument(
        "--sources",
        nargs="*",
        choices=["shmu", "dwd", "merged"],
        help="Sources to process (default: all available)"
    )
    
    parser.add_argument(
        "--products",
        nargs="*",
        help="Specific products to animate (default: all products per source)"
    )
    
    parser.add_argument(
        "--fps",
        type=int,
        default=12,
        help="Frames per second for animations (default: %(default)s)"
    )
    
    parser.add_argument(
        "--no-loop",
        action="store_true",
        help="Disable looping in GIF animations"
    )
    
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="Disable GIF optimization (larger files, faster processing)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be generated without creating files"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    return parser.parse_args()


def validate_data_directory(data_dir: Path, sources: list = None) -> dict:
    """
    Validate data directory and return available sources with file counts.
    
    Args:
        data_dir: Path to data directory
        sources: List of sources to check (None for all)
        
    Returns:
        Dictionary with source information
    """
    if not data_dir.exists():
        logger.error(f"Data directory does not exist: {data_dir}")
        return {}
    
    available_sources = {}
    all_sources = sources or ["shmu", "dwd", "merged"]
    
    for source in all_sources:
        source_dir = data_dir / source
        if source_dir.exists():
            png_files = list(source_dir.glob("*.png"))
            if png_files:
                available_sources[source] = {
                    'directory': source_dir,
                    'png_count': len(png_files),
                    'products': set()
                }
                
                # Detect products
                for png_file in png_files:
                    filename = png_file.name
                    if filename.startswith('zmax_'):
                        available_sources[source]['products'].add('zmax')
                    elif filename.startswith('cappi2km_'):
                        available_sources[source]['products'].add('cappi2km')
                    elif filename.startswith('dmax_'):
                        available_sources[source]['products'].add('dmax')
                    elif filename.startswith('merged_'):
                        available_sources[source]['products'].add('merged')
            else:
                logger.warning(f"No PNG files found in {source_dir}")
        else:
            logger.warning(f"Source directory not found: {source_dir}")
    
    return available_sources


def print_data_summary(available_sources: dict):
    """Print summary of available data."""
    print("\n📊 Data Summary")
    print("=" * 50)
    
    if not available_sources:
        print("❌ No data sources found")
        return
    
    total_files = 0
    for source, info in available_sources.items():
        products_str = ", ".join(sorted(info['products']))
        print(f"{source.upper():>8}: {info['png_count']:>3} files ({products_str})")
        total_files += info['png_count']
    
    print(f"{'TOTAL':>8}: {total_files:>3} files")


def print_animation_plan(available_sources: dict, animator: RadarAnimator, products_filter: list = None):
    """Print what animations will be generated."""
    print("\n🎬 Animation Plan")
    print("=" * 50)
    
    total_animations = 0
    
    for source, info in available_sources.items():
        source_products = info['products']
        
        # Apply product filter
        if products_filter:
            source_products = source_products.intersection(set(products_filter))
        
        if not source_products:
            print(f"{source.upper():>8}: No matching products")
            continue
        
        # For SHMU, we create separate animations per product
        if source == 'shmu' and len(source_products) > 1:
            animations_count = len(source_products)
        else:
            animations_count = 1
        
        products_str = ", ".join(sorted(source_products))
        print(f"{source.upper():>8}: {animations_count} animation(s) ({products_str})")
        total_animations += animations_count
    
    print(f"{'TOTAL':>8}: {total_animations} animation(s)")
    print(f"\nSettings: {animator.fps} fps, {'looped' if animator.loop else 'no loop'}")


def main():
    """Main function."""
    args = parse_arguments()
    
    # Set log level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print("🎯 Radar Animation Generator")
    print("=" * 50)
    
    # Validate data directory
    available_sources = validate_data_directory(args.data_dir, args.sources)
    
    if not available_sources:
        logger.error("No valid data sources found. Exiting.")
        sys.exit(1)
    
    # Print data summary
    print_data_summary(available_sources)
    
    # Create animator
    animator = RadarAnimator(
        fps=args.fps,
        loop=not args.no_loop
    )
    
    # Print animation plan
    print_animation_plan(available_sources, animator, args.products)
    
    if args.dry_run:
        print("\n🔍 Dry run - no files would be created")
        return
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n📁 Output directory: {args.output_dir}")
    
    # Generate animations
    print("\n🚀 Starting animation generation...")
    
    results = {}
    
    for source in available_sources.keys():
        logger.info(f"Processing {source.upper()}")
        
        source_dir = available_sources[source]['directory']
        
        # Filter products if specified
        if args.products:
            # Only animate specified products that are available for this source
            available_products = available_sources[source]['products']
            products_to_animate = set(args.products).intersection(available_products)
            
            if not products_to_animate:
                logger.warning(f"No matching products for {source}: {args.products}")
                continue
            
            # For SHMU, animate each product separately
            if source == 'shmu':
                source_results = {}
                for product in products_to_animate:
                    product_results = animator.create_source_animation(
                        source_dir, source, args.output_dir, product
                    )
                    source_results.update(product_results)
            else:
                # For other sources, animate all matching products together
                source_results = animator.create_source_animation(
                    source_dir, source, args.output_dir
                )
        else:
            # Animate all available products
            source_results = animator.create_source_animation(
                source_dir, source, args.output_dir
            )
        
        results[source] = source_results
    
    # Print final summary
    print("\n" + "=" * 50)
    print("🎯 Final Summary")
    print("=" * 50)
    
    total_success = 0
    total_attempts = 0
    
    for source, source_results in results.items():
        print(f"\n{source.upper()}:")
        for filename, success in source_results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"  {filename}: {status}")
            total_attempts += 1
            if success:
                total_success += 1
        
        if not source_results:
            print("  No animations generated")
    
    print(f"\nOverall Result: {total_success}/{total_attempts} animations created successfully")
    
    if total_success > 0:
        print(f"📁 Animations saved in: {args.output_dir}")
        
        # List generated files
        gif_files = list(args.output_dir.glob("*.gif"))
        if gif_files:
            print(f"\nGenerated files:")
            total_size_mb = 0
            for gif_file in sorted(gif_files):
                size_mb = gif_file.stat().st_size / (1024 * 1024)
                print(f"  {gif_file.name} ({size_mb:.1f} MB)")
                total_size_mb += size_mb
            print(f"  Total size: {total_size_mb:.1f} MB")
    
    # Exit with appropriate code
    sys.exit(0 if total_success == total_attempts else 1)


if __name__ == "__main__":
    main()