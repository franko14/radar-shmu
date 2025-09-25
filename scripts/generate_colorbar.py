#!/usr/bin/env python3
"""
Generate a standalone colorbar PNG for web overlay use.

This script creates a vertical colorbar with labels showing the radar reflectivity
scale in dBZ, suitable for use as an overlay on web maps.
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.colorbar import ColorbarBase
from pathlib import Path
import argparse
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from imeteo_radar.config.shmu_colormap import get_shmu_colormap, get_dbz_range


def generate_colorbar(
    output_path: str = "colorbar.png",
    orientation: str = "vertical",
    width_inches: float = 0.8,
    height_inches: float = 4.0,
    dpi: int = 100,
    transparent: bool = True,
    label_size: int = 10,
    title: str = "dBZ"
):
    """
    Generate a standalone colorbar image.

    Args:
        output_path: Path to save the colorbar PNG
        orientation: 'vertical' or 'horizontal'
        width_inches: Width of figure in inches
        height_inches: Height of figure in inches
        dpi: Resolution in dots per inch
        transparent: Whether background should be transparent
        label_size: Font size for labels
        title: Title for the colorbar (units)
    """

    # Get SHMU colormap and normalization
    cmap, norm = get_shmu_colormap()
    min_dbz, max_dbz = get_dbz_range()

    # Create bounds for the colorbar (1 dBZ intervals)
    bounds = list(range(int(min_dbz), int(max_dbz) + 1))

    # Create figure and axis
    if orientation == 'vertical':
        fig, ax = plt.subplots(figsize=(width_inches, height_inches))
        cbar_ax = ax
    else:
        fig, ax = plt.subplots(figsize=(height_inches, width_inches))
        cbar_ax = ax

    # Create colorbar
    cb = ColorbarBase(
        cbar_ax,
        cmap=cmap,
        norm=norm,
        orientation=orientation,
        extend='both'  # Add arrows at both ends
    )

    # Set label
    if orientation == 'vertical':
        cb.set_label(title, fontsize=label_size + 2, fontweight='bold')
    else:
        cb.set_label(title, fontsize=label_size + 2, fontweight='bold')

    # Customize ticks
    # Set major ticks at specific values for better readability
    major_ticks = [-30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80]
    cb.set_ticks([t for t in major_ticks if bounds[0] <= t <= bounds[-1]])
    cb.ax.tick_params(labelsize=label_size)

    # Add minor ticks for better granularity
    minor_ticks = []
    for i in range(int(bounds[0]), int(bounds[-1]) + 1, 5):
        if i not in major_ticks:
            minor_ticks.append(i)
    cb.ax.set_yticks(minor_ticks, minor=True) if orientation == 'vertical' else cb.ax.set_xticks(minor_ticks, minor=True)

    # Style adjustments
    if orientation == 'vertical':
        # For vertical, put ticks on the right side for better web overlay
        cb.ax.yaxis.tick_right()
        cb.ax.yaxis.set_label_position('right')

    # Remove the main axis frame
    ax.set_frame_on(False)

    # Adjust layout to minimize whitespace
    plt.tight_layout(pad=0.1)

    # Save figure
    plt.savefig(
        output_path,
        dpi=dpi,
        transparent=transparent,
        bbox_inches='tight',
        pad_inches=0.02
    )
    plt.close()

    print(f"✅ Colorbar saved to: {output_path}")
    print(f"📐 Size: {width_inches}x{height_inches} inches @ {dpi} DPI")
    print(f"📊 Range: {bounds[0]} to {bounds[-1]} {title}")
    if transparent:
        print("🎨 Background: Transparent")


def generate_web_colorbars(output_dir: str = "outputs"):
    """
    Generate multiple colorbar variants for web use.

    Args:
        output_dir: Directory to save colorbar images
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate vertical colorbar (most common for web maps)
    generate_colorbar(
        output_path / "colorbar_vertical.png",
        orientation="vertical",
        width_inches=0.8,
        height_inches=4.0,
        dpi=150,
        transparent=True,
        label_size=10,
        title="dBZ"
    )

    # Generate vertical colorbar without label (cleaner)
    generate_colorbar(
        output_path / "colorbar_vertical_no_label.png",
        orientation="vertical",
        width_inches=0.6,
        height_inches=4.0,
        dpi=150,
        transparent=True,
        label_size=10,
        title=""
    )

    # Generate horizontal colorbar
    generate_colorbar(
        output_path / "colorbar_horizontal.png",
        orientation="horizontal",
        width_inches=0.8,
        height_inches=4.0,
        dpi=150,
        transparent=True,
        label_size=10,
        title="dBZ"
    )

    # Generate small vertical colorbar for mobile
    generate_colorbar(
        output_path / "colorbar_vertical_small.png",
        orientation="vertical",
        width_inches=0.6,
        height_inches=3.0,
        dpi=100,
        transparent=True,
        label_size=8,
        title="dBZ"
    )

    # Generate high-res version for print/retina displays
    generate_colorbar(
        output_path / "colorbar_vertical_2x.png",
        orientation="vertical",
        width_inches=0.8,
        height_inches=4.0,
        dpi=300,
        transparent=True,
        label_size=10,
        title="dBZ"
    )

    print("\n📦 Generated colorbar variants:")
    print("  • colorbar_vertical.png - Standard vertical colorbar")
    print("  • colorbar_vertical_no_label.png - Vertical without label")
    print("  • colorbar_horizontal.png - Horizontal layout")
    print("  • colorbar_vertical_small.png - Mobile-optimized")
    print("  • colorbar_vertical_2x.png - High-resolution (retina)")


def main():
    parser = argparse.ArgumentParser(description="Generate radar colorbar for web overlay")
    parser.add_argument(
        "--output",
        "-o",
        default="colorbar.png",
        help="Output file path (default: colorbar.png)"
    )
    parser.add_argument(
        "--orientation",
        choices=["vertical", "horizontal"],
        default="vertical",
        help="Colorbar orientation (default: vertical)"
    )
    parser.add_argument(
        "--width",
        type=float,
        default=0.8,
        help="Width in inches (default: 0.8)"
    )
    parser.add_argument(
        "--height",
        type=float,
        default=4.0,
        help="Height in inches (default: 4.0)"
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=150,
        help="Resolution in DPI (default: 150)"
    )
    parser.add_argument(
        "--no-transparent",
        action="store_true",
        help="Use white background instead of transparent"
    )
    parser.add_argument(
        "--label-size",
        type=int,
        default=10,
        help="Font size for labels (default: 10)"
    )
    parser.add_argument(
        "--title",
        default="dBZ",
        help="Title/units for colorbar (default: dBZ)"
    )
    parser.add_argument(
        "--generate-all",
        action="store_true",
        help="Generate all web variants"
    )

    args = parser.parse_args()

    if args.generate_all:
        # Generate all variants
        output_dir = Path(args.output).parent if args.output != "colorbar.png" else "outputs"
        generate_web_colorbars(str(output_dir))
    else:
        # Generate single colorbar
        generate_colorbar(
            output_path=args.output,
            orientation=args.orientation,
            width_inches=args.width,
            height_inches=args.height,
            dpi=args.dpi,
            transparent=not args.no_transparent,
            label_size=args.label_size,
            title=args.title
        )


if __name__ == "__main__":
    main()