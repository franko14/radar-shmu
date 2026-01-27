#!/usr/bin/env python3
"""
SHMU Radar Colormap - Official SHMU colorscale for radar reflectivity visualization.

This module provides the authentic SHMU colorscale with discrete 1 dBZ intervals,
extracted from the official SHMU colorscale image. Each dBZ value from -35 to 85
gets exactly one specific color with clean boundaries and no interpolation artifacts.

Usage:
    from shmu_colormap import get_shmu_colormap, get_dbz_range

    cmap, norm = get_shmu_colormap()
    min_dbz, max_dbz = get_dbz_range()

    plt.pcolormesh(x, y, data, cmap=cmap, norm=norm, shading='nearest')
"""

import matplotlib.colors as mcolors

from ..core.logging import get_logger

logger = get_logger(__name__)


def get_shmu_colormap():
    """
    Get the official SHMU colormap with discrete 1 dBZ intervals.

    Returns:
        tuple: (cmap, norm) - Colormap and boundary normalization for matplotlib
    """

    # SHMU representative colors for key dBZ values
    # Extracted from official SHMU colorscale with linear interpolation
    key_colors = {
        -35: [0, 0, 0],  # Black (no data)
        -30: [115, 126, 139],  # Dark gray
        -25: [105, 117, 130],  # Gray
        -20: [172, 196, 212],  # Light blue
        -15: [140, 172, 200],  # Blue
        -10: [96, 140, 188],  # Medium blue
        -5: [52, 108, 180],  # Dark blue
        0: [12, 76, 168],  # Very dark blue
        5: [0, 100, 144],  # Dark blue-teal
        10: [0, 140, 108],  # Teal
        15: [0, 180, 64],  # Green
        20: [0, 232, 12],  # Bright green
        25: [84, 240, 0],  # Yellow-green
        30: [208, 228, 0],  # Yellow
        35: [252, 204, 0],  # Golden yellow
        40: [252, 168, 0],  # Orange
        45: [252, 100, 0],  # Red-orange
        50: [248, 20, 0],  # Red
        55: [216, 8, 0],  # Dark red
        60: [152, 16, 0],  # Very dark red
        65: [164, 40, 96],  # Purple-red
        70: [204, 72, 200],  # Purple
        75: [244, 108, 244],  # Light purple
        80: [252, 140, 252],  # Light pink
        85: [252, 172, 252],  # Very light pink
    }

    # Create discrete colors for each 1 dBZ increment with linear interpolation
    dbz_range = range(-35, 86)  # -35 to 85 dBZ
    colors = []

    # Sort key colors by dBZ for interpolation
    sorted_keys = sorted(key_colors.keys())

    for dbz in dbz_range:
        if dbz in key_colors:
            # Use exact key color
            rgb = key_colors[dbz]
        else:
            # Linear interpolation between surrounding key colors
            lower_key = None
            upper_key = None

            for _i, key in enumerate(sorted_keys):
                if key <= dbz:
                    lower_key = key
                if key > dbz and upper_key is None:
                    upper_key = key
                    break

            if lower_key is not None and upper_key is not None:
                # Interpolate between lower and upper key colors
                factor = (dbz - lower_key) / (upper_key - lower_key)
                lower_rgb = key_colors[lower_key]
                upper_rgb = key_colors[upper_key]

                rgb = [
                    int(lower_rgb[0] + factor * (upper_rgb[0] - lower_rgb[0])),
                    int(lower_rgb[1] + factor * (upper_rgb[1] - lower_rgb[1])),
                    int(lower_rgb[2] + factor * (upper_rgb[2] - lower_rgb[2])),
                ]
            elif lower_key is not None:
                rgb = key_colors[lower_key]
            elif upper_key is not None:
                rgb = key_colors[upper_key]
            else:
                rgb = [0, 0, 0]  # Fallback to black

        # Convert to matplotlib format (0-1 range)
        colors.append((rgb[0] / 255.0, rgb[1] / 255.0, rgb[2] / 255.0))

    # Create discrete colormap with clean boundaries
    cmap = mcolors.ListedColormap(colors, name="shmu_radar")

    # Create boundaries for discrete steps (centered on integer dBZ values)
    boundaries = [dbz - 0.5 for dbz in dbz_range] + [85.5]
    norm = mcolors.BoundaryNorm(boundaries, len(colors))

    return cmap, norm


def get_dbz_range():
    """
    Get the dBZ range covered by the SHMU colorscale.

    Returns:
        tuple: (min_dbz, max_dbz)
    """
    return (-35, 85)


def get_color_for_dbz(dbz_value):
    """
    Get the SHMU color for a specific dBZ value.

    Args:
        dbz_value (float): dBZ value

    Returns:
        tuple: RGB color (0-1 range)
    """
    cmap, norm = get_shmu_colormap()

    # Clamp to range
    dbz_clamped = max(-35, min(85, dbz_value))

    # Get normalized value
    norm_value = norm(dbz_clamped)

    # Get color from colormap
    return cmap(norm_value)


# For backward compatibility
create_discrete_colormap = get_shmu_colormap

if __name__ == "__main__":
    logger.info("SHMU Official Radar Colormap")

    cmap, norm = get_shmu_colormap()
    min_dbz, max_dbz = get_dbz_range()

    logger.info(f"dBZ range: {min_dbz} to {max_dbz}")
    logger.info(f"Number of discrete colors: {cmap.N}")
    logger.info(f"Colormap name: {cmap.name}")

    # Show sample colors
    logger.info("Sample colors:")
    test_values = [-35, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 85]
    for dbz in test_values:
        color = get_color_for_dbz(dbz)
        rgb_255 = tuple(int(c * 255) for c in color[:3])
        logger.info(f"  {dbz:3d} dBZ: RGB{rgb_255}")
