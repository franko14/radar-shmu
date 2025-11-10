#!/usr/bin/env python3
"""
Radar Compositor - Merge multiple radar sources into composite images

Combines data from multiple radar sources (DWD, SHMU, CHMI) using maximum
reflectivity strategy. Handles reprojection to common Web Mercator grid.
"""

import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from scipy.interpolate import griddata
import gc
import warnings

from ..core.base import lonlat_to_mercator, mercator_to_lonlat
from ..core.projection import ProjectionHandler


class RadarCompositor:
    """
    Merge multiple radar sources with maximum reflectivity strategy.

    Memory-efficient implementation:
    - Process sources sequentially, not all at once
    - Clear each source after merging
    - Target: <1.2 GB total memory usage
    """

    def __init__(self, target_extent: Dict[str, float], resolution_m: float = 500.0):
        """
        Initialize compositor with target grid.

        Args:
            target_extent: Geographic bounds in WGS84
                {
                    'west': min_lon,
                    'east': max_lon,
                    'south': min_lat,
                    'north': max_lat
                }
            resolution_m: Target resolution in meters (default: 500m)
        """
        self.target_extent = target_extent
        self.resolution_m = resolution_m
        self.projection_handler = ProjectionHandler()

        # Calculate target grid dimensions
        self._setup_target_grid()

        # Initialize composite array with NaN
        self.composite_data = np.full(
            (self.grid_height, self.grid_width),
            np.nan,
            dtype=np.float32
        )

        self.sources_merged = []

    def _setup_target_grid(self):
        """Calculate target grid dimensions in Web Mercator"""

        # Convert extent to Web Mercator
        west_m, south_m = lonlat_to_mercator(
            self.target_extent['west'],
            self.target_extent['south']
        )
        east_m, north_m = lonlat_to_mercator(
            self.target_extent['east'],
            self.target_extent['north']
        )

        # Calculate grid dimensions based on resolution
        width_m = east_m - west_m
        height_m = north_m - south_m

        self.grid_width = int(np.ceil(width_m / self.resolution_m))
        self.grid_height = int(np.ceil(height_m / self.resolution_m))

        # Store mercator bounds
        self.mercator_bounds = {
            'west': west_m,
            'east': east_m,
            'south': south_m,
            'north': north_m
        }

        # Create coordinate arrays for target grid
        self.target_x = np.linspace(west_m, east_m, self.grid_width)
        self.target_y = np.linspace(north_m, south_m, self.grid_height)

        print(f"🎯 Target grid: {self.grid_width}×{self.grid_height} pixels "
              f"@ {self.resolution_m}m resolution")
        print(f"   Extent: {self.target_extent['west']:.2f}°E to {self.target_extent['east']:.2f}°E, "
              f"{self.target_extent['south']:.2f}°N to {self.target_extent['north']:.2f}°N")

    def add_source(self, source_name: str, radar_data: Dict[str, Any]) -> bool:
        """
        Add data from one radar source and merge using maximum reflectivity.

        Args:
            source_name: Source identifier (e.g., 'dwd', 'shmu', 'chmi')
            radar_data: Dictionary from source.process_to_array() containing:
                - 'data': 2D array of reflectivity values (dBZ)
                - 'coordinates': {'lons': 1D or 2D array, 'lats': 1D or 2D array}
                - 'extent': WGS84 bounds
                - 'metadata': source metadata

        Returns:
            True if successfully merged, False otherwise
        """

        print(f"\n📡 Merging {source_name.upper()} data...")

        try:
            # Extract data
            source_data = radar_data['data']
            coordinates = radar_data['coordinates']

            # Get source coordinates
            source_lons = coordinates['lons']
            source_lats = coordinates['lats']

            # Handle 1D coordinate arrays (SHMU/CHMI style)
            if source_lons.ndim == 1 and source_lats.ndim == 1:
                # Create 2D meshgrid
                source_lons, source_lats = np.meshgrid(source_lons, source_lats)

            # Flatten arrays for reprojection
            source_lons_flat = source_lons.flatten()
            source_lats_flat = source_lats.flatten()
            source_data_flat = source_data.flatten()

            # Filter out NaN values (no need to reproject nodata)
            valid_mask = ~np.isnan(source_data_flat)
            valid_lons = source_lons_flat[valid_mask]
            valid_lats = source_lats_flat[valid_mask]
            valid_data = source_data_flat[valid_mask]

            if len(valid_data) == 0:
                print(f"⚠️  No valid data in {source_name}, skipping")
                return False

            print(f"   Valid pixels: {len(valid_data):,} / {len(source_data_flat):,} "
                  f"({100*len(valid_data)/len(source_data_flat):.1f}%)")

            # Convert source coordinates to Web Mercator
            source_x = np.zeros(len(valid_lons))
            source_y = np.zeros(len(valid_lons))

            for i in range(len(valid_lons)):
                source_x[i], source_y[i] = lonlat_to_mercator(valid_lons[i], valid_lats[i])

            # Filter points outside target extent (with small buffer)
            buffer = self.resolution_m * 2  # 2-pixel buffer
            in_bounds = (
                (source_x >= self.mercator_bounds['west'] - buffer) &
                (source_x <= self.mercator_bounds['east'] + buffer) &
                (source_y >= self.mercator_bounds['south'] - buffer) &
                (source_y <= self.mercator_bounds['north'] + buffer)
            )

            source_x = source_x[in_bounds]
            source_y = source_y[in_bounds]
            valid_data = valid_data[in_bounds]

            if len(valid_data) == 0:
                print(f"⚠️  No data from {source_name} overlaps target extent, skipping")
                return False

            print(f"   In-bounds pixels: {len(valid_data):,}")

            # Create target grid meshgrid
            target_xx, target_yy = np.meshgrid(self.target_x, self.target_y)

            # Interpolate source data onto target grid using nearest neighbor
            # (fast and preserves discrete dBZ values)
            print(f"   Interpolating to target grid...")
            interpolated = griddata(
                points=(source_x, source_y),
                values=valid_data,
                xi=(target_xx, target_yy),
                method='nearest',
                fill_value=np.nan
            )

            # Merge using maximum reflectivity (element-wise max, NaN-aware)
            # np.fmax ignores NaN values (unlike np.maximum)
            before_count = np.count_nonzero(~np.isnan(self.composite_data))
            self.composite_data = np.fmax(self.composite_data, interpolated)
            after_count = np.count_nonzero(~np.isnan(self.composite_data))

            new_pixels = after_count - before_count
            print(f"   ✅ Merged: +{new_pixels:,} new pixels, total: {after_count:,}")

            # Track merged sources
            self.sources_merged.append(source_name)

            # Cleanup
            del interpolated, target_xx, target_yy
            del source_x, source_y, valid_data
            del source_lons_flat, source_lats_flat, source_data_flat
            gc.collect()

            return True

        except Exception as e:
            print(f"❌ Failed to merge {source_name}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def get_composite(self) -> Dict[str, Any]:
        """
        Get the final composite data.

        Returns:
            Dictionary with:
                - 'data': 2D array of composite reflectivity (dBZ)
                - 'extent': WGS84 bounds
                - 'mercator_bounds': Web Mercator bounds
                - 'resolution_m': Resolution in meters
                - 'grid_size': (height, width)
                - 'sources': List of merged sources
                - 'coverage_percent': Percentage of grid with data
        """

        valid_pixels = np.count_nonzero(~np.isnan(self.composite_data))
        total_pixels = self.composite_data.size
        coverage = 100 * valid_pixels / total_pixels

        return {
            'data': self.composite_data,
            'extent': self.target_extent,
            'mercator_bounds': self.mercator_bounds,
            'resolution_m': self.resolution_m,
            'grid_size': (self.grid_height, self.grid_width),
            'sources': self.sources_merged,
            'coverage_percent': coverage,
            'valid_pixels': valid_pixels,
            'total_pixels': total_pixels
        }

    def get_summary(self) -> str:
        """Get human-readable summary of composite"""

        composite = self.get_composite()

        summary = [
            "\n" + "="*60,
            "RADAR COMPOSITE SUMMARY",
            "="*60,
            f"Sources merged: {', '.join(composite['sources']).upper()}",
            f"Grid size: {composite['grid_size'][1]}×{composite['grid_size'][0]} pixels",
            f"Resolution: {composite['resolution_m']}m",
            f"Extent: {composite['extent']['west']:.2f}°E to {composite['extent']['east']:.2f}°E",
            f"        {composite['extent']['south']:.2f}°N to {composite['extent']['north']:.2f}°N",
            f"Coverage: {composite['coverage_percent']:.1f}% "
            f"({composite['valid_pixels']:,} / {composite['total_pixels']:,} pixels)",
            "="*60
        ]

        return "\n".join(summary)


def create_composite(sources_data: List[Tuple[str, Dict[str, Any]]],
                    resolution_m: float = 500.0,
                    custom_extent: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """
    Convenience function to create a composite from multiple sources.

    Args:
        sources_data: List of (source_name, radar_data) tuples
        resolution_m: Target resolution in meters
        custom_extent: Optional custom extent, otherwise auto-calculated

    Returns:
        Composite data dictionary from RadarCompositor.get_composite()

    Example:
        >>> dwd_data = dwd_source.process_to_array(dwd_file)
        >>> shmu_data = shmu_source.process_to_array(shmu_file)
        >>> composite = create_composite([
        ...     ('dwd', dwd_data),
        ...     ('shmu', shmu_data)
        ... ])
    """

    print("\n" + "="*60)
    print("CREATING RADAR COMPOSITE")
    print("="*60)

    if not sources_data:
        raise ValueError("No source data provided")

    # Calculate combined extent if not provided
    if custom_extent is None:
        print("📐 Calculating combined extent from sources...")

        all_extents = []
        for source_name, radar_data in sources_data:
            if 'extent' in radar_data and 'wgs84' in radar_data['extent']:
                all_extents.append(radar_data['extent']['wgs84'])

        if not all_extents:
            raise ValueError("No extent information found in source data")

        # Calculate combined bounds
        custom_extent = {
            'west': min(ext['west'] for ext in all_extents),
            'east': max(ext['east'] for ext in all_extents),
            'south': min(ext['south'] for ext in all_extents),
            'north': max(ext['north'] for ext in all_extents)
        }

        print(f"   Combined extent: {custom_extent['west']:.2f}° to {custom_extent['east']:.2f}°E, "
              f"{custom_extent['south']:.2f}° to {custom_extent['north']:.2f}°N")

    # Create compositor
    compositor = RadarCompositor(custom_extent, resolution_m)

    # Add sources sequentially
    for source_name, radar_data in sources_data:
        compositor.add_source(source_name, radar_data)
        gc.collect()  # Aggressive cleanup after each source

    # Get final composite
    result = compositor.get_composite()

    # Print summary
    print(compositor.get_summary())

    return result
