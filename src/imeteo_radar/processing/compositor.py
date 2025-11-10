#!/usr/bin/env python3
"""
Radar Compositor - Merge multiple radar sources into composite images

Combines data from multiple radar sources (DWD, SHMU, CHMI) using maximum
reflectivity strategy. Handles reprojection to common Web Mercator grid.
"""

import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from scipy.interpolate import RegularGridInterpolator
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
        self.coordinate_cache = {}  # Cache coordinates during operation

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

            # Generate coordinates if not provided (lazy generation with caching)
            if coordinates is None:
                cache_key = source_name
                if cache_key not in self.coordinate_cache:
                    print(f"   Generating coordinates from HDF5 metadata...")
                    coordinates = self._generate_coordinates_from_metadata(
                        radar_data['dimensions'],
                        radar_data['extent'],
                        radar_data.get('projection')
                    )
                    self.coordinate_cache[cache_key] = coordinates
                else:
                    print(f"   Using cached coordinates...")
                    coordinates = self.coordinate_cache[cache_key]

            # Get source coordinates
            source_lons = coordinates['lons']
            source_lats = coordinates['lats']

            # Handle 1D coordinate arrays (SHMU/CHMI style) - keep as 1D for RegularGridInterpolator
            if source_lons.ndim == 1 and source_lats.ndim == 1:
                # Source is already on regular grid - perfect for RegularGridInterpolator
                source_lon_1d = source_lons
                source_lat_1d = source_lats
            else:
                # DWD 2D coordinates - extract 1D coordinate vectors
                # Assume coordinates form a regular grid (they should from projection)
                source_lon_1d = source_lons[0, :]  # First row
                source_lat_1d = source_lats[:, 0]  # First column

            # Convert source coordinate grid to Web Mercator
            # Create meshgrid for transformation
            source_lons_2d, source_lats_2d = np.meshgrid(source_lon_1d, source_lat_1d)

            # Convert to Mercator
            source_x_2d = np.zeros_like(source_lons_2d)
            source_y_2d = np.zeros_like(source_lats_2d)

            print(f"   Converting {source_lons_2d.size:,} coordinates to Mercator...")
            for i in range(source_lons_2d.shape[0]):
                for j in range(source_lons_2d.shape[1]):
                    source_x_2d[i, j], source_y_2d[i, j] = lonlat_to_mercator(
                        source_lons_2d[i, j], source_lats_2d[i, j]
                    )

            # Create 1D coordinate vectors in Mercator (for RegularGridInterpolator)
            source_x_1d = source_x_2d[0, :]  # X varies along columns
            source_y_1d = source_y_2d[:, 0]  # Y varies along rows

            # Count valid data
            valid_mask = ~np.isnan(source_data)
            valid_count = np.sum(valid_mask)
            total_count = source_data.size

            if valid_count == 0:
                print(f"⚠️  No valid data in {source_name}, skipping")
                return False

            print(f"   Valid pixels: {valid_count:,} / {total_count:,} "
                  f"({100*valid_count/total_count:.1f}%)")

            # Create RegularGridInterpolator with source data
            # Use 'nearest' method to preserve discrete dBZ values and avoid smoothing
            print(f"   Creating regular grid interpolator...")
            interpolator = RegularGridInterpolator(
                (source_y_1d, source_x_1d),  # Note: y first (rows), x second (cols)
                source_data,
                method='nearest',
                bounds_error=False,
                fill_value=np.nan
            )

            # Create target grid points
            target_xx, target_yy = np.meshgrid(self.target_x, self.target_y)
            target_points = np.column_stack([target_yy.ravel(), target_xx.ravel()])

            # Interpolate to target grid
            print(f"   Resampling to target grid...")
            interpolated_flat = interpolator(target_points)
            interpolated = interpolated_flat.reshape(self.grid_height, self.grid_width)

            # Count in-bounds pixels (non-NaN after interpolation)
            in_bounds_count = np.sum(~np.isnan(interpolated))
            if in_bounds_count == 0:
                print(f"⚠️  No data from {source_name} overlaps target extent, skipping")
                return False

            print(f"   In-bounds pixels: {in_bounds_count:,}")

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
            del interpolated, target_xx, target_yy, target_points
            del source_x_2d, source_y_2d, source_lons_2d, source_lats_2d
            del interpolator
            gc.collect()

            return True

        except Exception as e:
            print(f"❌ Failed to merge {source_name}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _generate_coordinates_from_metadata(self, dimensions: Tuple[int, int],
                                           extent: Dict[str, Any],
                                           projection_info: Optional[Dict[str, Any]]) -> Dict[str, np.ndarray]:
        """
        Generate coordinates from real HDF5 metadata.

        For DWD (stereographic): Uses projection_handler with real HDF5 where_attrs + proj_def
        For SHMU/CHMI (Web Mercator): Simple linspace from real corner coordinates

        Args:
            dimensions: Data shape (ny, nx)
            extent: Extent dict with wgs84 bounds from HDF5
            projection_info: Projection metadata from HDF5 (None for Web Mercator sources)

        Returns:
            Dict with 'lons' and 'lats' arrays (1D or 2D depending on projection)
        """
        if projection_info and projection_info.get('type') == 'stereographic':
            # DWD: Use projection_handler with REAL HDF5 metadata
            print(f"      Using stereographic projection from HDF5...")
            lons, lats = self.projection_handler.create_dwd_coordinates(
                shape=dimensions,
                where_attrs=projection_info['where_attrs'],  # Real HDF5 data
                proj_def=projection_info['proj_def']         # Real HDF5 data
            )
            return {'lons': lons, 'lats': lats}
        else:
            # SHMU/CHMI: Web Mercator - simple grid from real corner coordinates
            print(f"      Using Web Mercator grid from HDF5 corner coordinates...")
            wgs84 = extent['wgs84']
            lons = np.linspace(wgs84['west'], wgs84['east'], dimensions[1])
            lats = np.linspace(wgs84['north'], wgs84['south'], dimensions[0])
            return {'lons': lons, 'lats': lats}

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
