#!/usr/bin/env python3
"""
Radar Compositor - Merge multiple radar sources into composite images

Combines data from multiple radar sources (DWD, SHMU, CHMI) using maximum
reflectivity strategy. Handles reprojection to common Web Mercator grid
using proper geospatial transformation (rasterio).
"""

import gc
from typing import Any

import numpy as np
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, reproject

from ..core.base import lonlat_to_mercator
from ..core.logging import get_logger
from ..core.projections import get_crs_web_mercator, get_crs_wgs84

logger = get_logger(__name__)


class RadarCompositor:
    """
    Merge multiple radar sources with maximum reflectivity strategy.

    Uses proper geospatial reprojection (rasterio.warp.reproject) to handle:
    - Different source coordinate systems (WGS84, stereographic, etc.)
    - Proper pixel-to-coordinate mapping
    - Accurate reprojection to Web Mercator output grid
    """

    def __init__(self, target_extent: dict[str, float], resolution_m: float = 500.0):
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

        # Calculate target grid dimensions
        self._setup_target_grid()

        # Initialize composite array with NaN
        self.composite_data = np.full(
            (self.grid_height, self.grid_width), np.nan, dtype=np.float32
        )

        self.sources_merged = []

    def _setup_target_grid(self):
        """Calculate target grid dimensions in Web Mercator"""

        # Convert extent to Web Mercator
        west_m, south_m = lonlat_to_mercator(
            self.target_extent["west"], self.target_extent["south"]
        )
        east_m, north_m = lonlat_to_mercator(
            self.target_extent["east"], self.target_extent["north"]
        )

        # Calculate grid dimensions based on resolution
        width_m = east_m - west_m
        height_m = north_m - south_m

        self.grid_width = int(np.ceil(width_m / self.resolution_m))
        self.grid_height = int(np.ceil(height_m / self.resolution_m))

        # Store mercator bounds
        self.mercator_bounds = {
            "west": west_m,
            "east": east_m,
            "south": south_m,
            "north": north_m,
        }

        # Create target transform for rasterio reprojection
        # from_bounds(west, south, east, north, width, height)
        self.target_transform = from_bounds(
            west_m, south_m, east_m, north_m, self.grid_width, self.grid_height
        )

        logger.info(
            f"Target grid: {self.grid_width}x{self.grid_height} pixels "
            f"@ {self.resolution_m}m resolution"
        )
        logger.debug(
            f"   Extent: {self.target_extent['west']:.2f}E to {self.target_extent['east']:.2f}E, "
            f"{self.target_extent['south']:.2f}N to {self.target_extent['north']:.2f}N"
        )

    def add_source(self, source_name: str, radar_data: dict[str, Any]) -> bool:
        """
        Add data from one radar source and merge using maximum reflectivity.

        Uses rasterio.warp.reproject for proper geospatial transformation that
        correctly handles different source coordinate systems.

        Args:
            source_name: Source identifier (e.g., 'dwd', 'shmu', 'chmi')
            radar_data: Dictionary from source.process_to_array() containing:
                - 'data': 2D array of reflectivity values (dBZ)
                - 'extent': WGS84 bounds
                - 'projection': Optional projection info (for DWD stereographic)
                - 'metadata': source metadata

        Returns:
            True if successfully merged, False otherwise
        """

        logger.info(
            f"Merging {source_name.upper()} data...", extra={"source": source_name}
        )

        try:
            source_data = radar_data["data"]
            extent = radar_data.get("extent", {})
            projection_info = radar_data.get("projection")

            # Count valid data
            valid_mask = ~np.isnan(source_data)
            valid_count = np.sum(valid_mask)
            total_count = source_data.size

            if valid_count == 0:
                logger.warning(f"No valid data in {source_name}, skipping")
                return False

            logger.debug(
                f"   Valid pixels: {valid_count:,} / {total_count:,} "
                f"({100 * valid_count / total_count:.1f}%)"
            )

            # Determine source CRS and transform
            source_crs, source_transform = self._get_source_crs_and_transform(
                source_name, source_data.shape, extent, projection_info
            )

            if source_crs is None or source_transform is None:
                logger.error(f"Could not determine CRS/transform for {source_name}")
                return False

            logger.debug(f"   Source CRS: {source_crs}")

            # Reproject source data to target Web Mercator grid
            logger.debug("   Reprojecting to Web Mercator...")
            before_count = np.count_nonzero(~np.isnan(self.composite_data))

            # Create output array for reprojected data
            reprojected = np.full(
                (self.grid_height, self.grid_width), np.nan, dtype=np.float32
            )

            # Use rasterio.warp.reproject for proper geospatial transformation
            reproject(
                source=source_data.astype(np.float32),
                destination=reprojected,
                src_transform=source_transform,
                src_crs=source_crs,
                dst_transform=self.target_transform,
                dst_crs=get_crs_web_mercator(),
                resampling=Resampling.nearest,  # Preserve discrete dBZ values
                src_nodata=np.nan,
                dst_nodata=np.nan,
            )

            # Count reprojected valid pixels
            reprojected_valid = np.sum(~np.isnan(reprojected))
            if reprojected_valid == 0:
                logger.warning(
                    f"No data from {source_name} overlaps target extent, skipping"
                )
                return False

            # Merge into composite using NaN-aware max
            self.composite_data = np.fmax(self.composite_data, reprojected)

            after_count = np.count_nonzero(~np.isnan(self.composite_data))
            new_pixels = after_count - before_count

            logger.info(
                f"Merged {source_name.upper()}: +{new_pixels:,} new pixels, total: {after_count:,}",
                extra={
                    "source": source_name,
                    "new_pixels": new_pixels,
                    "total_pixels": after_count,
                },
            )

            # Track merged sources
            self.sources_merged.append(source_name)

            # Cleanup
            del reprojected
            gc.collect()

            return True

        except Exception as e:
            logger.error(f"Failed to merge {source_name}: {e}")
            import traceback

            traceback.print_exc()
            return False

    def _get_source_crs_and_transform(
        self,
        source_name: str,
        shape: tuple[int, int],
        extent: dict[str, Any],
        projection_info: dict[str, Any] | None,
    ) -> tuple[CRS | None, Any]:
        """
        Determine source CRS and affine transform for reprojection.

        Uses the unified build_native_params_from_projection_info() function
        which correctly calculates pixel size from corner coordinates rather
        than using the unreliable xscale/yscale values from HDF5.

        Args:
            source_name: Source identifier
            shape: Data shape (height, width)
            extent: Extent dict with wgs84 bounds
            projection_info: Optional projection metadata (for DWD)

        Returns:
            Tuple of (CRS, Affine transform) or (None, None) on error
        """
        # Try to use the unified native params builder for projected sources
        if projection_info and projection_info.get("proj_def"):
            from .reprojector import build_native_params_from_projection_info

            native_crs, native_transform, _native_bounds = (
                build_native_params_from_projection_info(shape, projection_info)
            )

            if native_crs is not None and native_transform is not None:
                logger.debug(
                    f"   {source_name.upper()} projection transform: {native_transform}"
                )
                return native_crs, native_transform

        # Fall back to WGS84 for sources without projection or if building failed
        return self._get_wgs84_transform(extent, shape)

    def _get_wgs84_transform(
        self,
        extent: dict[str, Any],
        shape: tuple[int, int],
    ) -> tuple[CRS, Any]:
        """
        Create WGS84 CRS and transform from extent bounds.

        IMPORTANT: HDF5 corner coordinates (LL_lon/lat, UR_lon/lat) represent
        PIXEL CENTERS, not pixel edges. We must expand bounds by half a pixel
        to get the true outer edges for from_bounds().

        Args:
            extent: Extent dict with 'wgs84' bounds (pixel centers)
            shape: Data shape (height, width)

        Returns:
            Tuple of (CRS, Affine transform)
        """
        height, width = shape
        wgs84 = extent.get("wgs84", {})

        # These coordinates are pixel CENTERS (from HDF5 LL/UR corners)
        center_west = wgs84.get("west", 0)
        center_east = wgs84.get("east", 0)
        center_south = wgs84.get("south", 0)
        center_north = wgs84.get("north", 0)

        # Calculate pixel size (distance between adjacent pixel centers)
        # For N pixels, there are N-1 intervals between centers
        pixel_width = (center_east - center_west) / (width - 1) if width > 1 else 0
        pixel_height = (center_north - center_south) / (height - 1) if height > 1 else 0

        # Expand bounds by half a pixel to get outer EDGES
        edge_west = center_west - pixel_width / 2
        edge_east = center_east + pixel_width / 2
        edge_south = center_south - pixel_height / 2
        edge_north = center_north + pixel_height / 2

        logger.debug(
            f"   WGS84 bounds: centers ({center_west:.4f},{center_south:.4f})-({center_east:.4f},{center_north:.4f}) "
            f"-> edges ({edge_west:.4f},{edge_south:.4f})-({edge_east:.4f},{edge_north:.4f})"
        )

        # from_bounds expects EDGES (outer boundary of raster)
        source_transform = from_bounds(
            edge_west, edge_south, edge_east, edge_north, width, height
        )

        return get_crs_wgs84(), source_transform

    def get_composite(self) -> dict[str, Any]:
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
            "data": self.composite_data,
            "extent": self.target_extent,
            "mercator_bounds": self.mercator_bounds,
            "resolution_m": self.resolution_m,
            "grid_size": (self.grid_height, self.grid_width),
            "sources": self.sources_merged,
            "coverage_percent": coverage,
            "valid_pixels": valid_pixels,
            "total_pixels": total_pixels,
        }

    def clear_cache(self):
        """Run garbage collection to free memory."""
        gc.collect()

    def get_summary(self) -> str:
        """Get human-readable summary of composite"""

        composite = self.get_composite()

        summary = [
            "\n" + "=" * 60,
            "RADAR COMPOSITE SUMMARY",
            "=" * 60,
            f"Sources merged: {', '.join(composite['sources']).upper()}",
            f"Grid size: {composite['grid_size'][1]}×{composite['grid_size'][0]} pixels",
            f"Resolution: {composite['resolution_m']}m",
            f"Extent: {composite['extent']['west']:.2f}°E to {composite['extent']['east']:.2f}°E",
            f"        {composite['extent']['south']:.2f}°N to {composite['extent']['north']:.2f}°N",
            f"Coverage: {composite['coverage_percent']:.1f}% "
            f"({composite['valid_pixels']:,} / {composite['total_pixels']:,} pixels)",
            "=" * 60,
        ]

        return "\n".join(summary)


def create_composite(
    sources_data: list[tuple[str, dict[str, Any]]],
    resolution_m: float = 500.0,
    custom_extent: dict[str, float] | None = None,
) -> dict[str, Any]:
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

    logger.info("=" * 60)
    logger.info("CREATING RADAR COMPOSITE")
    logger.info("=" * 60)

    if not sources_data:
        raise ValueError("No source data provided")

    # Calculate combined extent if not provided
    if custom_extent is None:
        logger.info("Calculating combined extent from sources...")

        all_extents = []
        for _source_name, radar_data in sources_data:
            if "extent" in radar_data and "wgs84" in radar_data["extent"]:
                all_extents.append(radar_data["extent"]["wgs84"])

        if not all_extents:
            raise ValueError("No extent information found in source data")

        # Calculate combined bounds
        custom_extent = {
            "west": min(ext["west"] for ext in all_extents),
            "east": max(ext["east"] for ext in all_extents),
            "south": min(ext["south"] for ext in all_extents),
            "north": max(ext["north"] for ext in all_extents),
        }

        logger.debug(
            f"Combined extent: {custom_extent['west']:.2f}° to {custom_extent['east']:.2f}°E, "
            f"{custom_extent['south']:.2f}° to {custom_extent['north']:.2f}°N"
        )

    # Create compositor
    compositor = RadarCompositor(custom_extent, resolution_m)

    # Add sources sequentially
    for source_name, radar_data in sources_data:
        compositor.add_source(source_name, radar_data)
        gc.collect()  # Aggressive cleanup after each source

    # Get final composite
    result = compositor.get_composite()

    # Log summary
    logger.info(compositor.get_summary())

    # Clear coordinate cache to free memory
    compositor.clear_cache()

    return result
