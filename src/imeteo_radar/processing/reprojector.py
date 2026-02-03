#!/usr/bin/env python3
"""
Unified Reprojection Module for Radar Data

Provides consistent reprojection of radar data from any source projection
to EPSG:3857 (Web Mercator) for use in web mapping applications.

This module extracts the reprojection logic from compositor.py into a reusable
function that can be used for both composite and individual source exports.
"""

from typing import Any

import numpy as np
from pyproj import Transformer
from rasterio.crs import CRS
from rasterio.transform import Affine, from_bounds
from rasterio.warp import Resampling, calculate_default_transform, reproject

from ..core.base import lonlat_to_mercator
from ..core.logging import get_logger
from ..core.projections import (
    PROJ4_WEB_MERCATOR,
    PROJ4_WGS84,
    get_crs_web_mercator,
    get_crs_wgs84,
)

logger = get_logger(__name__)


def reproject_to_web_mercator(
    data: np.ndarray,
    extent: dict[str, Any],
    projection_info: dict[str, Any] | None = None,
    target_resolution_m: float | None = None,
) -> tuple[np.ndarray, dict[str, float]]:
    """
    Reproject radar data from any source CRS to EPSG:3857 (Web Mercator).

    This function handles:
    - Different source coordinate systems (WGS84, stereographic, mercator, etc.)
    - Proper pixel-to-coordinate mapping
    - Accurate reprojection to Web Mercator output grid

    Args:
        data: 2D numpy array of radar data (e.g., reflectivity in dBZ)
        extent: Dictionary with WGS84 bounds:
            {
                'wgs84': {
                    'west': min_lon,
                    'east': max_lon,
                    'south': min_lat,
                    'north': max_lat
                }
            }
        projection_info: Optional projection metadata for non-WGS84 sources:
            {
                'type': 'stereographic' | 'mercator' | 'wgs84',
                'proj_def': proj4 string (for stereographic/mercator),
                'where_attrs': dict of HDF5 where attributes
            }
        target_resolution_m: Optional target resolution in meters.
            If None, resolution is calculated to preserve source pixel density.

    Returns:
        Tuple of:
            - Reprojected data array (2D numpy array)
            - WGS84 bounds dictionary for Leaflet overlay:
                {
                    'west': min_lon,
                    'east': max_lon,
                    'south': min_lat,
                    'north': max_lat
                }
    """
    height, width = data.shape

    # Get WGS84 bounds from extent
    wgs84 = extent.get("wgs84", extent)
    wgs84_bounds = {
        "west": wgs84.get("west", 0),
        "east": wgs84.get("east", 0),
        "south": wgs84.get("south", 0),
        "north": wgs84.get("north", 0),
    }

    # Determine source CRS and transform
    source_crs, source_transform = _get_source_crs_and_transform(
        data.shape, extent, projection_info
    )

    if source_crs is None or source_transform is None:
        logger.warning("Could not determine source CRS/transform, returning original data")
        return data, wgs84_bounds

    # Calculate target grid in Web Mercator
    west_m, south_m = lonlat_to_mercator(wgs84_bounds["west"], wgs84_bounds["south"])
    east_m, north_m = lonlat_to_mercator(wgs84_bounds["east"], wgs84_bounds["north"])

    # Calculate target resolution
    if target_resolution_m is None:
        # Preserve approximately the same number of pixels as source
        width_m = east_m - west_m
        height_m = north_m - south_m
        # Use the smaller dimension to determine resolution to avoid upscaling
        target_resolution_m = min(width_m / width, height_m / height)

    # Calculate target dimensions
    target_width = int(np.ceil((east_m - west_m) / target_resolution_m))
    target_height = int(np.ceil((north_m - south_m) / target_resolution_m))

    # Create target transform
    target_transform = from_bounds(
        west_m, south_m, east_m, north_m, target_width, target_height
    )

    logger.debug(
        f"Reprojecting: {width}x{height} -> {target_width}x{target_height} "
        f"@ {target_resolution_m:.0f}m resolution"
    )

    # Create output array
    reprojected = np.full((target_height, target_width), np.nan, dtype=np.float32)

    # Perform reprojection using rasterio
    reproject(
        source=data.astype(np.float32),
        destination=reprojected,
        src_transform=source_transform,
        src_crs=source_crs,
        dst_transform=target_transform,
        dst_crs=get_crs_web_mercator(),
        resampling=Resampling.nearest,  # Preserve discrete dBZ values
        src_nodata=np.nan,
        dst_nodata=np.nan,
    )

    # Check if reprojection produced valid data
    valid_count = np.sum(~np.isnan(reprojected))
    if valid_count == 0:
        logger.warning("Reprojection produced no valid data")
        return data, wgs84_bounds

    logger.debug(f"Reprojected {valid_count:,} valid pixels")

    # Calculate output bounds FROM the reprojected Web Mercator grid
    # target_transform is in EPSG:3857 (meters), convert back to WGS84
    merc_left = target_transform.c
    merc_top = target_transform.f
    merc_right = merc_left + target_width * target_transform.a
    merc_bottom = merc_top + target_height * target_transform.e

    transformer = Transformer.from_crs(
        PROJ4_WEB_MERCATOR, PROJ4_WGS84, always_xy=True
    )
    out_west, out_south = transformer.transform(merc_left, merc_bottom)
    out_east, out_north = transformer.transform(merc_right, merc_top)

    output_wgs84_bounds = {
        "west": out_west,
        "east": out_east,
        "south": out_south,
        "north": out_north,
    }

    return reprojected, output_wgs84_bounds


def _get_source_crs_and_transform(
    shape: tuple[int, int],
    extent: dict[str, Any],
    projection_info: dict[str, Any] | None,
) -> tuple[CRS | None, Affine | None]:
    """
    Determine source CRS and affine transform for reprojection.

    Uses proj_def directly from the source if available and if the grid
    is actually projected (corners curve). Falls back to WGS84 for regular
    lat/lon grids even if proj_def exists.

    Args:
        shape: Data shape (height, width)
        extent: Extent dict with wgs84 bounds
        projection_info: Optional projection metadata with proj_def

    Returns:
        Tuple of (CRS, Affine transform) or (None, None) on error
    """
    height, width = shape

    # Check if we have a proj_def and corner attributes
    proj_def = None
    where_attrs = {}
    if projection_info:
        proj_def = projection_info.get("proj_def")
        where_attrs = projection_info.get("where_attrs", {})

    # If no proj_def, use WGS84
    if not proj_def:
        return _get_wgs84_transform(extent, shape)

    # Check if corners indicate a regular lat/lon grid vs projected grid
    # If UL_lon == LL_lon (left edge is vertical), it's a regular lat/lon grid
    ul_lon = float(where_attrs.get("UL_lon", 0))
    ll_lon = float(where_attrs.get("LL_lon", 0))
    if abs(ul_lon - ll_lon) < 0.01:  # Less than 0.01 degree difference = regular grid
        logger.debug("Corners indicate regular lat/lon grid, using WGS84")
        return _get_wgs84_transform(extent, shape)

    # Use proj_def directly for projected grids
    try:
        source_crs = CRS.from_string(proj_def)

        # Transform WGS84 corners to native projection coordinates
        from pyproj import Transformer

        transformer = Transformer.from_crs(
            "EPSG:4326", proj_def, always_xy=True
        )

        # Get all corners in WGS84 from where_attrs
        ul_lat = float(where_attrs.get("UL_lat", 0))
        ur_lon = float(where_attrs.get("UR_lon", 0))
        ur_lat = float(where_attrs.get("UR_lat", 0))
        ll_lat = float(where_attrs.get("LL_lat", 0))

        # Transform corners to projection space
        ul_x, ul_y = transformer.transform(ul_lon, ul_lat)
        ur_x, ur_y = transformer.transform(ur_lon, ur_lat)
        ll_x, ll_y = transformer.transform(ll_lon, ll_lat)

        # Calculate actual pixel size from corners (more reliable than xscale/yscale)
        actual_xscale = (ur_x - ul_x) / (width - 1)
        actual_yscale = (ul_y - ll_y) / (height - 1)

        logger.debug(
            f"Projected grid pixel size: {actual_xscale:.2f} x {actual_yscale:.2f} m"
        )

        # Create transform using actual pixel size and upper-left corner
        # Row 0 is at the top (north), so we use UL corner
        # Adjust UL to pixel edge (UL coords are pixel center)
        ul_edge_x = ul_x - actual_xscale / 2
        ul_edge_y = ul_y + actual_yscale / 2

        source_transform = Affine(actual_xscale, 0, ul_edge_x, 0, -actual_yscale, ul_edge_y)

        logger.debug(f"Projection transform: {source_transform}")
        return source_crs, source_transform

    except Exception as e:
        logger.warning(f"Failed to parse projection: {e}")
        return _get_wgs84_transform(extent, shape)


def _get_wgs84_transform(
    extent: dict[str, Any],
    shape: tuple[int, int],
) -> tuple[CRS, Affine]:
    """
    Create WGS84 CRS and transform from extent bounds.

    HDF5 corner coordinates (LL_lon/lat, UR_lon/lat) are pixel centers.
    We use them directly with rasterio's from_bounds which expects bounds.

    Args:
        extent: Extent dict with 'wgs84' bounds (pixel centers from HDF5)
        shape: Data shape (height, width)

    Returns:
        Tuple of (CRS, Affine transform)
    """
    height, width = shape
    wgs84 = extent.get("wgs84", extent)

    # Use HDF5 corner coordinates directly (pixel centers)
    west = wgs84.get("west", 0)
    east = wgs84.get("east", 0)
    south = wgs84.get("south", 0)
    north = wgs84.get("north", 0)

    logger.debug(
        f"WGS84 bounds: ({west:.6f},{south:.6f})-({east:.6f},{north:.6f})"
    )

    # Create transform from bounds
    source_transform = from_bounds(west, south, east, north, width, height)

    return get_crs_wgs84(), source_transform


def calculate_web_mercator_bounds(wgs84_bounds: dict[str, float]) -> dict[str, float]:
    """
    Convert WGS84 bounds to Web Mercator (EPSG:3857) coordinates.

    Args:
        wgs84_bounds: Dictionary with west, east, south, north in WGS84

    Returns:
        Dictionary with x_min, x_max, y_min, y_max in Web Mercator meters
    """
    x_min, y_min = lonlat_to_mercator(wgs84_bounds["west"], wgs84_bounds["south"])
    x_max, y_max = lonlat_to_mercator(wgs84_bounds["east"], wgs84_bounds["north"])

    return {
        "x_min": x_min,
        "x_max": x_max,
        "y_min": y_min,
        "y_max": y_max,
        "bounds": [x_min, y_min, x_max, y_max],
    }


def reproject_to_web_mercator_accurate(
    data: np.ndarray,
    native_crs: CRS,
    native_transform: Affine,
    native_bounds: tuple[float, float, float, float],
) -> tuple[np.ndarray, dict[str, float], Affine]:
    """
    Reproject data to Web Mercator using calculate_default_transform.

    This is the ACCURATE reprojection method that ensures PNG bounds exactly
    match what a GeoTIFF would produce. The key difference from the original
    reproject_to_web_mercator() is that bounds are DERIVED from the reprojected
    transform, not calculated independently.

    Args:
        data: 2D numpy array of radar data
        native_crs: Source coordinate reference system
        native_transform: Source affine transform
        native_bounds: Native bounds (left, bottom, right, top)

    Returns:
        Tuple of:
            - Reprojected data array
            - WGS84 bounds dictionary for Leaflet overlay
            - Destination transform (for reference)
    """
    height, width = data.shape
    left, bottom, right, top = native_bounds

    # Get Web Mercator CRS
    web_mercator = get_crs_web_mercator()

    # Calculate optimal output transform and dimensions
    # This is the KEY function that ensures accuracy
    dst_transform, dst_width, dst_height = calculate_default_transform(
        native_crs,
        web_mercator,
        width,
        height,
        left=left,
        bottom=bottom,
        right=right,
        top=top,
    )

    # Create output array
    reprojected = np.full((dst_height, dst_width), np.nan, dtype=np.float32)

    # Perform reprojection
    reproject(
        source=data.astype(np.float32),
        destination=reprojected,
        src_transform=native_transform,
        src_crs=native_crs,
        dst_transform=dst_transform,
        dst_crs=web_mercator,
        resampling=Resampling.nearest,
        src_nodata=np.nan,
        dst_nodata=np.nan,
    )

    # Extract mercator bounds FROM the destination transform
    mercator_left = dst_transform.c
    mercator_top = dst_transform.f
    mercator_right = mercator_left + dst_width * dst_transform.a
    mercator_bottom = mercator_top + dst_height * dst_transform.e

    # Convert mercator bounds to WGS84 for Leaflet
    transformer = Transformer.from_crs(
        PROJ4_WEB_MERCATOR, PROJ4_WGS84, always_xy=True
    )
    west, south = transformer.transform(mercator_left, mercator_bottom)
    east, north = transformer.transform(mercator_right, mercator_top)

    wgs84_bounds = {
        "west": west,
        "east": east,
        "south": south,
        "north": north,
    }

    logger.debug(
        f"Accurate reproject: {width}x{height} -> {dst_width}x{dst_height}, "
        f"bounds: ({west:.4f}, {south:.4f}) to ({east:.4f}, {north:.4f})"
    )

    return reprojected, wgs84_bounds, dst_transform


def build_native_params_from_projection_info(
    data_shape: tuple[int, int],
    projection_info: dict[str, Any],
) -> tuple[CRS | None, Affine | None, tuple[float, float, float, float] | None]:
    """
    Build native CRS, transform, and bounds from projection info dictionary.

    This extracts the corner coordinates from HDF5 where_attrs and builds
    the proper affine transform matching the validation code pattern.

    Args:
        data_shape: Data array shape (height, width)
        projection_info: Projection info dict with proj_def and where_attrs

    Returns:
        Tuple of (native_crs, native_transform, native_bounds) or (None, None, None)
    """
    if not projection_info:
        return None, None, None

    proj_def = projection_info.get("proj_def")
    where_attrs = projection_info.get("where_attrs", {})

    if not proj_def:
        return None, None, None

    # Check projection type from proj_def string
    # If it's already WGS84 (+proj=longlat), skip caching
    if "+proj=longlat" in proj_def.lower():
        return None, None, None

    # For all other projections (merc, stere, aeqd, lcc, etc.), use them directly
    # even if the WGS84 corners appear "regular" (e.g., SHMU's custom Mercator)

    try:
        height, width = data_shape

        # Get corner coordinates
        ul_lon = float(where_attrs.get("UL_lon", 0))
        ul_lat = float(where_attrs.get("UL_lat", 0))
        ur_lon = float(where_attrs.get("UR_lon", 0))
        ur_lat = float(where_attrs.get("UR_lat", 0))
        ll_lon = float(where_attrs.get("LL_lon", 0))
        ll_lat = float(where_attrs.get("LL_lat", 0))

        native_crs = CRS.from_string(proj_def)

        # Transform WGS84 corners to native projection coordinates
        transformer = Transformer.from_crs(PROJ4_WGS84, proj_def, always_xy=True)
        ul_x, ul_y = transformer.transform(ul_lon, ul_lat)
        ur_x, ur_y = transformer.transform(ur_lon, ur_lat)
        ll_x, ll_y = transformer.transform(ll_lon, ll_lat)

        # Calculate pixel size from corners (ODIM_H5 corners are pixel centers)
        pixel_width = (ur_x - ul_x) / (width - 1)
        pixel_height = (ul_y - ll_y) / (height - 1)

        # Native transform (pixel edge, not center)
        native_transform = Affine(
            pixel_width,
            0,
            ul_x - pixel_width / 2,
            0,
            -pixel_height,
            ul_y + pixel_height / 2,
        )

        # Native bounds (pixel edges)
        native_left = ul_x - pixel_width / 2
        native_right = ur_x + pixel_width / 2
        native_top = ul_y + pixel_height / 2
        native_bottom = ll_y - pixel_height / 2

        native_bounds = (native_left, native_bottom, native_right, native_top)

        return native_crs, native_transform, native_bounds

    except Exception as e:
        logger.warning(f"Failed to build native params from projection_info: {e}")
        return None, None, None
