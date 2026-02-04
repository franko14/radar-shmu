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
from rasterio.transform import Affine
from rasterio.warp import Resampling, calculate_default_transform, reproject

from ..core.logging import get_logger
from ..core.projections import (
    PROJ4_WEB_MERCATOR,
    PROJ4_WGS84,
    get_crs_web_mercator,
)

logger = get_logger(__name__)


def reproject_to_web_mercator(
    data: np.ndarray,
    native_crs: CRS,
    native_transform: Affine,
    native_bounds: tuple[float, float, float, float],
) -> tuple[np.ndarray, dict[str, float], Affine]:
    """
    Reproject data to Web Mercator using calculate_default_transform.

    Ensures PNG bounds exactly match what a GeoTIFF would produce.
    Bounds are DERIVED from the reprojected transform, not calculated
    independently.

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
        f"Reproject: {width}x{height} -> {dst_width}x{dst_height}, "
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
