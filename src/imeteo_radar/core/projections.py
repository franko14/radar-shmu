#!/usr/bin/env python3
"""
Centralized Projection Constants and CRS Utilities

Single source of truth for PROJ4 strings and CRS objects used across
the codebase. Uses thread-safe caching via functools.lru_cache.
"""

import re
from functools import lru_cache

from rasterio.crs import CRS

# PROJ4 string constants - single source of truth
PROJ4_WGS84 = "+proj=longlat +datum=WGS84 +no_defs"
PROJ4_WEB_MERCATOR = (
    "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 "
    "+x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs"
)

# Cache version - increment when grid computation logic changes
CACHE_VERSION = "v1"

# Valid source name pattern (lowercase letters, 3-10 chars)
# Matches: dwd, shmu, chmi, imgw, omsz, arso
VALID_SOURCE_PATTERN = re.compile(r"^[a-z]{2,10}$")

# Maximum allowed grid dimensions (sanity check)
MAX_GRID_DIMENSION = 10000


@lru_cache(maxsize=1)
def get_crs_wgs84() -> CRS:
    """Get WGS84 CRS (thread-safe cached singleton)."""
    return CRS.from_string(PROJ4_WGS84)


@lru_cache(maxsize=1)
def get_crs_web_mercator() -> CRS:
    """Get Web Mercator CRS (thread-safe cached singleton)."""
    return CRS.from_string(PROJ4_WEB_MERCATOR)


def validate_source_name(source_name: str) -> str:
    """
    Validate source name to prevent path traversal attacks.

    Args:
        source_name: Source identifier to validate

    Returns:
        Validated source name (lowercase)

    Raises:
        ValueError: If source name is invalid
    """
    if not source_name:
        raise ValueError("Source name cannot be empty")

    # Normalize to lowercase
    normalized = source_name.lower().strip()

    # Check pattern
    if not VALID_SOURCE_PATTERN.match(normalized):
        raise ValueError(
            f"Invalid source name: '{source_name}'. "
            f"Must be 2-10 lowercase letters only (e.g., 'dwd', 'shmu', 'chmi')"
        )

    return normalized


def validate_grid_dimensions(height: int, width: int) -> tuple[int, int]:
    """
    Validate grid dimensions to prevent resource exhaustion.

    Args:
        height: Grid height in pixels
        width: Grid width in pixels

    Returns:
        Validated (height, width) tuple

    Raises:
        ValueError: If dimensions are invalid
    """
    if height <= 0 or width <= 0:
        raise ValueError(f"Grid dimensions must be positive: {height}x{width}")

    if height > MAX_GRID_DIMENSION or width > MAX_GRID_DIMENSION:
        raise ValueError(
            f"Grid dimensions exceed maximum ({MAX_GRID_DIMENSION}): {height}x{width}"
        )

    return (height, width)
