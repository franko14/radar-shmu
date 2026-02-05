#!/usr/bin/env python3
"""
Extent Loader - Centralized extent index loading with S3 fallback.

Provides S3-first caching for extent_index.json files:
1. Check local cache first
2. If not found, try to download from S3
3. If not in S3, return None (caller should compute and save)

This ensures fresh containers automatically load extent data from S3
without needing to recompute projections.
"""

import json
from pathlib import Path

from ..core.logging import get_logger

logger = get_logger(__name__)

# Canonical local directory for extent files
EXTENT_LOCAL_DIR = Path("/tmp/iradar-data/extent")

# S3 prefix for extent files
EXTENT_S3_PREFIX = "iradar-data/extent"


def _get_uploader():
    """Get cached SpacesUploader instance if configured, None otherwise."""
    from .spaces_uploader import get_uploader_if_configured

    return get_uploader_if_configured()


def get_extent_path(source_name: str) -> Path:
    """Get local path for extent_index.json.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu', 'composite')

    Returns:
        Path to extent_index.json file
    """
    return EXTENT_LOCAL_DIR / source_name / "extent_index.json"


def load_extent_index(source_name: str) -> dict | None:
    """Load extent index with S3 fallback.

    Loading order:
    1. Local cache (/tmp/iradar-data/extent/{source}/extent_index.json)
    2. S3 (iradar-data/extent/{source}/extent_index.json)
    3. Returns None if not found anywhere

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu', 'composite')

    Returns:
        Parsed extent_index.json data or None if not found
    """
    local_path = get_extent_path(source_name)

    # 1. Check local cache first
    if local_path.exists():
        try:
            with open(local_path) as f:
                data = json.load(f)
            logger.debug(f"Loaded extent from local: {source_name}")
            return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load local extent for {source_name}: {e}")

    # 2. Try to download from S3
    uploader = _get_uploader()
    if uploader:
        s3_key = f"{EXTENT_S3_PREFIX}/{source_name}/extent_index.json"
        if uploader.download_metadata(s3_key, local_path):
            try:
                with open(local_path) as f:
                    data = json.load(f)
                logger.info(f"Downloaded extent from S3: {source_name}")
                return data
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Failed to parse downloaded extent for {source_name}: {e}")

    # 3. Not found anywhere
    return None


def save_extent_index(
    source_name: str,
    data: dict,
    force: bool = False,
    upload_to_s3: bool = True,
) -> bool:
    """Save extent index locally and upload to S3.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu', 'composite')
        data: Extent data to save (should contain 'metadata' and 'wgs84' keys)
        force: Overwrite existing file even if present
        upload_to_s3: Whether to upload to S3 after saving locally

    Returns:
        True if saved successfully, False otherwise
    """
    local_path = get_extent_path(source_name)

    # Skip if file exists and not forced
    if local_path.exists() and not force:
        logger.debug(f"Extent already exists for {source_name}, skipping save")
        return False

    try:
        # Save locally
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved extent to: {local_path}")

        # Upload to S3
        if upload_to_s3:
            uploader = _get_uploader()
            if uploader:
                s3_key = f"{EXTENT_S3_PREFIX}/{source_name}/extent_index.json"
                uploader.upload_metadata(
                    local_path, s3_key, content_type="application/json"
                )

        return True

    except Exception as e:
        logger.error(f"Failed to save extent for {source_name}: {e}")
        return False


def ensure_extent_exists(source_name: str) -> bool:
    """Ensure extent is available (download from S3 if needed).

    This is a quick check that doesn't parse the file - just ensures
    it exists locally (downloading from S3 if necessary).

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu', 'composite')

    Returns:
        True if extent exists (local or downloaded from S3), False otherwise
    """
    local_path = get_extent_path(source_name)

    # Already exists locally
    if local_path.exists():
        return True

    # Try to download from S3
    uploader = _get_uploader()
    if uploader:
        s3_key = f"{EXTENT_S3_PREFIX}/{source_name}/extent_index.json"
        if uploader.download_metadata(s3_key, local_path):
            return True

    return False


def get_wgs84_from_extent(extent_data: dict) -> dict | None:
    """Extract WGS84 bounds from extent_index.json data.

    Handles all known formats:
    - Canonical: top-level 'wgs84' key
    - Legacy composite: nested under 'extent.wgs84'
    - Legacy fetch: nested under 'source.extent' (with west/east/south/north)

    Args:
        extent_data: Parsed extent_index.json data

    Returns:
        Dictionary with west, east, south, north or None
    """
    # Canonical format: top-level "wgs84" key
    if "wgs84" in extent_data:
        return extent_data["wgs84"]

    # Legacy composite format: nested under "extent.wgs84"
    extent = extent_data.get("extent", {})
    if "wgs84" in extent:
        return extent["wgs84"]

    # Legacy fetch format: nested under "source.extent"
    source = extent_data.get("source", {})
    source_extent = source.get("extent", {})
    if "west" in source_extent:
        return source_extent

    return None
