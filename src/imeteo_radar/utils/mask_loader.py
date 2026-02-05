#!/usr/bin/env python3
"""
Mask Loader - Centralized coverage mask loading with S3 fallback.

Provides S3-first caching for coverage_mask.png files:
1. Check local cache first
2. If not found, try to download from S3
3. If not in S3, return False (caller should generate and save)

This ensures fresh containers automatically load mask data from S3
without needing to regenerate coverage masks from radar data.
"""

from pathlib import Path

from ..core.logging import get_logger

logger = get_logger(__name__)

# Canonical local directory for mask files
MASK_LOCAL_DIR = Path("/tmp/iradar-data/mask")

# S3 prefix for mask files
MASK_S3_PREFIX = "iradar-data/mask"


def _get_uploader():
    """Get cached SpacesUploader instance if configured, None otherwise."""
    from .spaces_uploader import get_uploader_if_configured

    return get_uploader_if_configured()


def get_mask_path(source_name: str) -> Path:
    """Get local path for coverage_mask.png.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu', 'composite')

    Returns:
        Path to coverage_mask.png file
    """
    return MASK_LOCAL_DIR / source_name / "coverage_mask.png"


def ensure_mask_exists(source_name: str) -> bool:
    """Ensure mask is available (download from S3 if needed).

    This checks if the coverage mask exists locally, and if not,
    attempts to download it from S3.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu', 'composite')

    Returns:
        True if mask exists (local or downloaded from S3), False otherwise
    """
    local_path = get_mask_path(source_name)

    # Already exists locally
    if local_path.exists():
        logger.debug(f"Mask exists locally: {source_name}")
        return True

    # Try to download from S3
    uploader = _get_uploader()
    if uploader:
        s3_key = f"{MASK_S3_PREFIX}/{source_name}/coverage_mask.png"
        if uploader.download_metadata(s3_key, local_path):
            logger.info(f"Downloaded mask from S3: {source_name}")
            return True

    logger.debug(f"Mask not found for {source_name}")
    return False


def upload_mask_to_s3(source_name: str) -> bool:
    """Upload local mask to S3.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu', 'composite')

    Returns:
        True if upload successful, False otherwise
    """
    local_path = get_mask_path(source_name)

    if not local_path.exists():
        logger.debug(f"No local mask to upload for {source_name}")
        return False

    uploader = _get_uploader()
    if not uploader:
        logger.debug("S3 not configured, skipping mask upload")
        return False

    s3_key = f"{MASK_S3_PREFIX}/{source_name}/coverage_mask.png"
    result = uploader.upload_metadata(local_path, s3_key, content_type="image/png")

    if result:
        logger.info(f"Uploaded mask to S3: {source_name}")
        return True
    return False


def mask_exists_in_s3(source_name: str) -> bool:
    """Check if mask exists in S3.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu', 'composite')

    Returns:
        True if mask exists in S3, False otherwise
    """
    uploader = _get_uploader()
    if not uploader:
        return False

    s3_key = f"{MASK_S3_PREFIX}/{source_name}/coverage_mask.png"
    return uploader.metadata_exists(s3_key)
