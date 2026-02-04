#!/usr/bin/env python3
"""
Shared CLI helper functions for cache and S3 operations.

Centralizes common functionality used by both fetch and composite commands
to ensure consistent behavior and reduce code duplication.
"""

from pathlib import Path
from typing import Any

from ..core.logging import get_logger

logger = get_logger(__name__)


def init_cache_from_args(args: Any, upload_enabled: bool = True) -> Any:
    """Initialize ProcessedDataCache from CLI arguments.

    Used by both fetch and composite commands to ensure consistent
    cache initialization behavior.

    Args:
        args: CLI arguments namespace with cache-related attributes:
            - no_cache: Disable caching entirely
            - cache_dir: Directory for processed data cache
            - cache_ttl: Cache TTL in minutes
            - no_cache_upload: Disable S3 cache sync
            - clear_cache: Clear cache before running
        upload_enabled: Whether S3 upload is enabled (affects s3_enabled flag)

    Returns:
        ProcessedDataCache instance or None if caching disabled
    """
    if getattr(args, "no_cache", False):
        logger.debug("Caching disabled via --no-cache")
        return None

    from .processed_cache import ProcessedDataCache

    s3_enabled = upload_enabled and not getattr(args, "no_cache_upload", False)

    cache = ProcessedDataCache(
        local_dir=getattr(args, "cache_dir", Path("/tmp/iradar-data/data")),
        ttl_minutes=getattr(args, "cache_ttl", 60),
        s3_enabled=s3_enabled,
    )

    if getattr(args, "clear_cache", False):
        cleared = cache.clear()
        logger.info(f"Cleared {cleared} cache entries")

    # Cleanup expired entries on startup
    cache.cleanup_expired()

    logger.debug(
        f"Cache initialized: dir={cache.local_dir}, "
        f"ttl={cache.ttl_minutes}min, s3={s3_enabled}"
    )

    return cache


def output_exists(output_path: Path, source: str, filename: str, uploader: Any) -> bool:
    """Check if output file already exists locally or in S3.

    Used to skip redundant processing and uploads when the output
    file already exists either locally or in S3.

    Args:
        output_path: Local path to the output file
        source: Source identifier for S3 path (e.g., 'dwd', 'composite')
        filename: Filename within the source directory (e.g., '1738123400.png')
        uploader: SpacesUploader instance or None

    Returns:
        True if file exists and should be skipped, False otherwise
    """
    # Check local first (fast)
    if output_path.exists():
        logger.debug(f"Output exists locally: {output_path}")
        return True

    # Check S3 if uploader available
    if uploader is not None:
        try:
            if uploader.file_exists(source, filename):
                logger.debug(f"Output exists in S3: {source}/{filename}")
                return True
        except Exception as e:
            logger.debug(f"S3 existence check failed: {e}")
            # Fall through - proceed with processing if S3 check fails

    return False
