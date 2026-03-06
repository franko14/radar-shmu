#!/usr/bin/env python3
"""
Shared CLI helper functions for cache, S3, and argument parsing operations.

Centralizes common functionality used by both fetch and composite commands
to ensure consistent behavior and reduce code duplication.
"""

import argparse
from pathlib import Path
from typing import Any

from ..core.logging import get_logger

logger = get_logger(__name__)


def init_uploader(args: Any) -> Any:
    """Initialize DigitalOcean Spaces uploader from CLI arguments.

    Used by both fetch and composite commands.

    Args:
        args: CLI arguments namespace with disable_upload attribute

    Returns:
        SpacesUploader instance or None if upload disabled/unconfigured
    """
    if getattr(args, "disable_upload", False):
        logger.info("Local-only mode (--disable-upload flag used)")
        return None

    from ..utils.spaces_uploader import SpacesUploader, is_spaces_configured

    if not is_spaces_configured():
        logger.warning(
            "DigitalOcean Spaces not configured (missing environment variables)"
        )
        logger.warning("Falling back to local-only mode (upload disabled)")
        return None

    try:
        uploader = SpacesUploader()
        logger.info("DigitalOcean Spaces upload enabled")
        return uploader
    except Exception as e:
        logger.warning(f"Failed to initialize Spaces uploader: {e}")
        logger.warning("Falling back to local-only mode (upload disabled)")
        return None


def add_cache_args(parser: argparse.ArgumentParser) -> None:
    """Add cache-related arguments to a parser.

    Ensures fetch and composite commands have identical cache args.

    Args:
        parser: ArgumentParser or subparser to add args to
    """
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("/tmp/iradar-data/data"),
        help="Directory for processed data cache (default: /tmp/iradar-data/data)",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=60,
        help="Cache TTL in minutes (default: 60)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable caching entirely",
    )
    parser.add_argument(
        "--no-cache-upload",
        action="store_true",
        help="Disable S3 cache sync (local cache only)",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear cache before running",
    )


def add_export_format_args(parser: argparse.ArgumentParser) -> None:
    """Add export format arguments to a parser.

    Ensures fetch and composite commands have identical format args.

    Args:
        parser: ArgumentParser or subparser to add args to
    """
    parser.add_argument(
        "--resolutions",
        type=str,
        default="full",
        help="Comma-separated resolutions to export: 'full' for native, or meters (e.g., '1000,2000'). "
        "Default: 'full'. Examples: 'full,1000', '1000,2000', 'full'.",
    )
    parser.add_argument(
        "--formats",
        type=str,
        default="png",
        help="Comma-separated output formats: 'png', 'avif', or both. Default: 'png'.",
    )
    parser.add_argument(
        "--avif-quality",
        type=int,
        default=50,
        help="AVIF quality (1-100). Lower = smaller files. Default: 50 (optimized for radar images).",
    )
    parser.add_argument(
        "--avif-speed",
        type=int,
        default=6,
        help="AVIF encoding speed (0-10). Higher = faster but lower quality. "
        "Default: 6. Use 8+ for CPU-constrained environments.",
    )
    parser.add_argument(
        "--avif-codec",
        type=str,
        choices=["auto", "aom", "svt", "rav1e"],
        default="auto",
        help="AVIF codec. 'auto' lets Pillow decide, 'svt' is faster on multi-core. Default: auto.",
    )


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
