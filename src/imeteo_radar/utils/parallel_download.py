#!/usr/bin/env python3
"""
Parallel download utilities for radar data sources.

Consolidates the ThreadPoolExecutor pattern that was duplicated
across all source classes (DWD, SHMU, CHMI, OMSZ, IMGW).
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

from ..core.logging import get_logger

logger = get_logger(__name__)


def execute_parallel_downloads(
    tasks: list[tuple],
    download_func: Callable,
    source_name: str,
    max_workers: int = 6,
) -> list[dict[str, Any]]:
    """Execute downloads in parallel using ThreadPoolExecutor.

    Args:
        tasks: List of (timestamp, product) tuples to download
        download_func: Function to call for each download, signature: (timestamp, product) -> dict
        source_name: Name of the source for logging
        max_workers: Maximum concurrent downloads (default: 6)

    Returns:
        List of successful download results
    """
    if not tasks:
        return []

    logger.debug(
        f"Starting parallel downloads ({len(tasks)} files, max {max_workers} concurrent)..."
    )

    downloaded_files = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_task = {
            executor.submit(download_func, timestamp, product): (timestamp, product)
            for timestamp, product in tasks
        }

        # Process completed downloads
        for future in as_completed(future_to_task):
            timestamp, product = future_to_task[future]
            try:
                result = future.result()
                if result.get("success", False):
                    downloaded_files.append(result)
                    cached_indicator = " (cached)" if result.get("cached") else ""
                    logger.debug(f"Downloaded: {product} {timestamp}{cached_indicator}")
                else:
                    error = result.get("error", "Unknown error")
                    logger.warning(f"Failed {product} {timestamp}: {error}")
            except Exception as e:
                logger.warning(f"Exception {product} {timestamp}: {e}")

    success_count = len(downloaded_files)
    fail_count = len(tasks) - success_count
    logger.info(
        f"{source_name.upper()}: Downloaded {success_count} files ({fail_count} failed)",
        extra={"source": source_name, "count": success_count},
    )

    return downloaded_files


class SessionCache:
    """Simple session-level cache for downloaded files.

    Prevents downloading the same file twice within a single session.
    Used by source classes to track temp files.
    """

    def __init__(self):
        self._cache: dict[str, str] = {}

    def get_cache_key(self, timestamp: str, product: str) -> str:
        """Generate cache key for a timestamp/product pair."""
        return f"{product}_{timestamp}"

    def is_cached(self, timestamp: str, product: str) -> bool:
        """Check if file is already downloaded in this session."""
        key = self.get_cache_key(timestamp, product)
        return key in self._cache and os.path.exists(self._cache[key])

    def get_cached_path(self, timestamp: str, product: str) -> str | None:
        """Get path to cached file if it exists."""
        key = self.get_cache_key(timestamp, product)
        if key in self._cache and os.path.exists(self._cache[key]):
            return self._cache[key]
        return None

    def add(self, timestamp: str, product: str, file_path: str) -> None:
        """Add file to session cache."""
        key = self.get_cache_key(timestamp, product)
        self._cache[key] = file_path

    def get_cached_result(
        self,
        timestamp: str,
        product: str,
        url: str = "",
    ) -> dict[str, Any] | None:
        """Get a cached result dict if file exists.

        Returns:
            Result dict with cached=True if found, None otherwise
        """
        cached_path = self.get_cached_path(timestamp, product)
        if cached_path:
            return {
                "timestamp": timestamp,
                "product": product,
                "path": cached_path,
                "url": url,
                "cached": True,
                "success": True,
            }
        return None

    def cleanup(self) -> int:
        """Remove all cached temp files.

        Returns:
            Number of files removed
        """
        removed = 0
        for path in self._cache.values():
            try:
                if os.path.exists(path):
                    Path(path).unlink()
                    removed += 1
            except Exception:
                pass
        self._cache.clear()
        return removed

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __setitem__(self, key: str, value: str) -> None:
        self._cache[key] = value

    def __getitem__(self, key: str) -> str:
        return self._cache[key]

    def keys(self):
        return self._cache.keys()

    def values(self):
        return self._cache.values()

    def items(self):
        return self._cache.items()


def create_download_result(
    timestamp: str,
    product: str,
    path: str,
    url: str = "",
    cached: bool = False,
    success: bool = True,
    error: str | None = None,
) -> dict[str, Any]:
    """Create a standardized download result dictionary.

    Args:
        timestamp: Timestamp of the downloaded data
        product: Product identifier
        path: Path to the downloaded file
        url: Source URL
        cached: Whether the file was from cache
        success: Whether the download succeeded
        error: Error message if failed

    Returns:
        Standardized result dictionary
    """
    result = {
        "timestamp": timestamp,
        "product": product,
        "path": path,
        "url": url,
        "cached": cached,
        "success": success,
    }
    if error:
        result["error"] = error
    return result


def create_error_result(
    timestamp: str,
    product: str,
    error: str,
) -> dict[str, Any]:
    """Create a standardized error result dictionary.

    Args:
        timestamp: Timestamp of the failed download
        product: Product identifier
        error: Error message

    Returns:
        Standardized error result dictionary
    """
    return {
        "timestamp": timestamp,
        "product": product,
        "path": "",
        "url": "",
        "cached": False,
        "success": False,
        "error": error,
    }
