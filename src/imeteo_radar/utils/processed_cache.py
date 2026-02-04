#!/usr/bin/env python3
"""
Processed Radar Data Cache

Dual-layer caching system for processed radar data arrays.
Solves the timestamp mismatch problem where fast sources (ARSO ~7-8 min latency)
get dropped from composites because slow sources (SHMU/OMSZ/IMGW ~13 min) haven't
caught up yet.

Storage Layers:
- Layer 1: Local filesystem (/tmp) - fast, ephemeral
- Layer 2: S3/DO Spaces - persistent across pod restarts (production K8s)

Storage Format:
- Local: /tmp/iradar-data/data/{source}/{source}_{product}_{timestamp}.npz
- S3: iradar-data/data/{source}/{source}_{product}_{timestamp}.npz
- NPZ file containing: data (2D float32), lons, lats arrays
- JSON metadata file alongside with extent, dimensions, cached_at timestamp
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

from ..core.logging import get_logger

logger = get_logger(__name__)


def _make_json_serializable(obj):
    """Convert numpy types to native Python types for JSON serialization.

    HDF5 attributes often contain numpy int64/float64 values that
    json.dump cannot handle. This recursively converts them.
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_json_serializable(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


class ProcessedDataCache:
    """Dual-layer cache for processed radar data arrays.

    Layer 1: Local filesystem (/tmp) - fast, ephemeral
    Layer 2: S3/DO Spaces - persistent across pod restarts

    Usage:
        cache = ProcessedDataCache()

        # Check for cached data
        cached = cache.get("arso", "202501281005", "zm")
        if cached:
            radar_data = cached
        else:
            radar_data = source.process_to_array(file_path)
            cache.put("arso", radar_data["timestamp"], "zm", radar_data)

        # Get timestamps available in cache for matching
        available = cache.get_available_timestamps("arso", "zm")
    """

    def __init__(
        self,
        local_dir: Path | None = None,
        ttl_minutes: int = 60,
        s3_enabled: bool = True,
    ):
        """Initialize the processed data cache.

        Args:
            local_dir: Local cache directory (default: /tmp/iradar-data/data)
            ttl_minutes: Time-to-live for cache entries in minutes (default: 60)
            s3_enabled: Whether to use S3/DO Spaces as secondary layer
        """
        self.local_dir = local_dir or Path("/tmp/iradar-data/data")
        self.ttl_minutes = ttl_minutes
        self.s3_enabled = s3_enabled
        self._uploader = None
        self._s3_initialized = False

        # Create local cache directory
        self.local_dir.mkdir(parents=True, exist_ok=True)

        logger.debug(
            f"ProcessedDataCache initialized: local_dir={self.local_dir}, "
            f"ttl={ttl_minutes}min, s3_enabled={s3_enabled}"
        )

    def _get_uploader(self):
        """Lazy-initialize S3 uploader."""
        if not self.s3_enabled:
            return None

        if not self._s3_initialized:
            self._s3_initialized = True
            try:
                from .spaces_uploader import SpacesUploader, is_spaces_configured

                if is_spaces_configured():
                    self._uploader = SpacesUploader()
                    logger.debug("S3 cache layer enabled")
                else:
                    logger.debug("S3 not configured, using local cache only")
            except Exception as e:
                logger.warning(f"Failed to initialize S3 for cache: {e}")

        return self._uploader

    def _get_local_path(self, source: str, timestamp: str, product: str) -> Path:
        """Get local cache file path for a given source/timestamp/product."""
        source_dir = self.local_dir / source
        source_dir.mkdir(parents=True, exist_ok=True)
        # Normalize timestamp to 12 characters (YYYYMMDDHHMM)
        ts_normalized = timestamp[:12]
        return source_dir / f"{source}_{product}_{ts_normalized}.npz"

    def _get_metadata_path(self, npz_path: Path) -> Path:
        """Get metadata JSON path for a given NPZ file."""
        return npz_path.with_suffix(".json")

    def _get_s3_key(self, source: str, timestamp: str, product: str) -> str:
        """Get S3 key for cache file."""
        ts_normalized = timestamp[:12]
        return f"iradar-data/data/{source}/{source}_{product}_{ts_normalized}.npz"

    def _get_s3_metadata_key(self, source: str, timestamp: str, product: str) -> str:
        """Get S3 key for metadata file."""
        ts_normalized = timestamp[:12]
        return f"iradar-data/data/{source}/{source}_{product}_{ts_normalized}.json"

    def _is_expired(self, metadata_path: Path) -> bool:
        """Check if a cache entry is expired based on its metadata."""
        if not metadata_path.exists():
            return True

        try:
            with open(metadata_path) as f:
                metadata = json.load(f)

            cached_at = metadata.get("cached_at", 0)
            age_minutes = (time.time() - cached_at) / 60

            return age_minutes > self.ttl_minutes
        except Exception:
            return True

    def get(self, source: str, timestamp: str, product: str) -> dict[str, Any] | None:
        """Get cached radar data for a source/timestamp/product.

        Lookup order:
        1. Check local cache (fast)
        2. If miss, check S3 and download to local (slower, but persistent)

        Args:
            source: Source identifier (e.g., 'arso', 'shmu')
            timestamp: Timestamp string (YYYYMMDDHHMM format)
            product: Product identifier (e.g., 'zm', 'zmax')

        Returns:
            Dictionary with radar data or None if not found/expired
        """
        local_path = self._get_local_path(source, timestamp, product)
        metadata_path = self._get_metadata_path(local_path)

        # Try local cache first
        if local_path.exists() and metadata_path.exists():
            if not self._is_expired(metadata_path):
                try:
                    return self._load_from_local(local_path, metadata_path)
                except Exception as e:
                    logger.warning(f"Failed to load from local cache: {e}")

        # Try S3 if local miss
        if self._get_uploader():
            downloaded = self._download_from_s3(source, timestamp, product)
            if downloaded:
                # Verify it's not expired after download
                if not self._is_expired(metadata_path):
                    try:
                        return self._load_from_local(local_path, metadata_path)
                    except Exception as e:
                        logger.warning(f"Failed to load downloaded cache: {e}")

        return None

    def _load_from_local(self, npz_path: Path, metadata_path: Path) -> dict[str, Any]:
        """Load radar data from local cache files."""
        # Load NPZ data
        with np.load(npz_path) as npz:
            data = npz["data"]
            lons = npz.get("lons")
            lats = npz.get("lats")

        # Load metadata
        with open(metadata_path) as f:
            metadata = json.load(f)

        # Reconstruct radar_data dict (matching format from source.process_to_array())
        radar_data = {
            "data": data,
            "extent": metadata.get("extent", {}),
            "metadata": metadata.get("source_metadata", {}),
            "timestamp": metadata.get("timestamp", ""),
            "dimensions": tuple(metadata.get("dimensions", list(data.shape))),
        }

        # Restore projection info (required for accurate reprojection)
        projection = metadata.get("projection")
        if projection is not None:
            radar_data["projection"] = projection

        # Reconstruct coordinates dict (required by compositor)
        if lons is not None and lats is not None:
            radar_data["coordinates"] = {"lons": lons, "lats": lats}
        else:
            # Set to None so compositor will generate from metadata
            radar_data["coordinates"] = None

        logger.debug(
            f"Cache hit: {npz_path.name}",
            extra={
                "source": metadata.get("source"),
                "timestamp": metadata.get("timestamp"),
            },
        )

        return radar_data

    def put(
        self,
        source: str,
        timestamp: str,
        product: str,
        radar_data: dict[str, Any],
        force: bool = False,
    ) -> Path | None:
        """Save processed radar data to cache.

        Saves to local first, then uploads to S3 if enabled.
        Skips saving if valid cache entry already exists (unless force=True).

        Args:
            source: Source identifier (e.g., 'arso', 'shmu')
            timestamp: Timestamp string (YYYYMMDDHHMM format)
            product: Product identifier (e.g., 'zm', 'zmax')
            radar_data: Dictionary from source.process_to_array()
            force: If True, overwrite existing cache entry

        Returns:
            Path to the local cache file, or None if skipped
        """
        local_path = self._get_local_path(source, timestamp, product)
        metadata_path = self._get_metadata_path(local_path)

        # Skip if valid cache entry already exists
        if not force and local_path.exists() and metadata_path.exists():
            if not self._is_expired(metadata_path):
                logger.debug(
                    f"Cache entry already exists: {local_path.name}",
                    extra={"source": source, "timestamp": timestamp},
                )
                return local_path

        # Save NPZ with data arrays
        data = radar_data["data"]
        save_dict = {"data": data.astype(np.float32)}

        # Extract coordinates from the nested structure (radar_data["coordinates"])
        coordinates = radar_data.get("coordinates")
        if coordinates is not None:
            if "lons" in coordinates:
                save_dict["lons"] = coordinates["lons"]
            if "lats" in coordinates:
                save_dict["lats"] = coordinates["lats"]

        np.savez_compressed(local_path, **save_dict)

        # Save metadata
        metadata = {
            "source": source,
            "timestamp": timestamp,
            "product": product,
            "extent": radar_data.get("extent", {}),
            "projection": _make_json_serializable(radar_data.get("projection")),
            "dimensions": list(data.shape),
            "source_metadata": radar_data.get("metadata", {}),
            "cached_at": time.time(),
            "cached_at_iso": datetime.utcnow().isoformat() + "Z",
        }

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(
            f"Cached: {local_path.name}",
            extra={"source": source, "timestamp": timestamp, "product": product},
        )

        # Upload to S3 if enabled
        if self._get_uploader():
            self._upload_to_s3(local_path, metadata_path, source, timestamp, product)

        return local_path

    def _upload_to_s3(
        self,
        local_path: Path,
        metadata_path: Path,
        source: str,
        timestamp: str,
        product: str,
    ):
        """Upload cache files to S3."""
        uploader = self._get_uploader()
        if not uploader:
            return

        try:
            # Upload NPZ file
            npz_key = self._get_s3_key(source, timestamp, product)
            uploader.s3_client.upload_file(
                str(local_path),
                uploader.bucket,
                npz_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )

            # Upload metadata JSON
            json_key = self._get_s3_metadata_key(source, timestamp, product)
            uploader.s3_client.upload_file(
                str(metadata_path),
                uploader.bucket,
                json_key,
                ExtraArgs={"ContentType": "application/json"},
            )

            logger.debug(f"Uploaded to S3: {npz_key}")

        except Exception as e:
            logger.warning(f"Failed to upload cache to S3: {e}")

    def _download_from_s3(
        self, source: str, timestamp: str, product: str
    ) -> Path | None:
        """Download cache files from S3 to local cache.

        Returns:
            Local path if download successful, None otherwise
        """
        uploader = self._get_uploader()
        if not uploader:
            return None

        local_path = self._get_local_path(source, timestamp, product)
        metadata_path = self._get_metadata_path(local_path)
        npz_key = self._get_s3_key(source, timestamp, product)
        json_key = self._get_s3_metadata_key(source, timestamp, product)

        try:
            # Check if file exists in S3
            uploader.s3_client.head_object(Bucket=uploader.bucket, Key=npz_key)

            # Download both files
            uploader.s3_client.download_file(uploader.bucket, npz_key, str(local_path))
            uploader.s3_client.download_file(
                uploader.bucket, json_key, str(metadata_path)
            )

            logger.debug(f"Downloaded from S3: {npz_key}")
            return local_path

        except Exception:
            # File doesn't exist in S3 or download failed
            return None

    def get_available_timestamps(
        self, source: str, product: str | None = None
    ) -> list[str]:
        """Get all available timestamps for a source from both cache layers.

        Args:
            source: Source identifier (e.g., 'arso', 'shmu')
            product: Optional product filter (e.g., 'zm', 'zmax')

        Returns:
            List of timestamp strings (YYYYMMDDHHMM format), sorted newest first
        """
        timestamps = set()

        # Get from local cache
        source_dir = self.local_dir / source
        if source_dir.exists():
            for npz_file in source_dir.glob("*.npz"):
                metadata_path = self._get_metadata_path(npz_file)
                if self._is_expired(metadata_path):
                    continue

                # Parse filename: {source}_{product}_{timestamp}.npz
                parts = npz_file.stem.split("_")
                if len(parts) >= 3:
                    file_product = parts[1]
                    file_timestamp = parts[2]

                    if product is None or file_product == product:
                        timestamps.add(file_timestamp)

        # Get from S3 (if enabled and configured)
        if self._get_uploader():
            s3_timestamps = self._get_s3_timestamps(source, product)
            timestamps.update(s3_timestamps)

        # Sort newest first
        return sorted(timestamps, reverse=True)

    def _get_s3_timestamps(self, source: str, product: str | None = None) -> set[str]:
        """Get available timestamps from S3 cache."""
        uploader = self._get_uploader()
        if not uploader:
            return set()

        timestamps = set()
        prefix = f"iradar-data/data/{source}/"

        try:
            paginator = uploader.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=uploader.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(".npz"):
                        continue

                    # Parse key: iradar/cache/{source}/{source}_{product}_{timestamp}.npz
                    filename = key.split("/")[-1]
                    parts = filename.replace(".npz", "").split("_")
                    if len(parts) >= 3:
                        file_product = parts[1]
                        file_timestamp = parts[2]

                        if product is None or file_product == product:
                            timestamps.add(file_timestamp)

        except Exception as e:
            logger.warning(f"Failed to list S3 cache: {e}")

        return timestamps

    def cleanup_expired(self) -> int:
        """Remove expired cache entries from local cache and S3.

        Returns:
            Number of entries removed (local + S3)
        """
        local_removed = self._cleanup_local_expired()
        s3_removed = self._cleanup_s3_expired()

        total_removed = local_removed + s3_removed
        if total_removed > 0:
            logger.info(
                f"Cache cleanup: removed {local_removed} local + {s3_removed} S3 entries"
            )

        return total_removed

    def _cleanup_local_expired(self) -> int:
        """Remove expired cache entries from local filesystem."""
        removed = 0

        if not self.local_dir.exists():
            return 0

        for source_dir in self.local_dir.iterdir():
            if not source_dir.is_dir():
                continue

            for npz_file in source_dir.glob("*.npz"):
                metadata_path = self._get_metadata_path(npz_file)

                if self._is_expired(metadata_path):
                    try:
                        npz_file.unlink(missing_ok=True)
                        metadata_path.unlink(missing_ok=True)
                        removed += 1
                        logger.debug(f"Removed expired local cache: {npz_file.name}")
                    except Exception as e:
                        logger.warning(f"Failed to remove expired cache: {e}")

        return removed

    def _cleanup_s3_expired(self) -> int:
        """Remove expired cache entries from S3/DO Spaces."""
        uploader = self._get_uploader()
        if not uploader:
            return 0

        removed = 0
        cutoff_time = time.time() - (self.ttl_minutes * 60)

        try:
            # List all objects in the cache prefix
            paginator = uploader.s3_client.get_paginator("list_objects_v2")
            objects_to_delete = []

            for page in paginator.paginate(
                Bucket=uploader.bucket, Prefix="iradar-data/data/"
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]

                    # Check age using S3 LastModified
                    last_modified = obj.get("LastModified")
                    if last_modified:
                        # Convert to timestamp
                        obj_timestamp = last_modified.timestamp()
                        if obj_timestamp < cutoff_time:
                            objects_to_delete.append({"Key": key})

            # Delete expired objects in batches of 1000 (S3 limit)
            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i : i + 1000]
                    uploader.s3_client.delete_objects(
                        Bucket=uploader.bucket, Delete={"Objects": batch}
                    )
                    removed += len(batch)
                    logger.debug(f"Deleted {len(batch)} expired S3 cache objects")

        except Exception as e:
            logger.warning(f"Failed to cleanup S3 cache: {e}")

        return removed

    def clear(self, source: str | None = None) -> int:
        """Clear cache entries.

        Args:
            source: Optional source to clear (clears all if None)

        Returns:
            Number of entries removed
        """
        removed = 0

        if source:
            # Clear specific source
            source_dir = self.local_dir / source
            if source_dir.exists():
                for f in source_dir.glob("*"):
                    try:
                        f.unlink()
                        removed += 1
                    except Exception:
                        pass
        else:
            # Clear all
            for source_dir in self.local_dir.iterdir():
                if source_dir.is_dir():
                    for f in source_dir.glob("*"):
                        try:
                            f.unlink()
                            removed += 1
                        except Exception:
                            pass

        # Count NPZ files only (metadata files are paired)
        removed = removed // 2

        if removed > 0:
            logger.info(f"Cleared {removed} cache entries")

        return removed

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        stats = {
            "local_dir": str(self.local_dir),
            "ttl_minutes": self.ttl_minutes,
            "s3_enabled": self.s3_enabled and self._get_uploader() is not None,
            "sources": {},
        }

        total_size = 0
        total_entries = 0

        for source_dir in self.local_dir.iterdir():
            if not source_dir.is_dir():
                continue

            source_name = source_dir.name
            source_entries = 0
            source_size = 0

            for npz_file in source_dir.glob("*.npz"):
                metadata_path = self._get_metadata_path(npz_file)
                if not self._is_expired(metadata_path):
                    source_entries += 1
                    source_size += npz_file.stat().st_size

            if source_entries > 0:
                stats["sources"][source_name] = {
                    "entries": source_entries,
                    "size_mb": round(source_size / (1024 * 1024), 2),
                }
                total_entries += source_entries
                total_size += source_size

        stats["total_entries"] = total_entries
        stats["total_size_mb"] = round(total_size / (1024 * 1024), 2)

        return stats
