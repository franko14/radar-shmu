#!/usr/bin/env python3
"""
Optimized Storage with Time-based Partitioning

Organizes radar data storage by time partitions for improved performance and organization.
Storage structure: {base_path}/{source}/{YYYY}/{MM}/{DD}/{HH}/files
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import h5py


class TimePartitionedStorage:
    """Time-based partitioned storage for radar data"""

    def __init__(self, base_path: Union[str, Path] = "storage"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Metadata cache for fast lookups
        self.metadata_cache: Dict[str, Any] = {}

    def get_partition_path(self, timestamp: str, source: str) -> Path:
        """
        Get time-partitioned path for a timestamp

        Args:
            timestamp: Timestamp string in YYYYMMDDHHMMSS format
            source: Source name (shmu, dwd)

        Returns:
            Path: Partitioned directory path
        """
        if len(timestamp) < 10:
            raise ValueError(f"Invalid timestamp format: {timestamp}")

        year = timestamp[:4]
        month = timestamp[4:6]
        day = timestamp[6:8]
        hour = timestamp[8:10]

        partition_path = self.base_path / source / year / month / day / hour
        return partition_path

    def store_file(
        self,
        file_path: Union[str, Path],
        timestamp: str,
        source: str,
        product: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """
        Store a file in time-partitioned storage

        Args:
            file_path: Source file path to store
            timestamp: Timestamp string
            source: Source name (shmu, dwd)
            product: Product type (zmax, cappi2km, etc.)
            metadata: Optional metadata dict

        Returns:
            Path: Stored file path
        """
        file_path = Path(file_path)
        partition_path = self.get_partition_path(timestamp, source)
        partition_path.mkdir(parents=True, exist_ok=True)

        # Create filename with product type
        filename = f"{source}_{product}_{timestamp}{file_path.suffix}"
        stored_path = partition_path / filename

        # Copy file to partitioned storage
        shutil.copy2(file_path, stored_path)

        # Store metadata if provided
        if metadata:
            metadata_path = stored_path.with_suffix(".json")
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

        return stored_path

    def get_files(
        self,
        source: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        product: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get files from time-partitioned storage with optional filtering

        Args:
            source: Source name (shmu, dwd)
            start_time: Start timestamp (YYYYMMDDHHMMSS)
            end_time: End timestamp (YYYYMMDDHHMMSS)
            product: Product type filter

        Returns:
            List[Dict]: List of file info dicts
        """
        source_path = self.base_path / source
        if not source_path.exists():
            return []

        files = []

        # Walk through time partitions
        for year_dir in sorted(source_path.iterdir()):
            if not year_dir.is_dir():
                continue

            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue

                for day_dir in sorted(month_dir.iterdir()):
                    if not day_dir.is_dir():
                        continue

                    for hour_dir in sorted(day_dir.iterdir()):
                        if not hour_dir.is_dir():
                            continue

                        # Get all files in this hour partition
                        for file_path in hour_dir.iterdir():
                            if file_path.suffix in [".hdf", ".h5", ".hd5", ".nc"]:
                                file_info = self._parse_file_info(file_path)

                                # Apply filters
                                if start_time and file_info["timestamp"] < start_time:
                                    continue
                                if end_time and file_info["timestamp"] > end_time:
                                    continue
                                if product and file_info["product"] != product:
                                    continue

                                files.append(file_info)

        return sorted(files, key=lambda x: x["timestamp"])

    def _parse_file_info(self, file_path: Path) -> Dict[str, Any]:
        """Parse file information from path and metadata"""
        filename = file_path.stem
        parts = filename.split("_")

        file_info = {
            "path": str(file_path),
            "filename": file_path.name,
            "size": file_path.stat().st_size,
            "modified": datetime.fromtimestamp(
                file_path.stat().st_mtime, tz=timezone.utc
            ).isoformat(),
        }

        # Parse from filename: source_product_timestamp
        if len(parts) >= 3:
            file_info["source"] = parts[0]
            file_info["product"] = parts[1]
            file_info["timestamp"] = parts[2]
        else:
            # Fallback parsing
            file_info["source"] = "unknown"
            file_info["product"] = "unknown"
            file_info["timestamp"] = "00000000000000"

        # Load metadata if available
        metadata_path = file_path.with_suffix(".json")
        if metadata_path.exists():
            try:
                with open(metadata_path, "r") as f:
                    file_info["metadata"] = json.load(f)
            except:
                pass

        return file_info

    def get_latest_files(
        self, source: str, count: int = 10, product: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get latest files from storage

        Args:
            source: Source name
            count: Number of files to return
            product: Product type filter

        Returns:
            List[Dict]: Latest files sorted by timestamp (newest first)
        """
        all_files = self.get_files(source, product=product)
        return sorted(all_files, key=lambda x: x["timestamp"], reverse=True)[:count]

    def cleanup_old_files(self, source: str, keep_days: int = 30) -> int:
        """
        Clean up old files beyond keep_days

        Args:
            source: Source name
            keep_days: Number of days to keep

        Returns:
            int: Number of files deleted
        """
        from datetime import timedelta

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=keep_days)
        cutoff_timestamp = cutoff_time.strftime("%Y%m%d%H%M%S")

        files = self.get_files(source, end_time=cutoff_timestamp)
        deleted_count = 0

        for file_info in files:
            file_path = Path(file_info["path"])
            metadata_path = file_path.with_suffix(".json")

            # Delete file and metadata
            try:
                file_path.unlink()
                deleted_count += 1
                if metadata_path.exists():
                    metadata_path.unlink()
            except:
                pass

        # Clean up empty directories
        self._cleanup_empty_dirs(self.base_path / source)

        return deleted_count

    def _cleanup_empty_dirs(self, path: Path):
        """Recursively remove empty directories"""
        if not path.exists() or not path.is_dir():
            return

        # Clean up subdirectories first
        for subdir in path.iterdir():
            if subdir.is_dir():
                self._cleanup_empty_dirs(subdir)

        # Remove directory if empty
        try:
            if path.exists() and not any(path.iterdir()):
                path.rmdir()
        except:
            pass

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        stats = {"total_size": 0, "total_files": 0, "sources": {}}

        for source_dir in self.base_path.iterdir():
            if not source_dir.is_dir():
                continue

            source_name = source_dir.name
            source_stats = {
                "size": 0,
                "files": 0,
                "products": {},
                "date_range": {"earliest": None, "latest": None},
            }

            files = self.get_files(source_name)
            for file_info in files:
                file_path = Path(file_info["path"])
                if file_path.exists():
                    size = file_path.stat().st_size
                    source_stats["size"] += size
                    source_stats["files"] += 1

                    product = file_info.get("product", "unknown")
                    if product not in source_stats["products"]:
                        source_stats["products"][product] = {"files": 0, "size": 0}
                    source_stats["products"][product]["files"] += 1
                    source_stats["products"][product]["size"] += size

                    timestamp = file_info["timestamp"]
                    if (
                        not source_stats["date_range"]["earliest"]
                        or timestamp < source_stats["date_range"]["earliest"]
                    ):
                        source_stats["date_range"]["earliest"] = timestamp
                    if (
                        not source_stats["date_range"]["latest"]
                        or timestamp > source_stats["date_range"]["latest"]
                    ):
                        source_stats["date_range"]["latest"] = timestamp

            stats["sources"][source_name] = source_stats
            stats["total_size"] += source_stats["size"]
            stats["total_files"] += source_stats["files"]

        return stats

    def migrate_existing_data(self, old_cache_dir: Union[str, Path], source: str):
        """
        Migrate existing data from old cache directory to time-partitioned storage

        Args:
            old_cache_dir: Path to old cache directory
            source: Source name
        """
        old_cache = Path(old_cache_dir)
        if not old_cache.exists():
            return

        migrated_count = 0

        for file_path in old_cache.glob("*.hdf"):
            try:
                # Parse timestamp from filename
                timestamp = self._extract_timestamp_from_filename(file_path.name)
                if not timestamp:
                    continue

                # Determine product type
                product = self._extract_product_from_filename(file_path.name)
                if not product:
                    product = "unknown"

                # Store in partitioned structure
                stored_path = self.store_file(file_path, timestamp, source, product)
                print(f"✅ Migrated: {file_path.name} -> {stored_path}")
                migrated_count += 1

            except Exception as e:
                print(f"❌ Failed to migrate {file_path.name}: {e}")

        print(f"📦 Migrated {migrated_count} files from {old_cache_dir}")

    def _extract_timestamp_from_filename(self, filename: str) -> Optional[str]:
        """Extract timestamp from filename"""
        # Common patterns: T_PABV22_C_LZIB_20250909081000.hdf
        import re

        patterns = [
            r"(\d{14})",  # 14 digits
            r"(\d{12})",  # 12 digits (add 00 for seconds)
            r"(\d{8})",  # 8 digits (add 0000 for time)
        ]

        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                timestamp = match.group(1)
                if len(timestamp) == 8:
                    timestamp += "0000"  # Add HHMM
                elif len(timestamp) == 12:
                    timestamp += "00"  # Add SS
                return timestamp

        return None

    def _extract_product_from_filename(self, filename: str) -> Optional[str]:
        """Extract product type from filename"""
        # Map filename patterns to product types
        product_patterns = {
            "PABV": "zmax",
            "PANV": "cappi2km",
            "PADV": "etop",
            "PASV": "pac01",
        }

        for pattern, product in product_patterns.items():
            if pattern in filename:
                return product

        return None
