#!/usr/bin/env python3
"""
Tests for ProcessedDataCache

Tests the dual-layer caching system for processed radar data.
"""

import json
import shutil
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from imeteo_radar.utils.processed_cache import ProcessedDataCache


@pytest.fixture
def temp_cache_dir():
    """Create a temporary directory for cache tests."""
    temp_dir = tempfile.mkdtemp(prefix="radar_cache_test_")
    yield Path(temp_dir)
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_radar_data():
    """Create sample radar data for testing."""
    return {
        "data": np.random.rand(100, 100).astype(np.float32) * 50,
        "extent": {
            "wgs84": {
                "west": 13.5,
                "east": 17.5,
                "south": 45.5,
                "north": 49.5,
            }
        },
        "metadata": {
            "product": "zm",
            "source": "arso",
        },
        "timestamp": "202501281005",
        "lons": np.linspace(13.5, 17.5, 100),
        "lats": np.linspace(45.5, 49.5, 100),
    }


class TestProcessedDataCache:
    """Tests for ProcessedDataCache class."""

    def test_cache_initialization(self, temp_cache_dir):
        """Test cache initializes correctly."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=30,
            s3_enabled=False,
        )

        assert cache.local_dir == temp_cache_dir
        assert cache.ttl_minutes == 30
        assert cache.s3_enabled is False
        assert temp_cache_dir.exists()

    def test_cache_put_get_roundtrip(self, temp_cache_dir, sample_radar_data):
        """Test storing and retrieving data from cache."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Store data
        source = "arso"
        timestamp = "202501281005"
        product = "zm"

        local_path = cache.put(source, timestamp, product, sample_radar_data)

        assert local_path.exists()
        assert local_path.suffix == ".npz"

        # Retrieve data
        retrieved = cache.get(source, timestamp, product)

        assert retrieved is not None
        assert "data" in retrieved
        assert "extent" in retrieved
        np.testing.assert_array_almost_equal(
            retrieved["data"], sample_radar_data["data"], decimal=5
        )
        assert retrieved["extent"] == sample_radar_data["extent"]

    def test_cache_miss_returns_none(self, temp_cache_dir):
        """Test cache returns None for missing entries."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        result = cache.get("nonexistent", "202501281005", "zm")
        assert result is None

    def test_cache_exists(self, temp_cache_dir, sample_radar_data):
        """Test exists() method for checking cache entries."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        source = "arso"
        timestamp = "202501281005"
        product = "zm"

        # Should not exist initially
        assert cache.exists(source, timestamp, product) is False

        # Store data
        cache.put(source, timestamp, product, sample_radar_data)

        # Should exist now
        assert cache.exists(source, timestamp, product) is True

        # Different timestamp should not exist
        assert cache.exists(source, "202501281010", product) is False

    def test_cache_exists_respects_ttl(self, temp_cache_dir, sample_radar_data):
        """Test exists() returns False for expired entries."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=0,  # Immediate expiration
            s3_enabled=False,
        )

        # Store data
        cache.put("arso", "202501281005", "zm", sample_radar_data, force=True)

        # Wait for expiration
        time.sleep(0.1)

        # Should return False due to expiration
        assert cache.exists("arso", "202501281005", "zm") is False

    def test_put_skips_existing_entry(self, temp_cache_dir, sample_radar_data):
        """Test put() skips saving if entry already exists."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        source = "arso"
        timestamp = "202501281005"
        product = "zm"

        # First put should save
        path1 = cache.put(source, timestamp, product, sample_radar_data)
        assert path1 is not None
        assert path1.exists()
        mtime1 = path1.stat().st_mtime

        # Wait a bit to ensure different mtime if file is rewritten
        time.sleep(0.1)

        # Second put should skip (return existing path, not rewrite)
        path2 = cache.put(source, timestamp, product, sample_radar_data)
        assert path2 == path1
        mtime2 = path1.stat().st_mtime
        assert mtime1 == mtime2  # File should not have been rewritten

    def test_put_force_overwrites(self, temp_cache_dir, sample_radar_data):
        """Test put() with force=True overwrites existing entry."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        source = "arso"
        timestamp = "202501281005"
        product = "zm"

        # First put
        cache.put(source, timestamp, product, sample_radar_data)
        path = cache._get_local_path(source, timestamp, product)
        mtime1 = path.stat().st_mtime

        # Wait a bit
        time.sleep(0.1)

        # Second put with force should overwrite
        cache.put(source, timestamp, product, sample_radar_data, force=True)
        mtime2 = path.stat().st_mtime
        assert mtime2 > mtime1  # File should have been rewritten

    def test_cache_ttl_expiration(self, temp_cache_dir, sample_radar_data):
        """Test cache entries expire after TTL."""
        # Use very short TTL for testing
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=0,  # Immediate expiration
            s3_enabled=False,
        )

        # Store data
        cache.put("arso", "202501281005", "zm", sample_radar_data)

        # Wait a moment to ensure expiration
        time.sleep(0.1)

        # Should return None due to expiration
        result = cache.get("arso", "202501281005", "zm")
        assert result is None

    def test_get_available_timestamps(self, temp_cache_dir, sample_radar_data):
        """Test getting available timestamps from cache."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Store multiple timestamps
        timestamps = ["202501281000", "202501281005", "202501281010"]
        for ts in timestamps:
            data = sample_radar_data.copy()
            data["timestamp"] = ts
            cache.put("arso", ts, "zm", data)

        # Get available timestamps
        available = cache.get_available_timestamps("arso", "zm")

        assert len(available) == 3
        # Should be sorted newest first
        assert available[0] == "202501281010"
        assert available[1] == "202501281005"
        assert available[2] == "202501281000"

    def test_get_available_timestamps_filters_by_product(
        self, temp_cache_dir, sample_radar_data
    ):
        """Test that get_available_timestamps filters by product."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Store different products at different timestamps
        cache.put("shmu", "202501281005", "zmax", sample_radar_data)
        cache.put("shmu", "202501281010", "rr1h", sample_radar_data)

        # Filter by product
        zmax_timestamps = cache.get_available_timestamps("shmu", "zmax")
        rr1h_timestamps = cache.get_available_timestamps("shmu", "rr1h")
        all_timestamps = cache.get_available_timestamps("shmu")

        assert len(zmax_timestamps) == 1
        assert len(rr1h_timestamps) == 1
        # Without filter, we get all unique timestamps (2 different timestamps)
        assert len(all_timestamps) == 2

    def test_cleanup_expired(self, temp_cache_dir, sample_radar_data):
        """Test cleanup of expired entries."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=0,  # Immediate expiration
            s3_enabled=False,
        )

        # Store data
        cache.put("arso", "202501281005", "zm", sample_radar_data)
        cache.put("shmu", "202501281005", "zmax", sample_radar_data)

        # Wait for expiration
        time.sleep(0.1)

        # Cleanup
        removed = cache.cleanup_expired()

        assert removed == 2

        # Verify files are gone
        arso_dir = temp_cache_dir / "arso"
        shmu_dir = temp_cache_dir / "shmu"
        assert len(list(arso_dir.glob("*.npz"))) == 0
        assert len(list(shmu_dir.glob("*.npz"))) == 0

    def test_clear_all(self, temp_cache_dir, sample_radar_data):
        """Test clearing all cache entries."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Store data for multiple sources
        cache.put("arso", "202501281005", "zm", sample_radar_data)
        cache.put("shmu", "202501281005", "zmax", sample_radar_data)

        # Clear all
        removed = cache.clear()

        assert removed == 2

        # Verify cache is empty
        stats = cache.get_cache_stats()
        assert stats["total_entries"] == 0

    def test_clear_specific_source(self, temp_cache_dir, sample_radar_data):
        """Test clearing cache for a specific source."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Store data for multiple sources
        cache.put("arso", "202501281005", "zm", sample_radar_data)
        cache.put("shmu", "202501281005", "zmax", sample_radar_data)

        # Clear only ARSO
        removed = cache.clear("arso")

        assert removed == 1

        # Verify ARSO is empty but SHMU remains
        arso_data = cache.get("arso", "202501281005", "zm")
        shmu_data = cache.get("shmu", "202501281005", "zmax")

        assert arso_data is None
        assert shmu_data is not None

    def test_get_cache_stats(self, temp_cache_dir, sample_radar_data):
        """Test getting cache statistics."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Store data
        cache.put("arso", "202501281005", "zm", sample_radar_data)
        cache.put("arso", "202501281010", "zm", sample_radar_data)
        cache.put("shmu", "202501281005", "zmax", sample_radar_data)

        stats = cache.get_cache_stats()

        assert stats["total_entries"] == 3
        assert stats["total_size_mb"] > 0
        assert "arso" in stats["sources"]
        assert "shmu" in stats["sources"]
        assert stats["sources"]["arso"]["entries"] == 2
        assert stats["sources"]["shmu"]["entries"] == 1

    def test_npz_compression(self, temp_cache_dir, sample_radar_data):
        """Test that NPZ files are compressed."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Create larger data for better compression test
        large_data = sample_radar_data.copy()
        large_data["data"] = np.random.rand(500, 500).astype(np.float32) * 50

        local_path = cache.put("arso", "202501281005", "zm", large_data)

        # Check file size is reasonable (compressed should be much smaller than raw)
        file_size = local_path.stat().st_size
        raw_size = large_data["data"].nbytes

        # Compressed should be significantly smaller
        assert file_size < raw_size

    def test_metadata_json_structure(self, temp_cache_dir, sample_radar_data):
        """Test that metadata JSON has correct structure."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        local_path = cache.put("arso", "202501281005", "zm", sample_radar_data)
        metadata_path = local_path.with_suffix(".json")

        assert metadata_path.exists()

        with open(metadata_path) as f:
            metadata = json.load(f)

        assert "source" in metadata
        assert "timestamp" in metadata
        assert "product" in metadata
        assert "extent" in metadata
        assert "dimensions" in metadata
        assert "cached_at" in metadata
        assert "cached_at_iso" in metadata

        assert metadata["source"] == "arso"
        assert metadata["timestamp"] == "202501281005"
        assert metadata["product"] == "zm"

    def test_timestamp_normalization(self, temp_cache_dir, sample_radar_data):
        """Test that timestamps are normalized to 12 characters."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Store with 14-char timestamp
        cache.put("arso", "20250128100500", "zm", sample_radar_data)

        # Should be accessible with 12-char timestamp
        result = cache.get("arso", "202501281005", "zm")
        assert result is not None

    def test_lons_lats_preservation(self, temp_cache_dir, sample_radar_data):
        """Test that lons and lats arrays are preserved in coordinates dict."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Sample data has lons/lats at top level, but cache expects coordinates dict
        # Update sample to match expected format
        sample_with_coords = sample_radar_data.copy()
        sample_with_coords["coordinates"] = {
            "lons": sample_radar_data["lons"],
            "lats": sample_radar_data["lats"],
        }

        cache.put("arso", "202501281005", "zm", sample_with_coords)
        retrieved = cache.get("arso", "202501281005", "zm")

        assert "coordinates" in retrieved
        assert retrieved["coordinates"] is not None
        assert "lons" in retrieved["coordinates"]
        assert "lats" in retrieved["coordinates"]
        np.testing.assert_array_almost_equal(
            retrieved["coordinates"]["lons"],
            sample_with_coords["coordinates"]["lons"],
            decimal=5,
        )
        np.testing.assert_array_almost_equal(
            retrieved["coordinates"]["lats"],
            sample_with_coords["coordinates"]["lats"],
            decimal=5,
        )

    def test_data_without_lons_lats(self, temp_cache_dir):
        """Test caching data without optional lons/lats arrays."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Data without lons/lats
        data = {
            "data": np.random.rand(100, 100).astype(np.float32),
            "extent": {
                "wgs84": {"west": 13.5, "east": 17.5, "south": 45.5, "north": 49.5}
            },
            "metadata": {},
            "timestamp": "202501281005",
        }

        cache.put("arso", "202501281005", "zm", data)
        retrieved = cache.get("arso", "202501281005", "zm")

        assert retrieved is not None
        assert "data" in retrieved
        # lons/lats should not be present
        assert "lons" not in retrieved or retrieved.get("lons") is None
        assert "lats" not in retrieved or retrieved.get("lats") is None


class TestProcessedDataCacheS3:
    """Tests for S3 integration (mocked)."""

    def test_s3_disabled_by_default_when_not_configured(self, temp_cache_dir):
        """Test S3 is disabled when environment variables are not set."""
        with patch.dict("os.environ", {}, clear=True):
            cache = ProcessedDataCache(
                local_dir=temp_cache_dir,
                ttl_minutes=60,
                s3_enabled=True,  # Request S3, but it won't be available
            )

            # S3 uploader should be None when not configured
            uploader = cache._get_uploader()
            assert uploader is None

    def test_s3_upload_on_put(self, temp_cache_dir, sample_radar_data):
        """Test that put() uploads to S3 when enabled."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=True,
        )

        # Mock the uploader
        mock_uploader = MagicMock()
        mock_uploader.bucket = "test-bucket"
        cache._uploader = mock_uploader
        cache._s3_initialized = True

        cache.put("arso", "202501281005", "zm", sample_radar_data)

        # Verify S3 upload was called
        assert mock_uploader.s3_client.upload_file.call_count == 2  # NPZ + JSON

    def test_s3_download_on_cache_miss(self, temp_cache_dir):
        """Test that get() downloads from S3 on local cache miss."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=True,
        )

        # Mock the uploader with head_object raising error (file not found)
        mock_uploader = MagicMock()
        mock_uploader.bucket = "test-bucket"
        mock_uploader.s3_client.head_object.side_effect = Exception("Not found")
        cache._uploader = mock_uploader
        cache._s3_initialized = True

        result = cache.get("arso", "202501281005", "zm")

        # Should return None since S3 doesn't have it either
        assert result is None
        # head_object should have been called to check S3
        mock_uploader.s3_client.head_object.assert_called_once()


class TestCacheIntegration:
    """Integration tests for cache with composite workflow."""

    def test_cache_enables_timestamp_matching(self, temp_cache_dir, sample_radar_data):
        """Test that cache enables matching timestamps across runs."""
        cache = ProcessedDataCache(
            local_dir=temp_cache_dir,
            ttl_minutes=60,
            s3_enabled=False,
        )

        # Simulate Run 1: ARSO has 10:05, others have 10:00
        # Cache ARSO 10:05
        arso_data = sample_radar_data.copy()
        arso_data["timestamp"] = "202501281005"
        cache.put("arso", "202501281005", "zm", arso_data)

        # Simulate Run 2: Check what timestamps are available
        available = cache.get_available_timestamps("arso", "zm")
        assert "202501281005" in available

        # Now if SHMU catches up to 10:05, we can use cached ARSO
        cached_arso = cache.get("arso", "202501281005", "zm")
        assert cached_arso is not None
        np.testing.assert_array_almost_equal(
            cached_arso["data"], arso_data["data"], decimal=5
        )
