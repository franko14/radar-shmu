#!/usr/bin/env python3
"""
Tests for CLI helper functions.

Tests the shared CLI helpers used by both fetch and composite commands.
"""

import pytest
from argparse import Namespace
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from imeteo_radar.utils.cli_helpers import init_cache_from_args, output_exists


class TestInitCacheFromArgs:
    """Tests for init_cache_from_args function."""

    def test_returns_none_when_no_cache_flag_set(self):
        """Should return None when --no-cache is set."""
        args = Namespace(no_cache=True)
        result = init_cache_from_args(args, upload_enabled=True)
        assert result is None

    def test_initializes_cache_with_defaults(self):
        """Should initialize cache with default settings."""
        args = Namespace(
            no_cache=False,
            cache_dir=Path("/tmp/test-cache"),
            cache_ttl=30,
            no_cache_upload=False,
            clear_cache=False,
        )
        with patch("imeteo_radar.utils.processed_cache.ProcessedDataCache") as mock_cache_class:
            mock_cache = MagicMock()
            mock_cache_class.return_value = mock_cache

            result = init_cache_from_args(args, upload_enabled=True)

            mock_cache_class.assert_called_once_with(
                local_dir=Path("/tmp/test-cache"),
                ttl_minutes=30,
                s3_enabled=True,
            )
            mock_cache.cleanup_expired.assert_called_once()
            assert result == mock_cache

    def test_disables_s3_when_upload_disabled(self):
        """Should disable S3 when upload_enabled is False."""
        args = Namespace(
            no_cache=False,
            cache_dir=Path("/tmp/test-cache"),
            cache_ttl=60,
            no_cache_upload=False,
            clear_cache=False,
        )
        with patch("imeteo_radar.utils.processed_cache.ProcessedDataCache") as mock_cache_class:
            mock_cache = MagicMock()
            mock_cache_class.return_value = mock_cache

            init_cache_from_args(args, upload_enabled=False)

            mock_cache_class.assert_called_once_with(
                local_dir=Path("/tmp/test-cache"),
                ttl_minutes=60,
                s3_enabled=False,
            )

    def test_disables_s3_when_no_cache_upload_flag_set(self):
        """Should disable S3 when --no-cache-upload is set."""
        args = Namespace(
            no_cache=False,
            cache_dir=Path("/tmp/test-cache"),
            cache_ttl=60,
            no_cache_upload=True,
            clear_cache=False,
        )
        with patch("imeteo_radar.utils.processed_cache.ProcessedDataCache") as mock_cache_class:
            mock_cache = MagicMock()
            mock_cache_class.return_value = mock_cache

            init_cache_from_args(args, upload_enabled=True)

            mock_cache_class.assert_called_once_with(
                local_dir=Path("/tmp/test-cache"),
                ttl_minutes=60,
                s3_enabled=False,
            )

    def test_clears_cache_when_clear_cache_flag_set(self):
        """Should clear cache when --clear-cache is set."""
        args = Namespace(
            no_cache=False,
            cache_dir=Path("/tmp/test-cache"),
            cache_ttl=60,
            no_cache_upload=False,
            clear_cache=True,
        )
        with patch("imeteo_radar.utils.processed_cache.ProcessedDataCache") as mock_cache_class:
            mock_cache = MagicMock()
            mock_cache.clear.return_value = 5
            mock_cache_class.return_value = mock_cache

            init_cache_from_args(args, upload_enabled=True)

            mock_cache.clear.assert_called_once()

    def test_uses_default_values_when_args_missing(self):
        """Should use default values when args attributes are missing."""
        args = Namespace(no_cache=False)  # Minimal args
        with patch("imeteo_radar.utils.processed_cache.ProcessedDataCache") as mock_cache_class:
            mock_cache = MagicMock()
            mock_cache_class.return_value = mock_cache

            init_cache_from_args(args, upload_enabled=True)

            mock_cache_class.assert_called_once_with(
                local_dir=Path("/tmp/iradar-data"),
                ttl_minutes=60,
                s3_enabled=True,
            )


class TestOutputExists:
    """Tests for output_exists function."""

    def test_returns_true_when_local_file_exists(self, tmp_path):
        """Should return True when local file exists."""
        output_path = tmp_path / "test.png"
        output_path.touch()

        result = output_exists(output_path, "dwd", "test.png", None)

        assert result is True

    def test_returns_false_when_local_missing_and_no_uploader(self, tmp_path):
        """Should return False when local file missing and no uploader."""
        output_path = tmp_path / "missing.png"

        result = output_exists(output_path, "dwd", "missing.png", None)

        assert result is False

    def test_checks_s3_when_local_missing(self, tmp_path):
        """Should check S3 when local file missing."""
        output_path = tmp_path / "missing.png"
        mock_uploader = Mock()
        mock_uploader.file_exists.return_value = True

        result = output_exists(output_path, "dwd", "missing.png", mock_uploader)

        assert result is True
        mock_uploader.file_exists.assert_called_once_with("dwd", "missing.png")

    def test_returns_false_when_both_local_and_s3_missing(self, tmp_path):
        """Should return False when file missing from both local and S3."""
        output_path = tmp_path / "missing.png"
        mock_uploader = Mock()
        mock_uploader.file_exists.return_value = False

        result = output_exists(output_path, "dwd", "missing.png", mock_uploader)

        assert result is False
        mock_uploader.file_exists.assert_called_once_with("dwd", "missing.png")

    def test_returns_false_when_s3_check_fails(self, tmp_path):
        """Should return False and not raise when S3 check fails."""
        output_path = tmp_path / "missing.png"
        mock_uploader = Mock()
        mock_uploader.file_exists.side_effect = Exception("S3 error")

        result = output_exists(output_path, "dwd", "missing.png", mock_uploader)

        assert result is False

    def test_skips_s3_check_when_local_exists(self, tmp_path):
        """Should not check S3 when local file exists."""
        output_path = tmp_path / "test.png"
        output_path.touch()
        mock_uploader = Mock()

        result = output_exists(output_path, "dwd", "test.png", mock_uploader)

        assert result is True
        mock_uploader.file_exists.assert_not_called()

    def test_handles_composite_source(self, tmp_path):
        """Should work with composite source name."""
        output_path = tmp_path / "1738123400.png"
        mock_uploader = Mock()
        mock_uploader.file_exists.return_value = True

        result = output_exists(output_path, "composite", "1738123400.png", mock_uploader)

        assert result is True
        mock_uploader.file_exists.assert_called_once_with("composite", "1738123400.png")
