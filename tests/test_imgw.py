#!/usr/bin/env python3
"""
Unit tests for IMGW (Polish Institute of Meteorology and Water Management) Radar Source

Tests follow TDD methodology - written before implementation.
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import h5py
import numpy as np
import pytest
import pytz


class TestIMGWRadarSourceInitialization:
    """Test IMGW source initialization"""

    def test_imgw_source_initializes_with_correct_name(self):
        """Test that IMGW source initializes with name 'imgw'"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        assert source.name == "imgw"

    def test_imgw_source_has_correct_base_url(self):
        """Test that IMGW source has correct API and download URLs"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        assert "danepubliczne.imgw.pl" in source.api_url
        assert "danepubliczne.imgw.pl" in source.download_base_url
        assert "HVD" in source.download_base_url

    def test_imgw_source_has_temp_files_dict(self):
        """Test that IMGW source initializes with empty temp_files dict"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        assert isinstance(source.temp_files, dict)
        assert len(source.temp_files) == 0

    def test_imgw_source_has_product_mapping(self):
        """Test that IMGW source has product mapping with cmax"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        assert hasattr(source, "product_mapping")
        assert "cmax" in source.product_mapping


class TestIMGWProducts:
    """Test IMGW product-related methods"""

    def test_get_available_products_returns_list(self):
        """Test that get_available_products returns a list"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        products = source.get_available_products()
        assert isinstance(products, list)

    def test_get_available_products_includes_cmax(self):
        """Test that available products include cmax"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        products = source.get_available_products()
        assert "cmax" in products

    def test_get_product_metadata_returns_dict(self):
        """Test that get_product_metadata returns a dictionary"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        metadata = source.get_product_metadata("cmax")
        assert isinstance(metadata, dict)

    def test_get_product_metadata_includes_required_fields(self):
        """Test that product metadata includes required fields"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        metadata = source.get_product_metadata("cmax")
        assert "product" in metadata
        assert "source" in metadata
        assert metadata["source"] == "imgw"


class TestIMGWTimestamps:
    """Test timestamp generation using utility module (for IMGW YYYYMMDDHHMMSS format)"""

    def test_generate_timestamps_returns_list(self):
        """Test that generate_timestamp_candidates returns a list"""
        from imeteo_radar.utils.timestamps import (
            TimestampFormat,
            generate_timestamp_candidates,
        )

        timestamps = generate_timestamp_candidates(
            count=3, interval_minutes=5, delay_minutes=10, format_str=TimestampFormat.FULL
        )
        assert isinstance(timestamps, list)

    def test_generate_timestamps_returns_correct_count(self):
        """Test that generate_timestamp_candidates returns expected number of timestamps"""
        from imeteo_radar.utils.timestamps import (
            TimestampFormat,
            generate_timestamp_candidates,
        )

        timestamps = generate_timestamp_candidates(
            count=3, interval_minutes=5, delay_minutes=10, format_str=TimestampFormat.FULL
        )
        # Should return at least count to account for missing data
        assert len(timestamps) >= 3

    def test_generate_timestamps_format_is_14_digits(self):
        """Test that timestamps are 14 digits (YYYYMMDDHHMMSS)"""
        from imeteo_radar.utils.timestamps import (
            TimestampFormat,
            generate_timestamp_candidates,
        )

        timestamps = generate_timestamp_candidates(
            count=3, interval_minutes=5, delay_minutes=10, format_str=TimestampFormat.FULL
        )
        for ts in timestamps:
            assert len(ts) == 14
            assert ts.isdigit()

    def test_generate_timestamps_are_5_minute_intervals(self):
        """Test that timestamps are at 5-minute intervals"""
        from imeteo_radar.utils.timestamps import (
            TimestampFormat,
            generate_timestamp_candidates,
        )

        timestamps = generate_timestamp_candidates(
            count=5, interval_minutes=5, delay_minutes=10, format_str=TimestampFormat.FULL
        )
        for ts in timestamps:
            # Extract minutes
            minutes = int(ts[10:12])
            assert minutes % 5 == 0

    def test_generate_timestamps_are_unique(self):
        """Test that generated timestamps are unique"""
        from imeteo_radar.utils.timestamps import (
            TimestampFormat,
            generate_timestamp_candidates,
        )

        timestamps = generate_timestamp_candidates(
            count=10, interval_minutes=5, delay_minutes=10, format_str=TimestampFormat.FULL
        )
        assert len(timestamps) == len(set(timestamps))


class TestIMGWUrlGeneration:
    """Test IMGW URL generation"""

    def test_get_product_url_contains_base_url(self):
        """Test that generated URL contains base URL"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        url = source._get_product_url("20250127120000", "cmax")
        assert "danepubliczne.imgw.pl" in url

    def test_get_product_url_contains_product_directory(self):
        """Test that generated URL contains product directory"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        url = source._get_product_url("20250127120000", "cmax")
        assert "HVD_COMPO_CMAX_250" in url

    def test_get_product_url_contains_timestamp(self):
        """Test that generated URL contains timestamp"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        url = source._get_product_url("20250127120000", "cmax")
        assert "20250127120000" in url

    def test_get_product_url_ends_with_h5(self):
        """Test that generated URL ends with .h5"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        url = source._get_product_url("20250127120000", "cmax")
        assert url.endswith(".h5")

    def test_get_product_url_has_dbz_suffix(self):
        """Test that generated URL contains 00dBZ suffix pattern"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        url = source._get_product_url("20250127120000", "cmax")
        assert "00dBZ" in url

    def test_get_product_url_raises_for_unknown_product(self):
        """Test that _get_product_url raises for unknown product"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        with pytest.raises(ValueError):
            source._get_product_url("20250127120000", "unknown_product")


class TestIMGWAvailability:
    """Test IMGW timestamp availability checking via HEAD requests"""

    @patch("requests.head")
    def test_check_timestamp_availability_returns_true_when_file_exists(self, mock_head):
        """Test that availability check returns True when HEAD request succeeds with non-HTML content"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        # Mock successful HEAD response (non-HTML content type indicates real file)
        mock_head.return_value.status_code = 200
        mock_head.return_value.headers = {"Content-Type": "application/octet-stream"}

        source = IMGWRadarSource()
        result = source._check_timestamp_availability("20250127120000", "cmax")
        assert result is True

    @patch("requests.head")
    def test_check_timestamp_availability_returns_false_when_file_missing(self, mock_head):
        """Test that availability check returns False when server returns HTML (error page)"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        # Mock response that returns HTML (indicates error/404 page)
        mock_head.return_value.status_code = 200
        mock_head.return_value.headers = {"Content-Type": "text/html"}

        source = IMGWRadarSource()
        result = source._check_timestamp_availability("20250127120000", "cmax")
        assert result is False

    @patch("requests.head")
    def test_check_timestamp_availability_returns_false_on_exception(self, mock_head):
        """Test that availability check returns False on exception"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        mock_head.side_effect = Exception("Network error")
        source = IMGWRadarSource()
        result = source._check_timestamp_availability("20250127120000", "cmax")
        assert result is False


class TestIMGWDownload:
    """Test IMGW file download functionality"""

    @patch("requests.get")
    def test_download_single_file_returns_dict(self, mock_get):
        """Test that _download_single_file returns a dictionary"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        mock_get.return_value.content = b"test content"
        mock_get.return_value.raise_for_status = MagicMock()

        source = IMGWRadarSource()
        result = source._download_single_file("20250127120000", "cmax")
        assert isinstance(result, dict)

    @patch("requests.get")
    def test_download_single_file_includes_required_keys(self, mock_get):
        """Test that download result includes required keys"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        mock_get.return_value.content = b"test content"
        mock_get.return_value.raise_for_status = MagicMock()

        source = IMGWRadarSource()
        result = source._download_single_file("20250127120000", "cmax")

        assert "timestamp" in result
        assert "product" in result
        assert "success" in result

    @patch("requests.get")
    def test_download_single_file_success_flag(self, mock_get):
        """Test that successful download has success=True"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        mock_get.return_value.content = b"test content"
        mock_get.return_value.raise_for_status = MagicMock()

        source = IMGWRadarSource()
        result = source._download_single_file("20250127120000", "cmax")
        assert result["success"] is True

    @patch("requests.get")
    def test_download_single_file_caches_file(self, mock_get):
        """Test that downloaded files are cached"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        mock_get.return_value.content = b"test content"
        mock_get.return_value.raise_for_status = MagicMock()

        source = IMGWRadarSource()
        result1 = source._download_single_file("20250127120000", "cmax")
        result2 = source._download_single_file("20250127120000", "cmax")

        # Second call should use cache
        assert result2.get("cached", False) is True

        # Clean up
        source.cleanup_temp_files()

    def test_download_single_file_returns_error_for_unknown_product(self):
        """Test that unknown product returns error"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source._download_single_file("20250127120000", "unknown_product")
        assert result["success"] is False
        assert "error" in result


class TestIMGWProcessing:
    """Test IMGW HDF5 file processing"""

    @pytest.fixture
    def mock_hdf5_file(self, tmp_path):
        """Create a mock HDF5 file with IMGW-like structure

        IMGW ODIM_H5 structure:
        - dataset1/data1/data: raw uint8 data
        - dataset1/what: scaling info (gain, offset, nodata, undetect, quantity, product, startdate, starttime)
        - what: global metadata (date, time, source)
        - where: corner coordinates (LL_lon, LL_lat, UR_lon, UR_lat)
        """
        file_path = tmp_path / "test_imgw.h5"

        with h5py.File(file_path, "w") as f:
            # Create data with realistic shape
            data = np.random.randint(0, 255, (500, 600), dtype=np.uint8)

            # Create dataset structure
            dataset1 = f.create_group("dataset1")
            data1 = dataset1.create_group("data1")
            data1.create_dataset("data", data=data)

            # Add dataset1/what attributes - IMGW stores scaling AND product info here
            what_dataset = dataset1.create_group("what")
            what_dataset.attrs["gain"] = 0.5
            what_dataset.attrs["offset"] = -32.0
            what_dataset.attrs["nodata"] = 255.0
            what_dataset.attrs["undetect"] = 0.0
            what_dataset.attrs["quantity"] = b"DBZH"
            what_dataset.attrs["product"] = b"COMP"
            what_dataset.attrs["startdate"] = b"20250127"
            what_dataset.attrs["starttime"] = b"120000"

            # Add global what attributes (fallback metadata)
            what_global = f.create_group("what")
            what_global.attrs["date"] = b"20250127"
            what_global.attrs["time"] = b"120000"
            what_global.attrs["source"] = b"IMGW"

            # Add where attributes (extent)
            where = f.create_group("where")
            where.attrs["LL_lon"] = 14.0
            where.attrs["LL_lat"] = 49.0
            where.attrs["UR_lon"] = 24.3
            where.attrs["UR_lat"] = 54.9

        return str(file_path)

    def test_process_to_array_returns_dict(self, mock_hdf5_file):
        """Test that process_to_array returns a dictionary"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.process_to_array(mock_hdf5_file)
        assert isinstance(result, dict)

    def test_process_to_array_includes_data_array(self, mock_hdf5_file):
        """Test that processed result includes data array"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.process_to_array(mock_hdf5_file)
        assert "data" in result
        assert isinstance(result["data"], np.ndarray)

    def test_process_to_array_includes_coordinates(self, mock_hdf5_file):
        """Test that processed result includes coordinates"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.process_to_array(mock_hdf5_file)
        assert "coordinates" in result
        assert "lons" in result["coordinates"]
        assert "lats" in result["coordinates"]

    def test_process_to_array_includes_metadata(self, mock_hdf5_file):
        """Test that processed result includes metadata"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.process_to_array(mock_hdf5_file)
        assert "metadata" in result
        assert "source" in result["metadata"]
        assert result["metadata"]["source"] == "IMGW"

    def test_process_to_array_includes_extent(self, mock_hdf5_file):
        """Test that processed result includes extent"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.process_to_array(mock_hdf5_file)
        assert "extent" in result
        assert "wgs84" in result["extent"]

    def test_process_to_array_applies_scaling(self, mock_hdf5_file):
        """Test that data scaling is applied correctly"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.process_to_array(mock_hdf5_file)
        # Data should be scaled (gain=0.5, offset=-32)
        # So values should be in dBZ range, not raw uint8 0-255
        data = result["data"]
        valid_data = data[~np.isnan(data)]
        if len(valid_data) > 0:
            # Scaled data should be in reasonable dBZ range
            assert np.min(valid_data) >= -50  # Min reasonable dBZ
            assert np.max(valid_data) <= 100  # Max reasonable dBZ


class TestIMGWExtent:
    """Test IMGW extent methods"""

    def test_get_extent_returns_dict(self):
        """Test that get_extent returns a dictionary"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        extent = source.get_extent()
        assert isinstance(extent, dict)

    def test_get_extent_includes_wgs84(self):
        """Test that extent includes WGS84 bounds"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        extent = source.get_extent()
        assert "wgs84" in extent
        assert "west" in extent["wgs84"]
        assert "east" in extent["wgs84"]
        assert "south" in extent["wgs84"]
        assert "north" in extent["wgs84"]

    def test_get_extent_covers_poland(self):
        """Test that extent covers Poland's approximate bounds"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        extent = source.get_extent()
        wgs84 = extent["wgs84"]

        # Poland approximate bounds: 14.0-24.3E, 49.0-54.9N
        assert wgs84["west"] <= 14.5  # Should include western Poland
        assert wgs84["east"] >= 24.0  # Should include eastern Poland
        assert wgs84["south"] <= 49.5  # Should include southern Poland
        assert wgs84["north"] >= 54.5  # Should include northern Poland

    def test_get_extent_includes_mercator(self):
        """Test that extent includes Mercator projection"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        extent = source.get_extent()
        assert "mercator" in extent
        assert "x_min" in extent["mercator"]
        assert "x_max" in extent["mercator"]
        assert "y_min" in extent["mercator"]
        assert "y_max" in extent["mercator"]


class TestIMGWMemoryOptimization:
    """Test IMGW memory-efficient extent extraction"""

    @pytest.fixture
    def mock_hdf5_file(self, tmp_path):
        """Create a mock HDF5 file for extent extraction"""
        file_path = tmp_path / "test_imgw_extent.h5"

        with h5py.File(file_path, "w") as f:
            # Create minimal data structure
            data = np.zeros((100, 100), dtype=np.uint8)
            dataset1 = f.create_group("dataset1")
            data1 = dataset1.create_group("data1")
            data1.create_dataset("data", data=data)

            # Add where attributes (extent)
            where = f.create_group("where")
            where.attrs["LL_lon"] = 14.0
            where.attrs["LL_lat"] = 49.0
            where.attrs["UR_lon"] = 24.3
            where.attrs["UR_lat"] = 54.9

        return str(file_path)

    def test_extract_extent_only_returns_dict(self, mock_hdf5_file):
        """Test that extract_extent_only returns a dictionary"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.extract_extent_only(mock_hdf5_file)
        assert isinstance(result, dict)

    def test_extract_extent_only_includes_extent(self, mock_hdf5_file):
        """Test that result includes extent"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.extract_extent_only(mock_hdf5_file)
        assert "extent" in result
        assert "wgs84" in result["extent"]

    def test_extract_extent_only_includes_dimensions(self, mock_hdf5_file):
        """Test that result includes dimensions"""
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = IMGWRadarSource()
        result = source.extract_extent_only(mock_hdf5_file)
        assert "dimensions" in result
        assert result["dimensions"] == (100, 100)


class TestIMGWRegistry:
    """Test IMGW integration with source registry"""

    def test_imgw_in_source_registry(self):
        """Test that imgw is registered in SOURCE_REGISTRY"""
        from imeteo_radar.config.sources import SOURCE_REGISTRY

        assert "imgw" in SOURCE_REGISTRY

    def test_imgw_registry_has_correct_class(self):
        """Test that imgw registry entry has correct class name"""
        from imeteo_radar.config.sources import SOURCE_REGISTRY

        assert SOURCE_REGISTRY["imgw"]["class_name"] == "IMGWRadarSource"

    def test_imgw_registry_has_correct_module(self):
        """Test that imgw registry entry has correct module path"""
        from imeteo_radar.config.sources import SOURCE_REGISTRY

        assert SOURCE_REGISTRY["imgw"]["module"] == "imeteo_radar.sources.imgw"

    def test_imgw_registry_has_correct_product(self):
        """Test that imgw registry entry has correct default product"""
        from imeteo_radar.config.sources import SOURCE_REGISTRY

        assert SOURCE_REGISTRY["imgw"]["product"] == "cmax"

    def test_imgw_registry_has_correct_country(self):
        """Test that imgw registry entry has correct country"""
        from imeteo_radar.config.sources import SOURCE_REGISTRY

        assert SOURCE_REGISTRY["imgw"]["country"] == "poland"

    def test_get_source_instance_returns_imgw(self):
        """Test that get_source_instance creates IMGWRadarSource"""
        from imeteo_radar.config.sources import get_source_instance
        from imeteo_radar.sources.imgw import IMGWRadarSource

        source = get_source_instance("imgw")
        assert isinstance(source, IMGWRadarSource)


class TestIMGWCLI:
    """Test IMGW CLI integration"""

    def test_imgw_in_fetch_choices(self):
        """Test that imgw is available in fetch command choices"""
        from imeteo_radar.cli import create_parser

        parser = create_parser()
        # Get fetch subparser
        fetch_action = None
        for action in parser._subparsers._actions:
            if (
                hasattr(action, "choices")
                and action.choices
                and "fetch" in action.choices
            ):
                fetch_action = action.choices["fetch"]
                break

        assert fetch_action is not None
        # Check source argument choices
        found = False
        for action in fetch_action._actions:
            if hasattr(action, "dest") and action.dest == "source":
                assert "imgw" in action.choices
                found = True
                break
        assert found, "source argument not found in fetch parser"

    def test_imgw_in_extent_choices(self):
        """Test that imgw is available in extent command choices"""
        from imeteo_radar.cli import create_parser

        parser = create_parser()
        # Get extent subparser
        extent_action = None
        for action in parser._subparsers._actions:
            if (
                hasattr(action, "choices")
                and action.choices
                and "extent" in action.choices
            ):
                extent_action = action.choices["extent"]
                break

        assert extent_action is not None
        # Check source argument choices
        found = False
        for action in extent_action._actions:
            if hasattr(action, "dest") and action.dest == "source":
                assert "imgw" in action.choices
                found = True
                break
        assert found, "source argument not found in extent parser"

    def test_imgw_in_coverage_mask_choices(self):
        """Test that imgw is available in coverage-mask command choices"""
        from imeteo_radar.cli import create_parser

        parser = create_parser()
        # Get coverage-mask subparser
        coverage_action = None
        for action in parser._subparsers._actions:
            if (
                hasattr(action, "choices")
                and action.choices
                and "coverage-mask" in action.choices
            ):
                coverage_action = action.choices["coverage-mask"]
                break

        assert coverage_action is not None
        # Check source argument choices
        found = False
        for action in coverage_action._actions:
            if hasattr(action, "dest") and action.dest == "source":
                assert "imgw" in action.choices
                found = True
                break
        assert found, "source argument not found in coverage-mask parser"


class TestIMGWPackageExport:
    """Test IMGW package-level exports"""

    def test_imgw_source_in_all(self):
        """Test that IMGWRadarSource is in __all__"""
        from imeteo_radar import __all__

        assert "IMGWRadarSource" in __all__

    def test_imgw_source_importable_from_package(self):
        """Test that IMGWRadarSource can be imported from package"""
        from imeteo_radar import IMGWRadarSource

        assert IMGWRadarSource is not None

    def test_imgw_source_is_correct_class(self):
        """Test that imported IMGWRadarSource is the correct class"""
        from imeteo_radar import IMGWRadarSource
        from imeteo_radar.core.base import RadarSource

        assert issubclass(IMGWRadarSource, RadarSource)


class TestIMGWFilterTimestamps:
    """Test timestamp filtering by time range using utility module"""

    def test_filter_timestamps_by_range_returns_list(self):
        """Test that filter_timestamps_by_range returns a list"""
        from imeteo_radar.utils.timestamps import (
            TimestampFormat,
            filter_timestamps_by_range,
        )

        start = datetime(2025, 1, 27, 10, 0, tzinfo=pytz.UTC)
        end = datetime(2025, 1, 27, 12, 0, tzinfo=pytz.UTC)
        timestamps = ["20250127100000", "20250127110000", "20250127120000"]

        result = filter_timestamps_by_range(
            timestamps, start, end, parse_format=TimestampFormat.FULL
        )
        assert isinstance(result, list)

    def test_filter_timestamps_by_range_filters_correctly(self):
        """Test that timestamps outside range are filtered out"""
        from imeteo_radar.utils.timestamps import (
            TimestampFormat,
            filter_timestamps_by_range,
        )

        start = datetime(2025, 1, 27, 10, 0, tzinfo=pytz.UTC)
        end = datetime(2025, 1, 27, 11, 0, tzinfo=pytz.UTC)
        timestamps = [
            "20250127090000",  # Before range
            "20250127100000",  # In range
            "20250127103000",  # In range
            "20250127110000",  # In range
            "20250127120000",  # After range
        ]

        result = filter_timestamps_by_range(
            timestamps, start, end, parse_format=TimestampFormat.FULL
        )
        assert "20250127090000" not in result
        assert "20250127100000" in result
        assert "20250127103000" in result
        assert "20250127110000" in result
        assert "20250127120000" not in result
