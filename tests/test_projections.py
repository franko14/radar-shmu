#!/usr/bin/env python3
"""Tests for core.projections module - centralized projection utilities."""

import pytest
from rasterio.crs import CRS

from imeteo_radar.core.projections import (
    CACHE_VERSION,
    MAX_GRID_DIMENSION,
    PROJ4_WEB_MERCATOR,
    PROJ4_WGS84,
    VALID_SOURCE_PATTERN,
    get_crs_web_mercator,
    get_crs_wgs84,
    validate_grid_dimensions,
    validate_source_name,
)


class TestProj4Constants:
    """Test PROJ4 string constants."""

    def test_proj4_wgs84_is_longlat(self):
        """WGS84 PROJ4 string should specify longlat projection."""
        assert "+proj=longlat" in PROJ4_WGS84

    def test_proj4_wgs84_has_datum(self):
        """WGS84 PROJ4 string should specify WGS84 datum."""
        assert "+datum=WGS84" in PROJ4_WGS84

    def test_proj4_web_mercator_is_merc(self):
        """Web Mercator PROJ4 string should specify merc projection."""
        assert "+proj=merc" in PROJ4_WEB_MERCATOR

    def test_proj4_web_mercator_has_units(self):
        """Web Mercator PROJ4 string should specify meters."""
        assert "+units=m" in PROJ4_WEB_MERCATOR

    def test_proj4_web_mercator_has_spherical_params(self):
        """Web Mercator should use spherical earth (a=b)."""
        assert "+a=6378137" in PROJ4_WEB_MERCATOR
        assert "+b=6378137" in PROJ4_WEB_MERCATOR


class TestCRSFunctions:
    """Test CRS getter functions."""

    def test_get_crs_wgs84_returns_crs(self):
        """get_crs_wgs84 should return a CRS object."""
        crs = get_crs_wgs84()
        assert isinstance(crs, CRS)

    def test_get_crs_wgs84_is_geographic(self):
        """WGS84 CRS should be geographic."""
        crs = get_crs_wgs84()
        assert crs.is_geographic

    def test_get_crs_wgs84_is_cached(self):
        """Repeated calls should return the same object (cached)."""
        crs1 = get_crs_wgs84()
        crs2 = get_crs_wgs84()
        assert crs1 is crs2  # Same object, not just equal

    def test_get_crs_web_mercator_returns_crs(self):
        """get_crs_web_mercator should return a CRS object."""
        crs = get_crs_web_mercator()
        assert isinstance(crs, CRS)

    def test_get_crs_web_mercator_is_projected(self):
        """Web Mercator CRS should be projected."""
        crs = get_crs_web_mercator()
        assert crs.is_projected

    def test_get_crs_web_mercator_is_cached(self):
        """Repeated calls should return the same object (cached)."""
        crs1 = get_crs_web_mercator()
        crs2 = get_crs_web_mercator()
        assert crs1 is crs2


class TestValidateSourceName:
    """Test source name validation for path traversal prevention."""

    def test_accepts_valid_source_names(self):
        """Should accept valid lowercase source names."""
        valid_sources = ["dwd", "shmu", "chmi", "imgw", "omsz", "arso"]
        for source in valid_sources:
            result = validate_source_name(source)
            assert result == source

    def test_normalizes_to_lowercase(self):
        """Should normalize source names to lowercase."""
        assert validate_source_name("DWD") == "dwd"
        assert validate_source_name("SHMU") == "shmu"
        assert validate_source_name("Chmi") == "chmi"

    def test_strips_whitespace(self):
        """Should strip leading/trailing whitespace."""
        assert validate_source_name(" dwd ") == "dwd"
        assert validate_source_name("\tshmu\n") == "shmu"

    def test_rejects_empty_string(self):
        """Should reject empty source name."""
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_source_name("")

    def test_rejects_path_traversal_attempts(self):
        """Should reject path traversal sequences."""
        invalid_names = [
            "../etc",
            "..\\windows",
            "dwd/../etc",
            "../../tmp/evil",
            "foo/bar",
            "foo\\bar",
        ]
        for name in invalid_names:
            with pytest.raises(ValueError, match="Invalid source name"):
                validate_source_name(name)

    def test_rejects_too_short_names(self):
        """Should reject names shorter than 2 characters."""
        with pytest.raises(ValueError, match="Invalid source name"):
            validate_source_name("a")

    def test_rejects_too_long_names(self):
        """Should reject names longer than 10 characters."""
        with pytest.raises(ValueError, match="Invalid source name"):
            validate_source_name("verylongsourcename")

    def test_rejects_names_with_numbers(self):
        """Should reject names containing numbers."""
        with pytest.raises(ValueError, match="Invalid source name"):
            validate_source_name("dwd123")

    def test_rejects_names_with_special_chars(self):
        """Should reject names containing special characters."""
        invalid_names = ["dwd!", "shmu@", "chmi#", "dwd-test", "shmu_test", "test.txt"]
        for name in invalid_names:
            with pytest.raises(ValueError, match="Invalid source name"):
                validate_source_name(name)


class TestValidateGridDimensions:
    """Test grid dimension validation for resource exhaustion prevention."""

    def test_accepts_valid_dimensions(self):
        """Should accept valid dimensions within limits."""
        result = validate_grid_dimensions(1000, 2000)
        assert result == (1000, 2000)

    def test_accepts_max_dimensions(self):
        """Should accept dimensions at the maximum limit."""
        result = validate_grid_dimensions(MAX_GRID_DIMENSION, MAX_GRID_DIMENSION)
        assert result == (MAX_GRID_DIMENSION, MAX_GRID_DIMENSION)

    def test_accepts_minimum_dimensions(self):
        """Should accept minimum valid dimensions."""
        result = validate_grid_dimensions(1, 1)
        assert result == (1, 1)

    def test_rejects_zero_height(self):
        """Should reject zero height."""
        with pytest.raises(ValueError, match="must be positive"):
            validate_grid_dimensions(0, 100)

    def test_rejects_zero_width(self):
        """Should reject zero width."""
        with pytest.raises(ValueError, match="must be positive"):
            validate_grid_dimensions(100, 0)

    def test_rejects_negative_dimensions(self):
        """Should reject negative dimensions."""
        with pytest.raises(ValueError, match="must be positive"):
            validate_grid_dimensions(-100, 100)
        with pytest.raises(ValueError, match="must be positive"):
            validate_grid_dimensions(100, -100)

    def test_rejects_excessive_height(self):
        """Should reject height exceeding maximum."""
        with pytest.raises(ValueError, match="exceed maximum"):
            validate_grid_dimensions(MAX_GRID_DIMENSION + 1, 100)

    def test_rejects_excessive_width(self):
        """Should reject width exceeding maximum."""
        with pytest.raises(ValueError, match="exceed maximum"):
            validate_grid_dimensions(100, MAX_GRID_DIMENSION + 1)


class TestSourceNamePattern:
    """Test the regex pattern for source names."""

    def test_pattern_matches_valid_sources(self):
        """Pattern should match valid source names."""
        valid = ["dwd", "shmu", "chmi", "imgw", "omsz", "arso", "ab", "abcdefghij"]
        for name in valid:
            assert VALID_SOURCE_PATTERN.match(name) is not None

    def test_pattern_rejects_invalid_sources(self):
        """Pattern should reject invalid source names."""
        invalid = ["a", "abcdefghijk", "dwd1", "DWD", "dwd-test", "dwd_test", "../"]
        for name in invalid:
            assert VALID_SOURCE_PATTERN.match(name) is None


class TestCacheVersion:
    """Test cache version constant."""

    def test_cache_version_is_string(self):
        """Cache version should be a string."""
        assert isinstance(CACHE_VERSION, str)

    def test_cache_version_starts_with_v(self):
        """Cache version should start with 'v' for clarity."""
        assert CACHE_VERSION.startswith("v")
