#!/usr/bin/env python3
"""
Tests for Source Outage Detection & Resilient Composite Generation

Tests the outage detection logic and minimum core sources validation.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
import pytz

from imeteo_radar.cli_composite import (
    CORE_SOURCES,
    DEFAULT_MAX_DATA_AGE_MINUTES,
    DEFAULT_MIN_CORE_SOURCES,
    OPTIONAL_SOURCES,
    _count_available_core_sources,
    _detect_source_outages,
    _filter_available_sources,
    _find_multiple_common_timestamps,
)


@pytest.fixture
def mock_sources():
    """Create mock source configurations."""
    return {
        "dwd": (MagicMock(), "dmax"),
        "shmu": (MagicMock(), "zmax"),
        "chmi": (MagicMock(), "maxz"),
        "omsz": (MagicMock(), "cmax"),
        "imgw": (MagicMock(), "cmax"),
        "arso": (MagicMock(), "zm"),
    }


@pytest.fixture
def recent_timestamp():
    """Return a recent timestamp string (5 minutes ago)."""
    now = datetime.now(pytz.UTC) - timedelta(minutes=5)
    return now.strftime("%Y%m%d%H%M00")


@pytest.fixture
def stale_timestamp():
    """Return a stale timestamp string (45 minutes ago)."""
    now = datetime.now(pytz.UTC) - timedelta(minutes=45)
    return now.strftime("%Y%m%d%H%M00")


class TestSourceClassification:
    """Tests for source classification constants."""

    def test_core_sources_defined(self):
        """Test that core sources are defined correctly."""
        assert "dwd" in CORE_SOURCES
        assert "shmu" in CORE_SOURCES
        assert "chmi" in CORE_SOURCES
        assert "omsz" in CORE_SOURCES
        assert "imgw" in CORE_SOURCES
        assert len(CORE_SOURCES) == 5

    def test_optional_sources_defined(self):
        """Test that optional sources are defined correctly."""
        assert "arso" in OPTIONAL_SOURCES
        assert len(OPTIONAL_SOURCES) == 1

    def test_no_overlap_between_core_and_optional(self):
        """Test that core and optional sources don't overlap."""
        assert CORE_SOURCES.isdisjoint(OPTIONAL_SOURCES)


class TestOutageDetection:
    """Tests for _detect_source_outages function."""

    def test_no_data_is_outage(self, mock_sources, recent_timestamp):
        """Test that missing data is detected as outage."""
        all_source_files = {
            "dwd": [],  # No data
            "shmu": [{"timestamp": recent_timestamp}],
        }

        availability, reasons = _detect_source_outages(
            {"dwd": mock_sources["dwd"], "shmu": mock_sources["shmu"]},
            all_source_files,
        )

        assert availability["dwd"] is False
        assert availability["shmu"] is True
        assert "no data available" in reasons["dwd"]

    def test_stale_data_is_outage(self, mock_sources, stale_timestamp):
        """Test that stale data is detected as outage."""
        all_source_files = {
            "dwd": [{"timestamp": stale_timestamp}],
        }

        availability, reasons = _detect_source_outages(
            {"dwd": mock_sources["dwd"]},
            all_source_files,
            max_data_age_minutes=30,
        )

        assert availability["dwd"] is False
        assert "stale data" in reasons["dwd"]

    def test_fresh_data_is_available(self, mock_sources, recent_timestamp):
        """Test that fresh data is detected as available."""
        all_source_files = {
            "dwd": [{"timestamp": recent_timestamp}],
        }

        availability, reasons = _detect_source_outages(
            {"dwd": mock_sources["dwd"]},
            all_source_files,
            max_data_age_minutes=30,
        )

        assert availability["dwd"] is True
        assert "dwd" not in reasons

    def test_multiple_timestamps_uses_newest(self, mock_sources, recent_timestamp, stale_timestamp):
        """Test that newest timestamp is used for freshness check."""
        all_source_files = {
            "dwd": [
                {"timestamp": stale_timestamp},
                {"timestamp": recent_timestamp},
            ],
        }

        availability, reasons = _detect_source_outages(
            {"dwd": mock_sources["dwd"]},
            all_source_files,
        )

        # Should be available because newest is fresh
        assert availability["dwd"] is True

    def test_missing_source_in_files_is_outage(self, mock_sources, recent_timestamp):
        """Test that source not in all_source_files is outage."""
        all_source_files = {
            "shmu": [{"timestamp": recent_timestamp}],
        }

        availability, reasons = _detect_source_outages(
            {"dwd": mock_sources["dwd"], "shmu": mock_sources["shmu"]},
            all_source_files,
        )

        assert availability["dwd"] is False
        assert availability["shmu"] is True


class TestCoreSourcesCounting:
    """Tests for _count_available_core_sources function."""

    def test_all_core_available(self):
        """Test counting when all core sources are available."""
        availability = {
            "dwd": True,
            "shmu": True,
            "chmi": True,
            "omsz": True,
            "imgw": True,
            "arso": True,
        }

        available, total = _count_available_core_sources(availability)

        assert available == 5
        assert total == 5

    def test_some_core_unavailable(self):
        """Test counting when some core sources are unavailable."""
        availability = {
            "dwd": True,
            "shmu": True,
            "chmi": False,
            "omsz": True,
            "imgw": False,
            "arso": True,
        }

        available, total = _count_available_core_sources(availability)

        assert available == 3
        assert total == 5

    def test_arso_not_counted_as_core(self):
        """Test that ARSO (optional) is not counted as core."""
        availability = {
            "arso": False,  # Only ARSO, and it's down
        }

        available, total = _count_available_core_sources(availability)

        assert available == 0
        assert total == 0

    def test_partial_sources_configuration(self):
        """Test counting with partial source configuration."""
        availability = {
            "dwd": True,
            "shmu": True,
            "chmi": True,
        }

        available, total = _count_available_core_sources(availability)

        assert available == 3
        assert total == 3


class TestSourceFiltering:
    """Tests for _filter_available_sources function."""

    def test_filters_unavailable_sources(self, mock_sources):
        """Test that unavailable sources are filtered out."""
        availability = {
            "dwd": True,
            "shmu": False,
            "chmi": True,
        }

        filtered = _filter_available_sources(
            {
                "dwd": mock_sources["dwd"],
                "shmu": mock_sources["shmu"],
                "chmi": mock_sources["chmi"],
            },
            availability,
        )

        assert "dwd" in filtered
        assert "shmu" not in filtered
        assert "chmi" in filtered

    def test_filters_unknown_sources(self, mock_sources):
        """Test that sources not in availability dict are filtered out."""
        availability = {
            "dwd": True,
        }

        filtered = _filter_available_sources(
            {
                "dwd": mock_sources["dwd"],
                "shmu": mock_sources["shmu"],  # Not in availability
            },
            availability,
        )

        assert "dwd" in filtered
        assert "shmu" not in filtered


class TestMultipleTimestampFinding:
    """Tests for _find_multiple_common_timestamps function."""

    def test_finds_multiple_timestamps(self, mock_sources):
        """Test finding multiple common timestamps."""
        timestamp_groups = {
            "20260128120000": {"dwd": {"timestamp": "20260128120000"}, "shmu": {"timestamp": "20260128120000"}},
            "20260128115500": {"dwd": {"timestamp": "20260128115500"}, "shmu": {"timestamp": "20260128115500"}},
            "20260128115000": {"dwd": {"timestamp": "20260128115000"}, "shmu": {"timestamp": "20260128115000"}},
        }

        results = _find_multiple_common_timestamps(
            timestamp_groups,
            {"dwd": mock_sources["dwd"], "shmu": mock_sources["shmu"]},
            tolerance_minutes=2,
            max_count=3,
        )

        assert len(results) == 3
        # Should be sorted most recent first
        assert results[0][0] == "20260128120000"
        assert results[1][0] == "20260128115500"
        assert results[2][0] == "20260128115000"

    def test_respects_max_count(self, mock_sources):
        """Test that max_count limits results."""
        timestamp_groups = {
            f"2026012812{i:02d}00": {"dwd": {"timestamp": f"2026012812{i:02d}00"}}
            for i in range(10)
        }

        results = _find_multiple_common_timestamps(
            timestamp_groups,
            {"dwd": mock_sources["dwd"]},
            max_count=3,
        )

        assert len(results) == 3

    def test_respects_min_sources(self, mock_sources):
        """Test that min_sources filters timestamps."""
        timestamp_groups = {
            "20260128120000": {"dwd": {"timestamp": "20260128120000"}, "shmu": {"timestamp": "20260128120000"}},
            "20260128115500": {"dwd": {"timestamp": "20260128115500"}},  # Only DWD
        }

        results = _find_multiple_common_timestamps(
            timestamp_groups,
            {"dwd": mock_sources["dwd"], "shmu": mock_sources["shmu"]},
            min_sources=2,
        )

        assert len(results) == 1
        assert results[0][0] == "20260128120000"

    def test_finds_timestamps_within_tolerance(self, mock_sources, recent_timestamp):
        """Test finding timestamps within tolerance window."""
        # Create two timestamps 1 minute apart, both recent
        now = datetime.now(pytz.UTC)
        ts1 = now.strftime("%Y%m%d%H%M00")
        ts2 = (now - timedelta(minutes=1)).strftime("%Y%m%d%H%M00")

        timestamp_groups = {
            ts1: {"dwd": {"timestamp": ts1}},
            ts2: {"shmu": {"timestamp": ts2}},  # 1 min offset
        }

        results = _find_multiple_common_timestamps(
            timestamp_groups,
            {"dwd": mock_sources["dwd"], "shmu": mock_sources["shmu"]},
            tolerance_minutes=2,
            min_sources=2,
        )

        # At least one result should be found
        assert len(results) >= 1
        # Both sources should be matched in the result (within tolerance)
        assert "dwd" in results[0][1]
        assert "shmu" in results[0][1]

    def test_returns_empty_for_no_matches(self, mock_sources):
        """Test returning empty list when no matches found."""
        timestamp_groups = {
            "20260128120000": {"dwd": {"timestamp": "20260128120000"}},
            "20260128100000": {"shmu": {"timestamp": "20260128100000"}},  # 2 hours apart
        }

        results = _find_multiple_common_timestamps(
            timestamp_groups,
            {"dwd": mock_sources["dwd"], "shmu": mock_sources["shmu"]},
            tolerance_minutes=2,
            min_sources=2,
        )

        assert len(results) == 0


class TestDefaultValues:
    """Tests for default configuration values."""

    def test_default_min_core_sources(self):
        """Test default minimum core sources is 3."""
        assert DEFAULT_MIN_CORE_SOURCES == 3

    def test_default_max_data_age(self):
        """Test default max data age is 30 minutes."""
        assert DEFAULT_MAX_DATA_AGE_MINUTES == 30

    def test_min_core_sources_allows_two_outages(self):
        """Test that default min allows up to 2 core source outages."""
        # With 5 core sources and min=3, we can have 2 outages
        assert len(CORE_SOURCES) - DEFAULT_MIN_CORE_SOURCES == 2
