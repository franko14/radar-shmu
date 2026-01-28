#!/usr/bin/env python3
"""
Timestamp utilities for radar data processing.

Centralizes all timestamp parsing, normalization, and generation logic
that was previously duplicated across source classes.
"""

from datetime import datetime, timedelta
from typing import Callable

import pytz

from ..core.logging import get_logger

logger = get_logger(__name__)


# Standard timestamp formats used by different sources
class TimestampFormat:
    """Standard timestamp format definitions."""

    # 14-digit format: YYYYMMDDHHMMSS
    FULL = "%Y%m%d%H%M%S"
    # 12-digit format: YYYYMMDDHHMM
    SHORT = "%Y%m%d%H%M"
    # DWD/OMSZ format: YYYYMMDD_HHMM
    UNDERSCORE = "%Y%m%d_%H%M"
    # 8-digit date: YYYYMMDD
    DATE_ONLY = "%Y%m%d"
    # 6-digit time: HHMMSS
    TIME_ONLY = "%H%M%S"


def parse_timestamp(ts_str: str, fmt: str | None = None) -> datetime | None:
    """Parse a timestamp string to datetime.

    Args:
        ts_str: Timestamp string to parse
        fmt: Optional format string. If None, auto-detects format.

    Returns:
        Parsed datetime or None if parsing fails
    """
    if fmt:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            return None

    # Auto-detect format based on string pattern
    formats_to_try = [
        (TimestampFormat.FULL, 14),  # YYYYMMDDHHMMSS
        (TimestampFormat.SHORT, 12),  # YYYYMMDDHHMM
        (TimestampFormat.UNDERSCORE, 13),  # YYYYMMDD_HHMM
    ]

    for fmt, expected_len in formats_to_try:
        if len(ts_str) >= expected_len:
            try:
                return datetime.strptime(ts_str[:expected_len], fmt)
            except ValueError:
                continue

    return None


def normalize_timestamp(timestamp: str, target_length: int = 14) -> str:
    """Normalize timestamp to standard length.

    Args:
        timestamp: Input timestamp string
        target_length: Target length (12 or 14)

    Returns:
        Normalized timestamp string
    """
    # Remove underscores if present (DWD/OMSZ format)
    ts = timestamp.replace("_", "")

    if target_length == 14:
        # Pad to 14 characters with "00" for seconds
        if len(ts) == 12:
            return ts + "00"
        return ts[:14]
    elif target_length == 12:
        return ts[:12]

    return ts


def round_to_interval(dt: datetime, interval_minutes: int = 5) -> datetime:
    """Round datetime down to nearest interval.

    Args:
        dt: Datetime to round
        interval_minutes: Interval in minutes (default: 5)

    Returns:
        Rounded datetime
    """
    return dt.replace(
        minute=(dt.minute // interval_minutes) * interval_minutes,
        second=0,
        microsecond=0,
    )


def generate_timestamp_candidates(
    count: int,
    interval_minutes: int = 5,
    delay_minutes: int = 15,
    format_str: str = TimestampFormat.SHORT,
    timezone_offset_hours: int = 0,
) -> list[str]:
    """Generate candidate timestamps for checking data availability.

    Args:
        count: Number of timestamps to generate
        interval_minutes: Interval between timestamps (default: 5)
        delay_minutes: Minutes to subtract from current time for processing delay
        format_str: Output format string
        timezone_offset_hours: Offset from UTC (e.g., 1 for CET)

    Returns:
        List of timestamp strings, newest first
    """
    timestamps = []
    current_time = datetime.now(pytz.UTC) - timedelta(minutes=delay_minutes)

    if timezone_offset_hours:
        current_time = current_time + timedelta(hours=timezone_offset_hours)

    # Generate candidates going backwards
    for minutes_back in range(0, count * interval_minutes * 3, interval_minutes):
        check_time = current_time - timedelta(minutes=minutes_back)
        check_time = round_to_interval(check_time, interval_minutes)
        timestamp = check_time.strftime(format_str)

        if timestamp not in timestamps:
            timestamps.append(timestamp)

        if len(timestamps) >= count * 3:
            break

    return timestamps


def filter_timestamps_by_range(
    timestamps: list[str],
    start_time: datetime,
    end_time: datetime,
    parse_format: str | None = None,
) -> list[str]:
    """Filter timestamps to only include those within a time range.

    Args:
        timestamps: List of timestamp strings
        start_time: Start of time range (timezone-aware datetime)
        end_time: End of time range (timezone-aware datetime)
        parse_format: Format string for parsing timestamps

    Returns:
        Filtered list of timestamps within the range
    """
    filtered = []
    for ts in timestamps:
        dt = parse_timestamp(ts, parse_format)
        if dt is None:
            continue

        # Make timezone aware if needed
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)

        if start_time <= dt <= end_time:
            filtered.append(ts)

    return filtered


def find_common_timestamp(
    timestamp_groups: dict[str, dict],
    required_sources: set[str],
    tolerance_minutes: int = 2,
) -> tuple[str | None, dict | None]:
    """Find most recent timestamp where all sources have data within tolerance.

    Args:
        timestamp_groups: Dict mapping timestamp -> {source_name: file_info}
        required_sources: Set of source names that must all have data
        tolerance_minutes: Maximum time difference allowed

    Returns:
        (common_timestamp, source_files) or (None, None)
    """
    # Parse all timestamps to datetime
    timestamp_datetimes = {}
    for ts_str in timestamp_groups.keys():
        dt = parse_timestamp(ts_str)
        if dt:
            timestamp_datetimes[ts_str] = dt

    if not timestamp_datetimes:
        return None, None

    # Sort by datetime (most recent first)
    sorted_timestamps = sorted(
        timestamp_datetimes.keys(),
        key=lambda x: timestamp_datetimes[x],
        reverse=True,
    )

    tolerance = timedelta(minutes=tolerance_minutes)

    # For each timestamp, check if all sources have data within tolerance
    for candidate_ts in sorted_timestamps:
        candidate_dt = timestamp_datetimes[candidate_ts]
        sources_in_window = {}

        # Find sources with data in this time window
        for ts_str, ts_dt in timestamp_datetimes.items():
            if abs(ts_dt - candidate_dt) <= tolerance:
                for source_name, file_info in timestamp_groups[ts_str].items():
                    if source_name not in sources_in_window:
                        sources_in_window[source_name] = (ts_str, file_info, ts_dt)
                    else:
                        # Keep the closer timestamp
                        _, _, existing_dt = sources_in_window[source_name]
                        if abs(ts_dt - candidate_dt) < abs(existing_dt - candidate_dt):
                            sources_in_window[source_name] = (ts_str, file_info, ts_dt)

        # Check if all required sources are present
        if required_sources <= set(sources_in_window.keys()):
            logger.debug(f"Found common time window around {candidate_ts}")
            return candidate_ts, {
                src: info for src, (_, info, _) in sources_in_window.items()
            }

    return None, None


def extract_timestamp_from_hdf5_attrs(
    what_attrs: dict,
    date_key: str = "startdate",
    time_key: str = "starttime",
) -> str:
    """Extract timestamp from HDF5 'what' attributes.

    Args:
        what_attrs: Dictionary of HDF5 'what' group attributes
        date_key: Key for date attribute
        time_key: Key for time attribute

    Returns:
        Timestamp string in YYYYMMDDHHMMSS format
    """
    date_str = what_attrs.get(date_key, "")
    time_str = what_attrs.get(time_key, "")

    # Handle bytes
    if isinstance(date_str, bytes):
        date_str = date_str.decode("utf-8")
    if isinstance(time_str, bytes):
        time_str = time_str.decode("utf-8")

    # Combine and normalize
    timestamp = date_str + time_str
    return normalize_timestamp(timestamp, 14)


def is_timestamp_in_cache(timestamp: str, cached_set: set[str]) -> bool:
    """Check if a timestamp matches any entry in a cache set.

    Handles different timestamp formats (with/without underscores, 12/14 digits).

    Args:
        timestamp: Timestamp string to check
        cached_set: Set of cached timestamp strings

    Returns:
        True if timestamp matches any cached entry
    """
    if not cached_set:
        return False

    # Clean and normalize to different formats for comparison
    ts_clean = timestamp.replace("_", "")
    ts_12 = ts_clean[:12]  # YYYYMMDDHHMM
    ts_14 = ts_12 + "00" if len(ts_clean) <= 12 else ts_clean[:14]

    return (
        timestamp in cached_set or
        ts_clean in cached_set or
        ts_12 in cached_set or
        ts_14 in cached_set
    )


def timestamp_to_unix(timestamp: str, tz: str = "UTC") -> int:
    """Convert timestamp string to Unix timestamp.

    Args:
        timestamp: Timestamp string (auto-detected format)
        tz: Timezone name (default: UTC)

    Returns:
        Unix timestamp as integer
    """
    dt = parse_timestamp(timestamp)
    if dt is None:
        raise ValueError(f"Cannot parse timestamp: {timestamp}")

    if dt.tzinfo is None:
        dt = pytz.timezone(tz).localize(dt)

    return int(dt.timestamp())
