#!/usr/bin/env python3
"""
Composite command implementation for CLI

Separated into its own module to keep cli.py manageable.
"""

import gc
from pathlib import Path
from typing import Any, Optional

from .core.logging import get_logger
from .utils.spaces_uploader import SpacesUploader, is_spaces_configured
from .utils.timestamps import is_timestamp_in_cache, normalize_timestamp

logger = get_logger(__name__)

# Fixed reference extent covering all sources (DWD, SHMU, CHMI, OMSZ, ARSO, IMGW)
# Using this ensures consistent composite dimensions regardless of which sources are available
# Calculated from the union of all source extents
REFERENCE_EXTENT = {
    "west": 2.50,   # DWD westernmost
    "east": 26.40,  # IMGW easternmost
    "south": 44.00, # OMSZ southernmost
    "north": 56.20, # IMGW northernmost
}

# Source classification for outage detection
# Core sources are required for good geographic coverage
CORE_SOURCES = {"dwd", "shmu", "chmi", "omsz", "imgw"}
# Optional sources are nice to have but not required
OPTIONAL_SOURCES = {"arso"}

# Default thresholds for outage detection
DEFAULT_MIN_CORE_SOURCES = 3  # Allow up to 2 core source outages
DEFAULT_MAX_DATA_AGE_MINUTES = 30  # Data older than this is considered stale
DEFAULT_REPROCESS_COUNT = 6  # Process last 30 minutes (6 × 5 min intervals)


def _detect_source_outages(
    sources: dict,
    all_source_files: dict,
    max_data_age_minutes: int = DEFAULT_MAX_DATA_AGE_MINUTES,
) -> tuple[dict[str, bool], dict[str, str]]:
    """Detect which sources are in OUTAGE state.

    A source is considered in OUTAGE if:
    1. No data available - download fails or returns empty
    2. Stale data - newest timestamp is older than max_data_age_minutes

    Args:
        sources: Dict of source configurations {name: (source_obj, product)}
        all_source_files: Dict of downloaded files {name: [file_info, ...]}
        max_data_age_minutes: Maximum age of data in minutes (default: 30)

    Returns:
        Tuple of:
        - availability: Dict[str, bool] - source_name -> is_available (True) or in_outage (False)
        - reasons: Dict[str, str] - source_name -> reason for outage (if any)
    """
    from datetime import datetime, timedelta

    import pytz

    availability = {}
    reasons = {}
    now = datetime.now(pytz.UTC)
    max_age = timedelta(minutes=max_data_age_minutes)

    logger.info("Checking source availability...")

    for source_name in sources.keys():
        files = all_source_files.get(source_name, [])

        if not files:
            # No data available - OUTAGE
            availability[source_name] = False
            reasons[source_name] = "no data available"
            logger.warning(
                f"  {source_name.upper()}: OUTAGE (no data available)",
                extra={"source": source_name, "status": "outage"},
            )
            continue

        # Find newest timestamp
        newest_ts = None
        newest_dt = None

        for file_info in files:
            ts_str = file_info.get("timestamp", "")
            if not ts_str:
                continue

            try:
                # Parse timestamp (format: YYYYMMDDHHMMSS or YYYYMMDDHHMM)
                dt = datetime.strptime(ts_str[:12], "%Y%m%d%H%M")
                dt = pytz.UTC.localize(dt)

                if newest_dt is None or dt > newest_dt:
                    newest_dt = dt
                    newest_ts = ts_str
            except ValueError:
                continue

        if newest_dt is None:
            # No valid timestamps - OUTAGE
            availability[source_name] = False
            reasons[source_name] = "no valid timestamps"
            logger.warning(
                f"  {source_name.upper()}: OUTAGE (no valid timestamps)",
                extra={"source": source_name, "status": "outage"},
            )
            continue

        # Check data age
        data_age = now - newest_dt
        age_minutes = int(data_age.total_seconds() / 60)

        if data_age > max_age:
            # Stale data - OUTAGE
            availability[source_name] = False
            reasons[source_name] = f"stale data (age: {age_minutes} min, max: {max_data_age_minutes} min)"
            logger.warning(
                f"  {source_name.upper()}: OUTAGE (stale data, age: {age_minutes} min)",
                extra={"source": source_name, "status": "outage", "age_min": age_minutes},
            )
        else:
            # Available
            availability[source_name] = True
            logger.info(
                f"  {source_name.upper()}: AVAILABLE (newest: {newest_ts}, age: {age_minutes} min)",
                extra={"source": source_name, "status": "available", "timestamp": newest_ts, "age_min": age_minutes},
            )

    return availability, reasons


def _count_available_core_sources(availability: dict[str, bool]) -> tuple[int, int]:
    """Count available core sources.

    Args:
        availability: Dict of source_name -> is_available

    Returns:
        Tuple of (available_core_count, total_core_count)
    """
    available_core = sum(
        1 for src in CORE_SOURCES if src in availability and availability[src]
    )
    total_core = sum(1 for src in CORE_SOURCES if src in availability)
    return available_core, total_core


def _filter_available_sources(
    sources: dict, availability: dict[str, bool]
) -> dict:
    """Filter sources to only include available ones.

    Args:
        sources: Dict of source configurations
        availability: Dict of source_name -> is_available

    Returns:
        Filtered sources dict with only available sources
    """
    return {
        name: config
        for name, config in sources.items()
        if availability.get(name, False)
    }


def composite_command_impl(args: Any) -> int:
    """Handle composite generation command"""
    try:
        from .processing.exporter import PNGExporter
        from .sources.arso import ARSORadarSource
        from .sources.chmi import CHMIRadarSource
        from .sources.dwd import DWDRadarSource
        from .sources.imgw import IMGWRadarSource
        from .sources.omsz import OMSZRadarSource
        from .sources.shmu import SHMURadarSource

        # Parse sources list
        source_names = [s.strip() for s in args.sources.split(",")]
        logger.info(
            f"Creating composite from sources: {', '.join(source_names).upper()}"
        )

        # Create output directory
        output_dir = args.output
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")

        # Initialize DigitalOcean Spaces uploader
        uploader = None
        upload_enabled = not getattr(args, "disable_upload", False)

        if upload_enabled:
            if is_spaces_configured():
                try:
                    uploader = SpacesUploader()
                    logger.info("DigitalOcean Spaces upload enabled")
                except Exception as e:
                    logger.warning(f"Failed to initialize Spaces uploader: {e}")
                    logger.warning("Falling back to local-only mode (upload disabled)")
                    upload_enabled = False
            else:
                logger.warning(
                    "DigitalOcean Spaces not configured (missing environment variables)"
                )
                logger.warning("Falling back to local-only mode (upload disabled)")
                upload_enabled = False
        else:
            logger.info("Local-only mode (--disable-upload flag used)")

        # Initialize sources
        sources = {}
        for source_name in source_names:
            if source_name == "dwd":
                sources["dwd"] = (DWDRadarSource(), "dmax")
            elif source_name == "shmu":
                sources["shmu"] = (SHMURadarSource(), "zmax")
            elif source_name == "chmi":
                sources["chmi"] = (CHMIRadarSource(), "maxz")
            elif source_name == "arso":
                sources["arso"] = (ARSORadarSource(), "zm")
            elif source_name == "omsz":
                sources["omsz"] = (OMSZRadarSource(), "cmax")
            elif source_name == "imgw":
                sources["imgw"] = (IMGWRadarSource(), "cmax")
            else:
                logger.error(f"Unknown source: {source_name}")
                return 1

        # Initialize PNG exporter
        exporter = PNGExporter()

        # Determine what to process
        if args.backload:
            # Backload mode - process historical data
            return _process_backload(args, sources, exporter, output_dir, uploader)
        else:
            # Single timestamp mode - process latest available data
            return _process_latest(args, sources, exporter, output_dir, uploader)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback

        traceback.print_exc()
        return 1


def _find_common_timestamp_with_tolerance(
    timestamp_groups, sources, tolerance_minutes=2, min_sources=None
):
    """Find most recent timestamp where sources have data within tolerance window

    Args:
        timestamp_groups: Dict mapping timestamp str -> Dict[source_name, file_info]
        sources: Dict of source configurations
        tolerance_minutes: Maximum time difference allowed (default: 2 minutes)
        min_sources: Minimum sources required (default: all sources).
                     Set lower for resilience to source outages.

    Returns:
        (common_timestamp, source_files) or (None, None)
    """
    from datetime import datetime, timedelta

    # Default to requiring all sources
    if min_sources is None:
        min_sources = len(sources)

    # Parse all timestamps to datetime
    timestamp_datetimes = {}
    for ts_str in timestamp_groups.keys():
        try:
            dt = datetime.strptime(ts_str[:12], "%Y%m%d%H%M")
            timestamp_datetimes[ts_str] = dt
        except ValueError:
            continue

    # Sort by datetime (most recent first)
    sorted_timestamps = sorted(
        timestamp_datetimes.keys(), key=lambda x: timestamp_datetimes[x], reverse=True
    )

    tolerance = timedelta(minutes=tolerance_minutes)

    # Track best partial match for resilience
    best_partial_match = None
    best_partial_count = 0

    # For each timestamp, check if sources have data within tolerance
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
                        existing_ts, existing_info, existing_dt = sources_in_window[
                            source_name
                        ]
                        if abs(ts_dt - candidate_dt) < abs(existing_dt - candidate_dt):
                            sources_in_window[source_name] = (ts_str, file_info, ts_dt)

        # Check if we have enough sources (all or minimum required)
        if len(sources_in_window) == len(sources):
            logger.info(f"Found common time window around {candidate_ts}")
            for src, (ts, _, dt) in sources_in_window.items():
                offset = (dt - candidate_dt).total_seconds() / 60
                logger.debug(f"   {src.upper()}: {ts} (offset: {offset:+.1f} min)")

            return candidate_ts, {
                src: info for src, (_, info, _) in sources_in_window.items()
            }

        # Track best partial match if we allow partial composites
        if min_sources < len(sources) and len(sources_in_window) >= min_sources:
            if len(sources_in_window) > best_partial_count:
                best_partial_count = len(sources_in_window)
                best_partial_match = (candidate_ts, sources_in_window)

    # Return best partial match if no full match found and partial allowed
    if best_partial_match and best_partial_count >= min_sources:
        candidate_ts, sources_in_window = best_partial_match
        missing = set(sources.keys()) - set(sources_in_window.keys())
        logger.warning(
            f"No full match found. Using partial composite with {best_partial_count}/{len(sources)} sources"
        )
        logger.warning(f"   Missing sources: {', '.join(s.upper() for s in missing)}")
        logger.info(f"Found partial time window around {candidate_ts}")
        for src, (ts, _, dt) in sources_in_window.items():
            candidate_dt = timestamp_datetimes[candidate_ts]
            offset = (dt - candidate_dt).total_seconds() / 60
            logger.debug(f"   {src.upper()}: {ts} (offset: {offset:+.1f} min)")

        return candidate_ts, {
            src: info for src, (_, info, _) in sources_in_window.items()
        }

    return None, None


def _find_multiple_common_timestamps(
    timestamp_groups: dict,
    sources: dict,
    tolerance_minutes: int = 2,
    min_sources: int = None,
    max_count: int = 6,
) -> list[tuple[str, dict]]:
    """Find multiple recent timestamps where sources have data within tolerance window.

    Args:
        timestamp_groups: Dict mapping timestamp str -> Dict[source_name, file_info]
        sources: Dict of source configurations
        tolerance_minutes: Maximum time difference allowed (default: 2 minutes)
        min_sources: Minimum sources required (default: all sources)
        max_count: Maximum number of timestamps to return (default: 6)

    Returns:
        List of (timestamp, source_files) tuples, most recent first
    """
    from datetime import datetime, timedelta

    if min_sources is None:
        min_sources = len(sources)

    # Parse all timestamps to datetime
    timestamp_datetimes = {}
    for ts_str in timestamp_groups.keys():
        try:
            dt = datetime.strptime(ts_str[:12], "%Y%m%d%H%M")
            timestamp_datetimes[ts_str] = dt
        except ValueError:
            continue

    # Sort by datetime (most recent first)
    sorted_timestamps = sorted(
        timestamp_datetimes.keys(), key=lambda x: timestamp_datetimes[x], reverse=True
    )

    tolerance = timedelta(minutes=tolerance_minutes)
    results = []
    processed_windows = set()  # Track which time windows we've already matched

    for candidate_ts in sorted_timestamps:
        if len(results) >= max_count:
            break

        candidate_dt = timestamp_datetimes[candidate_ts]

        # Skip if we already have a result within this time window
        window_key = candidate_dt.strftime("%Y%m%d%H%M")
        if window_key in processed_windows:
            continue

        sources_in_window = {}

        # Find sources with data in this time window
        for ts_str, ts_dt in timestamp_datetimes.items():
            if abs(ts_dt - candidate_dt) <= tolerance:
                for source_name, file_info in timestamp_groups[ts_str].items():
                    if source_name not in sources_in_window:
                        sources_in_window[source_name] = (ts_str, file_info, ts_dt)
                    else:
                        # Keep the closer timestamp
                        existing_ts, existing_info, existing_dt = sources_in_window[source_name]
                        if abs(ts_dt - candidate_dt) < abs(existing_dt - candidate_dt):
                            sources_in_window[source_name] = (ts_str, file_info, ts_dt)

        # Check if we have enough sources
        if len(sources_in_window) >= min_sources:
            processed_windows.add(window_key)
            source_files = {src: info for src, (_, info, _) in sources_in_window.items()}
            results.append((candidate_ts, source_files))
            logger.debug(
                f"Found common timestamp {candidate_ts} with {len(source_files)} sources"
            )

    return results


def _process_latest(args, sources, exporter, output_dir, uploader=None):
    """Process latest available data from all sources with outage detection and reprocessing.

    OUTAGE DETECTION:
    - Detects sources that are unavailable or have stale data (>max_data_age)
    - Filters to available sources only
    - Validates minimum core sources requirement before proceeding

    MULTI-TIMESTAMP REPROCESSING:
    - Processes up to reprocess_count recent timestamps (default: 6 = 30 min)
    - Allows automatic reprocessing when providers backload data after outages
    - Skips timestamps where composite already exists (unless sources changed)

    TWO-PASS ARCHITECTURE for ~75% memory reduction:
    - Pass 1: Extract extents only (no data loading) -> Calculate combined extent
    - Pass 2: Process each source sequentially: Load -> Export individual -> Merge -> Delete

    CACHE INTEGRATION:
    - Caches processed radar data to bridge timestamp gaps between fast/slow sources
    - ARSO (~7-8 min latency) data is cached so it can be matched with slower sources
    """
    from datetime import datetime

    import pytz

    from .processing.compositor import RadarCompositor

    # Get configuration from args
    max_data_age = getattr(args, "max_data_age", DEFAULT_MAX_DATA_AGE_MINUTES)
    min_core_sources = getattr(args, "min_core_sources", DEFAULT_MIN_CORE_SOURCES)
    reprocess_count = getattr(args, "reprocess_count", DEFAULT_REPROCESS_COUNT)
    tolerance = getattr(args, "timestamp_tolerance", 2)

    # Initialize processed data cache
    cache = None
    if not getattr(args, "no_cache", False):
        from .utils.processed_cache import ProcessedDataCache

        s3_enabled = not getattr(args, "no_cache_upload", False)
        cache = ProcessedDataCache(
            local_dir=getattr(args, "cache_dir", Path("/tmp/iradar-data")),
            ttl_minutes=getattr(args, "cache_ttl", 60),
            s3_enabled=s3_enabled,
        )

        if getattr(args, "clear_cache", False):
            cleared = cache.clear()
            logger.info(f"Cleared {cleared} cache entries")

        # Cleanup expired entries on startup
        cache.cleanup_expired()

    # ========== STEP 1: DOWNLOAD DATA FROM ALL SOURCES ==========
    logger.info("Downloading data from all sources...")
    timestamp_groups = {}
    all_source_files = {}
    sources_with_cache = set()

    # Check cache first for available timestamps
    # This is especially important for ARSO which only provides latest data (no archive)
    # The cache serves as ARSO's "archive" for reprocessing
    if cache:
        logger.info("Checking cache for available timestamps...")
        for source_name in sources.keys():
            source, product = sources[source_name]
            cached_timestamps = cache.get_available_timestamps(source_name, product)
            if cached_timestamps:
                sources_with_cache.add(source_name)
                # Log ARSO cache specially since it's critical for reprocessing
                if source_name == "arso":
                    logger.info(
                        f"  ARSO cache: {len(cached_timestamps)} timestamps available "
                        f"(newest: {cached_timestamps[0] if cached_timestamps else 'none'})"
                    )
                else:
                    logger.debug(
                        f"  {source_name.upper()} cache: {len(cached_timestamps)} timestamps"
                    )
                for ts in cached_timestamps:
                    ts_normalized = ts[:12] + "00" if len(ts) == 12 else ts
                    if ts_normalized not in timestamp_groups:
                        timestamp_groups[ts_normalized] = {}
                    if source_name not in timestamp_groups[ts_normalized]:
                        timestamp_groups[ts_normalized][source_name] = {
                            "timestamp": ts_normalized,
                            "from_cache": True,
                            "product": product,
                        }
        if not sources_with_cache:
            logger.info("  No cached data found (first run or cache cleared)")

    # Download fresh data from each source, SKIPPING cached timestamps
    for source_name, (source, product) in sources.items():
        # Get cached timestamps for this source
        cached_ts_set = set()
        if cache:
            cached_ts_set = set(cache.get_available_timestamps(source_name, product))

        # Get available timestamps from provider (without downloading yet)
        available_timestamps = source.get_available_timestamps(
            count=reprocess_count + 2,
            products=[product],
        )

        if not available_timestamps:
            logger.warning(f"{source_name.upper()}: No timestamps available from provider")
            all_source_files[source_name] = []
            continue

        # Determine which timestamps need downloading
        timestamps_to_download = []
        timestamps_from_cache = []

        for ts in available_timestamps:
            if is_timestamp_in_cache(ts, cached_ts_set):
                timestamps_from_cache.append(ts)
            else:
                timestamps_to_download.append(ts)

        logger.info(
            f"{source_name.upper()}: {len(available_timestamps)} available, "
            f"{len(timestamps_from_cache)} in cache, "
            f"{len(timestamps_to_download)} to download"
        )

        # Download only non-cached timestamps
        downloaded_files = []
        if timestamps_to_download:
            downloaded_files = source.download_timestamps(
                timestamps=timestamps_to_download,
                products=[product],
            )
            if downloaded_files:
                for file_info in downloaded_files:
                    timestamp = file_info["timestamp"]
                    if timestamp not in timestamp_groups:
                        timestamp_groups[timestamp] = {}
                    timestamp_groups[timestamp][source_name] = file_info

                    # Cache downloaded data immediately (so next run won't re-download)
                    if cache and file_info.get("path"):
                        try:
                            radar_data = source.process_to_array(file_info["path"])
                            cache.put(source_name, timestamp, product, radar_data)
                            # Mark for Pass 2 to retrieve from cache, but track as downloaded for stats
                            file_info["from_cache"] = True
                            file_info["was_downloaded"] = True  # Track for logging
                            del radar_data
                            gc.collect()
                        except Exception as e:
                            logger.debug(f"Could not cache {source_name} {timestamp}: {e}")

        # Build cached file info entries for outage detection
        cached_file_infos = []
        for ts in timestamps_from_cache:
            ts_normalized = normalize_timestamp(ts, target_length=14)
            cached_file_info = {
                "timestamp": ts_normalized,
                "from_cache": True,
                "product": product,
            }
            cached_file_infos.append(cached_file_info)

            # Add to timestamp_groups
            if ts_normalized not in timestamp_groups:
                timestamp_groups[ts_normalized] = {}
            if source_name not in timestamp_groups[ts_normalized]:
                timestamp_groups[ts_normalized][source_name] = cached_file_info

        # Combine downloaded + cached for all_source_files (used by outage detection)
        all_source_files[source_name] = downloaded_files + cached_file_infos

        if not timestamps_to_download and timestamps_from_cache:
            logger.debug(f"  {source_name.upper()}: Using cached data, no download needed")

    # Re-add any cached timestamps not already in timestamp_groups
    if cache:
        for source_name in sources.keys():
            source, product = sources[source_name]
            cached_timestamps = cache.get_available_timestamps(source_name, product)
            for ts in cached_timestamps:
                ts_normalized = normalize_timestamp(ts, target_length=14)
                if ts_normalized not in timestamp_groups:
                    timestamp_groups[ts_normalized] = {}
                if source_name not in timestamp_groups[ts_normalized]:
                    timestamp_groups[ts_normalized][source_name] = {
                        "timestamp": ts_normalized,
                        "from_cache": True,
                        "product": product,
                    }

    # Log summary of timestamps per source (downloaded + cached)
    logger.info("Timestamps available for matching (download + cache):")
    for source_name in sources.keys():
        source_timestamps = sorted(
            [ts for ts, srcs in timestamp_groups.items() if source_name in srcs],
            reverse=True,
        )
        if source_timestamps:
            # Show total count and newest few timestamps
            recent = source_timestamps[:3]
            # Count downloads: either not from_cache, or was_downloaded (cached after download this run)
            download_count = sum(
                1 for ts in source_timestamps
                if not timestamp_groups[ts][source_name].get("from_cache", False)
                or timestamp_groups[ts][source_name].get("was_downloaded", False)
            )
            cache_count = len(source_timestamps) - download_count
            logger.info(
                f"  {source_name.upper()}: {len(source_timestamps)} timestamps "
                f"({download_count} downloaded, {cache_count} cached) "
                f"[recent: {', '.join(recent)}]"
            )
        else:
            logger.info(f"  {source_name.upper()}: 0 timestamps")

    # ========== STEP 2: DETECT SOURCE OUTAGES ==========
    availability, outage_reasons = _detect_source_outages(
        sources, all_source_files, max_data_age
    )

    # Count available sources
    available_count = sum(1 for v in availability.values() if v)
    outage_count = sum(1 for v in availability.values() if not v)

    if outage_count > 0:
        outage_sources = [s.upper() for s, v in availability.items() if not v]
        logger.warning(
            f"{outage_count} source(s) in OUTAGE: {', '.join(outage_sources)}"
        )

    # ========== STEP 3: CHECK MINIMUM CORE SOURCES REQUIREMENT ==========
    available_core, total_core = _count_available_core_sources(availability)

    if available_core < min_core_sources:
        unavailable_core = [
            s.upper() for s in CORE_SOURCES
            if s in availability and not availability[s]
        ]
        logger.error(
            f"Only {available_core}/{total_core} core sources available "
            f"(minimum required: {min_core_sources})"
        )
        logger.error(f"Unavailable core sources: {', '.join(unavailable_core)}")
        for src in unavailable_core:
            reason = outage_reasons.get(src.lower(), "unknown")
            logger.error(f"  {src}: {reason}")
        return 1

    logger.info(
        f"{available_core}/{total_core} core sources available "
        f"(minimum: {min_core_sources}), proceeding with composite generation"
    )

    # ========== STEP 4: FILTER TO AVAILABLE SOURCES ONLY ==========
    available_sources = _filter_available_sources(sources, availability)

    # Handle ARSO special case (optional source with single timestamp limitation)
    # ARSO only provides latest data (no archive), so we cache it for future reprocessing
    # When slower sources catch up, the cached ARSO data can be matched with them
    require_arso = getattr(args, "require_arso", False)
    arso_dropped = False

    if "arso" in available_sources and not require_arso:
        # Cache ARSO data for future timestamp matching
        # This builds ARSO's "archive" in the cache
        if cache and "arso" in all_source_files and all_source_files["arso"]:
            arso_source, arso_product = sources["arso"]
            for arso_file in all_source_files["arso"]:
                # Skip entries that are already from cache (no path to process)
                if arso_file.get("from_cache") or not arso_file.get("path"):
                    continue
                try:
                    arso_data = arso_source.process_to_array(arso_file["path"])
                    cache.put("arso", arso_file["timestamp"], arso_product, arso_data)
                    logger.info(
                        f"Cached ARSO {arso_file['timestamp']} for future reprocessing"
                    )
                    del arso_data
                    gc.collect()
                except Exception as e:
                    logger.warning(f"Failed to cache ARSO data: {e}")

    # ========== STEP 5: FIND MULTIPLE COMMON TIMESTAMPS ==========
    # Calculate minimum sources needed for matching
    # Use available_core as baseline, but allow fewer if sources were specified explicitly
    min_sources_for_match = max(min_core_sources, len(available_sources) - 1)

    # First try to find timestamps with all available sources
    common_timestamps = _find_multiple_common_timestamps(
        timestamp_groups,
        available_sources,
        tolerance,
        min_sources=len(available_sources),
        max_count=reprocess_count,
    )

    # If no full matches and ARSO is included, try without ARSO
    if not common_timestamps and "arso" in available_sources and len(available_sources) > 2:
        logger.info("No common timestamps with ARSO, retrying without ARSO...")
        logger.info("   (ARSO only provides single latest timestamp)")

        # Remove ARSO from available sources
        available_sources_no_arso = {
            k: v for k, v in available_sources.items() if k != "arso"
        }
        arso_dropped = True

        common_timestamps = _find_multiple_common_timestamps(
            timestamp_groups,
            available_sources_no_arso,
            tolerance,
            min_sources=min(min_sources_for_match, len(available_sources_no_arso)),
            max_count=reprocess_count,
        )

        if common_timestamps:
            available_sources = available_sources_no_arso

    # If still no matches, try with reduced source requirement
    if not common_timestamps:
        common_timestamps = _find_multiple_common_timestamps(
            timestamp_groups,
            available_sources,
            tolerance,
            min_sources=min_core_sources,
            max_count=reprocess_count,
        )

    if not common_timestamps:
        logger.error("No common timestamps found across available sources")
        logger.info(f"   Timestamp tolerance: {tolerance} minutes")
        logger.info("Available timestamps by source:")
        for source_name in available_sources.keys():
            if source_name in all_source_files and all_source_files[source_name]:
                timestamps = [f["timestamp"] for f in all_source_files[source_name][:3]]
                logger.info(f"   {source_name.upper()}: {', '.join(timestamps)}")
        logger.info("Try again in a few minutes or increase --timestamp-tolerance")
        return 1

    logger.info(f"Found {len(common_timestamps)} timestamps to process")

    # ========== STEP 6: PROCESS EACH TIMESTAMP ==========
    processed_count = 0
    last_composite = None

    # Track skip reasons for summary
    skip_reasons = {
        "already_exists": [],
        "insufficient_sources": [],
        "processing_failed": [],
    }

    for common_timestamp, source_files in common_timestamps:
        # Parse timestamp for filename generation
        try:
            dt = datetime.strptime(common_timestamp[:14], "%Y%m%d%H%M%S")
        except ValueError:
            dt = datetime.strptime(common_timestamp[:12], "%Y%m%d%H%M")
        dt_utc = pytz.UTC.localize(dt)
        unix_timestamp = int(dt_utc.timestamp())
        filename = f"{unix_timestamp}.png"
        output_path = output_dir / filename

        # Check if composite already exists locally or in S3 (skip if unchanged)
        exists_locally = output_path.exists()
        exists_in_s3 = False
        if not exists_locally and uploader:
            try:
                exists_in_s3 = uploader.file_exists("composite", filename)
            except Exception:
                pass  # S3 check failed, proceed with processing

        if exists_locally or exists_in_s3:
            skip_reasons["already_exists"].append(common_timestamp)
            continue

        logger.info(f"Processing timestamp {common_timestamp}...")

        # Filter source_files to only include available sources
        source_files = {
            k: v for k, v in source_files.items() if k in available_sources
        }

        if len(source_files) < min_core_sources:
            skip_reasons["insufficient_sources"].append(
                f"{common_timestamp} ({len(source_files)} sources)"
            )
            continue

        # ========== PASS 1: EXTRACT EXTENTS ONLY ==========
        logger.debug("Pass 1: Extracting extents...")
        source_metadata = {}

        for source_name, file_info in source_files.items():
            source, product = available_sources[source_name]
            from_cache = file_info.get("from_cache", False)

            try:
                if from_cache and cache:
                    extent = source.get_extent()
                    source_metadata[source_name] = {
                        "from_cache": True,
                        "dimensions": extent.get("grid_size", [0, 0]),
                    }
                else:
                    extent_info = source.extract_extent_only(file_info["path"])
                    source_metadata[source_name] = {
                        "file_path": file_info["path"],
                        "dimensions": extent_info["dimensions"],
                    }
            except Exception as e:
                logger.warning(f"Failed to extract extent from {source_name}: {e}")
                continue

        if len(source_metadata) < min_core_sources:
            skip_reasons["insufficient_sources"].append(
                f"{common_timestamp} ({len(source_metadata)} valid sources)"
            )
            continue

        # Always use fixed reference extent for consistent dimensions
        combined_extent = REFERENCE_EXTENT.copy()

        # ========== PASS 2: SEQUENTIAL PROCESSING ==========
        logger.debug("Pass 2: Processing sources sequentially...")
        compositor = RadarCompositor(combined_extent, resolution_m=args.resolution)
        sources_processed = 0

        for source_name in source_metadata:
            source, product = available_sources[source_name]
            file_info = source_files[source_name]
            from_cache = file_info.get("from_cache", False)

            try:
                if from_cache and cache:
                    radar_data = cache.get(source_name, file_info["timestamp"], product)
                    if radar_data is None:
                        logger.debug(f"Cache miss for {source_name}, skipping")
                        continue
                else:
                    file_path = source_metadata[source_name]["file_path"]
                    radar_data = source.process_to_array(file_path)

                    if cache:
                        cache.put(
                            source_name,
                            radar_data.get("timestamp", file_info["timestamp"]),
                            product,
                            radar_data,
                        )

                # Export individual source image if requested
                if not args.no_individual:
                    _export_single_source(
                        source_name,
                        radar_data,
                        exporter,
                        unix_timestamp,
                        common_timestamp,
                        args,
                        uploader,
                    )
                    gc.collect()

                # Merge into compositor
                compositor.add_source(source_name, radar_data)
                sources_processed += 1

                # Release memory immediately
                del radar_data
                gc.collect()

                # Delete temp file if not from cache
                if not from_cache and "file_path" in source_metadata.get(source_name, {}):
                    try:
                        Path(source_metadata[source_name]["file_path"]).unlink(missing_ok=True)
                    except Exception:
                        pass

            except Exception as e:
                logger.warning(f"Failed to process {source_name}: {e}")
                continue

        if sources_processed < min_core_sources:
            skip_reasons["processing_failed"].append(
                f"{common_timestamp} ({sources_processed} sources processed)"
            )
            compositor.clear_cache()
            del compositor
            gc.collect()
            continue

        # Get final composite and export
        try:
            composite = compositor.get_composite()

            radar_data_for_export = {
                "data": composite["data"],
                "timestamp": common_timestamp,
                "product": "composite",
                "source": "composite",
                "units": "dBZ",
            }
            exporter.export_png_fast(
                radar_data=radar_data_for_export,
                output_path=output_path,
                extent={"wgs84": composite["extent"]},
                colormap_type="shmu",
            )

            logger.info(
                f"Composite saved: {filename} ({sources_processed} sources)"
            )

            # Upload composite to DigitalOcean Spaces
            if uploader:
                try:
                    uploader.upload_file(output_path, "composite", filename)
                    logger.debug(f"Uploaded composite to Spaces: composite/{filename}")
                except Exception as e:
                    logger.warning(f"Failed to upload composite: {e}")

            processed_count += 1
            last_composite = {
                "extent": {"wgs84": composite["extent"]},
                "data": composite["data"],
            }

            compositor.clear_cache()
            del compositor, composite
            gc.collect()

        except Exception as e:
            logger.error(f"Failed to create composite for {common_timestamp}: {e}")
            import traceback
            traceback.print_exc()

    # Update extent index if processed any composites
    if processed_count > 0:
        if args.update_extent or not (output_dir / "extent_index.json").exists():
            if last_composite is not None:
                _save_extent_index(
                    output_dir, last_composite, list(available_sources.keys()), args.resolution
                )

    # Log processing summary with skip reasons
    total_skipped = sum(len(v) for v in skip_reasons.values())
    logger.info(
        f"Processed {processed_count} composite(s), skipped {total_skipped}",
        extra={"count": processed_count, "skipped": total_skipped},
    )

    if skip_reasons["already_exists"]:
        logger.info(
            f"  Already exist (local/S3): {len(skip_reasons['already_exists'])} "
            f"[{', '.join(skip_reasons['already_exists'][:3])}{'...' if len(skip_reasons['already_exists']) > 3 else ''}]"
        )
    if skip_reasons["insufficient_sources"]:
        logger.warning(
            f"  Insufficient sources: {len(skip_reasons['insufficient_sources'])} "
            f"[{', '.join(skip_reasons['insufficient_sources'][:3])}]"
        )
    if skip_reasons["processing_failed"]:
        logger.warning(
            f"  Processing failed: {len(skip_reasons['processing_failed'])} "
            f"[{', '.join(skip_reasons['processing_failed'][:3])}]"
        )

    return 0


def _export_individual_sources(
    sources_data, exporter, unix_timestamp, timestamp_str, args, uploader=None
):
    """Export individual source images with their native extents.

    Each source is exported to its own directory:
    - DWD -> /tmp/germany/
    - SHMU -> /tmp/slovakia/
    - CHMI -> /tmp/czechia/
    - ARSO -> /tmp/slovenia/
    - OMSZ -> /tmp/hungary/
    - IMGW -> /tmp/poland/

    Args:
        sources_data: List of (source_name, radar_data) tuples
        exporter: PNGExporter instance
        unix_timestamp: Unix timestamp for filename
        timestamp_str: Timestamp string for metadata
        args: CLI arguments
        uploader: Optional SpacesUploader instance for uploading to DO Spaces
    """
    import json

    # Source name to output directory mapping
    source_dirs = {
        "dwd": Path("/tmp/germany/"),
        "shmu": Path("/tmp/slovakia/"),
        "chmi": Path("/tmp/czechia/"),
        "arso": Path("/tmp/slovenia"),
        "omsz": Path("/tmp/hungary/"),
        "imgw": Path("/tmp/poland/"),
    }

    for source_name, radar_data in sources_data:
        # Get output directory for this source
        source_output_dir = source_dirs.get(source_name)
        if source_output_dir is None:
            logger.warning(f"Unknown source {source_name}, skipping individual export")
            continue

        # Create output directory
        source_output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        filename = f"{unix_timestamp}.png"
        output_path = source_output_dir / filename

        # Prepare radar data for export
        radar_data_for_export = {
            "data": radar_data["data"],
            "timestamp": timestamp_str,
            "product": radar_data.get("product", "unknown"),
            "source": source_name,
            "units": "dBZ",
            "metadata": radar_data.get("metadata", {}),
        }

        # Export to PNG with native extent
        logger.info(f"{source_name.upper()} -> {output_path}")
        exporter.export_png_fast(
            radar_data=radar_data_for_export,
            output_path=output_path,
            extent=radar_data["extent"],
            colormap_type="shmu",
        )

        # Upload individual source to DigitalOcean Spaces
        if uploader:
            try:
                uploader.upload_file(output_path, source_name, filename)
                logger.debug(f"Uploaded to Spaces: {source_name}/{filename}")
            except Exception as e:
                logger.warning(f"Failed to upload {source_name}: {e}")

        # Save extent_index.json if it doesn't exist
        extent_file = source_output_dir / "extent_index.json"
        if not extent_file.exists() or args.update_extent:
            extent_data = {
                "metadata": {
                    "title": f"{source_name.upper()} Radar Coverage",
                    "description": f"Native extent for {source_name.upper()} radar data",
                    "source": source_name,
                },
                "extent": radar_data["extent"],
            }
            with open(extent_file, "w") as f:
                json.dump(extent_data, f, indent=2)
            logger.info(f"Saved extent to {extent_file}")


def _export_single_source(
    source_name,
    radar_data,
    exporter,
    unix_timestamp,
    timestamp_str,
    args,
    uploader=None,
):
    """Export a single source image - called during sequential processing.

    This function is used in the two-pass architecture to export individual
    source images while processing sources one at a time.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')
        radar_data: Dictionary from source.process_to_array()
        exporter: PNGExporter instance
        unix_timestamp: Unix timestamp for filename
        timestamp_str: Timestamp string for metadata
        args: CLI arguments
        uploader: Optional SpacesUploader instance for uploading to DO Spaces
    """
    import json

    # Source name to output directory mapping
    source_dirs = {
        "dwd": Path("/tmp/germany/"),
        "shmu": Path("/tmp/slovakia/"),
        "chmi": Path("/tmp/czechia/"),
        "arso": Path("/tmp/slovenia/"),
        "omsz": Path("/tmp/hungary/"),
        "imgw": Path("/tmp/poland/"),
    }

    output_dir = source_dirs.get(source_name)
    if not output_dir:
        return

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename
    output_path = output_dir / f"{unix_timestamp}.png"

    # Prepare radar data for export
    radar_data_for_export = {
        "data": radar_data["data"],
        "timestamp": timestamp_str,
        "product": radar_data.get("metadata", {}).get("product", "unknown"),
        "source": source_name,
        "units": "dBZ",
        "metadata": radar_data.get("metadata", {}),
    }

    # Export to PNG with native extent
    filename = f"{unix_timestamp}.png"
    logger.debug(f"{source_name.upper()} -> {output_path}")
    exporter.export_png_fast(
        radar_data=radar_data_for_export,
        output_path=output_path,
        extent=radar_data["extent"],
        colormap_type="shmu",
    )

    # Upload individual source to DigitalOcean Spaces
    if uploader:
        try:
            uploader.upload_file(output_path, source_name, filename)
            logger.debug(f"Uploaded to Spaces: {source_name}/{filename}")
        except Exception as e:
            logger.warning(f"Failed to upload {source_name}: {e}")

    # Save extent_index.json if it doesn't exist
    extent_file = output_dir / "extent_index.json"
    if not extent_file.exists() or args.update_extent:
        extent_data = {
            "metadata": {
                "title": f"{source_name.upper()} Radar Coverage",
                "description": f"Native extent for {source_name.upper()} radar data",
                "source": source_name,
            },
            "extent": radar_data["extent"],
        }
        with open(extent_file, "w") as f:
            json.dump(extent_data, f, indent=2)


def _process_backload(args, sources, exporter, output_dir, uploader=None):
    """Process historical data from all sources - MEMORY OPTIMIZED TWO-PASS VERSION

    TWO-PASS ARCHITECTURE for ~75% memory reduction:
    - Pass 1: Extract extents only (no data loading) -> Calculate combined extent
    - Pass 2: Process each source sequentially: Load -> Export individual -> Merge -> Delete
    """
    import gc
    from datetime import datetime as dt

    import pytz

    from .cli import parse_time_range
    from .processing.compositor import RadarCompositor

    start, end = parse_time_range(args.from_time, args.to_time, args.hours)
    logger.info(
        f"Backload mode: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')}"
    )

    # Calculate number of 5-minute intervals
    time_diff = end - start
    intervals = int(time_diff.total_seconds() / 300) + 1  # 300 seconds = 5 minutes
    logger.info(f"Expected intervals: {intervals}")

    # Remove ARSO from sources for backload mode (no historical data available)
    if "arso" in sources:
        logger.warning(
            "Excluding ARSO from backload mode (no historical data available)"
        )
        logger.info("   ARSO only provides latest data")
        del sources["arso"]

    # Download data from all sources for the time range
    all_source_files = {}
    for source_name, (source, product) in sources.items():
        logger.info(
            f"Downloading {source_name.upper()} data...", extra={"source": source_name}
        )
        files = source.download_latest(
            count=intervals, products=[product], start_time=start, end_time=end
        )
        if files:
            all_source_files[source_name] = files
            logger.info(
                f"Downloaded {len(files)} files from {source_name.upper()}",
                extra={"source": source_name, "count": len(files)},
            )
        else:
            logger.warning(f"No data from {source_name.upper()}")

    if not all_source_files:
        logger.error("No data downloaded from any source")
        return 1

    # Group files by timestamp
    timestamp_groups = {}
    for source_name, files in all_source_files.items():
        for file_info in files:
            timestamp = file_info["timestamp"]
            if timestamp not in timestamp_groups:
                timestamp_groups[timestamp] = {}
            timestamp_groups[timestamp][source_name] = file_info

    logger.info(f"Found {len(timestamp_groups)} unique timestamps")

    # Process each timestamp with two-pass architecture
    processed_count = 0
    last_composite = None

    for timestamp in sorted(timestamp_groups.keys()):
        source_files = timestamp_groups[timestamp]

        # Skip if not all sources have data for this timestamp
        if len(source_files) < len(sources):
            missing = set(sources.keys()) - set(source_files.keys())
            logger.debug(
                f"Skipping {timestamp} (missing: {', '.join(missing).upper()})"
            )
            continue

        logger.info(f"Processing {timestamp}...")

        # Generate Unix timestamp for filenames
        dt_obj = dt.strptime(timestamp, "%Y%m%d%H%M%S")
        dt_obj = pytz.UTC.localize(dt_obj)
        unix_timestamp = int(dt_obj.timestamp())

        # ========== PASS 1: EXTRACT EXTENTS ONLY ==========
        logger.debug("   Pass 1: Extracting extents...")
        all_extents = []
        source_metadata = {}

        for source_name, file_info in source_files.items():
            source, _product = sources[source_name]
            try:
                extent_info = source.extract_extent_only(file_info["path"])
                all_extents.append(extent_info["extent"]["wgs84"])
                source_metadata[source_name] = {"file_path": file_info["path"]}
            except Exception as e:
                logger.warning(f"Failed to extract extent from {source_name}: {e}")
                continue

        # Get minimum sources required (for resilience)
        min_sources_required = getattr(args, "min_sources", 2)
        if len(all_extents) < min_sources_required:
            logger.warning(
                f"Not enough valid extents for composite ({len(all_extents)} < {min_sources_required}), skipping"
            )
            continue

        # Always use fixed reference extent for consistent dimensions
        combined_extent = REFERENCE_EXTENT.copy()

        # ========== PASS 2: SEQUENTIAL PROCESSING ==========
        logger.debug("   Pass 2: Processing sources sequentially...")
        compositor = RadarCompositor(combined_extent, resolution_m=args.resolution)
        sources_processed = 0

        for source_name in source_metadata:
            source, _product = sources[source_name]
            file_path = source_metadata[source_name]["file_path"]

            try:
                # Load ONE source at a time
                radar_data = source.process_to_array(file_path)

                # Export individual source image if requested
                if not args.no_individual:
                    _export_single_source(
                        source_name,
                        radar_data,
                        exporter,
                        unix_timestamp,
                        timestamp,
                        args,
                        uploader,
                    )

                # Merge into compositor
                compositor.add_source(source_name, radar_data)
                sources_processed += 1

                # CRITICAL: Release memory immediately
                del radar_data
                gc.collect()

                # Delete temp file
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception:
                    pass

            except Exception as e:
                logger.warning(f"Failed to process {source_name}: {e}")

        if sources_processed < 2:
            logger.warning("Not enough valid sources for composite, skipping")
            compositor.clear_cache()
            del compositor
            gc.collect()
            continue

        # Get final composite and export
        try:
            composite = compositor.get_composite()

            filename = f"{unix_timestamp}.png"
            output_path = output_dir / filename

            logger.info(f"Exporting composite to {filename}...")
            radar_data_for_export = {
                "data": composite["data"],
                "timestamp": timestamp,
                "product": "composite",
                "source": "composite",
                "units": "dBZ",
            }
            exporter.export_png_fast(
                radar_data=radar_data_for_export,
                output_path=output_path,
                extent={"wgs84": composite["extent"]},
                colormap_type="shmu",
            )

            # Upload composite to DigitalOcean Spaces
            if uploader:
                try:
                    uploader.upload_file(output_path, "composite", filename)
                    logger.debug(f"Uploaded composite to Spaces: composite/{filename}")
                except Exception as e:
                    logger.warning(f"Failed to upload composite: {e}")

            processed_count += 1
            last_composite = {
                "extent": {"wgs84": composite["extent"]},
                "data": composite["data"],
            }

            # Cleanup
            compositor.clear_cache()
            del compositor, composite
            gc.collect()

        except Exception as e:
            logger.error(f"Failed to create composite: {e}")
            import traceback

            traceback.print_exc()

    logger.info(
        f"Processed {processed_count} composites", extra={"count": processed_count}
    )

    # Update extent index if requested
    if args.update_extent or processed_count > 0:
        if last_composite is not None:
            _save_extent_index(
                output_dir, last_composite, list(sources.keys()), args.resolution
            )

    return 0


def _save_extent_index(output_dir, composite, source_names, resolution):
    """Save extent index JSON"""
    import json
    from datetime import datetime

    logger.info("Generating extent information...")

    extent_data = {
        "metadata": {
            "title": "Composite Radar Coverage",
            "description": f"Combined extent from sources: {', '.join(source_names)}",
            "generated": datetime.now().isoformat() + "Z",
            "sources": source_names,
            "resolution_m": resolution,
        },
        "extent": composite["extent"],
    }

    extent_path = output_dir / "extent_index.json"
    with open(extent_path, "w") as f:
        json.dump(extent_data, f, indent=2)

    logger.info(f"Extent info saved to {extent_path}")
