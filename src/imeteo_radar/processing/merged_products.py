#!/usr/bin/env python3
"""
Merged Radar Products System

Handles creation of merged radar products from multiple sources (SHMU, DWD, future sources).
Designed for extensibility and production use.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ..core.logging import get_logger
from ..sources.dwd import DWDRadarSource
from ..sources.shmu import SHMURadarSource
from ..utils.storage import TimePartitionedStorage
from .exporter import PNGExporter
from .merger import RadarMerger

logger = get_logger(__name__)


class MergedProductsManager:
    """
    Manages creation of merged radar products from multiple sources

    Features:
    - Extensible source registration system
    - Automatic timestamp matching across sources
    - Multiple merging strategies (average, priority, max, weighted)
    - PNG export with hierarchical directory structure
    - Configurable overlap regions and priorities
    """

    def __init__(self, extent_config_path: Path | None = None):
        self.merger = RadarMerger()
        self.exporter = PNGExporter()
        self.storage = TimePartitionedStorage()

        # Load extent configuration
        if extent_config_path is None:
            extent_config_path = Path("config/extent_index.json")

        self.extent_config = self._load_extent_config(extent_config_path)

        # Register available sources
        self.sources = self._register_sources()

        # Default merge configuration
        self.merge_config = {
            "strategies": ["average", "priority", "max"],
            "default_strategy": "average",
            "priority_order": ["shmu", "dwd"],  # SHMU has priority over DWD
            "target_resolution": None,  # Auto-determine from highest resolution source
            "time_tolerance_minutes": 2.5,  # Match timestamps within 2.5 minutes
            "products": {
                "maximum_reflectivity": {
                    "description": "Maximum reflectivity composite using SHMU zmax + DWD dmax",
                    "shmu_products": ["zmax"],  # SHMU maximum reflectivity
                    "dwd_products": ["dmax"],  # DWD maximum reflectivity
                    "priority": "shmu",  # SHMU data takes priority for reflectivity
                }
            },
        }

    def _load_extent_config(self, config_path: Path) -> dict[str, Any]:
        """Load extent and source configuration"""
        try:
            with open(config_path) as f:
                config = json.load(f)
            logger.info(f"Loaded extent config: {config_path}")
            return config
        except Exception as e:
            logger.warning(f"Failed to load extent config: {e}")
            return self._create_default_config()

    def _create_default_config(self) -> dict[str, Any]:
        """Create default configuration if config file not found"""
        return {
            "sources": {
                "shmu": {
                    "extent": {
                        "wgs84": {
                            "west": 13.6,
                            "east": 23.8,
                            "south": 46.0,
                            "north": 50.7,
                        },
                        "grid_size": [1560, 2270],
                    }
                },
                "dwd": {
                    "extent": {
                        "wgs84": {
                            "west": 3.0,
                            "east": 17.0,
                            "south": 47.0,
                            "north": 56.0,
                        },
                        "grid_size": [4800, 4400],
                    }
                },
            }
        }

    def _register_sources(self) -> dict[str, Any]:
        """Register radar sources with metadata"""
        return {
            "shmu": {
                "class": SHMURadarSource,
                "name": "Slovak Hydrometeorological Institute",
                "priority": 1,  # Higher number = higher priority
                "products": ["zmax", "cappi2km"],
                "enabled": True,
            },
            "dwd": {
                "class": DWDRadarSource,
                "name": "German Weather Service",
                "priority": 0,
                "products": ["dmax"],
                "enabled": True,
            },
            # Future sources can be added here:
            # 'imgw': {  # Polish Institute
            #     'class': IMGWRadarSource,
            #     'name': 'Institute of Meteorology and Water Management',
            #     'priority': 0,
            #     'products': ['reflectivity'],
            #     'enabled': True
            # }
        }

    def get_available_sources(self) -> list[str]:
        """Get list of enabled source names"""
        return [name for name, info in self.sources.items() if info["enabled"]]

    def find_matching_timestamps(
        self, sources: list[str], time_range_hours: int = 1, min_sources: int = 2
    ) -> list[tuple[datetime, dict[str, list[str]]]]:
        """
        Find timestamps where data is available from multiple sources

        Args:
            sources: List of source names to check
            time_range_hours: Hours back from now to check
            min_sources: Minimum number of sources required for a valid timestamp

        Returns:
            List of (timestamp, {source: [file_paths]}) tuples
        """
        logger.info(
            f"Finding matching timestamps across sources: {sources}",
            extra={"operation": "find"},
        )

        # Collect all available timestamps from each source
        source_timestamps = {}

        for source_name in sources:
            if (
                source_name not in self.sources
                or not self.sources[source_name]["enabled"]
            ):
                logger.warning(
                    f"Source {source_name} not available",
                    extra={"source": source_name},
                )
                continue

            try:
                # Look for files in time-partitioned storage
                timestamps = self._get_source_timestamps(source_name, time_range_hours)
                source_timestamps[source_name] = timestamps
                logger.info(
                    f"Found {len(timestamps)} timestamps",
                    extra={"source": source_name, "count": len(timestamps)},
                )

            except Exception as e:
                logger.error(
                    f"Error getting timestamps for {source_name}: {e}",
                    extra={"source": source_name},
                )
                continue

        # Find overlapping timestamps within tolerance
        matching = self._find_timestamp_matches(source_timestamps, min_sources)

        logger.info(
            f"Found {len(matching)} matching timestamps with {min_sources}+ sources",
            extra={"count": len(matching)},
        )
        return matching

    def _get_source_timestamps(
        self, source_name: str, hours_back: int
    ) -> dict[datetime, list[str]]:
        """Get available timestamps and file paths for a source"""
        timestamps = {}

        # Look in storage directory structure (time-partitioned)
        storage_path = Path("storage") / source_name
        if storage_path.exists():
            # Check recent time partitions
            now = datetime.utcnow()
            for hour_offset in range(hours_back + 1):
                check_time = now - timedelta(hours=hour_offset)

                # Build path: storage/source/YYYY/MM/DD/HH/
                time_path = (
                    storage_path
                    / str(check_time.year)
                    / f"{check_time.month:02d}"
                    / f"{check_time.day:02d}"
                    / f"{check_time.hour:02d}"
                )

                if time_path.exists():
                    # Find HDF files (processed data)
                    # For SHMU, prioritize zmax (maximum reflectivity) files
                    if source_name == "shmu":
                        # First try to find zmax files, then fallback to any .hdf
                        zmax_files = list(time_path.glob("*zmax*.hdf"))
                        if zmax_files:
                            hdf_files = zmax_files
                        else:
                            hdf_files = list(time_path.glob("*.hdf"))
                    else:
                        hdf_files = list(time_path.glob("*.hdf"))

                    for hdf_file in hdf_files:
                        # Extract timestamp from filename
                        try:
                            filename = hdf_file.stem
                            timestamp_str = filename.split("_")[
                                -1
                            ]  # Last part should be timestamp
                            dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

                            if dt not in timestamps:
                                timestamps[dt] = []
                            timestamps[dt].append(str(hdf_file))

                        except Exception:
                            continue  # Skip files that don't match expected format

        # Use unified storage for all sources including DWD
        if source_name == "dwd":
            # Get DWD files from unified storage system
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
            cutoff_timestamp = cutoff_time.strftime("%Y%m%d%H%M%S")

            # Get all DWD files from the last hours_back hours
            dwd_files = self.storage.get_files("dwd", start_time=cutoff_timestamp)

            for file_info in dwd_files:
                try:
                    # Parse timestamp from unified storage format
                    timestamp_str = file_info[
                        "timestamp"
                    ]  # Already in YYYYMMDDHHMM00 format
                    dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

                    if dt not in timestamps:
                        timestamps[dt] = []
                    timestamps[dt].append(file_info["path"])

                except Exception:
                    continue  # Skip files that don't match expected format

        return timestamps

    def _find_timestamp_matches(
        self, source_timestamps: dict[str, dict[datetime, list[str]]], min_sources: int
    ) -> list[tuple[datetime, dict[str, list[str]]]]:
        """Find timestamps where multiple sources have data within tolerance"""

        tolerance = timedelta(minutes=self.merge_config["time_tolerance_minutes"])
        matches = []

        # Get all unique timestamps across sources
        all_timestamps = set()
        for source_data in source_timestamps.values():
            all_timestamps.update(source_data.keys())

        # For each timestamp, find matching timestamps in other sources
        for base_timestamp in sorted(all_timestamps):
            matched_sources = {}

            for source_name, source_data in source_timestamps.items():
                # Find timestamps within tolerance
                for timestamp, files in source_data.items():
                    if abs(timestamp - base_timestamp) <= tolerance:
                        if source_name not in matched_sources:
                            matched_sources[source_name] = []
                        matched_sources[source_name].extend(files)

            # Only include if we have enough sources
            if len(matched_sources) >= min_sources:
                matches.append((base_timestamp, matched_sources))

        return matches

    def create_merged_products(
        self,
        sources: list[str] | None = None,
        time_range_hours: int = 1,
        strategies: list[str] | None = None,
        output_dir: Path | None = None,
        export_png: bool = True,
    ) -> dict[str, Any]:
        """
        Create merged radar products for recent timestamps

        Args:
            sources: Source names to merge (None = all available)
            time_range_hours: Hours back to process
            strategies: Merge strategies to use
            output_dir: Output directory (None = outputs/merged)
            export_png: Whether to export PNG files

        Returns:
            Dictionary with results and metadata
        """

        if sources is None:
            sources = self.get_available_sources()

        if strategies is None:
            strategies = [self.merge_config["default_strategy"]]

        if output_dir is None:
            output_dir = Path("outputs/merged")

        logger.info(
            f"Creating merged products from sources: {sources}",
            extra={"operation": "merge"},
        )
        logger.debug(
            f"Time range: {time_range_hours} hours, Strategies: {strategies}",
        )

        # Find matching timestamps
        matches = self.find_matching_timestamps(sources, time_range_hours)

        if not matches:
            logger.warning("No matching timestamps found")
            return {"success": False, "message": "No matching timestamps found"}

        results = {
            "success": True,
            "processed_timestamps": [],
            "failed_timestamps": [],
            "output_files": [],
        }

        # Process each matching timestamp
        for timestamp, source_files in matches:
            try:
                logger.info(
                    f"Processing timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
                    extra={"operation": "process", "timestamp": timestamp.strftime('%Y%m%d%H%M%S')},
                )

                # Load source data
                timestamp_data = self._load_timestamp_data(timestamp, source_files)
                if not timestamp_data:
                    results["failed_timestamps"].append(timestamp)
                    continue

                # Get source extent information
                source_data = {
                    name: self.extent_config["sources"][name]
                    for name in timestamp_data.keys()
                    if name in self.extent_config.get("sources", {})
                }

                # Create merged products for each strategy
                for strategy in strategies:
                    merged = self._create_single_merged_product(
                        timestamp, timestamp_data, source_data, strategy
                    )

                    if merged:
                        if export_png:
                            png_path = self._export_merged_png(
                                merged, timestamp, strategy, output_dir
                            )
                            if png_path:
                                results["output_files"].append(png_path)

                        results["processed_timestamps"].append(timestamp)
                    else:
                        results["failed_timestamps"].append(timestamp)

            except Exception as e:
                logger.error(
                    f"Failed to process {timestamp}: {e}",
                    extra={"timestamp": timestamp.strftime('%Y%m%d%H%M%S')},
                )
                results["failed_timestamps"].append(timestamp)
                continue

        success_count = len(results["processed_timestamps"])
        total_count = len(matches)

        logger.info("Merged products creation complete!")
        logger.info(
            f"Processed: {success_count}/{total_count} timestamps",
            extra={"count": success_count},
        )
        logger.info(
            f"Generated: {len(results['output_files'])} output files",
            extra={"count": len(results['output_files'])},
        )

        return results

    def _load_timestamp_data(
        self, timestamp: datetime, source_files: dict[str, list[str]]
    ) -> dict[str, list[dict[str, Any]]]:
        """Load radar data for a timestamp from all sources"""

        timestamp_data = {}

        for source_name, file_paths in source_files.items():
            if source_name not in self.sources:
                continue

            source_class = self.sources[source_name]["class"]
            source_instance = source_class()

            source_data = []

            for file_path in file_paths:
                try:
                    # Process the HDF file to get radar data
                    radar_data = source_instance.process_to_array(file_path)
                    if radar_data and "data" in radar_data:
                        # Add extent information
                        radar_data["extent"] = source_instance.get_extent()
                        radar_data["source_file"] = file_path
                        source_data.append(radar_data)

                except Exception as e:
                    logger.warning(f"Failed to load {file_path}: {e}")
                    continue

            if source_data:
                timestamp_data[source_name] = source_data

        return timestamp_data

    def _create_single_merged_product(
        self,
        timestamp: datetime,
        timestamp_data: dict[str, list[dict[str, Any]]],
        source_data: dict[str, dict[str, Any]],
        strategy: str,
    ) -> dict[str, Any] | None:
        """Create a single merged product for a timestamp"""

        try:
            # Use the merger to combine data
            merged = self.merger.merge_sources(
                timestamp_data=timestamp_data,
                source_data=source_data,
                strategy=strategy,
                target_resolution=self.merge_config["target_resolution"],
            )

            if merged:
                logger.info(
                    f"Created merged product using '{strategy}' strategy",
                    extra={"operation": "merge"},
                )

            return merged

        except Exception as e:
            logger.error(f"Merge failed for {strategy}: {e}")
            return None

    def _export_merged_png(
        self,
        merged_data: dict[str, Any],
        timestamp: datetime,
        strategy: str,
        output_dir: Path,
    ) -> Path | None:
        """Export merged data to PNG with hierarchical structure"""

        try:
            # Create hierarchical directory: outputs/merged/YYYY/MM/DD/
            time_dir = (
                output_dir
                / str(timestamp.year)
                / f"{timestamp.month:02d}"
                / f"{timestamp.day:02d}"
            )
            time_dir.mkdir(parents=True, exist_ok=True)

            # Generate filename
            timestamp_str = timestamp.strftime("%Y%m%d%H%M%S")
            filename = f"merged_{strategy}_{timestamp_str}.png"
            output_path = time_dir / filename

            # Export using fast PNG export
            extent = merged_data["extent"]
            self.exporter.export_png_fast(merged_data, output_path, extent)

            logger.info(
                f"Saved: {output_path}",
                extra={"operation": "export"},
            )
            return output_path

        except Exception as e:
            logger.error(f"PNG export failed: {e}")
            return None


def create_merged_products_cli(
    sources: list[str],
    time_range_hours: int = 1,
    strategies: list[str] = None,
    output_dir: str = "outputs/merged",
) -> int:
    """
    CLI interface for creating merged products

    Returns:
        Exit code (0 = success, 1 = failure)
    """

    try:
        manager = MergedProductsManager()

        results = manager.create_merged_products(
            sources=sources,
            time_range_hours=time_range_hours,
            strategies=strategies,
            output_dir=Path(output_dir),
            export_png=True,
        )

        if results["success"] and results["processed_timestamps"]:
            return 0  # Success
        else:
            return 1  # No data processed

    except Exception as e:
        logger.error(f"Merged products creation failed: {e}")
        return 1
