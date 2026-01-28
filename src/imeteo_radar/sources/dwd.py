#!/usr/bin/env python3
"""
DWD (German Weather Service) Radar Source

Handles downloading and processing of DWD radar composite data.
"""

import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import requests

from ..core.alerts import get_alert_manager
from ..core.base import RadarSource, lonlat_to_mercator
from ..core.logging import get_logger
from ..core.projection import projection_handler
from ..core.retry import retry_with_backoff
from ..utils.hdf5_utils import (
    decode_hdf5_attrs,
    get_quantity_units,
    get_scaling_params,
    scale_radar_data,
)
from ..utils.parallel_download import (
    create_download_result,
    create_error_result,
    execute_parallel_downloads,
)
from ..utils.timestamps import (
    TimestampFormat,
    filter_timestamps_by_range,
    generate_timestamp_candidates,
)

logger = get_logger(__name__)
alert_manager = get_alert_manager()


class DWDRadarSource(RadarSource):
    """DWD Radar data source implementation"""

    def __init__(self):
        super().__init__("dwd")
        self.base_url = "https://opendata.dwd.de/weather/radar/composite"

        # DWD product mapping (to be verified)
        self.product_mapping = {
            "dmax": "dmax",  # Maximum reflectivity (equivalent to SHMU's ZMAX)
            "pg": "pg",  # Possible CAPPI product (to be verified)
            "hg": "hg",  # Possible CAPPI product (to be verified)
            "hx": "hx",  # Possible CAPPI product (to be verified)
        }

        # Product metadata (preliminary - will be updated after analysis)
        self.product_info = {
            "dmax": {
                "name": "Maximum Reflectivity",
                "units": "dBZ",
                "description": "Column maximum reflectivity",
            },
            "pg": {
                "name": "PG Composite",
                "units": "unknown",
                "description": "DWD composite product PG",
            },
            "hg": {
                "name": "HG Composite",
                "units": "unknown",
                "description": "DWD composite product HG",
            },
            "hx": {
                "name": "HX Composite",
                "units": "unknown",
                "description": "DWD composite product HX",
            },
        }
        # temp_files is initialized in base class

    def get_available_products(self) -> list[str]:
        """Get list of available DWD radar products"""
        return list(self.product_mapping.keys())

    def get_product_metadata(self, product: str) -> dict[str, Any]:
        """Get metadata for a specific DWD product"""
        if product in self.product_info:
            return {
                "product": product,
                "source": self.name,
                **self.product_info[product],
            }
        return super().get_product_metadata(product)

    @retry_with_backoff(
        max_retries=2,
        base_delay=1.0,
        exceptions=(requests.RequestException,),
        on_retry=lambda attempt, delay, e: logger.warning(
            f"DWD directory listing retry {attempt}/2 after {delay:.1f}s: {e}"
        ),
    )
    def _get_available_timestamps_from_server(self, product: str) -> list[str]:
        """Get actually available timestamps by parsing DWD directory listing"""

        directory_url = f"{self.base_url}/{product}/"
        logger.debug(f"Fetching DWD directory: {directory_url}")
        response = requests.get(directory_url, timeout=15)
        response.raise_for_status()

        # Parse HTML directory listing to extract timestamps
        import re

        pattern = rf"composite_{product}_(\d{{8}}_\d{{4}})-hd5"
        matches = re.findall(pattern, response.text)

        if matches:
            # Sort by timestamp (newest first)
            timestamps = sorted(set(matches), reverse=True)
            logger.debug(
                f"Found {len(timestamps)} available timestamps: {timestamps[:3]}..."
            )
            alert_manager.record_success("dwd")
            return timestamps
        else:
            logger.warning("No timestamp patterns found in DWD directory listing")
            return []


    @retry_with_backoff(
        max_retries=2,
        base_delay=0.5,
        exceptions=(requests.RequestException,),
    )
    def _check_timestamp_availability(self, timestamp: str, product: str) -> bool:
        """Check if DWD data is available with robust error handling"""
        url = self._get_product_url(timestamp, product)

        # Try HEAD request first
        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                return True
        except requests.RequestException:
            pass

        # Fallback to GET request with range header to minimize data transfer
        headers = {"Range": "bytes=0-1024"}  # Just get first 1KB
        response = requests.get(url, headers=headers, timeout=10)
        return response.status_code in [200, 206]  # 206 = Partial Content

    def _get_product_url(self, timestamp: str, product: str) -> str:
        """Generate URL for DWD product"""
        if product not in self.product_mapping:
            raise ValueError(f"Unknown product: {product}")

        # Special case for latest data
        if timestamp == "LATEST":
            return f"{self.base_url}/{product}/composite_{product}_LATEST-hd5"

        # DWD URL format: composite_{product}_{YYYYMMDD_HHMM}-hd5
        return f"{self.base_url}/{product}/composite_{product}_{timestamp}-hd5"

    def _download_single_file(self, timestamp: str, product: str) -> dict[str, Any]:
        """Download a single DWD radar file (for parallel processing)"""
        if product not in self.product_mapping:
            return create_error_result(timestamp, product, f"Unknown product: {product}")

        try:
            result = self._download_single_file_with_retry(timestamp, product)
            alert_manager.record_success("dwd")
            return result
        except Exception as e:
            alert_manager.record_failure("dwd", str(e))
            logger.warning(f"DWD download failed for {timestamp}: {e}")
            return create_error_result(timestamp, product, str(e))

    @retry_with_backoff(
        max_retries=3,
        base_delay=1.0,
        max_delay=10.0,
        exceptions=(requests.RequestException,),
        on_retry=lambda attempt, delay, e: logger.warning(
            f"DWD download retry {attempt}/3 after {delay:.1f}s: {e}"
        ),
    )
    def _download_single_file_with_retry(
        self, timestamp: str, product: str
    ) -> dict[str, Any]:
        """Download a single DWD radar file with retry logic"""
        url = self._get_product_url(timestamp, product)
        # Download to temporary file
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Create a proper temporary file
        with tempfile.NamedTemporaryFile(
            suffix=f"_dwd_{product}_{timestamp}.hdf", delete=False
        ) as temp_file:
            temp_file.write(response.content)
            temp_path = Path(temp_file.name)

        # Handle LATEST timestamp specially - extract actual timestamp from HDF5 file
        if timestamp == "LATEST":
            # Extract actual timestamp from the downloaded HDF5 file
            try:
                with h5py.File(temp_path, "r") as f:
                    # Try to get timestamp from ODIM_H5 metadata
                    if (
                        "what" in f
                        and "date" in f["what"].attrs
                        and "time" in f["what"].attrs
                    ):
                        date_str = f["what"].attrs["date"]
                        time_str = f["what"].attrs["time"]
                        if isinstance(date_str, bytes):
                            date_str = date_str.decode("utf-8")
                        if isinstance(time_str, bytes):
                            time_str = time_str.decode("utf-8")
                        # Convert YYYYMMDD and HHMMSS to our format
                        normalized_timestamp = f"{date_str}{time_str[:4]}00"
                    else:
                        # Fallback to current time if metadata not found
                        normalized_timestamp = datetime.now().strftime("%Y%m%d%H%M00")
            except Exception as e:
                logger.warning(f"Could not extract timestamp from LATEST file: {e}")
                normalized_timestamp = datetime.now().strftime("%Y%m%d%H%M00")
        else:
            normalized_timestamp = (
                timestamp.replace("_", "") + "00"
            )  # Convert to YYYYMMDDHHMM00 (14-digit)

        # Check if we already have this file in current session
        cache_key = f"{product}_{normalized_timestamp}"
        if cache_key in self.temp_files and os.path.exists(self.temp_files[cache_key]):
            # File already downloaded in this session
            temp_path.unlink()  # Remove the duplicate
            return create_download_result(
                timestamp=normalized_timestamp,
                product=product,
                path=self.temp_files[cache_key],
                url=url,
                cached=True,
            )

        # Store temp file path for this session
        self.temp_files[cache_key] = str(temp_path)

        return create_download_result(
            timestamp=normalized_timestamp,
            product=product,
            path=str(temp_path),
            url=url,
            cached=False,
        )

    def get_available_timestamps(
        self,
        count: int = 8,
        products: list[str] = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[str]:
        """Get list of available DWD timestamps WITHOUT downloading.

        Args:
            count: Maximum number of timestamps to return
            products: List of products to check (default: ['dmax'])
            start_time: Optional start time for filtering
            end_time: Optional end time for filtering

        Returns:
            List of timestamp strings in YYYYMMDD_HHMM format, newest first
        """
        if products is None:
            products = ["dmax"]

        # Strategy 1: Try parsing directory listing first (most reliable)
        available_timestamps = []
        for product in products:
            server_timestamps = self._get_available_timestamps_from_server(product)
            if server_timestamps:
                # Filter by time range if specified
                if start_time and end_time:
                    filtered_timestamps = filter_timestamps_by_range(
                        server_timestamps, start_time, end_time, parse_format=TimestampFormat.UNDERSCORE
                    )
                    available_timestamps = filtered_timestamps[:count]
                else:
                    available_timestamps = server_timestamps[:count]
                break  # Use first working product for timestamp discovery

        # Strategy 2: Fallback to generated timestamps if directory parsing fails
        if not available_timestamps:
            test_timestamps = generate_timestamp_candidates(
                count=count * 4,
                interval_minutes=5,
                delay_minutes=15,
                format_str=TimestampFormat.UNDERSCORE,
                timezone_offset_hours=1,
            )

            for timestamp in test_timestamps:
                if len(available_timestamps) >= count:
                    break

                if self._check_timestamp_availability(timestamp, "dmax"):
                    available_timestamps.append(timestamp)

        # Remove duplicates and limit to requested count
        return list(dict.fromkeys(available_timestamps))[:count]

    # download_timestamps is inherited from RadarSource base class

    def download_latest(
        self,
        count: int = 1,
        products: list[str] = None,
        use_latest: bool = True,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Download latest DWD radar data

        Args:
            count: Maximum number of timestamps to download
            products: List of products to download (default: ['dmax'])
            use_latest: Use LATEST endpoint for single file downloads
            start_time: Optional start time for filtering (timezone-aware datetime)
            end_time: Optional end time for filtering (timezone-aware datetime)
        """

        if products is None:
            products = ["dmax"]  # Default to maximum reflectivity

        # If only fetching one file and use_latest is True, use LATEST endpoint
        if count == 1 and use_latest and not start_time and not end_time:
            logger.info("Using LATEST endpoint for most recent data...")
            downloaded_files = []
            for product in products:
                result = self._download_single_file("LATEST", product)
                if result["success"]:
                    downloaded_files.append(result)
                    logger.info(f"Downloaded latest {product} data")
                else:
                    logger.error(
                        f"Failed to download latest {product}: {result.get('error', 'Unknown error')}"
                    )
            return downloaded_files

        logger.info(f"Finding last {count} available DWD timestamps...")

        # Get available timestamps
        available_timestamps = self.get_available_timestamps(
            count=count,
            products=products,
            start_time=start_time,
            end_time=end_time,
        )

        if not available_timestamps:
            logger.error("No available DWD timestamps found")
            return []

        # Download the timestamps
        return self.download_timestamps(available_timestamps, products)

    def process_to_array(self, file_path: str) -> dict[str, Any]:
        """Process DWD HDF5 file to array with metadata"""

        try:
            with h5py.File(file_path, "r") as f:
                # DWD files might have different structure than SHMU
                # Need to explore the file structure first

                logger.debug(f"Analyzing DWD file structure: {file_path}")
                self._log_hdf5_structure(f)

                # Try to find the main dataset
                data = None
                main_dataset_paths = [
                    "dataset1/data1/data",  # ODIM_H5 standard
                    "dataset/data",  # Alternative
                    "data",  # Simple structure
                    "precipitation",  # Possible name
                    "reflectivity",  # Possible name
                ]

                for path in main_dataset_paths:
                    try:
                        data = f[path][:]
                        logger.debug(f"Found data at: {path}")
                        break
                    except KeyError:
                        continue

                if data is None:
                    # Try to find any large array
                    data = self._find_main_dataset(f)

                if data is None:
                    raise ValueError("Could not find main data array in DWD file")

                # Get metadata attributes
                metadata = self._extract_dwd_metadata(f, file_path)

                # Get scaling attributes from data1/what
                data_what_attrs = {}
                if "dataset1/data1/what" in f:
                    data_what_attrs = decode_hdf5_attrs(dict(f["dataset1/data1/what"].attrs))

                # Get scaling parameters using shared utility
                scaling = get_scaling_params(
                    data_what_attrs,
                    default_nodata=65535,
                    default_undetect=0,
                )

                logger.debug(
                    f"DWD scaling - gain: {scaling['gain']}, offset: {scaling['offset']}, nodata: {scaling['nodata']}, undetect: {scaling['undetect']}"
                )

                # Scale radar data using shared utility
                scaled_data = scale_radar_data(
                    data,
                    scaling["gain"],
                    scaling["offset"],
                    scaling["nodata"],
                    scaling["undetect"],
                    handle_uint8=False,  # DWD doesn't use uint8
                )

                logger.debug(
                    f"Scaled data range: {np.nanmin(scaled_data):.2f} to {np.nanmax(scaled_data):.2f}"
                )

                # Get geographic information from where attributes
                where_attrs = {}
                if "where" in f:
                    where_attrs = decode_hdf5_attrs(dict(f["where"].attrs))
                elif "dataset1/where" in f:
                    where_attrs = decode_hdf5_attrs(dict(f["dataset1/where"].attrs))

                # Get projection definition
                proj_def = None
                try:
                    if "where" in f and "projdef" in f["where"].attrs:
                        proj_def = f["where"].attrs["projdef"]
                    elif (
                        "dataset1/where" in f and "projdef" in f["dataset1/where"].attrs
                    ):
                        proj_def = f["dataset1/where"].attrs["projdef"]

                    if isinstance(proj_def, bytes):
                        proj_def = proj_def.decode("utf-8")

                    logger.debug(f"DWD projection: {proj_def}")
                except Exception:
                    logger.warning(
                        "No projection definition found - using corner approximation"
                    )

                # MEMORY OPTIMIZATION: Calculate extent from corners only (no meshgrids!)
                # Old method created 2D meshgrids consuming ~322 MB
                # New method: only use corner coordinates, ~0 MB overhead
                try:
                    extent_bounds = projection_handler.calculate_dwd_extent(
                        where_attrs, proj_def
                    )
                    logger.debug(
                        f"DWD bounds: W={extent_bounds['west']:.2f}, E={extent_bounds['east']:.2f}, "
                        f"N={extent_bounds['north']:.2f}, S={extent_bounds['south']:.2f}"
                    )

                except Exception as e:
                    logger.warning(f"Extent calculation failed: {e}")
                    # Fallback to default bounds
                    extent_bounds = {
                        "west": 3.0,
                        "east": 17.0,
                        "south": 47.0,
                        "north": 56.0,
                    }

                timestamp = self._extract_timestamp_from_path(file_path)

                return {
                    "data": scaled_data,
                    "coordinates": None,  # No longer generated to save memory
                    "projection": {
                        "type": "stereographic",
                        "proj_def": proj_def,
                        "where_attrs": where_attrs,
                    },
                    "metadata": {
                        "product": metadata.get("product", "UNKNOWN"),
                        "quantity": metadata.get("quantity", "UNKNOWN"),
                        "timestamp": timestamp,
                        "source": "DWD",
                        "units": get_quantity_units(metadata.get("quantity", "UNKNOWN")),
                        "nodata_value": np.nan,
                    },
                    "extent": {"wgs84": extent_bounds},
                    "dimensions": data.shape,
                    "timestamp": timestamp,
                }

        except Exception as e:
            raise RuntimeError(f"Failed to process DWD file {file_path}: {e}")

    def _extract_dwd_metadata(self, hdf_file, file_path: str) -> dict[str, Any]:
        """Extract metadata from DWD HDF5 file"""
        metadata = {}

        # Try to find standard ODIM_H5 attributes
        try:
            if "what" in hdf_file:
                what_attrs = decode_hdf5_attrs(dict(hdf_file["what"].attrs))
                metadata.update(what_attrs)
        except Exception:
            pass

        # Extract product from filename
        filename = Path(file_path).name
        if "dmax" in filename:
            metadata["product"] = "dmax"
            metadata["quantity"] = "DBZH"
            metadata["units"] = "dBZ"
        elif "pg" in filename:
            metadata["product"] = "pg"
            metadata["units"] = "unknown"
        elif "hg" in filename:
            metadata["product"] = "hg"
            metadata["units"] = "unknown"

        return metadata

    def _extract_timestamp_from_path(self, file_path: str) -> str:
        """Extract timestamp from DWD file path and normalize to 14-digit format"""
        filename = Path(file_path).name
        # Extract YYYYMMDD_HHMM pattern
        import re

        match = re.search(r"(\d{8})_(\d{4})", filename)
        if match:
            date_part = match.group(1)
            time_part = match.group(2)
            return f"{date_part}{time_part}00"  # Normalize to 14-digit: YYYYMMDDHHMM00
        return "unknown"

    def get_extent(self) -> dict[str, Any]:
        """Get DWD radar coverage extent"""

        # DWD radar coverage (actual data bounds - larger than Germany proper)
        wgs84 = {
            "west": 2.5,  # Extended to cover actual data range
            "east": 18.0,  # Extended to cover actual data range
            "south": 45.5,  # Extended to cover actual data range
            "north": 56.0,
        }

        # Convert to Web Mercator
        x_min, y_min = lonlat_to_mercator(wgs84["west"], wgs84["south"])
        x_max, y_max = lonlat_to_mercator(wgs84["east"], wgs84["north"])

        return {
            "wgs84": wgs84,
            "mercator": {
                "x_min": x_min,
                "x_max": x_max,
                "y_min": y_min,
                "y_max": y_max,
                "bounds": [x_min, y_min, x_max, y_max],  # [xmin, ymin, xmax, ymax]
            },
            "projection": "EPSG:3857",
            "grid_size": [900, 900],  # DWD standard grid
            "resolution_m": [1000, 1000],  # 1km resolution
        }

    def _log_hdf5_structure(self, hdf_file, level=0, max_items=10):
        """Helper method to log HDF5 file structure for debugging"""
        try:
            items = list(hdf_file.items())[:max_items]
            for key, value in items:
                indent = "  " * level
                if hasattr(value, "keys"):  # It's a group
                    logger.debug(f"{indent}{key}/ (group)")
                    if level < 2:  # Limit depth to avoid spam
                        self._log_hdf5_structure(value, level + 1, max_items)
                else:  # It's a dataset
                    logger.debug(
                        f"{indent}{key} (dataset): shape={getattr(value, 'shape', '?')}"
                    )
        except Exception as e:
            logger.debug(f"  Error logging structure: {e}")

    def extract_extent_only(self, file_path: str) -> dict[str, Any]:
        """Extract extent from DWD HDF5 without loading data array.

        MEMORY OPTIMIZATION: Reads only HDF5 metadata (~100 bytes) instead of
        loading the full data array (~160 MB for DWD 1900x1900 grid).

        Args:
            file_path: Path to DWD HDF5 file

        Returns:
            Dictionary with extent and dimensions
        """
        try:
            with h5py.File(file_path, "r") as f:
                # Read only projection metadata - no data array loaded
                where_attrs = dict(f["where"].attrs) if "where" in f else {}

                # Get projection definition
                proj_def = None
                if "where" in f and "projdef" in f["where"].attrs:
                    proj_def = f["where"].attrs["projdef"]
                    if isinstance(proj_def, bytes):
                        proj_def = proj_def.decode("utf-8")

                # Get dimensions from dataset shape WITHOUT loading data
                dimensions = f["dataset1/data1/data"].shape

                # Calculate extent from projection corners only (no meshgrid)
                extent_bounds = projection_handler.calculate_dwd_extent(
                    where_attrs, proj_def
                )

                return {
                    "extent": {"wgs84": extent_bounds},
                    "dimensions": dimensions,
                }
        except Exception as e:
            raise RuntimeError(f"Failed to extract DWD extent from {file_path}: {e}")

    # cleanup_temp_files() is inherited from RadarSource base class
