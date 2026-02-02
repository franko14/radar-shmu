#!/usr/bin/env python3
"""
IMGW (Polish Institute of Meteorology and Water Management) Radar Source

Handles downloading and processing of IMGW radar data in ODIM_H5 format.
Data is accessed via the IMGW public API at https://danepubliczne.imgw.pl/api/data/product
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import requests

from ..core.base import (
    RadarSource,
    extract_hdf5_corner_extent,
    lonlat_to_mercator,
)
from ..core.logging import get_logger
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


class IMGWRadarSource(RadarSource):
    """IMGW Radar data source implementation"""

    def __init__(self):
        super().__init__("imgw")
        # API endpoint for listing available files
        self.api_url = "https://danepubliczne.imgw.pl/api/data/product/id"
        # Base URL for actual file downloads (HVD path works, POLCOMP from API doesn't)
        self.download_base_url = "https://danepubliczne.imgw.pl/pl/datastore/getfiledown/Oper/Polrad/Produkty/HVD"

        # IMGW product mapping (user-facing name -> API product ID and HVD folder name)
        self.product_mapping = {
            "cmax": {
                "api_id": "COMPO_CMAX_250.comp.cmax",  # For API queries
                "hvd_folder": "HVD_COMPO_CMAX_250.comp.cmax",  # For downloads
            },
        }

        # Product metadata
        self.product_info = {
            "cmax": {
                "name": "Composite Maximum Reflectivity (CMAX)",
                "units": "dBZ",
                "description": "Column maximum reflectivity composite",
            }
        }
        # temp_files is initialized in base class
        # Cache for available files from API
        self._available_files_cache: dict[str, list[dict]] = {}

    def get_available_products(self) -> list[str]:
        """Get list of available IMGW radar products"""
        return list(self.product_mapping.keys())

    def get_product_metadata(self, product: str) -> dict[str, Any]:
        """Get metadata for a specific IMGW product"""
        if product in self.product_info:
            return {
                "product": product,
                "source": self.name,
                **self.product_info[product],
            }
        return super().get_product_metadata(product)

    def _fetch_available_files(self, product: str) -> list[dict]:
        """Fetch list of available files from IMGW API

        Args:
            product: Product name (e.g., 'cmax')

        Returns:
            List of file dictionaries with 'file' and 'url' keys
        """
        if product not in self.product_mapping:
            raise ValueError(f"Unknown product: {product}")

        # Check cache first
        if product in self._available_files_cache:
            return self._available_files_cache[product]

        product_config = self.product_mapping[product]
        api_endpoint = f"{self.api_url}/{product_config['api_id']}"

        try:
            response = requests.get(api_endpoint, timeout=30)
            response.raise_for_status()
            files = response.json()

            # Filter to only H5 files
            h5_files = [f for f in files if f["file"].endswith(".h5")]

            # Cache the result
            self._available_files_cache[product] = h5_files

            return h5_files
        except Exception as e:
            logger.warning(
                f"Failed to fetch file list from API: {e}",
                extra={"source": "imgw", "operation": "fetch"},
            )
            return []

    def _extract_timestamp_from_filename(self, filename: str) -> str | None:
        """Extract timestamp from IMGW filename

        Filename format: YYYYMMDDHHMMSS00dBZ.cmax.h5
        Returns: YYYYMMDDHHMMSS (14 digits)
        """
        # Remove file extension and suffix
        # Example: 2026012705300000dBZ.cmax.h5 -> 20260127053000
        try:
            # Extract digits before "00dBZ"
            ts_part = filename.split("00dBZ")[0]
            if len(ts_part) == 14 and ts_part.isdigit():
                return ts_part
        except Exception:
            pass
        return None


    def _check_timestamp_availability(self, timestamp: str, product: str) -> bool:
        """Check if data is available for a specific timestamp and product

        Uses HEAD request to check if the file exists on the server.
        """
        try:
            url = self._get_product_url(timestamp, product)
            response = requests.head(url, timeout=10)
            # Check if it's a real file (not HTML error page)
            content_type = response.headers.get("Content-Type", "")
            return response.status_code == 200 and "text/html" not in content_type
        except Exception:
            return False

    def _get_product_url(self, timestamp: str, product: str) -> str:
        """Generate download URL for IMGW product

        Uses HVD path which works for actual file downloads.
        The API returns POLCOMP URLs but those return HTML, not actual files.
        """
        if product not in self.product_mapping:
            raise ValueError(f"Unknown product: {product}")

        product_config = self.product_mapping[product]
        hvd_folder = product_config["hvd_folder"]
        return f"{self.download_base_url}/{hvd_folder}/{timestamp}00dBZ.cmax.h5"


    def _download_single_file(self, timestamp: str, product: str) -> dict[str, Any]:
        """Download a single radar file (for parallel processing)"""
        if product not in self.product_mapping:
            return create_error_result(
                timestamp, product, f"Unknown product: {product}"
            )

        try:
            # Check if we've already downloaded this file in this session
            cache_key = f"{timestamp}_{product}"
            if cache_key in self.temp_files:
                url = self._get_product_url(timestamp, product)
                return create_download_result(
                    timestamp=timestamp,
                    product=product,
                    path=self.temp_files[cache_key],
                    url=url,
                    cached=True,
                )

            # Get download URL
            url = self._get_product_url(timestamp, product)

            # Create a proper temporary file
            with tempfile.NamedTemporaryFile(
                suffix=f"_imgw_{product}_{timestamp}.h5", delete=False
            ) as temp_file:
                # Download directly to temp file
                response = requests.get(url, timeout=60)
                response.raise_for_status()

                # Verify we got actual HDF5 data, not HTML
                content = response.content
                if content[:4] == b"<!DO" or content[:5] == b"<html":
                    return create_error_result(
                        timestamp,
                        product,
                        "Server returned HTML instead of HDF5 data",
                    )

                temp_file.write(content)
                temp_path = Path(temp_file.name)

            # Track the temporary file
            self.temp_files[cache_key] = str(temp_path)

            return create_download_result(
                timestamp=timestamp,
                product=product,
                path=str(temp_path),
                url=url,
                cached=False,
            )

        except Exception as e:
            return create_error_result(timestamp, product, str(e))

    def get_available_timestamps(
        self,
        count: int = 8,
        products: list[str] = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[str]:
        """Get list of available IMGW timestamps WITHOUT downloading.

        Args:
            count: Maximum number of timestamps to return
            products: List of products to check (default: ['cmax'])
            start_time: Optional start time for filtering
            end_time: Optional end time for filtering

        Returns:
            List of timestamp strings in YYYYMMDDHHMMSS format, newest first
        """
        # Generate more timestamps if we're filtering by time range
        multiplier = 8 if (start_time and end_time) else 4
        test_timestamps = generate_timestamp_candidates(
            count=count * multiplier,
            interval_minutes=5,
            delay_minutes=10,  # IMGW has ~10 minute delay
            format_str=TimestampFormat.FULL,  # YYYYMMDDHHMMSS
        )

        # Filter by time range if specified
        if start_time and end_time:
            test_timestamps = filter_timestamps_by_range(
                test_timestamps, start_time, end_time
            )

        # Check which timestamps are available
        available_timestamps = []
        for timestamp in test_timestamps:
            if len(available_timestamps) >= count:
                break

            # Check availability via HEAD request
            if self._check_timestamp_availability(timestamp, "cmax"):
                available_timestamps.append(timestamp)

        return available_timestamps

    # download_timestamps is inherited from RadarSource base class

    def download_latest(
        self,
        count: int,
        products: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Download latest IMGW radar data

        Args:
            count: Maximum number of timestamps to download
            products: List of products to download (default: ['cmax'])
            start_time: Optional start time for filtering (timezone-aware datetime)
            end_time: Optional end time for filtering (timezone-aware datetime)
        """

        if products is None:
            products = ["cmax"]  # Default product

        logger.info(
            f"Finding last {count} available IMGW timestamps...",
            extra={"source": "imgw", "operation": "find"},
        )

        # Generate timestamps based on current time (like SHMU/CHMI)
        logger.info(
            "Checking IMGW server for current timestamps...",
            extra={"source": "imgw"},
        )

        # Get available timestamps
        available_timestamps = self.get_available_timestamps(
            count=count,
            products=products,
            start_time=start_time,
            end_time=end_time,
        )

        # Log found timestamps
        for ts in available_timestamps:
            logger.debug(
                f"Found current: {ts}",
                extra={"source": "imgw", "timestamp": ts},
            )

        if not available_timestamps:
            logger.warning(
                "No available timestamps found",
                extra={"source": "imgw"},
            )
            return []

        # Download the timestamps
        return self.download_timestamps(available_timestamps, products)

    def process_to_array(self, file_path: str) -> dict[str, Any]:
        """Process IMGW HDF5 file to array with metadata

        IMGW uses ODIM_H5/V2_3 format with structure:
        - dataset1/data1/data: raw uint8 data
        - dataset1/what: scaling info (gain, offset, nodata, etc.)
        - where: corner coordinates (LL, LR, UL, UR)
        - what: global metadata (date, time, source)
        """

        try:
            with h5py.File(file_path, "r") as f:
                # Read raw data
                data = f["dataset1/data1/data"][:]

                # Get and decode attributes - IMGW stores scaling in dataset1/what (NOT data1/what)
                what_attrs = decode_hdf5_attrs(dict(f["dataset1/what"].attrs))
                what_global = decode_hdf5_attrs(dict(f["what"].attrs))  # Global metadata
                where_attrs = decode_hdf5_attrs(dict(f["where"].attrs))

                # Extract projection definition from HDF5 (IMGW may use native projection)
                projdef = where_attrs.get("projdef", "")
                if isinstance(projdef, bytes):
                    projdef = projdef.decode()

                # Get scaling parameters
                scaling = get_scaling_params(
                    what_attrs,
                    default_gain=0.5,
                    default_offset=-32.0,
                    default_nodata=255,
                    default_undetect=0,
                )

                # Scale data
                scaled_data = scale_radar_data(
                    data,
                    scaling["gain"],
                    scaling["offset"],
                    scaling["nodata"],
                    scaling["undetect"],
                    handle_uint8=True,  # IMGW uses uint8 with 255 as nodata
                )

                # Get corner coordinates from where attributes
                # IMGW uses LL (lower-left), UR (upper-right) pattern
                if "LL_lon" in where_attrs and "UR_lon" in where_attrs:
                    ll_lon = float(where_attrs["LL_lon"])
                    ll_lat = float(where_attrs["LL_lat"])
                    ur_lon = float(where_attrs["UR_lon"])
                    ur_lat = float(where_attrs["UR_lat"])
                else:
                    # Fallback: approximate Poland coverage
                    logger.warning(
                        "Corner coordinates not found in HDF5, using approximate extent",
                        extra={"source": "imgw"},
                    )
                    ll_lon, ll_lat = 13.0, 48.1
                    ur_lon, ur_lat = 26.4, 56.2

                lons = np.linspace(ll_lon, ur_lon, data.shape[1])
                lats = np.linspace(ur_lat, ll_lat, data.shape[0])  # Note: flipped

                # Extract metadata
                product = what_attrs.get("product", "MAX")
                quantity = what_attrs.get("quantity", "DBZH")
                start_date = what_attrs.get("startdate", what_global.get("date", ""))
                start_time_str = what_attrs.get("starttime", what_global.get("time", ""))
                timestamp = str(start_date) + str(start_time_str)

                # Build projection info for reprojector
                # IMGW uses ODIM_H5 format - check for native projection (projdef)
                # If projdef exists, data is in that projection with WGS84 corner coords
                if projdef and projdef.strip():
                    # Native projection (similar to SHMU mercator handling)
                    projection_info = {
                        "type": "mercator",
                        "proj_def": projdef,
                        "where_attrs": where_attrs,
                    }
                else:
                    # Pure WGS84 lat/lon grid
                    projection_info = {
                        "type": "wgs84",
                        "where_attrs": where_attrs,
                    }

                return {
                    "data": scaled_data,
                    "coordinates": None,  # Use projection instead
                    "projection": projection_info,
                    "metadata": {
                        "product": product,
                        "quantity": quantity,
                        "timestamp": timestamp,
                        "source": "IMGW",
                        "units": get_quantity_units(quantity),
                        "nodata_value": np.nan,
                        "gain": scaling["gain"],
                        "offset": scaling["offset"],
                    },
                    "extent": {
                        "wgs84": {
                            "west": ll_lon,
                            "east": ur_lon,
                            "south": ll_lat,
                            "north": ur_lat,
                        }
                    },
                    "dimensions": data.shape,
                    "timestamp": (
                        timestamp[:14] if len(timestamp) >= 14 else timestamp
                    ),  # YYYYMMDDHHMMSS format
                }

        except Exception as e:
            raise RuntimeError(f"Failed to process IMGW file {file_path}: {e}")

    def get_extent(self) -> dict[str, Any]:
        """Get IMGW radar coverage extent"""

        # IMGW radar coverage - actual bounds from reprojected GeoTIFF
        # Azimuthal equidistant projection reprojected to Web Mercator
        wgs84 = {
            "west": 11.804019,
            "east": 26.376516,
            "south": 48.124377,
            "north": 56.398335,
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
            "projection": "EPSG:3857",  # Web Mercator
            "grid_size": None,  # To be determined from actual data
            "resolution_m": None,  # To be determined from actual data
        }

    def extract_extent_only(self, file_path: str) -> dict[str, Any]:
        """Extract extent from IMGW HDF5 without loading data array.

        Uses shared HDF5 corner extraction from base module with Poland fallback.
        """
        # Fallback extent for Poland (from actual IMGW data)
        fallback = {"west": 13.0, "east": 26.4, "south": 48.1, "north": 56.2}
        return extract_hdf5_corner_extent(file_path, fallback_extent=fallback)

    # cleanup_temp_files() is inherited from RadarSource base class
