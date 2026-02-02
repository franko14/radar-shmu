#!/usr/bin/env python3
"""
CHMI (Czech Hydrometeorological Institute) Radar Source

Handles downloading and processing of CHMI radar data in ODIM_H5 format.
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


class CHMIRadarSource(RadarSource):
    """CHMI Radar data source implementation"""

    def __init__(self):
        super().__init__("chmi")
        self.base_url = (
            "https://opendata.chmi.cz/meteorology/weather/radar/composite/maxz/hdf5"
        )

        # CHMI product mapping
        self.product_mapping = {
            "maxz": "PABV23",  # Maximum reflectivity
        }

        # Product metadata
        self.product_info = {
            "maxz": {
                "name": "Maximum Reflectivity (MAXZ)",
                "units": "dBZ",
                "description": "Column maximum reflectivity",
            }
        }
        # temp_files is initialized in base class

    def get_available_products(self) -> list[str]:
        """Get list of available CHMI radar products"""
        return list(self.product_mapping.keys())

    def get_product_metadata(self, product: str) -> dict[str, Any]:
        """Get metadata for a specific CHMI product"""
        if product in self.product_info:
            return {
                "product": product,
                "source": self.name,
                **self.product_info[product],
            }
        return super().get_product_metadata(product)


    def _check_timestamp_availability(self, timestamp: str, product: str) -> bool:
        """Check if data is available for a specific timestamp and product"""
        url = self._get_product_url(timestamp, product)
        try:
            response = requests.head(url, timeout=5)
            return response.status_code == 200
        except Exception:
            return False


    def _get_product_url(self, timestamp: str, product: str) -> str:
        """Generate URL for CHMI product

        Format: T_PABV23_C_OKPR_YYYYMMDDHHMMSS.hdf
        """
        if product not in self.product_mapping:
            raise ValueError(f"Unknown product: {product}")

        product_code = self.product_mapping[product]

        return f"{self.base_url}/T_{product_code}_C_OKPR_{timestamp}.hdf"

    def _download_single_file(self, timestamp: str, product: str) -> dict[str, Any]:
        """Download a single radar file (for parallel processing)"""
        if product not in self.product_mapping:
            return create_error_result(timestamp, product, f"Unknown product: {product}")

        try:
            # Check session cache
            cache_key = f"{timestamp}_{product}"
            if cache_key in self.temp_files:
                return create_download_result(
                    timestamp=timestamp,
                    product=product,
                    path=self.temp_files[cache_key],
                    url=self._get_product_url(timestamp, product),
                    cached=True,
                )

            # Download to temporary file
            url = self._get_product_url(timestamp, product)

            with tempfile.NamedTemporaryFile(
                suffix=f"_chmi_{product}_{timestamp}.hdf", delete=False
            ) as temp_file:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                temp_file.write(response.content)
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
        """Get list of available CHMI timestamps WITHOUT downloading.

        Args:
            count: Maximum number of timestamps to return
            products: List of products to check (default: ['maxz'])
            start_time: Optional start time for filtering
            end_time: Optional end time for filtering

        Returns:
            List of timestamp strings in YYYYMMDDHHMMSS format, newest first
        """
        # Generate candidate timestamps using shared utility
        multiplier = 8 if (start_time and end_time) else 4
        test_timestamps = generate_timestamp_candidates(
            count=count * multiplier,
            interval_minutes=5,
            delay_minutes=0,  # CHMI is usually current
            format_str=TimestampFormat.FULL,  # YYYYMMDDHHMMSS
        )

        # Filter by time range if specified
        if start_time and end_time:
            test_timestamps = filter_timestamps_by_range(
                test_timestamps, start_time, end_time
            )

        # Find available timestamps
        available_timestamps = []
        for timestamp in test_timestamps:
            if len(available_timestamps) >= count:
                break
            if self._check_timestamp_availability(timestamp, "maxz"):
                available_timestamps.append(timestamp)

        return available_timestamps

    # download_timestamps is inherited from RadarSource base class

    def download_latest(
        self,
        count: int,
        products: list[str] = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Download latest CHMI radar data

        Args:
            count: Maximum number of timestamps to download
            products: List of products to download (default: ['maxz'])
            start_time: Optional start time for filtering (timezone-aware datetime)
            end_time: Optional end time for filtering (timezone-aware datetime)
        """

        if products is None:
            products = ["maxz"]  # Default product

        logger.info(
            f"Finding last {count} available CHMI timestamps...",
            extra={"source": "chmi"},
        )
        logger.info(
            "Checking CHMI server for current timestamps...",
            extra={"source": "chmi"},
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
            logger.info(f"Found current: {ts}", extra={"source": "chmi"})

        if not available_timestamps:
            logger.warning("No available timestamps found", extra={"source": "chmi"})
            return []

        # Download the timestamps
        return self.download_timestamps(available_timestamps, products)

    def process_to_array(self, file_path: str) -> dict[str, Any]:
        """Process CHMI HDF5 file to array with metadata"""

        try:
            with h5py.File(file_path, "r") as f:
                # Read raw data
                data = f["dataset1/data1/data"][:]

                # Get and decode attributes - CHMI stores scaling in data1/what
                what_attrs = decode_hdf5_attrs(dict(f["dataset1/data1/what"].attrs))
                what_dataset_attrs = decode_hdf5_attrs(
                    dict(f["dataset1/what"].attrs)
                )  # For product/timestamp
                where_attrs = decode_hdf5_attrs(dict(f["where"].attrs))

                # Extract projection definition from HDF5 (CHMI may use native projection)
                projdef = where_attrs.get("projdef", "")
                if isinstance(projdef, bytes):
                    projdef = projdef.decode()

                # Get scaling parameters and scale data
                scaling = get_scaling_params(
                    what_attrs,
                    default_nodata=-32768,
                    default_undetect=0,
                )

                scaled_data = scale_radar_data(
                    data,
                    scaling["gain"],
                    scaling["offset"],
                    scaling["nodata"],
                    scaling["undetect"],
                    handle_uint8=True,  # CHMI uses 255 as nodata for uint8
                )

                # Get corner coordinates from where attributes
                # CHMI uses similar structure to SHMU (LL_lon/lat, UR_lon/lat)
                if "LL_lon" in where_attrs and "UR_lon" in where_attrs:
                    ll_lon = float(where_attrs["LL_lon"])
                    ll_lat = float(where_attrs["LL_lat"])
                    ur_lon = float(where_attrs["UR_lon"])
                    ur_lat = float(where_attrs["UR_lat"])
                else:
                    # Fallback: actual CHMI extent from HDF5 metadata
                    logger.warning(
                        "Corner coordinates not found in HDF5, using known extent",
                        extra={"source": "chmi"},
                    )
                    ll_lon, ll_lat = 11.266869, 48.047275
                    ur_lon, ur_lat = 19.623974, 51.458369

                lons = np.linspace(ll_lon, ur_lon, data.shape[1])
                lats = np.linspace(ur_lat, ll_lat, data.shape[0])  # Note: flipped

                # Extract metadata
                product = what_dataset_attrs.get("product", "UNKNOWN")
                quantity = what_attrs.get("quantity", "UNKNOWN")
                start_date = what_dataset_attrs.get("startdate", "")
                start_time = what_dataset_attrs.get("starttime", "")
                timestamp = start_date + start_time

                # Build projection info for reprojector
                # CHMI uses a Mercator projection with false easting/northing:
                # +proj=merc +lat_ts=0 +lon_0=0 +x_0=-1254222.15 +y_0=-6702777.85
                # This MUST be reprojected from native to Web Mercator.
                # Although WGS84 corners appear "regular", the data is stored in
                # native Mercator coordinates (xscale/yscale in meters).
                projection_info = {
                    "type": "mercator",
                    "proj_def": projdef,  # Native projection for reprojection
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
                        "source": "CHMI",
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
            raise RuntimeError(f"Failed to process CHMI file {file_path}: {e}")

    def get_extent(self) -> dict[str, Any]:
        """Get CHMI radar coverage extent"""

        # CHMI radar coverage - actual bounds from HDF5 metadata
        # These are the WGS84 corner coordinates from the where attributes
        wgs84 = {
            "west": 11.266869,
            "east": 19.623974,
            "south": 48.047275,
            "north": 51.458369,
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
            "grid_size": [598, 378],  # xsize, ysize from HDF5
            "resolution_m": [1556, 1556],  # xscale, yscale in meters
        }

    def extract_extent_only(self, file_path: str) -> dict[str, Any]:
        """Extract extent from CHMI HDF5 without loading data array.

        Uses shared HDF5 corner extraction from base module with Czech fallback.
        """
        # Fallback extent based on actual CHMI HDF5 metadata
        fallback = {
            "west": 11.266869,
            "east": 19.623974,
            "south": 48.047275,
            "north": 51.458369,
        }
        return extract_hdf5_corner_extent(file_path, fallback_extent=fallback)

    # cleanup_temp_files() is inherited from RadarSource base class
