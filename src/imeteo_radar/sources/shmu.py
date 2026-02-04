#!/usr/bin/env python3
"""
SHMU (Slovak Hydrometeorological Institute) Radar Source

Handles downloading and processing of SHMU radar data in ODIM_H5 format.
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import urllib3

# Suppress SSL verification warnings for SHMU (their certificate has issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
)
from ..utils.timestamps import (
    TimestampFormat,
    filter_timestamps_by_range,
    generate_timestamp_candidates,
)

logger = get_logger(__name__)


# SHMU-specific constants - actual bounds from reprojected GeoTIFF
# Custom Mercator projection reprojected to Web Mercator
SHMU_FALLBACK_EXTENT = {
    "west": 13.597751,
    "east": 23.806870,
    "south": 46.045447,
    "north": 50.701424,
}


class SHMURadarSource(RadarSource):
    """SHMU Radar data source implementation"""

    def __init__(self):
        super().__init__("shmu")
        self.base_url = (
            "https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp"
        )

        # SHMU product mapping
        self.product_mapping = {
            "zmax": "PABV",  # Maximum reflectivity
            "cappi2km": "PANV",  # CAPPI at 2km
            "etop": "PADV",  # Echo top height
            "pac01": "PASV",  # 1h accumulated precipitation
        }

        # Product metadata
        self.product_info = {
            "zmax": {
                "name": "Maximum Reflectivity (ZMAX)",
                "units": "dBZ",
                "description": "Column maximum reflectivity",
            },
            "cappi2km": {
                "name": "CAPPI 2km",
                "units": "dBZ",
                "description": "Reflectivity at 2km altitude",
            },
            "etop": {
                "name": "Echo Top Height",
                "units": "km",
                "description": "Echo top height",
            },
            "pac01": {
                "name": "1h Precipitation",
                "units": "mm",
                "description": "1-hour accumulated precipitation",
            },
        }

    def get_available_products(self) -> list[str]:
        """Get list of available SHMU radar products"""
        return list(self.product_mapping.keys())

    def get_product_metadata(self, product: str) -> dict[str, Any]:
        """Get metadata for a specific SHMU product"""
        if product in self.product_info:
            return {
                "product": product,
                "source": self.name,
                **self.product_info[product],
            }
        return super().get_product_metadata(product)

    def _get_product_url(self, timestamp: str, product: str) -> str:
        """Generate URL for SHMU product"""
        if product not in self.product_mapping:
            raise ValueError(f"Unknown product: {product}")

        composite_type = self.product_mapping[product]
        date_str = timestamp[:8]  # YYYYMMDD

        return (
            f"{self.base_url}/{product}/{date_str}/"
            f"T_{composite_type}22_C_LZIB_{timestamp}.hdf"
        )

    def _check_timestamp_availability(self, timestamp: str, product: str) -> bool:
        """Check if data is available for a specific timestamp and product"""
        url = self._get_product_url(timestamp, product)
        try:
            response = requests.head(url, timeout=5, verify=False)
            return response.status_code == 200
        except Exception:
            return False

    def _download_single_file(self, timestamp: str, product: str) -> dict[str, Any]:
        """Download a single radar file (for parallel processing)"""
        if product not in self.product_mapping:
            return create_error_result(
                timestamp, product, f"Unknown product: {product}"
            )

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
                suffix=f"_shmu_{product}_{timestamp}.hdf", delete=False
            ) as temp_file:
                response = requests.get(url, timeout=30, verify=False)
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
        """Get list of available SHMU timestamps WITHOUT downloading.

        Args:
            count: Maximum number of timestamps to return
            products: List of products to check (default: ['zmax'])
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
            delay_minutes=0,  # SHMU is usually current
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
            if self._check_timestamp_availability(timestamp, "zmax"):
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
        """Download latest SHMU radar data"""
        if products is None:
            products = ["zmax"]  # cappi2km available but disabled by default

        logger.info(
            f"Finding last {count} available SHMU timestamps...",
            extra={"source": "shmu"},
        )
        logger.info(
            "Checking SHMU server for current timestamps...",
            extra={"source": "shmu"},
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
            logger.info(f"Found current: {ts}", extra={"source": "shmu"})

        if not available_timestamps:
            logger.warning("No available timestamps found", extra={"source": "shmu"})
            return []

        # Download the timestamps
        return self.download_timestamps(available_timestamps, products)

    def process_to_array(self, file_path: str) -> dict[str, Any]:
        """Process SHMU HDF5 file to array with metadata"""
        import h5py
        import numpy as np

        try:
            with h5py.File(file_path, "r") as f:
                # Read raw data
                data = f["dataset1/data1/data"][:]

                # Get and decode attributes
                what_attrs = decode_hdf5_attrs(dict(f["dataset1/what"].attrs))
                where_attrs = decode_hdf5_attrs(dict(f["where"].attrs))

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
                    handle_uint8=True,  # SHMU uses 255 as nodata for uint8
                )

                # Extract corner coordinates directly from HDF5 data
                ll_lon = float(where_attrs["LL_lon"])
                ll_lat = float(where_attrs["LL_lat"])
                ur_lon = float(where_attrs["UR_lon"])
                ur_lat = float(where_attrs["UR_lat"])

                _lons = np.linspace(ll_lon, ur_lon, data.shape[1])
                _lats = np.linspace(ur_lat, ll_lat, data.shape[0])

                # Extract metadata
                product = what_attrs.get("product", "UNKNOWN")
                quantity = what_attrs.get("quantity", "UNKNOWN")
                start_date = what_attrs.get("startdate", "")
                start_time = what_attrs.get("starttime", "")
                timestamp = start_date + start_time

                # Build projection info for reprojector
                # SHMU data is in a custom Mercator projection (projdef)
                # The corner coordinates (LL/UL/UR/LR) are WGS84 positions of grid corners
                # NOTE: xscale/yscale in HDF5 are INCORRECT - do not use them
                projdef = where_attrs.get("projdef", "")
                if isinstance(projdef, bytes):
                    projdef = projdef.decode()

                projection_info = {
                    "type": "mercator",
                    "proj_def": projdef,
                    "where_attrs": where_attrs,
                }

                return {
                    "data": scaled_data,
                    "coordinates": None,  # Don't use coordinates, use projection instead
                    "projection": projection_info,
                    "metadata": {
                        "product": product,
                        "quantity": quantity,
                        "timestamp": timestamp,
                        "source": "SHMU",
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
                    "timestamp": timestamp[:14],
                }

        except Exception as e:
            raise RuntimeError(f"Failed to process SHMU file {file_path}: {e}") from e

    def get_extent(self) -> dict[str, Any]:
        """Get SHMU radar coverage extent"""
        wgs84 = SHMU_FALLBACK_EXTENT.copy()

        x_min, y_min = lonlat_to_mercator(wgs84["west"], wgs84["south"])
        x_max, y_max = lonlat_to_mercator(wgs84["east"], wgs84["north"])

        return {
            "wgs84": wgs84,
            "mercator": {
                "x_min": x_min,
                "x_max": x_max,
                "y_min": y_min,
                "y_max": y_max,
                "bounds": [x_min, y_min, x_max, y_max],
            },
            "projection": "EPSG:3857",
            "grid_size": [1560, 2270],
            "resolution_m": [480, 330],
        }

    def extract_extent_only(self, file_path: str) -> dict[str, Any]:
        """Extract extent from SHMU HDF5 without loading data array."""
        return extract_hdf5_corner_extent(file_path)
