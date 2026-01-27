#!/usr/bin/env python3
"""
IMGW (Polish Institute of Meteorology and Water Management) Radar Source

Handles downloading and processing of IMGW radar data in ODIM_H5 format.
Data is accessed via the IMGW public API at https://danepubliczne.imgw.pl/api/data/product
"""

import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pytz
import requests

from ..core.base import (
    RadarSource,
    extract_hdf5_corner_extent,
    lonlat_to_mercator,
)


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
            print(f"⚠️  Failed to fetch file list from API: {e}")
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

    def _generate_timestamps(self, count: int) -> list[str]:
        """Generate recent timestamps to search for available data

        IMGW updates every 5 minutes. Generate timestamps starting from
        current UTC time and working backwards.
        """
        from datetime import timedelta

        timestamps = []
        current_time = datetime.now(pytz.UTC)

        # Start from current time and work backwards in 5-minute intervals
        # IMGW files are typically available with ~10 minute delay
        for minutes_back in range(10, count * 30, 5):
            check_time = current_time - timedelta(minutes=minutes_back)
            # Round down to nearest 5 minutes
            check_time = check_time.replace(
                minute=(check_time.minute // 5) * 5, second=0, microsecond=0
            )
            timestamp = check_time.strftime("%Y%m%d%H%M%S")

            if timestamp not in timestamps:
                timestamps.append(timestamp)

        return timestamps

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

    def _filter_timestamps_by_range(
        self, timestamps: list[str], start_time: datetime, end_time: datetime
    ) -> list[str]:
        """Filter timestamps to only include those within the specified time range

        Args:
            timestamps: List of timestamp strings in format YYYYMMDDHHMMSS (14 digits)
            start_time: Start of time range (timezone-aware datetime)
            end_time: End of time range (timezone-aware datetime)

        Returns:
            Filtered list of timestamps within the range
        """
        filtered = []
        for ts in timestamps:
            try:
                # Parse IMGW timestamp format: YYYYMMDDHHMMSS (14 digits)
                ts_dt = datetime.strptime(ts, "%Y%m%d%H%M%S")
                # Make timezone aware (IMGW uses UTC)
                ts_dt = pytz.UTC.localize(ts_dt)

                # Check if timestamp is within range
                if start_time <= ts_dt <= end_time:
                    filtered.append(ts)
            except ValueError:
                # Skip timestamps that can't be parsed
                continue

        return filtered

    def _download_single_file(
        self, timestamp: str, product: str, url: str | None = None
    ) -> dict[str, Any]:
        """Download a single radar file (for parallel processing)"""
        if product not in self.product_mapping:
            return {
                "error": f"Unknown product: {product}",
                "timestamp": timestamp,
                "product": product,
                "success": False,
            }

        try:
            # Check if we've already downloaded this file in this session
            cache_key = f"{timestamp}_{product}"
            if cache_key in self.temp_files:
                return {
                    "timestamp": timestamp,
                    "product": product,
                    "path": self.temp_files[cache_key],
                    "url": url or self._get_product_url(timestamp, product),
                    "cached": True,
                    "success": True,
                }

            # Get download URL
            if url is None:
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
                    return {
                        "error": "Server returned HTML instead of HDF5 data",
                        "timestamp": timestamp,
                        "product": product,
                        "success": False,
                    }

                temp_file.write(content)
                temp_path = Path(temp_file.name)

            # Track the temporary file
            self.temp_files[cache_key] = str(temp_path)

            return {
                "timestamp": timestamp,
                "product": product,
                "path": str(temp_path),
                "url": url,
                "cached": False,
                "success": True,
            }

        except Exception as e:
            return {
                "error": str(e),
                "timestamp": timestamp,
                "product": product,
                "success": False,
            }

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

        print(f"🔍 Finding last {count} available IMGW timestamps...")

        # Generate timestamps based on current time (like SHMU/CHMI)
        print("🌐 Checking IMGW server for current timestamps...")

        # Generate more timestamps if we're filtering by time range
        multiplier = 8 if (start_time and end_time) else 4
        test_timestamps = self._generate_timestamps(count * multiplier)

        # Filter by time range if specified
        if start_time and end_time:
            test_timestamps = self._filter_timestamps_by_range(
                test_timestamps, start_time, end_time
            )
            print(
                f"📅 Filtered to range: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}"
            )

        # Check which timestamps are available
        available_timestamps = []
        for timestamp in test_timestamps:
            if len(available_timestamps) >= count:
                break

            # Check availability via HEAD request
            if self._check_timestamp_availability(timestamp, "cmax"):
                available_timestamps.append(timestamp)
                print(f"✅ Found current: {timestamp}")

        if not available_timestamps:
            print("❌ No available timestamps found")
            return []

        print(
            f"📥 Downloading {len(available_timestamps)} timestamps × {len(products)} products..."
        )

        # Create download tasks
        download_tasks = []
        for timestamp in available_timestamps:
            for product in products:
                url = self._get_product_url(timestamp, product)
                download_tasks.append((timestamp, product, url))

        print(
            f"📥 Starting parallel downloads ({len(download_tasks)} files, max 6 concurrent)..."
        )

        # Execute downloads in parallel
        downloaded_files = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            # Submit all download tasks
            future_to_task = {
                executor.submit(
                    self._download_single_file, timestamp, product, url
                ): (timestamp, product)
                for timestamp, product, url in download_tasks
            }

            # Process completed downloads
            for future in as_completed(future_to_task):
                timestamp, product = future_to_task[future]
                try:
                    result = future.result()
                    if result["success"]:
                        downloaded_files.append(result)
                        if result["cached"]:
                            print(f"📁 Using cached: {product} {timestamp}")
                        else:
                            print(f"✅ Downloaded: {product} {timestamp}")
                    else:
                        print(
                            f"❌ Failed {product} {timestamp}: {result.get('error', 'Unknown error')}"
                        )
                except Exception as e:
                    print(f"❌ Exception {product} {timestamp}: {e}")

        print(
            f"📋 IMGW: Downloaded {len(downloaded_files)} files ({len(download_tasks) - len(downloaded_files)} failed)"
        )
        return downloaded_files

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

                # Get attributes - IMGW stores scaling in dataset1/what (NOT data1/what)
                what_attrs = dict(f["dataset1/what"].attrs)
                what_global = dict(f["what"].attrs)  # Global metadata
                where_attrs = dict(f["where"].attrs)

                # Decode byte strings
                for attr_dict in [what_attrs, what_global, where_attrs]:
                    for key, value in attr_dict.items():
                        if isinstance(value, bytes):
                            attr_dict[key] = value.decode("utf-8")

                # Apply scaling (from dataset1/what)
                gain = what_attrs.get("gain", 0.5)
                offset = what_attrs.get("offset", -32.0)
                nodata = what_attrs.get("nodata", 255.0)
                undetect = what_attrs.get("undetect", 0.0)

                # Scale data
                scaled_data = data.astype(np.float32) * gain + offset

                # Handle special values
                scaled_data[data == int(nodata)] = np.nan
                scaled_data[data == int(undetect)] = np.nan

                # Get corner coordinates from where attributes
                # IMGW uses LL (lower-left), UR (upper-right) pattern
                if "LL_lon" in where_attrs and "UR_lon" in where_attrs:
                    ll_lon = float(where_attrs["LL_lon"])
                    ll_lat = float(where_attrs["LL_lat"])
                    ur_lon = float(where_attrs["UR_lon"])
                    ur_lat = float(where_attrs["UR_lat"])
                else:
                    # Fallback: approximate Poland coverage
                    print(
                        "⚠️  Corner coordinates not found in HDF5, using approximate extent"
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

                return {
                    "data": scaled_data,
                    "coordinates": {"lons": lons, "lats": lats},
                    "metadata": {
                        "product": product,
                        "quantity": quantity,
                        "timestamp": timestamp,
                        "source": "IMGW",
                        "units": self._get_units(quantity),
                        "nodata_value": np.nan,
                        "gain": gain,
                        "offset": offset,
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

    def _get_units(self, quantity: str) -> str:
        """Get units for a quantity"""
        units_map = {"DBZH": "dBZ", "TH": "dBZ"}
        return units_map.get(quantity, "dBZ")  # Default to dBZ for reflectivity

    def get_extent(self) -> dict[str, Any]:
        """Get IMGW radar coverage extent"""

        # IMGW radar coverage (from actual HDF5 data)
        # Covers Poland and surrounding areas
        wgs84 = {"west": 13.0, "east": 26.4, "south": 48.1, "north": 56.2}

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
