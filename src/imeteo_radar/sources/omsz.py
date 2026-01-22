#!/usr/bin/env python3
"""
OMSZ (Hungarian Meteorological Service) Radar Source

Handles downloading and processing of OMSZ radar data in netCDF format.
Data source: https://odp.met.hu/weather/radar/composite/
"""

import os
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import netCDF4 as nc
import numpy as np
import requests

from ..core.base import RadarSource, lonlat_to_mercator


class OMSZRadarSource(RadarSource):
    """OMSZ Radar data source implementation (netCDF format)"""

    def __init__(self):
        super().__init__("omsz")
        self.base_url = "https://odp.met.hu/weather/radar/composite/nc"

        # OMSZ product mapping (user-facing name -> server directory name)
        self.product_mapping = {
            "cmax": "refl2D",  # Column Maximum (ZMAX equivalent)
            "pscappi": "refl2D_pscappi",  # PseudoCAPPI (ground-level)
            "refl3d": "refl3D",  # 3D composite
        }

        # Product metadata
        self.product_info = {
            "cmax": {
                "name": "Column Maximum (CMax)",
                "units": "dBZ",
                "description": "Column maximum reflectivity (ZMAX equivalent)",
            },
            "pscappi": {
                "name": "PseudoCAPPI",
                "units": "dBZ",
                "description": "Pseudo-CAPPI ground-level precipitation estimate",
            },
            "refl3d": {
                "name": "3D Composite",
                "units": "dBZ",
                "description": "3D radar composite volume",
            },
        }
        # temp_files is initialized in base class

        # Data scaling parameters (from netCDF: dBZ = raw/2 - 32)
        self.gain = 0.5
        self.offset = -32.0

    def get_available_products(self) -> list[str]:
        """Get list of available OMSZ radar products"""
        return list(self.product_mapping.keys())

    def get_product_metadata(self, product: str) -> dict[str, Any]:
        """Get metadata for a specific OMSZ product"""
        if product in self.product_info:
            return {
                "product": product,
                "source": self.name,
                **self.product_info[product],
            }
        return super().get_product_metadata(product)

    def _generate_timestamps(self, count: int) -> list[str]:
        """Generate recent timestamps to search for available data"""
        timestamps = []
        import pytz

        current_time = datetime.now(pytz.UTC)

        # Start from current time and work backwards in 5-minute intervals
        # OMSZ updates every 5 minutes, files available ~5 minutes after nominal time
        for minutes_back in range(5, count * 30, 5):  # Start 5 min back
            check_time = current_time - timedelta(minutes=minutes_back)
            # Round down to nearest 5 minutes
            check_time = check_time.replace(
                minute=(check_time.minute // 5) * 5, second=0, microsecond=0
            )
            # OMSZ format: YYYYMMDD_HHMM
            timestamp = check_time.strftime("%Y%m%d_%H%M")

            if timestamp not in timestamps:
                timestamps.append(timestamp)

            if len(timestamps) >= count * 4:
                break

        return timestamps

    def _check_timestamp_availability(self, timestamp: str, product: str) -> bool:
        """Check if data is available for a specific timestamp and product"""
        url = self._get_product_url(timestamp, product)
        try:
            response = requests.head(url, timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    def _filter_timestamps_by_range(
        self, timestamps: list[str], start_time: datetime, end_time: datetime
    ) -> list[str]:
        """Filter timestamps to only include those within the specified time range

        Args:
            timestamps: List of timestamp strings in format YYYYMMDD_HHMM
            start_time: Start of time range (timezone-aware datetime)
            end_time: End of time range (timezone-aware datetime)

        Returns:
            Filtered list of timestamps within the range
        """
        import pytz

        filtered = []
        for ts in timestamps:
            try:
                # Parse OMSZ timestamp format: YYYYMMDD_HHMM
                ts_dt = datetime.strptime(ts, "%Y%m%d_%H%M")
                # Make timezone aware (OMSZ uses UTC)
                ts_dt = pytz.UTC.localize(ts_dt)

                # Check if timestamp is within range
                if start_time <= ts_dt <= end_time:
                    filtered.append(ts)
            except ValueError:
                continue

        return filtered

    def _get_product_url(self, timestamp: str, product: str) -> str:
        """Generate URL for OMSZ product

        Args:
            timestamp: Timestamp in format YYYYMMDD_HHMM
            product: Product name (cmax, pscappi, refl3d)

        Returns:
            Full URL to the ZIP file
        """
        if product not in self.product_mapping:
            raise ValueError(f"Unknown product: {product}")

        nc_product = self.product_mapping[product]

        # URL format: https://odp.met.hu/weather/radar/composite/nc/{product}/radar_composite-{product}-{YYYYMMDD}_{HHMM}.nc.zip
        return (
            f"{self.base_url}/{nc_product}/"
            f"radar_composite-{nc_product}-{timestamp}.nc.zip"
        )

    def _download_and_extract(self, url: str, timestamp: str, product: str) -> str:
        """Download ZIP and extract netCDF file

        Args:
            url: URL to download
            timestamp: Timestamp string for temp file naming
            product: Product name for temp file naming

        Returns:
            Path to extracted netCDF file
        """
        # Download ZIP file
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Save to temporary ZIP file
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
            tmp_zip.write(response.content)
            tmp_zip_path = tmp_zip.name

        try:
            # Extract netCDF from ZIP
            with zipfile.ZipFile(tmp_zip_path, "r") as zf:
                nc_files = [f for f in zf.namelist() if f.endswith(".nc")]
                if not nc_files:
                    raise ValueError(f"No netCDF file found in ZIP from {url}")

                nc_filename = nc_files[0]

                # Extract to temp directory
                temp_dir = tempfile.mkdtemp(prefix="omsz_")
                zf.extract(nc_filename, temp_dir)
                nc_path = os.path.join(temp_dir, nc_filename)

                return nc_path
        finally:
            # Clean up ZIP file
            if os.path.exists(tmp_zip_path):
                os.unlink(tmp_zip_path)

    def _download_single_file(self, timestamp: str, product: str) -> dict[str, Any]:
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
                # Normalize timestamp from YYYYMMDD_HHMM to YYYYMMDDHHMM00
                normalized_timestamp = timestamp.replace("_", "") + "00"
                return {
                    "timestamp": normalized_timestamp,
                    "product": product,
                    "path": self.temp_files[cache_key],
                    "url": self._get_product_url(timestamp, product),
                    "cached": True,
                    "success": True,
                }

            # Download and extract
            url = self._get_product_url(timestamp, product)
            nc_path = self._download_and_extract(url, timestamp, product)

            # Track the temporary file
            self.temp_files[cache_key] = nc_path

            # Normalize timestamp from YYYYMMDD_HHMM to YYYYMMDDHHMM00
            normalized_timestamp = timestamp.replace("_", "") + "00"
            return {
                "timestamp": normalized_timestamp,
                "product": product,
                "path": nc_path,
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
        """Download latest OMSZ radar data

        Args:
            count: Maximum number of timestamps to download
            products: List of products to download (default: ['cmax'])
            start_time: Optional start time for filtering (timezone-aware datetime)
            end_time: Optional end time for filtering (timezone-aware datetime)

        Returns:
            List of downloaded file information dictionaries
        """
        if products is None:
            products = ["cmax"]  # Default to ZMAX equivalent

        print(f"[OMSZ] Finding last {count} available timestamps...")
        print("[OMSZ] Checking server for current timestamps...")

        # Generate more timestamps if we're filtering by time range
        multiplier = 8 if (start_time and end_time) else 4
        test_timestamps = self._generate_timestamps(count * multiplier)

        # Filter by time range if specified
        if start_time and end_time:
            test_timestamps = self._filter_timestamps_by_range(
                test_timestamps, start_time, end_time
            )
            print(
                f"[OMSZ] Filtered timestamps to range: "
                f"{start_time.strftime('%Y-%m-%d %H:%M')} to "
                f"{end_time.strftime('%Y-%m-%d %H:%M')}"
            )

        available_timestamps = []

        for timestamp in test_timestamps:
            if len(available_timestamps) >= count:
                break

            # Test with cmax (most reliable product)
            if self._check_timestamp_availability(timestamp, "cmax"):
                available_timestamps.append(timestamp)
                print(f"[OMSZ] Found: {timestamp}")

        if not available_timestamps:
            print("[OMSZ] No available timestamps found")
            return []

        print(
            f"[OMSZ] Downloading {len(available_timestamps)} timestamps "
            f"x {len(products)} products..."
        )

        # Create download tasks
        download_tasks = []
        for timestamp in available_timestamps:
            for product in products:
                download_tasks.append((timestamp, product))

        print(
            f"[OMSZ] Starting parallel downloads "
            f"({len(download_tasks)} files, max 6 concurrent)..."
        )

        # Execute downloads in parallel
        downloaded_files = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_to_task = {
                executor.submit(self._download_single_file, timestamp, product): (
                    timestamp,
                    product,
                )
                for timestamp, product in download_tasks
            }

            for future in as_completed(future_to_task):
                timestamp, product = future_to_task[future]
                try:
                    result = future.result()
                    if result["success"]:
                        downloaded_files.append(result)
                        if result["cached"]:
                            print(f"[OMSZ] Using cached: {product} {timestamp}")
                        else:
                            print(f"[OMSZ] Downloaded: {product} {timestamp}")
                    else:
                        print(
                            f"[OMSZ] Failed {product} {timestamp}: "
                            f"{result.get('error', 'Unknown error')}"
                        )
                except Exception as e:
                    print(f"[OMSZ] Exception {product} {timestamp}: {e}")

        success_count = len(downloaded_files)
        fail_count = len(download_tasks) - success_count
        print(f"[OMSZ] Downloaded {success_count} files ({fail_count} failed)")

        return downloaded_files

    def process_to_array(self, file_path: str) -> dict[str, Any]:
        """Process OMSZ netCDF file to array with metadata

        The netCDF structure:
        - refl2D: byte array (813 x 961)
        - Scaling: dBZ = raw_value / 2 - 32
        - La1, Lo1: first lat/lon
        - Dx, Dy: grid increments
        - GMTime: timestamp string

        Args:
            file_path: Path to netCDF file

        Returns:
            Dictionary with processed data, coordinates, and metadata
        """
        try:
            with nc.Dataset(file_path, "r") as dataset:
                # Determine which variable to read based on filename
                if "refl2D_pscappi" in file_path:
                    var_name = "refl2D_pscappi"
                elif "refl3D" in file_path:
                    var_name = "refl3D"
                else:
                    var_name = "refl2D"

                # Read raw data
                raw_data = dataset.variables[var_name][:]

                # Get coordinate parameters
                la1 = float(dataset.variables["La1"][:])  # First latitude (north)
                lo1 = float(dataset.variables["Lo1"][:])  # First longitude (west)
                dx = float(dataset.variables["Dx"][:])  # Longitude increment
                dy = float(dataset.variables["Dy"][:])  # Latitude increment

                # Get timestamp
                gm_time = dataset.variables["GMTime"][:]
                timestamp_str = "".join(
                    [c.decode("utf-8") if isinstance(c, bytes) else c for c in gm_time]
                )

                # IMPORTANT: Data is stored as int8 but should be interpreted as uint8
                # Values >= 128 wrap around to negative in int8 representation
                # Convert to uint8 view for correct interpretation
                if raw_data.dtype == np.int8:
                    raw_data = raw_data.view(np.uint8)

                # Apply scaling: dBZ = raw / 2 - 32
                scaled_data = raw_data.astype(np.float32) * self.gain + self.offset

                # Handle nodata (based on data analysis):
                # - 255 (uint8): nodata/outside coverage (37.5% of pixels)
                # - 0 (uint8): static background/coverage mask (62.3% of pixels) → -32 dBZ
                #   This is the grey coverage mask that should be transparent
                # - Actual radar data starts at raw value 1 (0.5 dBZ)
                scaled_data[raw_data == 255] = np.nan  # Outside coverage
                scaled_data[raw_data == 0] = np.nan  # Grey coverage mask (background)

                # Get dimensions
                n_lat, n_lon = raw_data.shape

                # Calculate extent (la1 is the NORTH boundary for OMSZ)
                north = la1
                south = la1 - (n_lat - 1) * dy
                west = lo1
                east = lo1 + (n_lon - 1) * dx

                # Create coordinate arrays
                lons = np.linspace(west, east, n_lon)
                lats = np.linspace(north, south, n_lat)  # North to south

                return {
                    "data": scaled_data,
                    "coordinates": {"lons": lons, "lats": lats},
                    "metadata": {
                        "product": var_name,
                        "quantity": "DBZH",
                        "timestamp": timestamp_str,
                        "source": "OMSZ",
                        "units": "dBZ",
                        "nodata_value": np.nan,
                        "gain": self.gain,
                        "offset": self.offset,
                    },
                    "extent": {
                        "wgs84": {
                            "west": west,
                            "east": east,
                            "south": south,
                            "north": north,
                        }
                    },
                    "dimensions": raw_data.shape,
                    "timestamp": timestamp_str,  # YYYYMMDDHHMM format
                }

        except Exception as e:
            raise RuntimeError(f"Failed to process OMSZ file {file_path}: {e}")

    def get_extent(self) -> dict[str, Any]:
        """Get OMSZ radar coverage extent

        Based on actual netCDF data:
        - La1=50.5, Lo1=13.5
        - Grid: 813 x 961
        - Dx=0.0125, Dy=0.008
        - Calculated extent: N=50.5, S=44.0, W=13.5, E=25.5
        """
        # OMSZ radar coverage (from actual data)
        wgs84 = {"west": 13.5, "east": 25.5, "south": 44.0, "north": 50.5}

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
                "bounds": [x_min, y_min, x_max, y_max],
            },
            "projection": "EPSG:3857",
            "grid_size": [813, 961],  # [height, width]
            "resolution_m": [890, 930],  # Approximate [y_res, x_res] in meters
        }

    def extract_extent_only(self, file_path: str) -> dict[str, Any]:
        """Extract extent from OMSZ netCDF without loading data array.

        MEMORY OPTIMIZATION: Reads only coordinate parameters (~100 bytes)
        instead of loading the full data array (~6 MB for OMSZ grid).

        Args:
            file_path: Path to OMSZ netCDF file

        Returns:
            Dictionary with extent and dimensions
        """
        try:
            with nc.Dataset(file_path, "r") as dataset:
                # Determine which variable to check based on filename
                if "refl2D_pscappi" in file_path:
                    var_name = "refl2D_pscappi"
                elif "refl3D" in file_path:
                    var_name = "refl3D"
                else:
                    var_name = "refl2D"

                # Get dimensions WITHOUT loading data
                dimensions = dataset.variables[var_name].shape

                # Get coordinate parameters
                la1 = float(dataset.variables["La1"][:])  # First latitude (north)
                lo1 = float(dataset.variables["Lo1"][:])  # First longitude (west)
                dx = float(dataset.variables["Dx"][:])  # Longitude increment
                dy = float(dataset.variables["Dy"][:])  # Latitude increment

                n_lat, n_lon = dimensions

                # Calculate extent (la1 is the NORTH boundary for OMSZ)
                north = la1
                south = la1 - (n_lat - 1) * dy
                west = lo1
                east = lo1 + (n_lon - 1) * dx

                return {
                    "extent": {
                        "wgs84": {
                            "west": west,
                            "east": east,
                            "south": south,
                            "north": north,
                        }
                    },
                    "dimensions": dimensions,
                }
        except Exception as e:
            raise RuntimeError(f"Failed to extract OMSZ extent from {file_path}: {e}")

    def cleanup_temp_files(self) -> int:
        """Clean up temporary files and parent temp directories.

        Overrides base class to also clean up OMSZ-specific temp directories.
        """
        cleaned_count = 0
        for cache_key, file_path in list(self.temp_files.items()):
            try:
                if os.path.exists(file_path):
                    # Also clean up parent temp directory (OMSZ-specific)
                    parent_dir = os.path.dirname(file_path)
                    os.unlink(file_path)
                    if (
                        parent_dir.startswith(tempfile.gettempdir())
                        and "omsz_" in parent_dir
                    ):
                        try:
                            os.rmdir(parent_dir)
                        except OSError:
                            pass  # Directory not empty or other issue
                    cleaned_count += 1
                del self.temp_files[cache_key]
            except Exception as e:
                print(f"⚠️  Could not delete temp file {file_path}: {e}")

        if cleaned_count > 0:
            print(f"🧹 Cleaned up {cleaned_count} temporary OMSZ files")
        return cleaned_count
