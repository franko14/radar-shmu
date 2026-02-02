#!/usr/bin/env python3
"""
ARSO (Agencija Republike Slovenije za okolje) Radar Source

Handles downloading and processing of ARSO radar data in SRD-3 format.
SRD-3 is a proprietary Slovenian Radar Data format with ASCII header
and byte-encoded data using Lambert Conformal Conic projection.

Documentation: https://meteo.arso.gov.si/uploads/meteo/help/sl/SRD3Format.html
"""

import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import requests
from pyproj import CRS, Transformer

from ..core.base import RadarSource, lonlat_to_mercator
from ..core.logging import get_logger
from ..utils.parallel_download import (
    create_download_result,
    create_error_result,
)

logger = get_logger(__name__)


class ARSORadarSource(RadarSource):
    """ARSO Slovenia radar data source using SRD-3 format"""

    BASE_URL = "https://meteo.arso.gov.si/uploads/probase/www/observ/radar"

    # Product mapping: internal name -> (filename, description)
    PRODUCTS = {
        "zm": {
            "file": "si0-zm.srd",
            "name": "Maximum Reflectivity (ZM)",
            "units": "dBZ",
            "description": "Column maximum reflectivity",
        },
        "rrg": {
            "file": "si0-rrg.srd",
            "name": "Ground Rain Rate (RRG)",
            "units": "dBR/h",
            "description": "Precipitation intensity at ground level",
        },
    }

    # SIRAD projection parameters (from SRD-3 specification)
    # Lambert Conformal Conic centered on GEOSS reference point
    SIRAD_PROJ4 = (
        "+proj=lcc +lat_1=46.12 +lat_2=46.12 +lat_0=46.12 "
        "+lon_0=14.815 +x_0=0 +y_0=0 +R=6371000 +units=km +no_defs"
    )

    # Grid parameters (from SRD-3 specification)
    # Domain: 400x300 km, cells: 401x301, cellsize: 1km
    # Center offset: 4 km west, 6 km south of GEOSS (14.815, 46.12)
    GRID_NCELL = (401, 301)  # (width, height) = (i, j) = (zonal, meridional)
    GRID_CELLSIZE = 1.0  # km
    GRID_CENTER = (201, 151)  # Grid center cell indices
    GEOSS_CELL = (205, 145)  # GEOSS reference point cell indices

    def __init__(self):
        super().__init__("arso")
        # temp_files is initialized in base class

        # Initialize projection transformer
        self.sirad_crs = CRS.from_proj4(self.SIRAD_PROJ4)
        self.wgs84_crs = CRS.from_epsg(4326)
        self.transformer = Transformer.from_crs(
            self.sirad_crs, self.wgs84_crs, always_xy=True
        )

        # Pre-compute grid coordinates (only done once)
        self._lons = None
        self._lats = None
        self._extent_wgs84 = None

    def get_available_products(self) -> list[str]:
        """Get list of available ARSO radar products"""
        return list(self.PRODUCTS.keys())

    def get_product_metadata(self, product: str) -> dict[str, Any]:
        """Get metadata for a specific ARSO product"""
        if product in self.PRODUCTS:
            return {"product": product, "source": self.name, **self.PRODUCTS[product]}
        return super().get_product_metadata(product)

    def _get_product_url(self, product: str) -> str:
        """Generate URL for ARSO product"""
        if product not in self.PRODUCTS:
            raise ValueError(f"Unknown product: {product}")

        filename = self.PRODUCTS[product]["file"]
        return f"{self.BASE_URL}/{filename}"

    def _parse_srd_header(self, content: str) -> dict[str, Any]:
        """Parse SRD-3 format header

        Header format: "key value1 value2 ... # optional comment"
        """
        header = {}
        lines = content.split("\n")

        for line in lines:
            # Stop at DATA marker
            if line.strip() == "DATA":
                break

            # Remove comments and strip whitespace
            line = line.split("#")[0].strip()
            if not line:
                continue

            # Parse key-value pairs
            parts = line.split()
            if len(parts) >= 2:
                key = parts[0]
                values = parts[1:]
                # Convert to appropriate types
                if len(values) == 1:
                    # Single value - try to convert to number
                    try:
                        header[key] = int(values[0])
                    except ValueError:
                        try:
                            header[key] = float(values[0])
                        except ValueError:
                            header[key] = values[0]
                else:
                    # Multiple values - try to convert each
                    converted = []
                    for v in values:
                        try:
                            converted.append(int(v))
                        except ValueError:
                            try:
                                converted.append(float(v))
                            except ValueError:
                                converted.append(v)
                    header[key] = converted
            elif len(parts) == 1:
                header[parts[0]] = None

        return header

    def _parse_srd_data(self, content: str, header: dict[str, Any]) -> np.ndarray:
        """Parse SRD-3 data section

        Data is byte-encoded as ASCII characters starting from offset (default 64='@').
        Values are calculated as: value = start + slope * (byte_value - offset)
        """
        # Find DATA marker
        data_start = content.find("\nDATA\n")
        if data_start == -1:
            data_start = content.find("\nDATA\r\n")
        if data_start == -1:
            raise ValueError("No DATA marker found in SRD file")

        # Extract data section (everything after DATA\n)
        data_section = content[data_start + 6 :]  # Skip "\nDATA\n"

        # Get grid dimensions from header
        ncell = header.get("ncell", self.GRID_NCELL)
        if isinstance(ncell, list):
            width, height = ncell[0], ncell[1]
        else:
            width, height = self.GRID_NCELL

        # Get quantization parameters
        offset = header.get("offset", 64)
        start = header.get("start", 12.0)
        slope = header.get("slope", 3.0)

        # Parse data - each character represents one byte value
        # Data is organized: k (vertical), j (meridional, N-S), i (zonal, W-E)
        # For 2D fields, rows are j (north to south), columns are i (west to east)
        raw_data = []
        for char in data_section:
            byte_val = ord(char)
            # Skip newlines and control characters
            if byte_val < 32:
                continue
            raw_data.append(byte_val)

        # Convert to numpy array
        raw_array = np.array(raw_data, dtype=np.int32)

        # Check if we have enough data
        expected_size = width * height
        if len(raw_array) < expected_size:
            logger.warning(f"Expected {expected_size} values, got {len(raw_array)}", extra={"source": "arso"})
            # Pad with nodata
            raw_array = np.pad(
                raw_array, (0, expected_size - len(raw_array)), constant_values=offset
            )
        elif len(raw_array) > expected_size:
            raw_array = raw_array[:expected_size]

        # Reshape to 2D (height x width) - data is stored row by row (j=N-S, i=W-E)
        raw_array = raw_array.reshape((height, width))

        # Apply quantization formula: value = start + slope * (byte - offset)
        scaled_data = start + slope * (raw_array.astype(np.float32) - offset)

        # Values at offset (typically '@' = 64) represent no data / below threshold
        scaled_data[raw_array == offset] = np.nan

        return scaled_data

    def _compute_grid_coordinates(self):
        """Compute WGS84 coordinates for each grid cell (cached)"""
        if self._lons is not None:
            return

        width, height = self.GRID_NCELL

        # Grid cell indices
        # i = 1 to 401 (zonal/W-E), j = 1 to 301 (meridional/N-S)
        # Grid center at [201, 151], GEOSS at [205, 145]
        # GEOSS offset: 4 km west, 6 km south from center

        # Calculate x,y coordinates in km from GEOSS reference
        # Center cell offset from GEOSS: (201-205, 151-145) = (-4, 6) cells = (-4, 6) km
        # So GEOSS is at (0, 0) in projection coordinates

        # Create grid of cell indices (1-indexed as per spec)
        i_indices = np.arange(1, width + 1)
        j_indices = np.arange(1, height + 1)

        # Convert cell indices to km coordinates
        # x = (i - GEOSS_i) * cellsize, y = (GEOSS_j - j) * cellsize
        # Note: j increases southward in data, but y increases northward in projection
        geoss_i, geoss_j = self.GEOSS_CELL
        x_km = (i_indices - geoss_i) * self.GRID_CELLSIZE
        y_km = (geoss_j - j_indices) * self.GRID_CELLSIZE

        # Create 2D meshgrid
        x_grid, y_grid = np.meshgrid(x_km, y_km)

        # Transform to WGS84
        lons, lats = self.transformer.transform(x_grid, y_grid)

        self._lons = lons
        self._lats = lats

        # Calculate extent
        self._extent_wgs84 = {
            "west": float(np.min(lons)),
            "east": float(np.max(lons)),
            "south": float(np.min(lats)),
            "north": float(np.max(lats)),
        }

    def _download_single_file(self, product: str) -> dict[str, Any]:
        """Download a single radar file"""
        if product not in self.PRODUCTS:
            return create_error_result("", product, f"Unknown product: {product}")

        try:
            url = self._get_product_url(product)

            # Check if we've already downloaded this file in this session
            cache_key = f"latest_{product}"
            if cache_key in self.temp_files:
                # Check if file still exists
                if os.path.exists(self.temp_files[cache_key]):
                    return create_download_result(
                        timestamp="",
                        product=product,
                        path=self.temp_files[cache_key],
                        url=url,
                        cached=True,
                    )

            # Download to temporary file
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Save to temp file
            with tempfile.NamedTemporaryFile(
                suffix=f"_arso_{product}.srd", delete=False, mode="wb"
            ) as temp_file:
                temp_file.write(response.content)
                temp_path = Path(temp_file.name)

            # Track the temporary file
            self.temp_files[cache_key] = str(temp_path)

            return create_download_result(
                timestamp="",
                product=product,
                path=str(temp_path),
                url=url,
                cached=False,
            )

        except Exception as e:
            return create_error_result("", product, str(e))

    def get_available_timestamps(
        self,
        count: int = 1,
        products: list[str] = None,
    ) -> list[str]:
        """Get list of available ARSO timestamps.

        ARSO only provides the latest timestamp (no archive).
        This method downloads the file header to extract the timestamp.

        Args:
            count: Ignored (ARSO only provides latest)
            products: List of products to check (default: ['zm'])

        Returns:
            List with single timestamp string, or empty list if unavailable
        """
        if products is None:
            products = ["zm"]

        # Try to get timestamp from the first available product
        for product in products:
            try:
                url = self._get_product_url(product)
                # Download just enough to parse the header (first 2KB)
                response = requests.get(url, timeout=30, headers={"Range": "bytes=0-2048"})
                if response.status_code in [200, 206]:
                    content = response.text
                    header = self._parse_srd_header(content)
                    time_parts = header.get("time", [])
                    if isinstance(time_parts, list) and len(time_parts) >= 5:
                        timestamp = f"{time_parts[0]:04d}{time_parts[1]:02d}{time_parts[2]:02d}{time_parts[3]:02d}{time_parts[4]:02d}00"
                        return [timestamp]
            except Exception as e:
                logger.debug(f"Could not fetch ARSO timestamp: {e}", extra={"source": "arso"})
                continue

        return []

    def download_timestamps(
        self,
        timestamps: list[str],
        products: list[str] = None,
    ) -> list[dict[str, Any]]:
        """Download specific ARSO timestamps.

        ARSO only provides the latest data. This method downloads the latest
        and returns it only if it matches one of the requested timestamps.

        Args:
            timestamps: List of requested timestamps (YYYYMMDDHHMMSS format)
            products: List of products to download (default: ['zm'])

        Returns:
            List of file info dicts (may be empty if latest doesn't match)
        """
        if products is None:
            products = ["zm"]

        if not timestamps:
            return []

        # Download latest and check if it matches
        downloaded_files = self.download_latest(count=1, products=products)

        # Filter to only return files whose timestamp matches requested
        matching_files = []
        for file_info in downloaded_files:
            file_ts = file_info.get("timestamp", "")
            # Normalize timestamps for comparison (first 12 digits = YYYYMMDDHHMM)
            file_ts_normalized = file_ts[:12]
            for requested_ts in timestamps:
                requested_normalized = requested_ts[:12]
                if file_ts_normalized == requested_normalized:
                    matching_files.append(file_info)
                    break

        if matching_files:
            logger.info(
                f"ARSO: Latest timestamp matches requested, returning {len(matching_files)} files",
                extra={"source": "arso"},
            )
        else:
            logger.debug(
                f"ARSO: Latest timestamp doesn't match any requested timestamps",
                extra={"source": "arso"},
            )

        return matching_files

    def download_latest(
        self,
        count: int = 1,
        products: list[str] = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Download latest ARSO radar data

        Note: ARSO only provides the latest data at fixed URLs (no archive).
        The count, start_time, and end_time parameters are accepted for
        interface compatibility but are effectively ignored.

        Args:
            count: Not used (ARSO only provides latest data)
            products: List of products to download (default: ['zm'])
            start_time: Not used
            end_time: Not used

        Returns:
            List of downloaded file information dictionaries
        """
        if products is None:
            products = ["zm"]  # Default to max reflectivity

        logger.info(f"Downloading ARSO radar data ({', '.join(products)})...", extra={"source": "arso"})

        # Note: ARSO doesn't provide historical data via public URL
        if count > 1 or start_time or end_time:
            logger.warning("ARSO only provides latest data (no archive access)", extra={"source": "arso"})

        downloaded_files = []

        for product in products:
            result = self._download_single_file(product)
            if result["success"]:
                # Get timestamp from header by reading file
                try:
                    with open(result["path"], encoding="latin-1") as f:
                        content = f.read()
                    header = self._parse_srd_header(content)
                    time_parts = header.get("time", [])
                    if isinstance(time_parts, list) and len(time_parts) >= 5:
                        timestamp = f"{time_parts[0]:04d}{time_parts[1]:02d}{time_parts[2]:02d}{time_parts[3]:02d}{time_parts[4]:02d}00"
                    else:
                        # Fallback to current time
                        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M00")
                    result["timestamp"] = timestamp
                except Exception as e:
                    logger.warning(f"Could not parse timestamp: {e}", extra={"source": "arso"})
                    result["timestamp"] = datetime.utcnow().strftime("%Y%m%d%H%M00")

                downloaded_files.append(result)
                if result["cached"]:
                    logger.debug(f"Using cached: {product}", extra={"source": "arso"})
                else:
                    logger.info(
                        f"Downloaded: {product} (timestamp: {result['timestamp']})",
                        extra={"source": "arso"},
                    )
            else:
                logger.error(f"Failed {product}: {result.get('error', 'Unknown error')}", extra={"source": "arso"})

        logger.info(f"ARSO: Downloaded {len(downloaded_files)} files", extra={"source": "arso", "count": len(downloaded_files)})
        return downloaded_files

    def process_to_array(self, file_path: str) -> dict[str, Any]:
        """Process ARSO SRD file to array with metadata"""

        try:
            # Read file content
            with open(file_path, encoding="latin-1") as f:
                content = f.read()

            # Parse header and data
            header = self._parse_srd_header(content)
            data = self._parse_srd_data(content, header)

            # Ensure grid coordinates are computed
            self._compute_grid_coordinates()

            # Extract timestamp from header
            time_parts = header.get("time", [])
            if isinstance(time_parts, list) and len(time_parts) >= 5:
                timestamp = f"{time_parts[0]:04d}{time_parts[1]:02d}{time_parts[2]:02d}{time_parts[3]:02d}{time_parts[4]:02d}00"
            else:
                timestamp = datetime.utcnow().strftime("%Y%m%d%H%M00")

            # Determine product type from filename or header
            domain = header.get("domain", "SI0")
            unit = header.get("unit", "DBZ")
            product = "ZM" if "zm" in file_path.lower() else "RRG"
            quantity = "DBZH" if unit == "DBZ" else "RATE"

            # Build projection info for reprojector
            # ARSO uses Lambert Conformal Conic (LCC/SIRAD) projection natively
            # Data has already been transformed to WGS84 coordinates during processing
            # Document the native projection for reference and potential future use
            projection_info = {
                "type": "wgs84",  # Output is WGS84 after transformation
                "native_projection": "lcc",
                "native_proj_def": self.SIRAD_PROJ4,
                "grid_params": {
                    "ncell": self.GRID_NCELL,
                    "cellsize_km": self.GRID_CELLSIZE,
                    "center_cell": self.GRID_CENTER,
                    "geoss_cell": self.GEOSS_CELL,
                },
            }

            return {
                "data": data,
                "coordinates": None,  # Use projection instead
                "projection": projection_info,
                "metadata": {
                    "product": product,
                    "quantity": quantity,
                    "timestamp": timestamp,
                    "source": "ARSO",
                    "units": self._get_units(unit),
                    "nodata_value": np.nan,
                    "domain": domain,
                    "native_projection": "LCC (SIRAD)",
                },
                "extent": {"wgs84": self._extent_wgs84},
                "dimensions": data.shape,
                "timestamp": timestamp[:14],  # YYYYMMDDHHMMSS format
            }

        except Exception as e:
            raise RuntimeError(f"Failed to process ARSO file {file_path}: {e}")

    def _get_units(self, unit: str) -> str:
        """Get human-readable units"""
        units_map = {"DBZ": "dBZ", "DBRH": "dBR/h", "MM": "mm"}
        return units_map.get(unit.upper(), unit)

    def get_extent(self) -> dict[str, Any]:
        """Get ARSO radar coverage extent"""

        # Ensure coordinates are computed
        self._compute_grid_coordinates()

        # Convert to Web Mercator
        x_min, y_min = lonlat_to_mercator(
            self._extent_wgs84["west"], self._extent_wgs84["south"]
        )
        x_max, y_max = lonlat_to_mercator(
            self._extent_wgs84["east"], self._extent_wgs84["north"]
        )

        return {
            "wgs84": self._extent_wgs84,
            "mercator": {
                "x_min": x_min,
                "x_max": x_max,
                "y_min": y_min,
                "y_max": y_max,
                "bounds": [x_min, y_min, x_max, y_max],
            },
            "projection": "EPSG:3857",
            "grid_size": [self.GRID_NCELL[1], self.GRID_NCELL[0]],  # [height, width]
            "resolution_m": [1000, 1000],  # 1 km resolution
        }

    def extract_extent_only(self, file_path: str) -> dict[str, Any]:
        """Extract extent from ARSO SRD file without loading full data.

        MEMORY OPTIMIZATION: ARSO extent is pre-computed from grid parameters.
        Only reads header to verify grid dimensions.

        Args:
            file_path: Path to ARSO SRD file

        Returns:
            Dictionary with extent and dimensions
        """
        # Ensure coordinates are computed (uses cached extent)
        self._compute_grid_coordinates()

        # Read header to get dimensions
        try:
            with open(file_path, encoding="latin-1") as f:
                content = f.read(2000)  # Read only first 2KB for header

            header = self._parse_srd_header(content)
            ncell = header.get("ncell", self.GRID_NCELL)
            if isinstance(ncell, list):
                width, height = ncell[0], ncell[1]
            else:
                width, height = self.GRID_NCELL

            return {
                "extent": {"wgs84": self._extent_wgs84},
                "dimensions": (height, width),
            }
        except Exception:
            # Fallback to pre-computed values
            return {
                "extent": {"wgs84": self._extent_wgs84},
                "dimensions": (self.GRID_NCELL[1], self.GRID_NCELL[0]),
            }

    # cleanup_temp_files() is inherited from RadarSource base class
