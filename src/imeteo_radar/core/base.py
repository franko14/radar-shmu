#!/usr/bin/env python3
"""
Base classes for radar data sources
"""

import os
from abc import ABC, abstractmethod
from typing import Any

import numpy as np

from .logging import get_logger

logger = get_logger(__name__)


class RadarSource(ABC):
    """Abstract base class for radar data sources"""

    def __init__(self, name: str):
        self.name = name
        self.cache_dir = f"processed/{name}_data"
        self.temp_files: dict[str, str] = {}  # Track temporary files for cleanup

    @abstractmethod
    def download_latest(
        self, count: int, products: list[str] = None
    ) -> list[dict[str, Any]]:
        """
        Download latest available radar data files

        Args:
            count: Number of timestamps to download
            products: List of product types to download

        Returns:
            List of downloaded file information dictionaries
        """
        pass

    @abstractmethod
    def process_to_array(self, file_path: str) -> dict[str, Any]:
        """
        Process radar file to numpy array with metadata

        Args:
            file_path: Path to radar data file

        Returns:
            Dictionary with processed data, coordinates, and metadata
        """
        pass

    @abstractmethod
    def get_extent(self) -> dict[str, Any]:
        """
        Get geographic extent information for this radar source

        Returns:
            Dictionary with extent information in various projections
        """
        pass

    @abstractmethod
    def get_available_products(self) -> list[str]:
        """
        Get list of available radar products for this source

        Returns:
            List of product identifiers
        """
        pass

    def get_product_metadata(self, product: str) -> dict[str, Any]:
        """
        Get metadata for a specific product

        Args:
            product: Product identifier

        Returns:
            Dictionary with product metadata
        """
        return {
            "product": product,
            "source": self.name,
            "units": "unknown",
            "description": "No description available",
        }

    def extract_extent_only(self, file_path: str) -> dict[str, Any]:
        """Extract only extent and dimensions without loading full data array.

        MEMORY OPTIMIZATION: This method reads only HDF5 metadata (extent + dimensions)
        without loading the full data array into memory. Subclasses should override
        this method for memory-efficient implementations.

        Default implementation falls back to full processing - override in subclasses
        to avoid loading the entire data array just to get extent information.

        Args:
            file_path: Path to radar data file

        Returns:
            Dictionary with:
                - 'extent': Geographic extent in WGS84 {'wgs84': {west, east, south, north}}
                - 'dimensions': Data array shape as tuple (height, width)
        """
        # Default implementation: fall back to full processing
        # Subclasses should override for memory efficiency
        full_data = self.process_to_array(file_path)
        return {
            "extent": full_data["extent"],
            "dimensions": full_data["dimensions"],
        }

    def cleanup_temp_files(self) -> int:
        """Clean up all temporary files created during this session.

        This method is called after processing to remove temporary downloaded files.
        Subclasses can override this for source-specific cleanup logic.

        Returns:
            Number of files cleaned up
        """
        cleaned_count = 0
        for cache_key, file_path in list(self.temp_files.items()):
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
                    cleaned_count += 1
                del self.temp_files[cache_key]
            except Exception as e:
                logger.warning(f"Could not delete temp file {file_path}: {e}")

        if cleaned_count > 0:
            logger.debug(
                f"Cleaned up {cleaned_count} temporary {self.name.upper()} files",
                extra={"source": self.name, "count": cleaned_count},
            )
        return cleaned_count


class RadarData:
    """Container for processed radar data"""

    def __init__(
        self,
        data: np.ndarray,
        coordinates: dict[str, np.ndarray],
        metadata: dict[str, Any],
        extent: dict[str, Any],
    ):
        self.data = data
        self.coordinates = coordinates
        self.metadata = metadata
        self.extent = extent

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "data": self.data.tolist() if hasattr(self.data, "tolist") else self.data,
            "coordinates": {
                key: arr.tolist() if hasattr(arr, "tolist") else arr
                for key, arr in self.coordinates.items()
            },
            "metadata": self.metadata,
            "extent": self.extent,
        }


def lonlat_to_mercator(lon, lat):
    """Convert WGS84 coordinates to Web Mercator (EPSG:3857)

    Supports both scalar and array inputs for vectorized operations.

    Args:
        lon: Longitude in degrees (scalar or numpy array)
        lat: Latitude in degrees (scalar or numpy array)

    Returns:
        Tuple of (x, y) in meters (scalars or numpy arrays)
    """
    # Check if inputs are arrays
    is_array = isinstance(lon, np.ndarray) or isinstance(lat, np.ndarray)

    if is_array:
        # Vectorized NumPy operations (100-1000x faster!)
        x = lon * 20037508.34 / 180.0
        y = np.log(np.tan((90.0 + lat) * np.pi / 360.0)) / (np.pi / 180.0)
        y = y * 20037508.34 / 180.0
        return x, y
    else:
        # Scalar operations (backward compatible)
        import math

        x = lon * 20037508.34 / 180.0
        y = math.log(math.tan((90.0 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
        y = y * 20037508.34 / 180.0
        return x, y


def mercator_to_lonlat(x: float, y: float) -> tuple[float, float]:
    """Convert Web Mercator (EPSG:3857) to WGS84 coordinates"""
    import math

    lon = x / 20037508.34 * 180.0
    lat = (
        math.atan(math.exp(y / 20037508.34 * math.pi / 180.0)) * 360.0 / math.pi - 90.0
    )
    return lon, lat


def extract_hdf5_corner_extent(
    file_path: str, fallback_extent: dict[str, float] = None
) -> dict[str, Any]:
    """Extract extent from HDF5 file using corner coordinates (LL/UR pattern).

    This is a shared utility for sources that use ODIM_H5 format with corner
    coordinates stored in the 'where' group (SHMU, CHMI pattern).

    MEMORY OPTIMIZATION: Reads only HDF5 metadata (~100 bytes) without loading
    the full data array.

    Args:
        file_path: Path to HDF5 file
        fallback_extent: Optional fallback extent if coordinates not found
            Format: {"west": float, "east": float, "south": float, "north": float}

    Returns:
        Dictionary with:
            - 'extent': Geographic extent in WGS84 {'wgs84': {west, east, south, north}}
            - 'dimensions': Data array shape as tuple (height, width)

    Raises:
        RuntimeError: If extraction fails and no fallback provided
    """
    import h5py

    try:
        with h5py.File(file_path, "r") as f:
            # Read only where attributes - no data array loaded
            where_attrs = dict(f["where"].attrs)

            # Decode byte strings
            for key, value in where_attrs.items():
                if isinstance(value, bytes):
                    where_attrs[key] = value.decode("utf-8")

            # Get dimensions from dataset shape WITHOUT loading data
            dimensions = f["dataset1/data1/data"].shape

            # Extract corner coordinates
            if "LL_lon" in where_attrs and "UR_lon" in where_attrs:
                extent = {
                    "west": float(where_attrs["LL_lon"]),
                    "east": float(where_attrs["UR_lon"]),
                    "south": float(where_attrs["LL_lat"]),
                    "north": float(where_attrs["UR_lat"]),
                }
            elif fallback_extent:
                extent = fallback_extent
            else:
                raise ValueError(
                    "Corner coordinates not found and no fallback provided"
                )

            return {
                "extent": {"wgs84": extent},
                "dimensions": dimensions,
            }
    except Exception as e:
        if fallback_extent:
            # Return fallback on error
            return {
                "extent": {"wgs84": fallback_extent},
                "dimensions": (0, 0),
            }
        raise RuntimeError(f"Failed to extract HDF5 extent from {file_path}: {e}")
