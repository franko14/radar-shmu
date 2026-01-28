#!/usr/bin/env python3
"""
HDF5 processing utilities for ODIM_H5 radar data.

Consolidates common HDF5 reading, attribute extraction, and data scaling
logic that was duplicated across source classes (DWD, SHMU, CHMI, IMGW).
"""

from typing import Any

import h5py
import numpy as np

from ..core.logging import get_logger

logger = get_logger(__name__)


def decode_hdf5_attrs(attrs: dict) -> dict[str, Any]:
    """Decode HDF5 attributes, converting bytes to strings.

    Args:
        attrs: Dictionary of HDF5 attributes (may contain bytes)

    Returns:
        Dictionary with bytes decoded to strings
    """
    decoded = {}
    for key, value in attrs.items():
        if isinstance(value, bytes):
            decoded[key] = value.decode("utf-8")
        elif isinstance(value, np.bytes_):
            decoded[key] = value.decode("utf-8")
        else:
            decoded[key] = value
    return decoded


def get_scaling_params(
    data_what_attrs: dict,
    default_gain: float = 1.0,
    default_offset: float = 0.0,
    default_nodata: int = 65535,
    default_undetect: int = 0,
) -> dict[str, float | int]:
    """Extract scaling parameters from HDF5 data/what attributes.

    Args:
        data_what_attrs: Attributes from dataset1/data1/what
        default_gain: Default gain value
        default_offset: Default offset value
        default_nodata: Default nodata value
        default_undetect: Default undetect value

    Returns:
        Dictionary with gain, offset, nodata, undetect values
    """
    return {
        "gain": float(data_what_attrs.get("gain", default_gain)),
        "offset": float(data_what_attrs.get("offset", default_offset)),
        "nodata": int(data_what_attrs.get("nodata", default_nodata)),
        "undetect": int(data_what_attrs.get("undetect", default_undetect)),
    }


def scale_radar_data(
    data: np.ndarray,
    gain: float,
    offset: float,
    nodata: int | float,
    undetect: int | float,
    handle_uint8: bool = False,
) -> np.ndarray:
    """Scale radar data using gain and offset, handling special values.

    Applies the standard ODIM_H5 scaling formula: value = gain * data + offset

    Args:
        data: Raw data array from HDF5
        gain: Scaling gain factor
        offset: Scaling offset
        nodata: Value indicating no data (set to NaN)
        undetect: Value indicating no detection (set to NaN)
        handle_uint8: If True, also handle 255 as nodata for uint8 arrays

    Returns:
        Scaled data array with special values as NaN
    """
    # Apply scaling
    scaled_data = data.astype(np.float32) * gain + offset

    # Handle special values
    scaled_data[data == nodata] = np.nan
    scaled_data[data == undetect] = np.nan

    # Handle uint8 255 value (common for no-data in some sources)
    if handle_uint8 and data.dtype == np.uint8:
        scaled_data[data == 255] = np.nan

    return scaled_data


def extract_corner_coordinates(
    where_attrs: dict,
    fallback_extent: dict[str, float] | None = None,
) -> dict[str, float]:
    """Extract corner coordinates from HDF5 where attributes.

    Args:
        where_attrs: Attributes from 'where' group
        fallback_extent: Fallback extent if coordinates not found
            Format: {"west": float, "east": float, "south": float, "north": float}

    Returns:
        Dictionary with west, east, south, north coordinates
    """
    # Try to extract coordinates from where attributes
    ll_lon = where_attrs.get("LL_lon") or where_attrs.get("ll_lon")
    ll_lat = where_attrs.get("LL_lat") or where_attrs.get("ll_lat")
    ur_lon = where_attrs.get("UR_lon") or where_attrs.get("ur_lon")
    ur_lat = where_attrs.get("UR_lat") or where_attrs.get("ur_lat")

    # Check if all coordinates are present
    if all(v is not None for v in [ll_lon, ll_lat, ur_lon, ur_lat]):
        return {
            "west": float(ll_lon),
            "east": float(ur_lon),
            "south": float(ll_lat),
            "north": float(ur_lat),
        }

    # Use fallback if provided
    if fallback_extent:
        logger.debug("Using fallback extent (coordinates not in HDF5)")
        return fallback_extent

    raise ValueError("Could not extract coordinates and no fallback provided")


def create_coordinate_arrays(
    extent: dict[str, float],
    shape: tuple[int, int],
    flip_lat: bool = True,
) -> dict[str, np.ndarray]:
    """Create longitude and latitude coordinate arrays.

    Args:
        extent: Dictionary with west, east, south, north
        shape: Data array shape (rows, cols)
        flip_lat: Whether to flip latitude array (north to south)

    Returns:
        Dictionary with 'lons' and 'lats' arrays
    """
    rows, cols = shape

    lons = np.linspace(extent["west"], extent["east"], cols)
    lats = np.linspace(extent["south"], extent["north"], rows)

    if flip_lat:
        lats = np.flip(lats)

    return {"lons": lons, "lats": lats}


def get_quantity_units(quantity: str) -> str:
    """Get units for a radar quantity.

    Args:
        quantity: ODIM_H5 quantity string (e.g., "DBZH", "TH", "ACRR")

    Returns:
        Units string (e.g., "dBZ", "mm")
    """
    units_map = {
        "DBZH": "dBZ",
        "DBZ": "dBZ",
        "TH": "dBZ",
        "TV": "dBZ",
        "HGHT": "km",
        "ACRR": "mm",
        "RATE": "mm/h",
        "VRAD": "m/s",
        "WRAD": "m/s",
        "RHOHV": "ratio",
        "ZDR": "dB",
        "KDP": "deg/km",
        "PHIDP": "deg",
    }
    return units_map.get(quantity.upper(), "unknown")


def find_main_dataset(hdf_file: h5py.File) -> np.ndarray | None:
    """Find the main data array in an HDF5 file.

    Tries standard ODIM_H5 paths first, then searches for large arrays.

    Args:
        hdf_file: Open HDF5 file handle

    Returns:
        Data array or None if not found
    """
    # Standard ODIM_H5 paths to try
    standard_paths = [
        "dataset1/data1/data",
        "dataset1/data/data",
        "dataset/data1/data",
        "dataset/data",
        "data1/data",
        "data",
    ]

    for path in standard_paths:
        try:
            data = hdf_file[path][:]
            logger.debug(f"Found data at: {path}")
            return data
        except KeyError:
            continue

    # Search for any large 2D array
    def find_large_array(group, min_size=10000):
        for key in group.keys():
            item = group[key]
            if isinstance(item, h5py.Dataset):
                if item.ndim == 2 and item.size > min_size:
                    return item[:]
            elif isinstance(item, h5py.Group):
                result = find_large_array(item, min_size)
                if result is not None:
                    return result
        return None

    return find_large_array(hdf_file)


def extract_odim_metadata(hdf_file: h5py.File) -> dict[str, Any]:
    """Extract standard ODIM_H5 metadata from file.

    Args:
        hdf_file: Open HDF5 file handle

    Returns:
        Dictionary with extracted metadata
    """
    metadata = {}

    # Extract root 'what' attributes
    if "what" in hdf_file:
        what_attrs = decode_hdf5_attrs(dict(hdf_file["what"].attrs))
        metadata["source"] = what_attrs.get("source", "")
        metadata["date"] = what_attrs.get("date", "")
        metadata["time"] = what_attrs.get("time", "")
        metadata["object"] = what_attrs.get("object", "")
        metadata["version"] = what_attrs.get("version", "")

    # Extract dataset1/what attributes
    if "dataset1/what" in hdf_file:
        ds_what_attrs = decode_hdf5_attrs(dict(hdf_file["dataset1/what"].attrs))
        metadata["product"] = ds_what_attrs.get("product", "")
        metadata["startdate"] = ds_what_attrs.get("startdate", "")
        metadata["starttime"] = ds_what_attrs.get("starttime", "")
        metadata["enddate"] = ds_what_attrs.get("enddate", "")
        metadata["endtime"] = ds_what_attrs.get("endtime", "")

    # Extract data1/what attributes (quantity info)
    if "dataset1/data1/what" in hdf_file:
        data_what_attrs = decode_hdf5_attrs(dict(hdf_file["dataset1/data1/what"].attrs))
        metadata["quantity"] = data_what_attrs.get("quantity", "")
        metadata["gain"] = data_what_attrs.get("gain", 1.0)
        metadata["offset"] = data_what_attrs.get("offset", 0.0)
        metadata["nodata"] = data_what_attrs.get("nodata", 65535)
        metadata["undetect"] = data_what_attrs.get("undetect", 0)

    # Extract where attributes (projection/extent)
    where_group = None
    if "where" in hdf_file:
        where_group = hdf_file["where"]
    elif "dataset1/where" in hdf_file:
        where_group = hdf_file["dataset1/where"]

    if where_group is not None:
        where_attrs = decode_hdf5_attrs(dict(where_group.attrs))
        metadata["projdef"] = where_attrs.get("projdef", "")
        metadata["xsize"] = where_attrs.get("xsize", 0)
        metadata["ysize"] = where_attrs.get("ysize", 0)
        metadata["xscale"] = where_attrs.get("xscale", 0)
        metadata["yscale"] = where_attrs.get("yscale", 0)

    return metadata


def process_odim_file(
    file_path: str,
    fallback_extent: dict[str, float],
    source_name: str,
    handle_uint8: bool = True,
) -> dict[str, Any]:
    """Process an ODIM_H5 file and return standardized radar data.

    This is a common processing function that can be used by sources
    that follow the ODIM_H5 standard (SHMU, CHMI, IMGW, etc.).

    Args:
        file_path: Path to HDF5 file
        fallback_extent: Fallback extent if not in file
        source_name: Source identifier for logging
        handle_uint8: Handle 255 as nodata for uint8 arrays

    Returns:
        Standardized radar data dictionary
    """
    from .timestamps import extract_timestamp_from_hdf5_attrs

    with h5py.File(file_path, "r") as f:
        # Find main data array
        data = find_main_dataset(f)
        if data is None:
            raise ValueError(f"Could not find data array in {file_path}")

        # Get scaling parameters
        data_what_attrs = {}
        if "dataset1/data1/what" in f:
            data_what_attrs = decode_hdf5_attrs(dict(f["dataset1/data1/what"].attrs))

        scaling = get_scaling_params(data_what_attrs)

        # Scale data
        scaled_data = scale_radar_data(
            data,
            scaling["gain"],
            scaling["offset"],
            scaling["nodata"],
            scaling["undetect"],
            handle_uint8=handle_uint8,
        )

        # Get extent
        where_attrs = {}
        if "where" in f:
            where_attrs = decode_hdf5_attrs(dict(f["where"].attrs))
        elif "dataset1/where" in f:
            where_attrs = decode_hdf5_attrs(dict(f["dataset1/where"].attrs))

        try:
            extent = extract_corner_coordinates(where_attrs, fallback_extent)
        except ValueError:
            extent = fallback_extent

        # Create coordinate arrays
        coordinates = create_coordinate_arrays(extent, data.shape)

        # Extract timestamp
        what_attrs = {}
        ds_what_attrs = {}
        if "what" in f:
            what_attrs = decode_hdf5_attrs(dict(f["what"].attrs))
        if "dataset1/what" in f:
            ds_what_attrs = decode_hdf5_attrs(dict(f["dataset1/what"].attrs))

        # Try dataset what first, then root what
        timestamp = extract_timestamp_from_hdf5_attrs(
            ds_what_attrs if ds_what_attrs else what_attrs
        )

        # Get quantity
        quantity = data_what_attrs.get("quantity", "DBZH")
        units = get_quantity_units(quantity)

        return {
            "data": scaled_data,
            "coordinates": coordinates,
            "metadata": {
                "product": ds_what_attrs.get("product", "UNKNOWN"),
                "quantity": quantity,
                "timestamp": timestamp,
                "source": source_name.upper(),
                "units": units,
                "nodata_value": np.nan,
            },
            "extent": {"wgs84": extent},
            "dimensions": data.shape,
            "timestamp": timestamp,
        }


def extract_extent_only(
    file_path: str,
    fallback_extent: dict[str, float],
) -> dict[str, Any]:
    """Extract extent from ODIM_H5 file without loading data array.

    Memory-efficient method that reads only metadata.

    Args:
        file_path: Path to HDF5 file
        fallback_extent: Fallback extent if not in file

    Returns:
        Dictionary with extent and dimensions
    """
    with h5py.File(file_path, "r") as f:
        # Get where attributes
        where_attrs = {}
        if "where" in f:
            where_attrs = decode_hdf5_attrs(dict(f["where"].attrs))
        elif "dataset1/where" in f:
            where_attrs = decode_hdf5_attrs(dict(f["dataset1/where"].attrs))

        try:
            extent = extract_corner_coordinates(where_attrs, fallback_extent)
        except ValueError:
            extent = fallback_extent

        # Get dimensions without loading data
        dimensions = None
        if "dataset1/data1/data" in f:
            dimensions = f["dataset1/data1/data"].shape
        elif "dataset1/data/data" in f:
            dimensions = f["dataset1/data/data"].shape

        if dimensions is None:
            # Fallback to xsize/ysize from where attrs
            xsize = where_attrs.get("xsize", 0)
            ysize = where_attrs.get("ysize", 0)
            if xsize and ysize:
                dimensions = (int(ysize), int(xsize))

        return {
            "extent": {"wgs84": extent},
            "dimensions": dimensions,
        }
