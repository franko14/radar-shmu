#!/usr/bin/env python3
"""
Coverage Mask Generator - Create coverage masks from actual radar data.

Generates PNG files showing radar coverage:
- Transparent: Inside radar coverage (where data can exist)
- Gray: Outside radar coverage (physically beyond radar range)

Masks are systematically aligned with extent_index.json files generated
during fetch/composite operations. Individual source masks are reprojected
through the same CRS pipeline as the data PNGs to ensure pixel-perfect
alignment.
"""

import glob
import json
import os
from typing import Any

import h5py
import numpy as np
from PIL import Image
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, reproject
from scipy.ndimage import zoom

from ..config.sources import (
    get_all_source_names,
    get_source_config,
    get_source_instance,
)
from ..core.base import lonlat_to_mercator
from ..core.logging import get_logger
from ..core.projections import get_crs_web_mercator, get_crs_wgs84

logger = get_logger(__name__)

# Coverage mask color for uncovered areas
UNCOVERED_COLOR = (128, 128, 128, 255)  # Gray, fully opaque

# Nodata values for each source (used to detect coverage boundary)
# These are the raw values that indicate "outside radar coverage"
NODATA_VALUES: dict[str, int] = {
    "dwd": 65535,  # uint16 max
    "shmu": 255,  # uint8 max
    "chmi": 255,  # uint8 max
    "arso": 64,  # offset byte (ASCII '@')
    "omsz": 255,  # uint8 representation of outside coverage
    "imgw": 255,  # uint8 max (CMAX product)
}


def _load_extent_index(output_dir: str) -> dict[str, Any] | None:
    """
    Load extent_index.json from output directory.

    Args:
        output_dir: Directory containing extent_index.json

    Returns:
        Parsed extent_index data or None if not found
    """
    extent_path = os.path.join(output_dir, "extent_index.json")
    if os.path.exists(extent_path):
        try:
            with open(extent_path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load extent_index.json from {output_dir}: {e}")
    return None


def _get_wgs84_from_extent_index(extent_data: dict[str, Any]) -> dict[str, float] | None:
    """
    Extract WGS84 bounds from extent_index.json data.

    Handles both individual source format (top-level 'wgs84' key)
    and composite format (nested under 'extent' key).

    Args:
        extent_data: Parsed extent_index.json data

    Returns:
        Dictionary with west, east, south, north or None
    """
    # Individual source format: top-level "wgs84" key
    if "wgs84" in extent_data:
        return extent_data["wgs84"]

    # Composite format: nested under "extent"
    extent = extent_data.get("extent", {})
    if "wgs84" in extent:
        return extent["wgs84"]

    return None


def _load_source_extent(
    source_name: str, output_base_dir: str
) -> dict[str, float] | None:
    """
    Load actual reprojected extent for a source from its extent_index.json.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')
        output_base_dir: Base output directory (parent of source folders)

    Returns:
        WGS84 bounds dict or None if not found
    """
    config = get_source_config(source_name)
    if not config:
        return None

    source_dir = os.path.join(output_base_dir, config["folder"])
    extent_data = _load_extent_index(source_dir)

    if extent_data is None:
        return None

    return _get_wgs84_from_extent_index(extent_data)


def _resize_coverage_to_target(
    coverage: np.ndarray, target_shape: tuple[int, int]
) -> np.ndarray:
    """Resize coverage array to match target dimensions using nearest neighbor."""
    if coverage.shape == target_shape:
        return coverage

    zoom_factors = (
        target_shape[0] / coverage.shape[0],
        target_shape[1] / coverage.shape[1],
    )
    return zoom(coverage.astype(float), zoom_factors, order=0) > 0.5


def _save_coverage_mask_png(coverage: np.ndarray, output_path: str) -> str:
    """
    Create and save coverage mask PNG from boolean coverage array.

    Args:
        coverage: Boolean array (True = covered/transparent, False = uncovered/gray)
        output_path: Full path for output PNG file

    Returns:
        Path to saved file
    """
    height, width = coverage.shape
    mask = np.zeros((height, width, 4), dtype=np.uint8)
    mask[~coverage] = UNCOVERED_COLOR  # Uncovered = gray opaque
    # Covered pixels stay (0,0,0,0) = transparent

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    Image.fromarray(mask, "RGBA").save(output_path, optimize=True)
    return output_path


def _read_raw_hdf5_data(file_path: str) -> tuple[np.ndarray, int]:
    """
    Read raw data from HDF5 file without NaN conversion.

    Args:
        file_path: Path to HDF5 file

    Returns:
        Tuple of (raw_data array, nodata_value)
    """
    with h5py.File(file_path, "r") as hdf:
        # Find the data array
        data = None
        nodata = 255  # Default for uint8

        # Try standard ODIM_H5 paths
        for dataset_path in ["dataset1/data1/data", "dataset1/data/data", "data1/data"]:
            if dataset_path in hdf:
                data = hdf[dataset_path][:]
                break

        if data is None:
            # Try to find any data array
            def find_data(name, obj):
                nonlocal data
                if isinstance(obj, h5py.Dataset) and len(obj.shape) == 2:
                    if obj.shape[0] > 100 and obj.shape[1] > 100:
                        data = obj[:]
                        return True

            hdf.visititems(find_data)

        if data is None:
            raise ValueError(f"Could not find data array in {file_path}")

        # Get nodata value from attributes
        for what_path in [
            "dataset1/data1/what",
            "dataset1/what",
            "data1/what",
            "what",
        ]:
            if what_path in hdf:
                nodata_attr = hdf[what_path].attrs.get("nodata", None)
                if nodata_attr is not None:
                    nodata = int(nodata_attr)
                break

        # For uint8 data, nodata is typically 255 (max value)
        if data.dtype == np.uint8:
            nodata = 255

        return data, nodata


def _read_raw_netcdf_data(file_path: str) -> tuple[np.ndarray, int]:
    """
    Read raw data from netCDF file (OMSZ format).

    Args:
        file_path: Path to netCDF file

    Returns:
        Tuple of (raw_data array, nodata_value)
    """
    import netCDF4 as nc

    with nc.Dataset(file_path, "r") as dataset:
        # OMSZ uses refl2D or similar variable names
        var_name = None
        for name in ["refl2D", "refl2D_pscappi", "refl3D", "param"]:
            if name in dataset.variables:
                var_name = name
                break

        if var_name is None:
            raise ValueError(f"Could not find data variable in {file_path}")

        # Disable auto-masking to prevent _FillValue hiding valid data
        dataset.variables[var_name].set_auto_mask(False)
        data = dataset.variables[var_name][:]

        # View int8 as uint8 (OMSZ stores as int8 but values are uint8)
        if data.dtype == np.int8:
            data = data.view(np.uint8)

        # Handle MaskedArray if present
        if hasattr(data, "filled"):
            data = data.filled(255)

        # OMSZ: 255 = outside coverage, 0 = coverage with no precipitation
        return data, 255


def _read_raw_arso_data(file_path: str) -> tuple[np.ndarray, int]:
    """
    Read raw data from ARSO SRD-3 binary file.

    IMPORTANT: For ARSO, the entire SRD-3 grid represents the radar's coverage area.
    The offset byte (64) indicates "no precipitation detected", NOT "outside coverage".
    Therefore, we return nodata=-1 which will never match any uint8 value,
    effectively marking all pixels as "covered".

    Args:
        file_path: Path to SRD-3 file

    Returns:
        Tuple of (raw_data array, nodata_value)
    """
    with open(file_path, "rb") as f:
        content = f.read()

    # Find header end
    header_end = content.find(b"\r\n\r\n")
    if header_end == -1:
        header_end = content.find(b"\n\n")
        data_start = header_end + 2
    else:
        data_start = header_end + 4

    # Parse header for dimensions
    header = content[:header_end].decode("ascii", errors="ignore")
    ncell_i = 401  # Default ARSO grid width
    ncell_j = 301  # Default ARSO grid height

    for line in header.split("\n"):
        line = line.strip()
        if line.startswith("ncell_i"):
            parts = line.split()
            if len(parts) >= 2:
                ncell_i = int(parts[1])
        elif line.startswith("ncell_j"):
            parts = line.split()
            if len(parts) >= 2:
                ncell_j = int(parts[1])

    # Read binary data
    binary_data = content[data_start:]
    expected_size = ncell_i * ncell_j
    if len(binary_data) < expected_size:
        raise ValueError(f"Binary data too short: {len(binary_data)} < {expected_size}")

    # Convert to numpy array
    data = np.frombuffer(binary_data[:expected_size], dtype=np.uint8)
    data = data.reshape((ncell_j, ncell_i))

    # Return -1 as nodata value - this ensures ALL pixels are marked as "covered"
    # because no uint8 value will ever equal -1.
    # In ARSO SRD-3 format, the entire grid IS the radar's coverage area.
    return data, -1


def _get_coverage_and_projection(
    source_name: str,
) -> tuple[np.ndarray, dict[str, Any] | None, dict[str, Any] | None] | None:
    """
    Download latest data and extract both coverage mask and projection info.

    Returns coverage boolean, projection info dict, and extent dict -
    everything needed to reproject the mask through the same pipeline
    as the data PNGs.

    Args:
        source_name: Source identifier

    Returns:
        Tuple of (coverage_bool, projection_info, extent) or None on failure
    """
    source_name = source_name.lower()
    logger.info(
        f"Downloading latest {source_name} radar file...",
        extra={"source": source_name, "operation": "download"},
    )

    try:
        source = get_source_instance(source_name)
        config = get_source_config(source_name)
        product = config["product"] if config else "dmax"

        # Download latest file
        if source_name == "dwd":
            files = source.download_latest(count=1, products=[product], use_latest=True)
        else:
            files = source.download_latest(count=1, products=[product])

        if not files:
            logger.warning(
                f"No data available for {source_name}",
                extra={"source": source_name},
            )
            return None

        file_path = files[0]["path"]

        # Read raw data for coverage detection
        if source_name == "omsz":
            raw_data, nodata_value = _read_raw_netcdf_data(file_path)
        elif source_name == "arso":
            raw_data, nodata_value = _read_raw_arso_data(file_path)
        else:
            raw_data, nodata_value = _read_raw_hdf5_data(file_path)

        # Create coverage mask
        coverage = raw_data != nodata_value

        # Also process through the source to get projection info and extent
        radar_data = source.process_to_array(file_path)
        projection_info = radar_data.get("projection")
        extent = radar_data.get("extent")

        # Clean up
        source.cleanup_temp_files()

        logger.debug(
            f"Coverage: {np.sum(coverage):,} / {coverage.size:,} pixels "
            f"({100 * np.sum(coverage) / coverage.size:.1f}%)",
            extra={"source": source_name},
        )

        return coverage, projection_info, extent

    except Exception as e:
        logger.error(
            f"Error getting coverage for {source_name}: {e}",
            extra={"source": source_name},
        )
        return None


def get_coverage_from_source(source_name: str) -> np.ndarray | None:
    """
    Download latest radar file and extract coverage mask.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')

    Returns:
        Boolean array where True = covered, False = not covered
    """
    result = _get_coverage_and_projection(source_name)
    if result is None:
        return None
    coverage, _, _ = result
    return coverage


def _reproject_coverage_to_mercator(
    coverage: np.ndarray,
    projection_info: dict[str, Any] | None,
    extent: dict[str, Any] | None,
    target_shape: tuple[int, int] | None = None,
) -> tuple[np.ndarray, dict[str, float]] | None:
    """
    Reproject a coverage boolean through the same CRS pipeline as data PNGs.

    Uses rasterio.warp.reproject for projected sources (DWD, SHMU, CHMI, IMGW)
    and simple bounds-based transform for WGS84 sources (OMSZ, ARSO).

    Args:
        coverage: Boolean coverage array in native projection
        projection_info: Source projection info (from process_to_array)
        extent: Source extent dict with wgs84 bounds
        target_shape: Optional target (height, width) to resize result

    Returns:
        Tuple of (reprojected_coverage, wgs84_bounds) or None on failure
    """
    from .reprojector import build_native_params_from_projection_info

    height, width = coverage.shape

    # Convert boolean to float for reprojection (rasterio needs numeric)
    coverage_float = coverage.astype(np.float32)

    # Check if source has a native projection (non-WGS84)
    native_crs = None
    if projection_info:
        result = build_native_params_from_projection_info(
            coverage.shape, projection_info
        )
        if result[0] is not None:
            native_crs, native_transform, native_bounds = result

    if native_crs is not None:
        # Projected source: reproject through rasterio
        from rasterio.warp import calculate_default_transform

        web_mercator = get_crs_web_mercator()
        left, bottom, right, top = native_bounds

        # Calculate optimal output grid (same as data PNG pipeline)
        dst_transform, dst_width, dst_height = calculate_default_transform(
            native_crs, web_mercator, width, height,
            left=left, bottom=bottom, right=right, top=top,
        )

        # Reproject coverage
        reprojected = np.zeros((dst_height, dst_width), dtype=np.float32)
        reproject(
            source=coverage_float,
            destination=reprojected,
            src_transform=native_transform,
            src_crs=native_crs,
            dst_transform=dst_transform,
            dst_crs=web_mercator,
            resampling=Resampling.nearest,
            src_nodata=0.0,
            dst_nodata=0.0,
        )

        # Convert back to boolean
        result_coverage = reprojected > 0.5

        # Calculate WGS84 bounds from reprojected transform
        from pyproj import Transformer
        from ..core.projections import PROJ4_WEB_MERCATOR, PROJ4_WGS84

        merc_left = dst_transform.c
        merc_top = dst_transform.f
        merc_right = merc_left + dst_width * dst_transform.a
        merc_bottom = merc_top + dst_height * dst_transform.e

        transformer = Transformer.from_crs(
            PROJ4_WEB_MERCATOR, PROJ4_WGS84, always_xy=True
        )
        west, south = transformer.transform(merc_left, merc_bottom)
        east, north = transformer.transform(merc_right, merc_top)

        wgs84_bounds = {
            "west": west, "east": east,
            "south": south, "north": north,
        }

    else:
        # WGS84 source: reproject from WGS84 to Web Mercator
        if extent is None:
            return None

        wgs84 = extent.get("wgs84", extent)
        w = wgs84.get("west", 0)
        e = wgs84.get("east", 0)
        s = wgs84.get("south", 0)
        n = wgs84.get("north", 0)

        # Source transform in WGS84
        src_transform = from_bounds(w, s, e, n, width, height)

        # Destination in Web Mercator
        west_m, south_m = lonlat_to_mercator(w, s)
        east_m, north_m = lonlat_to_mercator(e, n)

        # Keep similar pixel count
        dst_width = width
        dst_height = height
        dst_transform = from_bounds(
            west_m, south_m, east_m, north_m, dst_width, dst_height
        )

        reprojected = np.zeros((dst_height, dst_width), dtype=np.float32)
        reproject(
            source=coverage_float,
            destination=reprojected,
            src_transform=src_transform,
            src_crs=get_crs_wgs84(),
            dst_transform=dst_transform,
            dst_crs=get_crs_web_mercator(),
            resampling=Resampling.nearest,
            src_nodata=0.0,
            dst_nodata=0.0,
        )

        result_coverage = reprojected > 0.5
        wgs84_bounds = {"west": w, "east": e, "south": s, "north": n}

    # Resize to target if needed
    if target_shape and result_coverage.shape != target_shape:
        result_coverage = _resize_coverage_to_target(result_coverage, target_shape)

    return result_coverage, wgs84_bounds


def _get_target_dimensions_from_pngs(output_dir: str) -> tuple[int, int] | None:
    """Get target dimensions from existing radar PNG files in the output directory."""
    # Find PNG files (excluding coverage_mask.png itself)
    png_files = glob.glob(os.path.join(output_dir, "*.png"))
    for png_file in png_files:
        if "coverage_mask" in png_file:
            continue
        try:
            with Image.open(png_file) as img:
                return (img.height, img.width)
        except Exception:
            continue
    return None


def generate_source_coverage_mask(
    source_name: str, output_dir: str, filename: str = "coverage_mask.png"
) -> str | None:
    """
    Generate a coverage mask PNG for a single radar source.

    Reads actual radar data, reprojects coverage through the same CRS
    pipeline as the data PNGs, and resizes to match existing PNG dimensions.
    This ensures pixel-perfect alignment with extent_index.json.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')
        output_dir: Directory to save the mask
        filename: Output filename (default: coverage_mask.png)

    Returns:
        Path to generated mask file, or None if failed
    """
    source_name = source_name.lower()

    logger.info(
        f"Generating coverage mask for {source_name}...",
        extra={"source": source_name, "operation": "generate"},
    )

    # Get target dimensions from existing radar PNGs
    target_shape = _get_target_dimensions_from_pngs(output_dir)
    if target_shape:
        logger.debug(
            f"Target dimensions from existing PNGs: {target_shape[1]}x{target_shape[0]}",
            extra={"source": source_name},
        )

    # Get coverage AND projection info from actual radar data
    result = _get_coverage_and_projection(source_name)
    if result is None:
        return None

    coverage, projection_info, extent = result

    logger.debug(
        f"Source data dimensions: {coverage.shape[1]}x{coverage.shape[0]} pixels",
        extra={"source": source_name},
    )

    # Reproject coverage through same CRS pipeline as data PNGs
    reproj_result = _reproject_coverage_to_mercator(
        coverage, projection_info, extent, target_shape
    )

    if reproj_result is not None:
        coverage, wgs84_bounds = reproj_result
        logger.debug(
            f"Reprojected coverage: {coverage.shape[1]}x{coverage.shape[0]} pixels, "
            f"bounds: ({wgs84_bounds['west']:.4f}, {wgs84_bounds['south']:.4f}) to "
            f"({wgs84_bounds['east']:.4f}, {wgs84_bounds['north']:.4f})",
            extra={"source": source_name},
        )
    elif target_shape and coverage.shape != target_shape:
        # Fallback: simple resize (should not normally happen)
        logger.warning(
            f"Reprojection failed, falling back to resize for {source_name}",
            extra={"source": source_name},
        )
        coverage = _resize_coverage_to_target(coverage, target_shape)

    logger.debug(
        f"Final mask dimensions: {coverage.shape[1]}x{coverage.shape[0]} pixels",
        extra={"source": source_name},
    )

    # Save coverage mask PNG
    output_path = os.path.join(output_dir, filename)
    _save_coverage_mask_png(coverage, output_path)
    logger.info(
        f"Saved: {output_path}",
        extra={"source": source_name, "operation": "save"},
    )

    return output_path


def _reproject_coverage_to_composite(
    coverage: np.ndarray,
    source_extent: dict[str, float],
    composite_extent: dict[str, float],
    composite_shape: tuple[int, int],
) -> np.ndarray:
    """
    Map a source coverage mask (already in Web Mercator) to the composite grid.

    Both source and composite extents should be actual reprojected WGS84
    bounds from extent_index.json for accurate alignment.

    Args:
        coverage: Source coverage boolean array (in Web Mercator projection)
        source_extent: Source extent in WGS84 (from extent_index.json)
        composite_extent: Composite extent in WGS84 (from extent_index.json)
        composite_shape: (height, width) of composite grid

    Returns:
        Boolean array in composite grid coordinates
    """
    comp_height, comp_width = composite_shape
    src_height, src_width = coverage.shape

    # Convert extents to mercator
    src_west_m, src_south_m = lonlat_to_mercator(
        source_extent["west"], source_extent["south"]
    )
    src_east_m, src_north_m = lonlat_to_mercator(
        source_extent["east"], source_extent["north"]
    )

    comp_west_m, comp_south_m = lonlat_to_mercator(
        composite_extent["west"], composite_extent["south"]
    )
    comp_east_m, comp_north_m = lonlat_to_mercator(
        composite_extent["east"], composite_extent["north"]
    )

    # Calculate pixel coordinates in composite grid
    comp_width_m = comp_east_m - comp_west_m
    comp_height_m = comp_north_m - comp_south_m

    # Source position in composite (pixel coordinates)
    x_start = int((src_west_m - comp_west_m) / comp_width_m * comp_width)
    x_end = int((src_east_m - comp_west_m) / comp_width_m * comp_width)
    y_start = int((comp_north_m - src_north_m) / comp_height_m * comp_height)
    y_end = int((comp_north_m - src_south_m) / comp_height_m * comp_height)

    # Clamp to valid range
    x_start = max(0, x_start)
    x_end = min(comp_width, x_end)
    y_start = max(0, y_start)
    y_end = min(comp_height, y_end)

    target_width = x_end - x_start
    target_height = y_end - y_start

    if target_width <= 0 or target_height <= 0:
        return np.zeros(composite_shape, dtype=bool)

    # Resize coverage to target size
    zoom_y = target_height / src_height
    zoom_x = target_width / src_width
    resized = zoom(coverage.astype(float), (zoom_y, zoom_x), order=0) > 0.5

    # Place in composite grid
    result = np.zeros(composite_shape, dtype=bool)
    result[y_start:y_end, x_start:x_end] = resized[: y_end - y_start, : x_end - x_start]

    return result


def generate_composite_coverage_mask(
    sources: list[str] | None = None,
    output_dir: str = "/tmp/composite",
    output_base_dir: str | None = None,
    filename: str = "coverage_mask.png",
    resolution_m: float = 500.0,
) -> str | None:
    """
    Generate a composite coverage mask PNG from multiple sources.

    Reprojects each source's coverage to the composite grid and combines
    them using OR logic. Uses extent_index.json files for accurate alignment:
    - Composite extent from {output_dir}/extent_index.json
    - Per-source extents from {output_base_dir}/{folder}/extent_index.json

    Args:
        sources: List of source names to include (default: all sources)
        output_dir: Directory to save the composite mask
        output_base_dir: Base directory containing source output folders.
            If None, derived from output_dir parent.
        filename: Output filename (default: coverage_mask.png)
        resolution_m: Resolution in meters (default: 500m, overridden by extent_index)

    Returns:
        Path to generated mask file, or None if failed
    """
    if sources is None:
        sources = get_all_source_names()

    # Derive output_base_dir from output_dir if not specified
    if output_base_dir is None:
        output_base_dir = os.path.dirname(output_dir)

    logger.info(
        "Generating composite coverage mask...",
        extra={"operation": "generate"},
    )
    logger.debug(f"Sources: {', '.join(s for s in sources)}")

    # First, try to get dimensions from existing composite PNGs
    target_shape = _get_target_dimensions_from_pngs(output_dir)
    if target_shape:
        logger.debug(
            f"Target dimensions from existing PNGs: {target_shape[1]}x{target_shape[0]}",
        )

    # Load composite extent from extent_index.json
    extent_info = _load_extent_index(output_dir)
    combined_extent = None

    if extent_info:
        combined_extent = _get_wgs84_from_extent_index(extent_info)
        metadata = extent_info.get("metadata", {})
        resolution_m = metadata.get("resolution_m", resolution_m)
        logger.debug("Using extent from extent_index.json")
        logger.debug(f"Resolution: {resolution_m}m")

    if combined_extent is None:
        # Fallback: load extents from per-source extent_index.json files
        logger.debug("No composite extent_index.json, loading per-source extents")
        source_extents = {}
        for s in sources:
            ext = _load_source_extent(s, output_base_dir)
            if ext:
                source_extents[s] = ext

        if source_extents:
            combined_extent = {
                "west": min(ext["west"] for ext in source_extents.values()),
                "east": max(ext["east"] for ext in source_extents.values()),
                "south": min(ext["south"] for ext in source_extents.values()),
                "north": max(ext["north"] for ext in source_extents.values()),
            }
            logger.debug(f"Combined extent from {len(source_extents)} source extent_index.json files")
        else:
            logger.error(
                "No extent_index.json files found. Run 'imeteo-radar fetch' first "
                "to generate extent data for each source."
            )
            return None

    # Calculate grid dimensions from extent and resolution
    west_m, south_m = lonlat_to_mercator(
        combined_extent["west"], combined_extent["south"]
    )
    east_m, north_m = lonlat_to_mercator(
        combined_extent["east"], combined_extent["north"]
    )

    width_m = east_m - west_m
    height_m = north_m - south_m

    grid_width = int(np.ceil(width_m / resolution_m))
    grid_height = int(np.ceil(height_m / resolution_m))

    # Use PNG dimensions if available (they should match calculated dimensions)
    if target_shape:
        grid_height, grid_width = target_shape

    logger.debug(
        f"Extent: {combined_extent['west']:.4f}E to {combined_extent['east']:.4f}E, "
        f"{combined_extent['south']:.4f}N to {combined_extent['north']:.4f}N"
    )
    logger.debug(f"Dimensions: {grid_width}x{grid_height} pixels")

    # Initialize composite coverage (all False = uncovered)
    composite_coverage = np.zeros((grid_height, grid_width), dtype=bool)

    # Get coverage from each source and reproject to composite grid
    for source_name in sources:
        logger.info(
            f"Processing {source_name}...",
            extra={"source": source_name, "operation": "process"},
        )

        # Get coverage + projection info
        result = _get_coverage_and_projection(source_name)
        if result is None:
            logger.warning(
                f"Skipping {source_name} (no data)",
                extra={"source": source_name},
            )
            continue

        coverage, projection_info, extent = result

        # Reproject coverage to Web Mercator first
        reproj_result = _reproject_coverage_to_mercator(
            coverage, projection_info, extent
        )

        if reproj_result is None:
            logger.warning(
                f"Skipping {source_name} (reprojection failed)",
                extra={"source": source_name},
            )
            continue

        reprojected_coverage, source_wgs84 = reproj_result

        # Load actual reprojected extent from extent_index.json if available
        source_extent = _load_source_extent(source_name, output_base_dir)
        if source_extent is None:
            # Use bounds from reprojection
            source_extent = source_wgs84

        # Map reprojected coverage into composite grid
        mapped = _reproject_coverage_to_composite(
            reprojected_coverage, source_extent, combined_extent,
            (grid_height, grid_width),
        )

        # Combine using OR logic
        pixels_before = np.sum(composite_coverage)
        composite_coverage |= mapped
        pixels_after = np.sum(composite_coverage)
        new_pixels = pixels_after - pixels_before

        logger.debug(
            f"Added {new_pixels:,} pixels from {source_name} "
            f"(total: {pixels_after:,})",
            extra={"source": source_name, "count": int(new_pixels)},
        )

    # Calculate final coverage stats
    total_covered = int(np.sum(composite_coverage))
    total_pixels = composite_coverage.size
    logger.info(
        f"Final coverage: {total_covered:,} / {total_pixels:,} pixels "
        f"({100 * total_covered / total_pixels:.1f}%)",
    )

    # Save coverage mask PNG
    output_path = os.path.join(output_dir, filename)
    _save_coverage_mask_png(composite_coverage, output_path)
    logger.info(
        f"Saved: {output_path}",
        extra={"operation": "save"},
    )

    return output_path


def generate_all_coverage_masks(
    output_base_dir: str = "/tmp", resolution_m: float = 500.0
) -> dict[str, str]:
    """
    Generate coverage masks for all sources and composite.

    Masks are saved alongside radar data and extent_index.json files.

    Args:
        output_base_dir: Base directory (default: /tmp)
            Individual masks go to {base}/{country}/ (e.g., /tmp/germany/)
            Composite mask goes to {base}/composite/
        resolution_m: Resolution for composite mask

    Returns:
        Dictionary mapping source names to output paths
    """
    results = {}

    logger.info(
        "Generating all coverage masks...",
        extra={"operation": "generate"},
    )

    # Generate individual source masks
    for source_name in get_all_source_names():
        config = get_source_config(source_name)
        if config:
            folder = config["folder"]
            output_dir = os.path.join(output_base_dir, folder)
            path = generate_source_coverage_mask(source_name, output_dir)
            if path:
                results[source_name] = path

    # Generate composite mask
    composite_dir = os.path.join(output_base_dir, "composite")
    path = generate_composite_coverage_mask(
        sources=None,
        output_dir=composite_dir,
        output_base_dir=output_base_dir,
        resolution_m=resolution_m,
    )
    if path:
        results["composite"] = path

    logger.info(
        f"Generated {len(results)} coverage masks",
        extra={"count": len(results)},
    )

    return results
