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
from pathlib import Path
from typing import Any

import h5py
import numpy as np
from PIL import Image
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, reproject
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


def _get_wgs84_from_extent_index(
    extent_data: dict[str, Any],
) -> dict[str, float] | None:
    """
    Extract WGS84 bounds from extent_index.json data.

    Handles all three known formats:
    - Composite pipeline: top-level 'wgs84' key
    - Composite metadata: nested under 'extent.wgs84'
    - CLI fetch: nested under 'source.extent' (with west/east/south/north)

    Args:
        extent_data: Parsed extent_index.json data

    Returns:
        Dictionary with west, east, south, north or None
    """
    # Composite pipeline format: top-level "wgs84" key
    if "wgs84" in extent_data:
        return extent_data["wgs84"]

    # Composite metadata format: nested under "extent.wgs84"
    extent = extent_data.get("extent", {})
    if "wgs84" in extent:
        return extent["wgs84"]

    # CLI fetch format: nested under "source.extent"
    source = extent_data.get("source", {})
    source_extent = source.get("extent", {})
    if "west" in source_extent:
        return source_extent

    return None


def _load_source_extent(
    source_name: str, output_base_dir: str | None = None
) -> dict[str, float] | None:
    """
    Load actual reprojected extent for a source from its extent_index.json.

    Reads from /tmp/iradar-data/extent/{source_name}/ (canonical location).
    Falls back to legacy location ({output_base_dir}/{folder}/) for compat.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')
        output_base_dir: Legacy base output directory (parent of source folders)

    Returns:
        WGS84 bounds dict or None if not found
    """
    # Try canonical location first: /tmp/iradar-data/extent/{source}/
    canonical_dir = os.path.join("/tmp/iradar-data/extent", source_name)
    extent_data = _load_extent_index(canonical_dir)
    if extent_data is not None:
        result = _get_wgs84_from_extent_index(extent_data)
        if result is not None:
            return result

    # Fallback to legacy location: {output_base_dir}/{folder}/
    if output_base_dir:
        config = get_source_config(source_name)
        if config:
            source_dir = os.path.join(output_base_dir, config["folder"])
            extent_data = _load_extent_index(source_dir)
            if extent_data is not None:
                return _get_wgs84_from_extent_index(extent_data)

    return None


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
            def find_data(_name, obj):
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


def _reproject_coverage_to_target(
    coverage: np.ndarray,
    projection_info: dict[str, Any] | None,
    extent: dict[str, Any] | None,
    target_wgs84: dict[str, float],
    target_shape: tuple[int, int],
) -> np.ndarray | None:
    """
    Reproject coverage directly into the target grid defined by extent_index.json.

    Instead of reprojecting to an intermediate grid and resizing (which causes
    bounds mismatch), this reprojects directly into the exact pixel grid that
    the data PNGs occupy. The target grid is defined by:
    - target_wgs84: WGS84 bounds from extent_index.json
    - target_shape: (height, width) from existing data PNGs

    Args:
        coverage: Boolean coverage array in native projection
        projection_info: Source projection info (from process_to_array)
        extent: Source extent dict with wgs84 bounds (for WGS84 sources)
        target_wgs84: WGS84 bounds from extent_index.json
        target_shape: (height, width) matching existing data PNGs

    Returns:
        Boolean coverage array in target grid, or None on failure
    """
    from .reprojector import build_native_params_from_projection_info

    height, width = coverage.shape
    dst_height, dst_width = target_shape

    # Convert boolean to float for reprojection (rasterio needs numeric)
    coverage_float = coverage.astype(np.float32)

    # Build destination transform from extent_index.json bounds in Web Mercator
    west_m, south_m = lonlat_to_mercator(target_wgs84["west"], target_wgs84["south"])
    east_m, north_m = lonlat_to_mercator(target_wgs84["east"], target_wgs84["north"])
    dst_transform = from_bounds(west_m, south_m, east_m, north_m, dst_width, dst_height)
    dst_crs = get_crs_web_mercator()

    # Determine source CRS and transform
    native_crs = None
    if projection_info:
        result = build_native_params_from_projection_info(
            coverage.shape, projection_info
        )
        if result[0] is not None:
            native_crs, native_transform, _ = result

    if native_crs is not None:
        # Projected source (DWD, SHMU, CHMI, IMGW): reproject from native CRS
        src_crs = native_crs
        src_transform = native_transform
    else:
        # WGS84 source (OMSZ, ARSO): reproject from WGS84
        if extent is None:
            return None

        wgs84 = extent.get("wgs84", extent)
        w = wgs84.get("west", 0)
        e = wgs84.get("east", 0)
        s = wgs84.get("south", 0)
        n = wgs84.get("north", 0)

        src_crs = get_crs_wgs84()
        src_transform = from_bounds(w, s, e, n, width, height)

    # Reproject directly into target grid
    reprojected = np.zeros((dst_height, dst_width), dtype=np.float32)
    reproject(
        source=coverage_float,
        destination=reprojected,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        resampling=Resampling.nearest,
        src_nodata=0.0,
        dst_nodata=0.0,
    )

    return reprojected > 0.5


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


def _get_dimensions_from_transform_cache(source_name: str) -> tuple[int, int] | None:
    """Get output dimensions from transform cache grid files.

    The transform cache stores precomputed reprojection grids with dst_shape
    containing the exact output dimensions used by the reprojection pipeline.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')

    Returns:
        Tuple of (height, width) or None if not found
    """
    grid_dir = Path("/tmp/iradar-data/grid")
    if not grid_dir.exists():
        return None

    # Find grid file for this source (format: {source}_{HxW}_{hash}_v1.npz)
    for grid_file in grid_dir.glob(f"{source_name}_*.npz"):
        try:
            # Load just the dst_shape array
            with np.load(grid_file, allow_pickle=False) as data:
                if "dst_shape" in data:
                    dst_shape = data["dst_shape"]
                    # dst_shape is [height, width]
                    return (int(dst_shape[0]), int(dst_shape[1]))
        except Exception as e:
            logger.debug(f"Error reading grid file {grid_file}: {e}")
            continue

    return None


def generate_source_coverage_mask(
    source_name: str,
    output_dir: str | None = None,
    filename: str = "coverage_mask.png",
    png_dir: str | None = None,
) -> str | None:
    """
    Generate a coverage mask PNG for a single radar source.

    Reads actual radar data and reprojects coverage directly into the target
    grid defined by extent_index.json bounds. If existing data PNGs are available,
    uses their dimensions for pixel-perfect alignment. Otherwise, gets dimensions
    from the transform cache grid files.

    Mask is saved to /tmp/iradar-data/mask/{source}/ (canonical location).

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')
        output_dir: Directory to save the mask (default: /tmp/iradar-data/mask/{source})
        filename: Output filename (default: coverage_mask.png)
        png_dir: Directory containing data PNGs for dimension detection
            (default: /tmp/iradar/{folder}/)

    Returns:
        Path to generated mask file, or None if failed
    """
    source_name = source_name.lower()

    # Resolve default output_dir for mask
    if output_dir is None:
        output_dir = os.path.join("/tmp/iradar-data/mask", source_name)

    # Resolve png_dir for target dimension detection
    if png_dir is None:
        config = get_source_config(source_name)
        folder = config["folder"] if config else source_name
        png_dir = os.path.join("/tmp/iradar", folder)

    logger.info(
        f"Generating coverage mask for {source_name}...",
        extra={"source": source_name, "operation": "generate"},
    )

    # Load target extent from extent_index.json (canonical location)
    extent_dir = os.path.join("/tmp/iradar-data/extent", source_name)
    extent_data = _load_extent_index(extent_dir)
    # Fallback to legacy location (colocated with PNGs)
    if extent_data is None:
        extent_data = _load_extent_index(png_dir)
    if extent_data is None:
        logger.error(
            f"No extent_index.json for {source_name}. "
            "Run 'imeteo-radar fetch' first to generate extent data.",
            extra={"source": source_name},
        )
        return None

    target_wgs84 = _get_wgs84_from_extent_index(extent_data)
    if target_wgs84 is None:
        logger.error(
            f"No WGS84 bounds in extent_index.json for {source_name}",
            extra={"source": source_name},
        )
        return None

    # Get target dimensions from existing radar PNGs or transform cache
    target_shape = _get_target_dimensions_from_pngs(png_dir)
    if target_shape is None:
        # No PNGs exist - get dimensions from transform cache grid
        # The grid stores the exact output dimensions used by the reprojection pipeline
        target_shape = _get_dimensions_from_transform_cache(source_name)
        if target_shape:
            logger.debug(
                f"Got dimensions from transform cache: {target_shape[1]}x{target_shape[0]}",
                extra={"source": source_name},
            )
        else:
            logger.error(
                f"No PNGs and no transform cache for {source_name}. "
                "Run a fetch first to generate transform grids.",
                extra={"source": source_name},
            )
            return None

    logger.debug(
        f"Target: {target_shape[1]}x{target_shape[0]} pixels, "
        f"bounds: ({target_wgs84['west']:.4f}, {target_wgs84['south']:.4f}) to "
        f"({target_wgs84['east']:.4f}, {target_wgs84['north']:.4f})",
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

    # Reproject coverage directly into the target grid (extent_index bounds + PNG dims)
    reprojected = _reproject_coverage_to_target(
        coverage, projection_info, extent, target_wgs84, target_shape
    )

    if reprojected is None:
        logger.error(
            f"Reprojection failed for {source_name}",
            extra={"source": source_name},
        )
        return None

    logger.debug(
        f"Final mask dimensions: {reprojected.shape[1]}x{reprojected.shape[0]} pixels",
        extra={"source": source_name},
    )

    # Save coverage mask PNG
    output_path = os.path.join(output_dir, filename)
    _save_coverage_mask_png(reprojected, output_path)
    logger.info(
        f"Saved: {output_path}",
        extra={"source": source_name, "operation": "save"},
    )

    # Upload to S3
    from ..utils.mask_loader import upload_mask_to_s3

    upload_mask_to_s3(source_name)

    return output_path


def _reproject_coverage_to_composite(
    coverage: np.ndarray,
    source_extent: dict[str, float],
    composite_extent: dict[str, float],
    composite_shape: tuple[int, int],
) -> np.ndarray:
    """
    Map a source coverage mask (already in Web Mercator) to the composite grid.

    Uses rasterio.warp.reproject for accurate coordinate mapping between
    the source and composite Mercator grids. This correctly handles sources
    that extend beyond composite bounds (clips rather than compresses).

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

    # Build source transform in Web Mercator
    src_west_m, src_south_m = lonlat_to_mercator(
        source_extent["west"], source_extent["south"]
    )
    src_east_m, src_north_m = lonlat_to_mercator(
        source_extent["east"], source_extent["north"]
    )
    src_transform = from_bounds(
        src_west_m, src_south_m, src_east_m, src_north_m, src_width, src_height
    )

    # Build destination transform in Web Mercator
    dst_west_m, dst_south_m = lonlat_to_mercator(
        composite_extent["west"], composite_extent["south"]
    )
    dst_east_m, dst_north_m = lonlat_to_mercator(
        composite_extent["east"], composite_extent["north"]
    )
    dst_transform = from_bounds(
        dst_west_m, dst_south_m, dst_east_m, dst_north_m, comp_width, comp_height
    )

    crs = get_crs_web_mercator()

    # Reproject using rasterio (handles clipping and coordinate mapping)
    coverage_float = coverage.astype(np.float32)
    reprojected = np.zeros((comp_height, comp_width), dtype=np.float32)

    reproject(
        source=coverage_float,
        destination=reprojected,
        src_transform=src_transform,
        src_crs=crs,
        dst_transform=dst_transform,
        dst_crs=crs,
        resampling=Resampling.nearest,
    )

    return reprojected > 0.5


def generate_composite_coverage_mask(
    sources: list[str] | None = None,
    output_dir: str = "/tmp/iradar-data/mask/composite",
    output_base_dir: str | None = None,
    filename: str = "coverage_mask.png",
    resolution_m: float = 500.0,
) -> str | None:
    """
    Generate a composite coverage mask PNG from multiple sources.

    Reprojects each source's coverage to the composite grid and combines
    them using OR logic. Uses extent_index.json from iradar-data/extent/
    for accurate alignment. The composite mask uses the composite
    extent_index.json bounds directly.

    Mask is saved to /tmp/iradar-data/mask/composite/ (canonical location).

    Args:
        sources: List of source names to include (default: all sources)
        output_dir: Directory to save the composite mask
        output_base_dir: Base directory containing source output folders.
            If None, defaults to /tmp/iradar.
        filename: Output filename (default: coverage_mask.png)
        resolution_m: Resolution in meters (default: 500m, overridden by extent_index)

    Returns:
        Path to generated mask file, or None if failed
    """
    if sources is None:
        sources = get_all_source_names()

    # Derive output_base_dir (for PNG directories) if not specified
    if output_base_dir is None:
        output_base_dir = "/tmp/iradar"

    logger.info(
        "Generating composite coverage mask...",
        extra={"operation": "generate"},
    )
    logger.debug(f"Sources: {', '.join(s for s in sources)}")

    # Load composite extent_index.json — mask uses same bounds as composite data
    extent_info = _load_extent_index("/tmp/iradar-data/extent/composite")
    if extent_info is None:
        extent_info = _load_extent_index(os.path.join(output_base_dir, "composite"))
    if extent_info is None:
        logger.error(
            "No composite extent_index.json found. Run 'imeteo-radar composite' first."
        )
        return None

    metadata = extent_info.get("metadata", {})
    resolution_m = metadata.get("resolution_m", resolution_m)

    mask_extent = _get_wgs84_from_extent_index(extent_info)
    if mask_extent is None:
        logger.error("No WGS84 bounds in composite extent_index.json")
        return None

    # Load per-source extents for individual mask placement
    source_extents = {}
    for s in sources:
        ext = _load_source_extent(s, output_base_dir)
        if ext:
            source_extents[s] = ext

    logger.debug(
        f"Mask extent (from composite): "
        f"({mask_extent['west']:.4f}, {mask_extent['south']:.4f}) to "
        f"({mask_extent['east']:.4f}, {mask_extent['north']:.4f})"
    )

    # Calculate grid dimensions from mask extent at composite resolution
    west_m, south_m = lonlat_to_mercator(mask_extent["west"], mask_extent["south"])
    east_m, north_m = lonlat_to_mercator(mask_extent["east"], mask_extent["north"])

    width_m = east_m - west_m
    height_m = north_m - south_m

    grid_width = int(np.ceil(width_m / resolution_m))
    grid_height = int(np.ceil(height_m / resolution_m))

    logger.debug(f"Resolution: {resolution_m}m")
    logger.debug(f"Dimensions: {grid_width}x{grid_height} pixels")

    # Initialize composite coverage (all False = uncovered)
    composite_coverage = np.zeros((grid_height, grid_width), dtype=bool)

    # Load existing individual coverage_mask.png files and composite them.
    # These are already in Web Mercator, already sized to match data PNGs,
    # so using their extent_index.json bounds guarantees alignment.
    for source_name in sources:
        config = get_source_config(source_name)
        if not config:
            continue

        # Look for mask in canonical location first, then legacy
        mask_dir = os.path.join("/tmp/iradar-data/mask", source_name)
        mask_path = os.path.join(mask_dir, "coverage_mask.png")
        if not os.path.exists(mask_path):
            # Legacy fallback: colocated with PNGs
            source_dir = os.path.join(output_base_dir, config["folder"])
            mask_path = os.path.join(source_dir, "coverage_mask.png")

        if not os.path.exists(mask_path):
            logger.debug(
                f"Skipping {source_name} (no coverage_mask.png)",
                extra={"source": source_name},
            )
            continue

        # Load source extent from extent_index.json
        source_extent = _load_source_extent(source_name, output_base_dir)
        if source_extent is None:
            logger.warning(
                f"Skipping {source_name} (no extent_index.json)",
                extra={"source": source_name},
            )
            continue

        # Read the existing mask PNG: transparent (alpha=0) = covered
        mask_img = np.array(Image.open(mask_path).convert("RGBA"))
        source_coverage = mask_img[:, :, 3] == 0  # transparent = covered

        logger.info(
            f"Loading {source_name} mask: {source_coverage.shape[1]}x{source_coverage.shape[0]}",
            extra={"source": source_name, "operation": "load"},
        )

        # Map source coverage into mask grid using extent_index.json bounds
        mapped = _reproject_coverage_to_composite(
            source_coverage,
            source_extent,
            mask_extent,
            (grid_height, grid_width),
        )

        # Combine using OR logic
        pixels_before = np.sum(composite_coverage)
        composite_coverage |= mapped
        pixels_after = np.sum(composite_coverage)
        new_pixels = pixels_after - pixels_before

        logger.debug(
            f"Added {new_pixels:,} pixels from {source_name} (total: {pixels_after:,})",
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

    # Upload to S3
    from ..utils.mask_loader import upload_mask_to_s3

    upload_mask_to_s3("composite")

    return output_path


def generate_all_coverage_masks(
    output_base_dir: str = "/tmp/iradar", resolution_m: float = 500.0
) -> dict[str, str]:
    """
    Generate coverage masks for all sources and composite.

    Individual masks go to /tmp/iradar-data/mask/{source}/
    Composite mask goes to /tmp/iradar-data/mask/composite/

    Args:
        output_base_dir: Base directory for PNGs (default: /tmp/iradar)
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
            mask_dir = os.path.join("/tmp/iradar-data/mask", source_name)
            png_dir = os.path.join(output_base_dir, folder)
            path = generate_source_coverage_mask(
                source_name, output_dir=mask_dir, png_dir=png_dir
            )
            if path:
                results[source_name] = path

    # Generate composite mask
    path = generate_composite_coverage_mask(
        sources=None,
        output_dir="/tmp/iradar-data/mask/composite",
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
