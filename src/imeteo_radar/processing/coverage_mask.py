#!/usr/bin/env python3
"""
Coverage Mask Generator - Create coverage masks from actual radar data.

Generates PNG files showing radar coverage:
- Transparent: Inside radar coverage (where data can exist)
- Gray: Outside radar coverage (physically beyond radar range)
"""

import glob
import json
import os
from typing import Any

import h5py
import numpy as np
from PIL import Image
from scipy.ndimage import zoom

from ..config.sources import (
    get_all_source_names,
    get_source_config,
    get_source_instance,
)
from ..core.base import lonlat_to_mercator
from ..core.logging import get_logger

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

# Source extents in WGS84 (for composite calculation)
SOURCE_EXTENTS: dict[str, dict[str, float]] = {
    "dwd": {"west": 2.5, "east": 18.0, "south": 45.5, "north": 56.0},
    "shmu": {"west": 13.6, "east": 23.8, "south": 46.0, "north": 50.7},
    "chmi": {"west": 12.0, "east": 19.0, "south": 48.5, "north": 51.1},
    "arso": {
        "west": 12.105563,
        "east": 17.418262,
        "south": 44.687429,
        "north": 47.414912,
    },
    "omsz": {"west": 13.5, "east": 25.5, "south": 44.0, "north": 50.5},
    "imgw": {"west": 14.0, "east": 24.1, "south": 49.0, "north": 54.8},
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
        with open(extent_path) as f:
            return json.load(f)
    return None


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

        data = dataset.variables[var_name][:]

        # View int8 as uint8 (OMSZ stores as int8 but values are uint8)
        if data.dtype == np.int8:
            data = data.view(np.uint8)

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


def get_coverage_from_source(source_name: str) -> np.ndarray | None:
    """
    Download latest radar file and extract coverage mask.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')

    Returns:
        Boolean array where True = covered, False = not covered
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
        logger.debug(
            f"Reading raw data from: {os.path.basename(file_path)}",
            extra={"source": source_name},
        )

        # Read raw data based on source type
        if source_name == "omsz":
            raw_data, nodata_value = _read_raw_netcdf_data(file_path)
        elif source_name == "arso":
            raw_data, nodata_value = _read_raw_arso_data(file_path)
        else:
            raw_data, nodata_value = _read_raw_hdf5_data(file_path)

        # Create coverage mask: True where data exists (not nodata)
        coverage = raw_data != nodata_value

        # Clean up
        source.cleanup_temp_files()

        logger.debug(
            f"Coverage: {np.sum(coverage):,} / {coverage.size:,} pixels "
            f"({100 * np.sum(coverage) / coverage.size:.1f}%)",
            extra={"source": source_name},
        )

        return coverage

    except Exception as e:
        logger.error(
            f"Error getting coverage for {source_name}: {e}",
            extra={"source": source_name},
        )
        import traceback

        traceback.print_exc()
        return None


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

    Reads actual radar data to determine coverage boundaries.
    Dimensions match existing radar PNG files in the output directory.

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

    # Try to get target dimensions from existing radar PNGs in output directory
    target_shape = _get_target_dimensions_from_pngs(output_dir)
    if target_shape:
        logger.debug(
            f"Target dimensions from existing PNGs: {target_shape[1]}×{target_shape[0]}",
            extra={"source": source_name},
        )

    # Get coverage from actual radar data
    coverage = get_coverage_from_source(source_name)
    if coverage is None:
        return None

    logger.debug(
        f"Source data dimensions: {coverage.shape[1]}×{coverage.shape[0]} pixels",
        extra={"source": source_name},
    )

    # Resize to match target dimensions if specified and different
    if target_shape and coverage.shape != target_shape:
        logger.debug(
            f"Resizing to match target: {target_shape[1]}×{target_shape[0]}",
            extra={"source": source_name},
        )
        coverage = _resize_coverage_to_target(coverage, target_shape)

    logger.debug(
        f"Final mask dimensions: {coverage.shape[1]}×{coverage.shape[0]} pixels",
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
    Reproject a source coverage mask to the composite grid.

    Args:
        coverage: Source coverage boolean array
        source_extent: Source extent in WGS84
        composite_extent: Composite extent in WGS84
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
    filename: str = "coverage_mask.png",
    resolution_m: float = 500.0,
) -> str | None:
    """
    Generate a composite coverage mask PNG from multiple sources.

    Reprojects each source's coverage to the composite grid and combines
    them using OR logic. Uses extent_index.json if present to match
    composite dimensions exactly.

    Args:
        sources: List of source names to include (default: all sources)
        output_dir: Directory to save the mask
        filename: Output filename (default: coverage_mask.png)
        resolution_m: Resolution in meters (default: 500m, overridden by extent_index)

    Returns:
        Path to generated mask file, or None if failed
    """
    if sources is None:
        sources = get_all_source_names()

    logger.info(
        f"Generating composite coverage mask...",
        extra={"operation": "generate"},
    )
    logger.debug(
        f"Sources: {', '.join(s for s in sources)}",
    )

    # First, try to get dimensions from existing composite PNGs
    target_shape = _get_target_dimensions_from_pngs(output_dir)
    if target_shape:
        logger.debug(
            f"Target dimensions from existing PNGs: {target_shape[1]}×{target_shape[0]}",
        )

    # Try to load extent_index.json for extent and resolution
    extent_info = _load_extent_index(output_dir)
    combined_extent = None
    grid_width = None
    grid_height = None

    if extent_info:
        # Use extent from extent_index.json
        combined_extent = extent_info.get("extent")
        metadata = extent_info.get("metadata", {})
        resolution_m = metadata.get("resolution_m", resolution_m)
        logger.debug(
            "Using extent from extent_index.json",
        )
        logger.debug(
            f"Resolution: {resolution_m}m",
        )

    if combined_extent is None:
        # Fallback: Calculate combined extent from SOURCE_EXTENTS
        all_extents = [SOURCE_EXTENTS[s] for s in sources if s in SOURCE_EXTENTS]
        if not all_extents:
            logger.error("No valid source extents found")
            return None

        combined_extent = {
            "west": min(ext["west"] for ext in all_extents),
            "east": max(ext["east"] for ext in all_extents),
            "south": min(ext["south"] for ext in all_extents),
            "north": max(ext["north"] for ext in all_extents),
        }
        logger.debug(
            f"Resolution: {resolution_m}m (fallback)",
        )

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
        f"Extent: {combined_extent['west']:.2f}°E to {combined_extent['east']:.2f}°E, "
        f"{combined_extent['south']:.2f}°N to {combined_extent['north']:.2f}°N"
    )
    logger.debug(
        f"Dimensions: {grid_width}×{grid_height} pixels",
    )

    # Initialize composite coverage (all False = uncovered)
    composite_coverage = np.zeros((grid_height, grid_width), dtype=bool)

    # Get coverage from each source and reproject to composite grid
    for source_name in sources:
        if source_name not in SOURCE_EXTENTS:
            continue

        logger.info(
            f"Processing {source_name}...",
            extra={"source": source_name, "operation": "process"},
        )
        coverage = get_coverage_from_source(source_name)

        if coverage is None:
            logger.warning(
                f"Skipping {source_name} (no data)",
                extra={"source": source_name},
            )
            continue

        # Reproject to composite grid
        source_extent = SOURCE_EXTENTS[source_name]
        reprojected = _reproject_coverage_to_composite(
            coverage, source_extent, combined_extent, (grid_height, grid_width)
        )

        # Combine using OR logic
        pixels_before = np.sum(composite_coverage)
        composite_coverage |= reprojected
        pixels_after = np.sum(composite_coverage)
        new_pixels = pixels_after - pixels_before

        logger.debug(
            f"Added {new_pixels:,} pixels from {source_name} "
            f"(total: {pixels_after:,})",
            extra={"source": source_name, "count": new_pixels},
        )

    # Calculate final coverage stats
    total_covered = np.sum(composite_coverage)
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
        sources=None, output_dir=composite_dir, resolution_m=resolution_m
    )
    if path:
        results["composite"] = path

    logger.info(
        f"Generated {len(results)} coverage masks",
        extra={"count": len(results)},
    )

    return results
