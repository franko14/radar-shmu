#!/usr/bin/env python3
"""
PNG Exporter for Radar Data

Exports radar data as transparent PNG overlays with consistent colorscale.
"""

import numpy as np

from pathlib import Path
from typing import Any

from PIL import Image

from ..config.shmu_colormap import get_shmu_colormap
from ..core.logging import get_logger

logger = get_logger(__name__)


class PNGExporter:
    """Exports radar data as transparent PNG overlays"""

    def __init__(self, use_transform_cache: bool = True):
        """Initialize PNG exporter.

        Args:
            use_transform_cache: Whether to use cached transform grids for
                faster reprojection (10-50x speedup). Default True.
        """
        self.colormaps = self._initialize_colormaps()
        self.colormap_luts = self._build_colormap_luts()
        self._use_transform_cache = use_transform_cache
        self._transform_cache = None

    def _get_transform_cache(self):
        """Lazy-initialize transform cache."""
        if self._transform_cache is None and self._use_transform_cache:
            try:
                from .transform_cache import TransformCache

                self._transform_cache = TransformCache()
            except Exception as e:
                logger.warning(f"Failed to initialize transform cache: {e}")
                self._use_transform_cache = False
        return self._transform_cache

    def _reproject_with_cache(
        self,
        data: np.ndarray,
        projection_info: dict[str, Any],
        source_name: str,
        extent: dict[str, Any] | None = None,
    ) -> tuple[np.ndarray, dict[str, float], bool]:
        """Attempt to reproject using cached transform grid.

        Args:
            data: Source data array
            projection_info: Projection info dict with proj_def and where_attrs
            source_name: Source identifier for cache lookup
            extent: Extent dict with wgs84 bounds (used for WGS84 sources)

        Returns:
            Tuple of (reprojected_data, wgs84_bounds, success_flag)
            If success_flag is False, caller should fall back to runtime reprojection
        """
        try:
            from rasterio.transform import from_bounds

            from ..core.projections import get_crs_wgs84
            from .reprojector import build_native_params_from_projection_info
            from .transform_cache import fast_reproject

            cache = self._get_transform_cache()
            if cache is None:
                return data, {}, False

            # Build native CRS, transform, and bounds from projection info
            native_crs, native_transform, native_bounds = (
                build_native_params_from_projection_info(data.shape, projection_info)
            )

            if native_crs is None:
                # Try WGS84 path from extent bounds (for OMSZ, ARSO)
                wgs84 = {}
                if extent:
                    wgs84 = extent.get("wgs84", extent)
                if all(k in wgs84 for k in ("west", "east", "south", "north")):
                    native_crs = get_crs_wgs84()
                    native_transform = from_bounds(
                        wgs84["west"], wgs84["south"],
                        wgs84["east"], wgs84["north"],
                        data.shape[1], data.shape[0],
                    )
                    native_bounds = (
                        wgs84["west"], wgs84["south"],
                        wgs84["east"], wgs84["north"],
                    )
                else:
                    return data, {}, False

            # Get or compute transform grid (checks memory, disk, S3)
            grid = cache.get_or_compute(
                source_name=source_name,
                src_shape=data.shape,
                native_crs=native_crs,
                native_transform=native_transform,
                native_bounds=native_bounds,
            )

            # Fast reprojection using precomputed indices
            reprojected = fast_reproject(data, grid)

            logger.debug(
                f"Used cached transform for {source_name}: "
                f"{data.shape} -> {grid.dst_shape}"
            )

            return reprojected, grid.wgs84_bounds, True

        except Exception as e:
            logger.warning(f"Cache reproject failed for {source_name}: {e}")
            return data, {}, False

    def _initialize_colormaps(self):
        """Initialize colormaps - SHMU colormap is the single source of truth"""
        colormaps = {}

        shmu_cmap, shmu_norm = get_shmu_colormap()
        colormaps["reflectivity_shmu"] = {
            "name": "reflectivity_shmu",
            "colormap": shmu_cmap,
            "norm": shmu_norm,
            "units": "dBZ",
            "range": [-35, 85],
        }
        logger.info("SHMU colormap loaded as single source of colorscale")

        return colormaps

    def _build_colormap_luts(self):
        """
        Build uint8 lookup tables for fast, memory-efficient colormap application.

        Pre-computes 256-entry RGBA uint8 LUT for each colormap, eliminating need for
        float64 intermediate arrays. Reduces memory usage by ~800 MB per image.
        """
        luts = {}

        for name, cmap_info in self.colormaps.items():
            # Get the data range for this colormap
            vmin, vmax = cmap_info["range"]

            # Create 256 evenly-spaced values across the data range
            values = np.linspace(vmin, vmax, 256)

            # Apply normalization and colormap to get colors
            norm_values = cmap_info["norm"](values)
            colors_float = cmap_info["colormap"](
                norm_values
            )  # Returns RGBA float [0,1]

            # Convert to uint8 RGBA (this is tiny: only 256 x 4 = 1 KB!)
            lut_rgba = (colors_float * 255).astype(np.uint8)

            luts[name] = {
                "lut": lut_rgba,
                "vmin": vmin,
                "vmax": vmax,
                "units": cmap_info["units"],
            }

        return luts

    def export_png(
        self,
        radar_data: dict[str, Any],
        output_path: Path,
        extent: dict[str, Any],
        colormap_type: str = "auto",
        transparent_background: bool = True,
        reproject: bool = True,
        use_cached_transform: bool = True,
    ) -> tuple[Path, dict[str, Any]]:
        """
        Export radar data as transparent PNG using PIL with LUT-based colormap.

        Args:
            radar_data: Radar data dictionary with 'data' array
            output_path: Path where to save the PNG file
            extent: Geographic extent information
            colormap_type: Type of colormap to use ('auto', 'shmu', etc.)
            transparent_background: Whether to make background transparent
            reproject: Whether to reproject data to Web Mercator (EPSG:3857)
            use_cached_transform: Whether to use cached transform grids for
                faster reprojection. Default True.

        Returns:
            Tuple of (saved_path, metadata_dict)
        """

        try:
            data = radar_data["data"]

            if data is None or data.size == 0:
                raise ValueError("Empty or invalid radar data")

            # Optionally reproject to Web Mercator for proper web map display
            output_extent = extent
            used_cache = False

            if reproject:
                # Get projection info from radar_data if available
                projection_info = radar_data.get("projection")
                source_name = radar_data.get("metadata", {}).get("source", "").lower()

                # Try fast path with cached transform first
                # Works for both projected sources (with proj_def) and WGS84 sources
                if (
                    use_cached_transform
                    and self._use_transform_cache
                    and projection_info
                ):
                    data, wgs84_bounds, used_cache = self._reproject_with_cache(
                        data, projection_info, source_name,
                        extent=radar_data.get("extent", extent),
                    )
                    if used_cache:
                        output_extent = {"wgs84": wgs84_bounds}

                # Runtime reprojection if cache not used
                if not used_cache:
                    from rasterio.transform import from_bounds as rt_from_bounds

                    from ..core.projections import get_crs_wgs84
                    from .reprojector import (
                        build_native_params_from_projection_info,
                        reproject_to_web_mercator,
                    )

                    data_extent = radar_data.get("extent", extent)

                    # Build native CRS params from projection info
                    native_crs, native_transform, native_bounds = (
                        build_native_params_from_projection_info(data.shape, projection_info)
                    )

                    # WGS84 sources (OMSZ, ARSO) — build params from extent
                    if native_crs is None:
                        wgs84 = {}
                        if data_extent:
                            wgs84 = data_extent.get("wgs84", data_extent) if isinstance(data_extent, dict) else {}
                        if not all(k in wgs84 for k in ("west", "east", "south", "north")):
                            raise ValueError(
                                f"Cannot reproject: no projection info and no WGS84 extent bounds"
                            )
                        native_crs = get_crs_wgs84()
                        native_transform = rt_from_bounds(
                            wgs84["west"], wgs84["south"],
                            wgs84["east"], wgs84["north"],
                            data.shape[1], data.shape[0],
                        )
                        native_bounds = (
                            wgs84["west"], wgs84["south"],
                            wgs84["east"], wgs84["north"],
                        )

                    # Single reprojection path for all sources
                    data, wgs84_bounds, _ = reproject_to_web_mercator(
                        data, native_crs, native_transform, native_bounds
                    )
                    output_extent = {"wgs84": wgs84_bounds}

            logger.debug(
                f"Fast PNG export: {data.shape} -> {output_path}",
                extra={"operation": "export"},
            )

            # Get colormap name (for LUT lookup)
            cmap_info = self._select_colormap(radar_data, colormap_type)
            cmap_name = cmap_info["name"]

            # Get pre-computed LUT for this colormap
            lut_info = self.colormap_luts[cmap_name]

            # MEMORY OPTIMIZATION: Use uint8 LUT instead of float64 colormap
            # Old method used ~800 MB in intermediate float64 arrays
            # New method: direct uint8 lookup, only ~80 MB peak

            # Map data values to LUT indices (0-255)
            # Handle NaN/invalid values separately
            valid_mask = np.isfinite(data)

            # Clip and scale valid data to 0-255 index range
            indices = np.zeros(data.shape, dtype=np.uint8)
            if np.any(valid_mask):
                valid_data = data[valid_mask]
                valid_indices = np.clip(
                    (
                        (valid_data - lut_info["vmin"])
                        / (lut_info["vmax"] - lut_info["vmin"])
                        * 255
                    ),
                    0,
                    255,
                ).astype(np.uint8)
                indices[valid_mask] = valid_indices

            # Apply LUT: direct uint8 RGBA lookup (very fast, low memory!)
            rgba_data = lut_info["lut"][indices]

            # Handle transparency for NaN/invalid values
            if transparent_background:
                # Set alpha to 0 for invalid data (NaN values)
                rgba_data[~valid_mask, 3] = 0  # Set alpha channel to 0

            # Create PIL image directly from RGBA array
            # PIL expects (height, width, channels)
            img = Image.fromarray(rgba_data, mode="RGBA")

            # Convert to indexed PNG (8-bit palette) for much smaller file size
            # SHMU colormap has ~24 discrete colors, indexed PNG supports 256
            # This gives ~75% size reduction with zero quality loss
            if transparent_background:
                # Convert RGBA to palette mode (P) with alpha channel preserved
                # Using ADAPTIVE palette with 256 colors (more than enough for ~24 SHMU colors)
                img = img.convert("P", palette=Image.ADAPTIVE, colors=256)
            else:
                # Without transparency, convert directly to RGB then to palette
                img = img.convert("RGB").convert(
                    "P", palette=Image.ADAPTIVE, colors=256
                )

            # Save with maximum PNG compression
            img.save(
                output_path,
                format="PNG",
                optimize=True,
                compress_level=9,  # Maximum compression for smallest file size
            )

            # Create metadata with extent info for Leaflet overlays
            wgs84_extent = output_extent.get("wgs84", extent.get("wgs84", {}))
            metadata = {
                "file_path": str(output_path),
                "dimensions": data.shape,
                "extent": wgs84_extent,  # WGS84 bounds for Leaflet ImageOverlay
                "extent_reference": "config/extent_index.json",
                "source": radar_data.get("metadata", {}).get("source", "unknown"),
                "colormap": cmap_name,
                "units": lut_info["units"],
                "data_range": [float(np.nanmin(data)), float(np.nanmax(data))],
                "valid_pixels": int(np.sum(valid_mask)),
                "used_cached_transform": used_cache,
                "transparent": transparent_background,
                "timestamp": radar_data.get("timestamp", "unknown"),
                "export_method": "PIL_fast_LUT",
                "reprojected": reproject,
                "format": "PNG (8-bit indexed palette, optimized)",
            }

            logger.info(
                f"Saved: {output_path}",
                extra={"operation": "export"},
            )
            logger.debug(
                f"Size: {data.shape}, Range: [{metadata['data_range'][0]:.1f}, {metadata['data_range'][1]:.1f}] {lut_info['units']}",
            )

            return output_path, metadata

        except Exception:
            logger.error(f"PNG export failed for {output_path}", exc_info=True)
            raise

    def _select_colormap(
        self, radar_data: dict[str, Any], colormap_type: str
    ) -> dict[str, Any]:
        """Select appropriate colormap for data - SHMU colormap is the single source"""

        if colormap_type != "auto":
            if colormap_type in self.colormaps:
                return {"name": colormap_type, **self.colormaps[colormap_type]}

        # Auto-select based on data - ALWAYS prefer SHMU colormap for reflectivity
        units = radar_data.get("metadata", {}).get("units", "unknown").lower()
        quantity = radar_data.get("metadata", {}).get("quantity", "").upper()

        if "dbz" in units or "DBZH" in quantity or "TH" in quantity:
            # Reflectivity data - MUST use SHMU colormap
            if "reflectivity_shmu" not in self.colormaps:
                raise RuntimeError(
                    "SHMU colormap not available but required as single source of truth"
                )

            return {"name": "reflectivity_shmu", **self.colormaps["reflectivity_shmu"]}
        elif "mm" in units or "ACRR" in quantity:
            # Precipitation data
            if "precipitation" in self.colormaps:
                return {"name": "precipitation", **self.colormaps["precipitation"]}
            else:
                # Fallback to SHMU colormap even for precipitation to maintain consistency
                logger.warning(
                    "Using SHMU reflectivity colormap for precipitation data (single source principle)",
                )
                return {
                    "name": "reflectivity_shmu",
                    **self.colormaps["reflectivity_shmu"],
                }
        else:
            # Default to SHMU reflectivity colormap for all unknowns
            logger.warning(
                f"Unknown data type (units: {units}, quantity: {quantity}), using SHMU colormap",
            )
            return {"name": "reflectivity_shmu", **self.colormaps["reflectivity_shmu"]}

