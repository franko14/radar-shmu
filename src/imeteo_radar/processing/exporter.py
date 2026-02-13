#!/usr/bin/env python3
"""
Multi-Format Exporter for Radar Data

Exports radar data as transparent PNG and AVIF overlays with consistent colorscale.
Supports multiple resolutions and formats per timestamp.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from ..config.shmu_colormap import get_shmu_colormap
from ..core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ExportConfig:
    """Configuration for multi-format/multi-resolution export.

    Attributes:
        resolutions_m: List of target resolutions in meters to generate scaled variants.
            Empty list = full resolution only. Example: [1000.0, 2000.0]
        include_full: Whether to include full resolution variants.
        formats: List of output formats. Supported: "png", "avif"
        avif_quality: AVIF quality (1-100, higher = better quality, larger file).
            Default 50 is optimized for radar images with limited color palette.
        avif_speed: AVIF encoding speed (0-10, higher = faster, lower quality).
            Default 6 (Pillow default). Use 8+ for CPU-constrained environments.
        avif_codec: AVIF codec to use. "auto" lets Pillow decide, or specify
            "aom", "svt", "rav1e". SVT-AV1 is significantly faster on multi-core.
    """

    resolutions_m: list[float] = field(default_factory=list)
    include_full: bool = True
    formats: list[str] = field(default_factory=lambda: ["png"])
    avif_quality: int = 50
    avif_speed: int = 6
    avif_codec: str = "auto"


# Source native resolutions in meters (approximate)
SOURCE_RESOLUTIONS: dict[str, float] = {
    "dwd": 1000.0,
    "shmu": 450.0,
    "chmi": 1550.0,
    "omsz": 900.0,
    "arso": 1000.0,
    "imgw": 1500.0,
    "composite": 500.0,
}


class MultiFormatExporter:
    """Exports radar data as transparent PNG/AVIF overlays in multiple resolutions"""

    def __init__(self, use_transform_cache: bool = True):
        """Initialize multi-format exporter.

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
                        wgs84["west"],
                        wgs84["south"],
                        wgs84["east"],
                        wgs84["north"],
                        data.shape[1],
                        data.shape[0],
                    )
                    native_bounds = (
                        wgs84["west"],
                        wgs84["south"],
                        wgs84["east"],
                        wgs84["north"],
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

    def _render_to_rgba(
        self,
        data: np.ndarray,
        cmap_name: str,
        transparent_background: bool = True,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Render radar data to RGBA array using LUT-based colormap.

        Args:
            data: 2D radar data array
            cmap_name: Colormap name for LUT lookup
            transparent_background: Whether to make NaN values transparent

        Returns:
            Tuple of (rgba_data, valid_mask)
        """
        lut_info = self.colormap_luts[cmap_name]

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
            rgba_data[~valid_mask, 3] = 0  # Set alpha channel to 0

        return rgba_data, valid_mask

    def _save_png(
        self,
        rgba_data: np.ndarray,
        output_path: Path,
        transparent_background: bool = True,
    ) -> None:
        """Save RGBA array as optimized indexed PNG.

        Args:
            rgba_data: RGBA uint8 array (H, W, 4)
            output_path: Path to save PNG file
            transparent_background: Whether to preserve transparency
        """
        img = Image.fromarray(rgba_data, mode="RGBA")

        # Convert to indexed PNG (8-bit palette) for smaller file size
        if transparent_background:
            img = img.convert("P", palette=Image.ADAPTIVE, colors=256)
        else:
            img = img.convert("RGB").convert("P", palette=Image.ADAPTIVE, colors=256)

        img.save(
            output_path,
            format="PNG",
            optimize=True,
            compress_level=9,
        )

    def _save_avif(
        self,
        rgba_data: np.ndarray,
        output_path: Path,
        quality: int = 85,
        speed: int = 6,
        codec: str = "auto",
    ) -> None:
        """Save RGBA array as AVIF.

        Args:
            rgba_data: RGBA uint8 array (H, W, 4)
            output_path: Path to save AVIF file
            quality: AVIF quality (1-100)
            speed: Encoding speed (0-10, higher = faster)
            codec: AVIF codec ("auto", "aom", "svt", "rav1e")
        """
        import time as _time

        h, w = rgba_data.shape[:2]
        logger.info(
            f"AVIF encode start: {w}x{h}, quality={quality}, speed={speed}, codec={codec}"
        )
        t0 = _time.monotonic()

        img = Image.fromarray(rgba_data, mode="RGBA")
        save_kwargs = {"format": "AVIF", "quality": quality, "speed": speed}
        if codec != "auto":
            save_kwargs["codec"] = codec
        img.save(output_path, **save_kwargs)

        elapsed = _time.monotonic() - t0
        file_size_kb = output_path.stat().st_size / 1024
        logger.info(
            f"AVIF encode done: {w}x{h} in {elapsed:.1f}s ({file_size_kb:.0f} KB)"
        )

    def _calculate_scaled_dimensions(
        self,
        shape: tuple[int, int],
        wgs84_bounds: dict[str, float],
        target_resolution_m: float,
        source_name: str = "",
    ) -> tuple[int, int]:
        """Calculate target dimensions for given resolution in meters.

        Uses geographic extent to determine scale factor. Will upscale if needed
        to provide consistent output across all sources.

        Args:
            shape: Current (height, width) of image
            wgs84_bounds: Dict with west, east, south, north bounds
            target_resolution_m: Target resolution in meters
            source_name: Source name for native resolution lookup

        Returns:
            (new_height, new_width)
        """
        import math

        height, width = shape

        # Get native resolution from source or estimate from extent
        native_res = SOURCE_RESOLUTIONS.get(source_name.lower())
        if native_res is None:
            # Estimate from extent - use latitude center for meters per degree
            lat_center = (wgs84_bounds["south"] + wgs84_bounds["north"]) / 2
            lat_span = wgs84_bounds["north"] - wgs84_bounds["south"]
            meters_per_deg_lat = 111320  # Approximate meters per degree latitude
            extent_height_m = lat_span * meters_per_deg_lat * math.cos(
                math.radians(lat_center)
            )
            native_res = extent_height_m / height

        # Calculate scale factor (may be >1 for upscaling, <1 for downscaling)
        scale_factor = native_res / target_resolution_m
        new_height = max(1, int(height * scale_factor))
        new_width = max(1, int(width * scale_factor))

        return (new_height, new_width)

    def _resize_rgba(
        self,
        rgba_data: np.ndarray,
        target_shape: tuple[int, int],
    ) -> np.ndarray:
        """Resize RGBA array using high-quality resampling.

        Args:
            rgba_data: RGBA uint8 array (H, W, 4)
            target_shape: (new_height, new_width)

        Returns:
            Resized RGBA array
        """
        img = Image.fromarray(rgba_data, mode="RGBA")
        # Use LANCZOS for high-quality downsampling
        resized = img.resize((target_shape[1], target_shape[0]), Image.LANCZOS)
        return np.array(resized)

    def export_variants(
        self,
        radar_data: dict[str, Any],
        output_base_path: Path,
        extent: dict[str, Any],
        config: ExportConfig | None = None,
        colormap_type: str = "auto",
        transparent_background: bool = True,
        reproject: bool = True,
        use_cached_transform: bool = True,
    ) -> dict[str, tuple[Path, dict[str, Any]]]:
        """Export all configured variants (formats and resolutions).

        Args:
            radar_data: Radar data dictionary with 'data' array
            output_base_path: Base path without extension (e.g., /output/germany/1738675200)
            extent: Geographic extent information
            config: Export configuration. Defaults to PNG+AVIF with 1000m scaled variant.
            colormap_type: Type of colormap to use ('auto', 'shmu', etc.)
            transparent_background: Whether to make background transparent
            reproject: Whether to reproject data to Web Mercator (EPSG:3857)
            use_cached_transform: Whether to use cached transform grids

        Returns:
            Dict mapping variant_name to (path, metadata) tuple.
            Variant names: "full.png", "full.avif", "@1000m.png", "@1000m.avif", etc.
        """
        if config is None:
            config = ExportConfig()

        try:
            data = radar_data["data"]

            if data is None or data.size == 0:
                raise ValueError("Empty or invalid radar data")

            # Optionally reproject to Web Mercator for proper web map display
            output_extent = extent
            used_cache = False

            if reproject:
                projection_info = radar_data.get("projection")
                source_name = radar_data.get("metadata", {}).get("source", "").lower()

                # Try fast path with cached transform first
                if (
                    use_cached_transform
                    and self._use_transform_cache
                    and projection_info
                ):
                    data, wgs84_bounds, used_cache = self._reproject_with_cache(
                        data,
                        projection_info,
                        source_name,
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

                    native_crs, native_transform, native_bounds = (
                        build_native_params_from_projection_info(
                            data.shape, projection_info
                        )
                    )

                    if native_crs is None:
                        wgs84 = {}
                        if data_extent:
                            wgs84 = (
                                data_extent.get("wgs84", data_extent)
                                if isinstance(data_extent, dict)
                                else {}
                            )
                        if not all(
                            k in wgs84 for k in ("west", "east", "south", "north")
                        ):
                            raise ValueError(
                                "Cannot reproject: no projection info and no WGS84 extent bounds"
                            )
                        native_crs = get_crs_wgs84()
                        native_transform = rt_from_bounds(
                            wgs84["west"],
                            wgs84["south"],
                            wgs84["east"],
                            wgs84["north"],
                            data.shape[1],
                            data.shape[0],
                        )
                        native_bounds = (
                            wgs84["west"],
                            wgs84["south"],
                            wgs84["east"],
                            wgs84["north"],
                        )

                    data, wgs84_bounds, _ = reproject_to_web_mercator(
                        data, native_crs, native_transform, native_bounds
                    )
                    output_extent = {"wgs84": wgs84_bounds}

            # Get colormap and render to RGBA
            cmap_info = self._select_colormap(radar_data, colormap_type)
            cmap_name = cmap_info["name"]
            lut_info = self.colormap_luts[cmap_name]

            rgba_data, valid_mask = self._render_to_rgba(
                data, cmap_name, transparent_background
            )

            # Get WGS84 bounds for scaling calculations
            wgs84_extent = output_extent.get("wgs84", extent.get("wgs84", {}))
            source_name = radar_data.get("metadata", {}).get("source", "").lower()

            # Build base metadata
            base_metadata = {
                "dimensions": data.shape,
                "extent": wgs84_extent,
                "extent_reference": "config/extent_index.json",
                "source": source_name or "unknown",
                "colormap": cmap_name,
                "units": lut_info["units"],
                "data_range": [float(np.nanmin(data)), float(np.nanmax(data))],
                "valid_pixels": int(np.sum(valid_mask)),
                "used_cached_transform": used_cache,
                "transparent": transparent_background,
                "timestamp": radar_data.get("timestamp", "unknown"),
                "reprojected": reproject,
            }

            variants: dict[str, tuple[Path, dict[str, Any]]] = {}
            output_base_path = Path(output_base_path)

            # Export full resolution variants
            if config.include_full:
                for fmt in config.formats:
                    variant_name = f"full.{fmt}"
                    output_path = output_base_path.with_suffix(f".{fmt}")

                    if fmt == "png":
                        self._save_png(rgba_data, output_path, transparent_background)
                    elif fmt == "avif":
                        self._save_avif(
                            rgba_data,
                            output_path,
                            config.avif_quality,
                            config.avif_speed,
                            config.avif_codec,
                        )

                    metadata = {
                        **base_metadata,
                        "file_path": str(output_path),
                        "format": fmt.upper(),
                        "resolution": "full",
                        "export_method": "PIL_fast_LUT",
                    }
                    variants[variant_name] = (output_path, metadata)
                    logger.info(f"Saved: {output_path}")

            # Export scaled variants
            for target_res in config.resolutions_m:
                scaled_dims = self._calculate_scaled_dimensions(
                    data.shape, wgs84_extent, target_res, source_name
                )
                scaled_rgba = self._resize_rgba(rgba_data, scaled_dims)

                for fmt in config.formats:
                    res_suffix = f"@{int(target_res)}m"
                    variant_name = f"{res_suffix}.{fmt}"
                    output_path = output_base_path.parent / (
                        f"{output_base_path.stem}{res_suffix}.{fmt}"
                    )

                    if fmt == "png":
                        self._save_png(scaled_rgba, output_path, transparent_background)
                    elif fmt == "avif":
                        self._save_avif(
                            scaled_rgba,
                            output_path,
                            config.avif_quality,
                            config.avif_speed,
                            config.avif_codec,
                        )

                    metadata = {
                        **base_metadata,
                        "file_path": str(output_path),
                        "dimensions": scaled_dims,
                        "format": fmt.upper(),
                        "resolution": f"{int(target_res)}m",
                        "export_method": "PIL_fast_LUT_scaled",
                    }
                    variants[variant_name] = (output_path, metadata)
                    logger.info(f"Saved: {output_path}")

            return variants

        except Exception:
            logger.error(f"Export failed for {output_base_path}", exc_info=True)
            raise

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

        This is a backward-compatible method that exports a single PNG file.
        For multi-format export, use export_variants() instead.

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
        # Use export_variants with PNG-only config
        config = ExportConfig(
            resolutions_m=[],  # No scaled variants
            include_full=True,
            formats=["png"],
        )

        # Ensure output_path has .png extension for base path calculation
        output_path = Path(output_path)
        base_path = output_path.with_suffix("")

        variants = self.export_variants(
            radar_data=radar_data,
            output_base_path=base_path,
            extent=extent,
            config=config,
            colormap_type=colormap_type,
            transparent_background=transparent_background,
            reproject=reproject,
            use_cached_transform=use_cached_transform,
        )

        # Return the full PNG variant
        if "full.png" in variants:
            return variants["full.png"]

        raise RuntimeError("PNG export failed - no variant produced")

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


# Backward compatibility alias
PNGExporter = MultiFormatExporter
