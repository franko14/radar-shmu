#!/usr/bin/env python3
"""
PNG Exporter for Radar Data

Exports radar data as transparent PNG overlays with consistent colorscale.
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use("Agg")  # Use non-interactive backend
from pathlib import Path
from typing import Any

from PIL import Image

from ..core.logging import get_logger

logger = get_logger(__name__)

# Import SHMU colormap if available
try:
    from ..config.shmu_colormap import get_shmu_colormap

    SHMU_COLORMAP_AVAILABLE = True
except ImportError:
    try:
        from ..shmu_colormap import get_shmu_colormap

        SHMU_COLORMAP_AVAILABLE = True
    except ImportError:
        try:
            from shmu_colormap import get_shmu_colormap

            SHMU_COLORMAP_AVAILABLE = True
        except ImportError:
            SHMU_COLORMAP_AVAILABLE = False


class PNGExporter:
    """Exports radar data as transparent PNG overlays"""

    def __init__(self):
        self.colormaps = self._initialize_colormaps()
        self.colormap_luts = self._build_colormap_luts()

    def _initialize_colormaps(self):
        """Initialize colormaps - SHMU colormap is the single source of truth"""
        colormaps = {}

        # SHMU reflectivity colormap - REQUIRED, no fallbacks
        if not SHMU_COLORMAP_AVAILABLE:
            raise ImportError(
                "shmu_colormap.py is required and must be available. "
                "This is the single source of truth for colorscales."
            )

        try:
            shmu_cmap, shmu_norm = get_shmu_colormap()
            colormaps["reflectivity_shmu"] = {
                "name": "reflectivity_shmu",
                "colormap": shmu_cmap,
                "norm": shmu_norm,
                "units": "dBZ",
                "range": [-35, 85],
            }
            logger.info("SHMU colormap loaded as single source of colorscale")
        except Exception as e:
            raise RuntimeError(
                f"Failed to load SHMU colormap: {e}. "
                "shmu_colormap.py must be available as single source of truth."
            )

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
        dpi: int = 150,
        transparent_background: bool = True,
    ) -> tuple[Path, dict[str, Any]]:
        """
        Export radar data as transparent PNG

        Args:
            radar_data: Processed radar data dictionary
            output_path: Output PNG file path
            extent: Geographic extent information
            colormap_type: Colormap to use ('auto', 'reflectivity_shmu', 'precipitation')
            dpi: Output resolution in DPI
            transparent_background: Whether to make background transparent

        Returns:
            Tuple of (output_path, metadata)
        """

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            data = radar_data["data"]
            if isinstance(data, list):
                data = np.array(data)

            logger.debug(
                f"Exporting {data.shape} array to PNG: {output_path}",
                extra={"operation": "export"},
            )

            # Determine colormap
            cmap_info = self._select_colormap(radar_data, colormap_type)

            # Create figure with exact data dimensions
            fig_width = data.shape[1] / dpi
            fig_height = data.shape[0] / dpi

            fig, ax = plt.subplots(figsize=(fig_width, fig_height), frameon=False)

            # Remove all margins and axes
            ax.set_position((0, 0, 1, 1))
            ax.axis("off")

            # Plot image using colormap directly (like your example code)
            plot_extent = self._get_plot_extent(radar_data, extent)

            # Mask values below minimum threshold as NaN for transparency
            data_masked = data.copy()
            min_threshold = cmap_info.get("range", [-35, 85])[0]
            data_masked[data < min_threshold] = np.nan

            # Create colormap copy with transparency for NaN/bad values
            cmap_copy = cmap_info["colormap"].copy()
            cmap_copy.set_bad(alpha=0)  # Make NaN values transparent
            cmap_copy.set_under(alpha=0)  # Make below-threshold values transparent

            ax.imshow(
                data_masked,  # Use masked data
                extent=tuple(plot_extent),
                origin="upper",
                interpolation="nearest",
                cmap=cmap_copy,  # Use colormap with transparency
                norm=cmap_info["norm"],  # Use the discrete normalization
                aspect="auto",
            )

            # Save with transparency
            plt.savefig(
                output_path,
                dpi=dpi,
                bbox_inches="tight",
                pad_inches=0,
                transparent=transparent_background,
                facecolor="none" if transparent_background else "white",
                pil_kwargs={"optimize": True, "compress_level": 9},
            )

            plt.close(fig)

            # Post-process: Convert to indexed PNG for smaller file size
            try:
                img = Image.open(output_path)
                if transparent_background:
                    img = img.convert("P", palette=Image.ADAPTIVE, colors=256)
                else:
                    img = img.convert("RGB").convert(
                        "P", palette=Image.ADAPTIVE, colors=256
                    )
                img.save(output_path, format="PNG", optimize=True, compress_level=9)
            except Exception as e:
                logger.warning(
                    f"Could not convert to indexed PNG: {e}, keeping original",
                    extra={"operation": "export"},
                )

            # Create metadata without duplicating extent information
            metadata = {
                "file_path": str(output_path),
                "dimensions": data.shape,
                "extent_reference": "config/extent_index.json",
                "source": radar_data.get("metadata", {}).get("source", "unknown"),
                "colormap": cmap_info["name"],
                "units": cmap_info["units"],
                "data_range": [float(np.nanmin(data)), float(np.nanmax(data))],
                "valid_pixels": int(np.sum(~np.isnan(data))),
                "dpi": dpi,
                "transparent": transparent_background,
                "timestamp": radar_data.get("timestamp", "unknown"),
                "format": "PNG (8-bit indexed palette, optimized)",
            }

            logger.info(
                f"Saved: {output_path}",
                extra={"operation": "export"},
            )
            logger.debug(
                f"Size: {data.shape}, Range: [{metadata['data_range'][0]:.1f}, {metadata['data_range'][1]:.1f}] {cmap_info['units']}",
            )

            return output_path, metadata

        except Exception as e:
            logger.error(f"PNG export failed: {e}")
            raise

    def export_png_fast(
        self,
        radar_data: dict[str, Any],
        output_path: Path,
        extent: dict[str, Any],
        colormap_type: str = "auto",
        transparent_background: bool = True,
    ) -> tuple[Path, dict[str, Any]]:
        """
        Fast PNG export using PIL (4-10x faster than matplotlib)

        Args:
            radar_data: Radar data dictionary with 'data' array
            output_path: Path where to save the PNG file
            extent: Geographic extent information
            colormap_type: Type of colormap to use ('auto', 'shmu', etc.)
            transparent_background: Whether to make background transparent

        Returns:
            Tuple of (saved_path, metadata_dict)
        """

        try:
            data = radar_data["data"]

            if data is None or data.size == 0:
                raise ValueError("Empty or invalid radar data")

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

            # Create metadata
            metadata = {
                "file_path": str(output_path),
                "dimensions": data.shape,
                "extent_reference": "config/extent_index.json",
                "source": radar_data.get("metadata", {}).get("source", "unknown"),
                "colormap": cmap_name,
                "units": lut_info["units"],
                "data_range": [float(np.nanmin(data)), float(np.nanmax(data))],
                "valid_pixels": int(np.sum(valid_mask)),
                "transparent": transparent_background,
                "timestamp": radar_data.get("timestamp", "unknown"),
                "export_method": "PIL_fast_LUT",
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

        except Exception as e:
            logger.error(f"Fast PNG export failed: {e}")
            # Fallback to matplotlib method
            logger.info("Falling back to matplotlib export...", extra={"operation": "export"})
            return self.export_png(
                radar_data,
                output_path,
                extent,
                colormap_type,
                150,
                transparent_background,
            )

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

    def _get_plot_extent(
        self, radar_data: dict[str, Any], extent: dict[str, Any]
    ) -> list:
        """Get extent for matplotlib plot"""

        if "wgs84" in extent:
            wgs84 = extent["wgs84"]
            return [wgs84["west"], wgs84["east"], wgs84["south"], wgs84["north"]]
        else:
            # Fallback to data coordinates
            coords = radar_data.get("coordinates", {})
            if "lons" in coords and "lats" in coords:
                lons = coords["lons"]
                lats = coords["lats"]
                return [lons[0], lons[-1], lats[-1], lats[0]]

        return [0, 1, 0, 1]  # Default

    def create_colorbar_legend(
        self, colormap_type: str, output_path: Path, width: int = 300, height: int = 50
    ) -> Path:
        """Create a separate colorbar legend PNG"""

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if colormap_type not in self.colormaps:
            raise ValueError(f"Unknown colormap: {colormap_type}")

        cmap_info = self.colormaps[colormap_type]

        fig, ax = plt.subplots(figsize=(width / 100, height / 100))

        # Create colorbar
        colorbar = plt.colorbar(
            plt.cm.ScalarMappable(norm=cmap_info["norm"], cmap=cmap_info["colormap"]),
            cax=ax,
            orientation="horizontal",
        )

        colorbar.set_label(f"Radar {cmap_info['units']}", fontsize=10)

        plt.savefig(output_path, dpi=100, bbox_inches="tight", transparent=True)

        plt.close(fig)

        logger.info(
            f"Saved: {output_path}",
            extra={"operation": "export"},
        )

        return output_path
