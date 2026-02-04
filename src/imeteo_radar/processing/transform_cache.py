#!/usr/bin/env python3
"""
Transform Grid Cache for Fast Reprojection

Three-tier caching system for precomputed transformation grids:
- Tier 1: Memory cache (same process, instant)
- Tier 2: Local disk (/tmp) - fast, persists across function calls
- Tier 3: S3/DO Spaces - persistent across pod restarts

Since radar source extents are STATIC, we can precompute pixel-to-pixel index
mappings once and reuse them for every subsequent image. This provides 10-50x
speedup over runtime reprojection.

Storage Format:
- Local: {cache_dir}/{source}_{width}x{height}_v{version}.npz
- S3: iradar-data/grid/{source}_{width}x{height}_v{version}.npz
- NPZ contains: row_indices (int16), col_indices (int16), dst_shape, wgs84_bounds, metadata
"""

import hashlib
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from pyproj import Transformer
from rasterio.crs import CRS
from rasterio.transform import Affine
from rasterio.warp import calculate_default_transform

from ..core.logging import get_logger
from ..core.projections import (
    CACHE_VERSION,
    PROJ4_WEB_MERCATOR,
    PROJ4_WGS84,
    get_crs_web_mercator,
    validate_grid_dimensions,
    validate_source_name,
)

logger = get_logger(__name__)


@dataclass
class TransformGrid:
    """Precomputed transformation grid for fast reprojection.

    Contains pixel-to-pixel index mappings from destination grid to source grid.
    Using int16 indices keeps memory usage low (~4 bytes per pixel vs 8 for float).

    Attributes:
        row_indices: int16 array [dst_h, dst_w] - which source row maps to each output pixel
        col_indices: int16 array [dst_h, dst_w] - which source column maps to each output pixel
        dst_shape: Output grid dimensions (height, width)
        wgs84_bounds: WGS84 bounds derived FROM the reprojected transform
        source_name: Source identifier (e.g., 'dwd', 'shmu')
        version: Cache version for invalidation
        src_shape: Original source grid dimensions
    """

    row_indices: np.ndarray
    col_indices: np.ndarray
    dst_shape: tuple[int, int]
    wgs84_bounds: dict[str, float]
    source_name: str
    version: str
    src_shape: tuple[int, int] = field(default_factory=lambda: (0, 0))
    mercator_bounds: dict[str, float] = field(default_factory=dict)

    def memory_size_mb(self) -> float:
        """Calculate memory size in MB."""
        return (self.row_indices.nbytes + self.col_indices.nbytes) / (1024 * 1024)


class TransformCache:
    """Three-tier cache for precomputed transformation grids.

    Caching Strategy:
    1. Memory cache (dict) - instant access, same process
    2. Local disk (/tmp) - fast access, persists in container
    3. S3/DO Spaces - slow but persistent across deployments

    Usage:
        cache = TransformCache()

        # Get or compute transform grid (checks all tiers)
        grid = cache.get_or_compute(
            source_name="shmu",
            src_shape=(1560, 2270),
            native_crs=CRS.from_string(projdef),
            native_transform=Affine(...),
            native_bounds=(left, bottom, right, top),
        )

        # Fast reprojection using cached indices
        output = fast_reproject(data, grid)
    """

    def __init__(
        self,
        local_cache_dir: Path | None = None,
        s3_bucket: str | None = None,
        s3_prefix: str = "iradar-data/grid/",
        s3_enabled: bool = True,
    ):
        """Initialize the transform cache.

        Args:
            local_cache_dir: Local cache directory (default: /tmp/iradar-data/grid)
            s3_bucket: S3 bucket name (from env if None)
            s3_prefix: S3 key prefix for transform grids
            s3_enabled: Whether to use S3 as tier 3 cache
        """
        self.local_cache_dir = local_cache_dir or Path("/tmp/iradar-data/grid")
        self.s3_prefix = s3_prefix
        self.s3_enabled = s3_enabled
        self._s3_bucket = s3_bucket  # Will use env var if None
        self._memory_cache: dict[str, TransformGrid] = {}
        self._uploader = None
        self._s3_initialized = False

        # Create local cache directory
        self.local_cache_dir.mkdir(parents=True, exist_ok=True)

        logger.debug(
            f"TransformCache initialized: local_dir={self.local_cache_dir}, "
            f"s3_enabled={s3_enabled}"
        )

    def _get_uploader(self):
        """Lazy-initialize S3 uploader."""
        if not self.s3_enabled:
            return None

        if not self._s3_initialized:
            self._s3_initialized = True
            try:
                from ..utils.spaces_uploader import SpacesUploader, is_spaces_configured

                if is_spaces_configured():
                    self._uploader = SpacesUploader()
                    # Use bucket from uploader if not explicitly set
                    if self._s3_bucket is None:
                        self._s3_bucket = self._uploader.bucket
                    logger.debug("S3 transform cache layer enabled")
                else:
                    logger.debug("S3 not configured, using local transform cache only")
            except Exception as e:
                logger.warning(f"Failed to initialize S3 for transform cache: {e}")

        return self._uploader

    @property
    def s3_bucket(self) -> str | None:
        """Get S3 bucket name, initializing uploader if needed."""
        if self._s3_bucket is None:
            self._get_uploader()
        return self._s3_bucket

    def _make_key(
        self,
        source_name: str,
        src_shape: tuple[int, int],
        native_bounds: tuple[float, float, float, float] | None = None,
    ) -> str:
        """Generate cache key from source parameters.

        Args:
            source_name: Source identifier
            src_shape: Source grid shape (height, width)
            native_bounds: Optional native bounds for hash

        Returns:
            Cache key string like 'shmu_1560x2270_v1'

        Raises:
            ValueError: If source_name is invalid (path traversal prevention)
        """
        # Validate source name to prevent path traversal attacks
        validated_source = validate_source_name(source_name)

        # Validate dimensions
        height, width = validate_grid_dimensions(src_shape[0], src_shape[1])

        # Include bounds hash if provided (for sources with dynamic bounds)
        if native_bounds:
            bounds_str = f"{native_bounds[0]:.0f}_{native_bounds[1]:.0f}_{native_bounds[2]:.0f}_{native_bounds[3]:.0f}"
            bounds_hash = hashlib.md5(bounds_str.encode()).hexdigest()[:8]
            return f"{validated_source}_{height}x{width}_{bounds_hash}_{CACHE_VERSION}"

        return f"{validated_source}_{height}x{width}_{CACHE_VERSION}"

    def _get_local_path(self, cache_key: str) -> Path:
        """Get local cache file path for a given key.

        Includes path traversal protection by resolving and validating path.
        """
        local_path = (self.local_cache_dir / f"{cache_key}.npz").resolve()

        # Ensure path is within cache directory (path traversal protection)
        if not str(local_path).startswith(str(self.local_cache_dir.resolve())):
            raise ValueError(f"Path traversal detected in cache key: {cache_key}")

        return local_path

    def _get_s3_key(self, cache_key: str) -> str:
        """Get S3 key for a given cache key."""
        return f"{self.s3_prefix}{cache_key}.npz"

    def get_or_compute(
        self,
        source_name: str,
        src_shape: tuple[int, int],
        native_crs: CRS,
        native_transform: Affine,
        native_bounds: tuple[float, float, float, float],
    ) -> TransformGrid:
        """Get cached transform grid or compute and cache it.

        Lookup order: memory → local disk → S3 → compute

        Args:
            source_name: Source identifier (e.g., 'dwd', 'shmu')
            src_shape: Source data shape (height, width)
            native_crs: Source coordinate reference system
            native_transform: Source affine transform
            native_bounds: Native bounds (left, bottom, right, top)

        Returns:
            TransformGrid with precomputed indices
        """
        cache_key = self._make_key(source_name, src_shape, native_bounds)

        # Tier 1: Memory cache (instant)
        if cache_key in self._memory_cache:
            logger.debug(f"Transform cache hit (memory): {source_name}")
            return self._memory_cache[cache_key]

        # Tier 2: Local disk cache (fast)
        local_path = self._get_local_path(cache_key)
        if local_path.exists():
            grid = self._load_from_disk(local_path)
            if grid:
                self._memory_cache[cache_key] = grid
                logger.debug(f"Transform cache hit (disk): {source_name}")
                return grid

        # Tier 3: S3 cache (slower but persistent)
        if self._get_uploader():
            grid = self._try_load_from_s3(cache_key)
            if grid:
                self._save_to_disk(local_path, grid)  # Warm local cache
                self._memory_cache[cache_key] = grid
                logger.debug(f"Transform cache hit (S3): {source_name}")
                return grid

        # Cache miss - compute and save to all tiers
        logger.info(f"Computing transform grid for {source_name}...")
        grid = self._compute_transform_grid(
            source_name=source_name,
            src_shape=src_shape,
            native_crs=native_crs,
            native_transform=native_transform,
            native_bounds=native_bounds,
        )

        # Save to all cache layers
        self._save_to_disk(local_path, grid)
        self._memory_cache[cache_key] = grid

        if self._get_uploader():
            self._save_to_s3(cache_key, grid)
            logger.info(f"Transform grid cached to S3: {source_name}")

        logger.info(
            f"Transform grid computed for {source_name}: "
            f"src={src_shape[0]}x{src_shape[1]} -> dst={grid.dst_shape[0]}x{grid.dst_shape[1]}, "
            f"{grid.memory_size_mb():.1f} MB"
        )

        return grid

    def _compute_transform_grid(
        self,
        source_name: str,
        src_shape: tuple[int, int],
        native_crs: CRS,
        native_transform: Affine,
        native_bounds: tuple[float, float, float, float],
    ) -> TransformGrid:
        """Compute transformation grid from source to Web Mercator.

        Uses calculate_default_transform() to determine optimal output grid,
        then computes pixel-to-pixel index mappings.

        Args:
            source_name: Source identifier
            src_shape: Source grid shape (height, width)
            native_crs: Source CRS
            native_transform: Source affine transform
            native_bounds: Native bounds (left, bottom, right, top)

        Returns:
            TransformGrid with precomputed indices
        """
        src_height, src_width = src_shape
        left, bottom, right, top = native_bounds

        # Get Web Mercator CRS
        web_mercator = CRS.from_string(PROJ4_WEB_MERCATOR)

        # Calculate optimal output transform and dimensions
        # This is the KEY fix - use calculate_default_transform()
        dst_transform, dst_width, dst_height = calculate_default_transform(
            native_crs,
            web_mercator,
            src_width,
            src_height,
            left=left,
            bottom=bottom,
            right=right,
            top=top,
        )

        # Extract mercator bounds FROM the destination transform
        mercator_left = dst_transform.c
        mercator_top = dst_transform.f
        mercator_right = mercator_left + dst_width * dst_transform.a
        mercator_bottom = mercator_top + dst_height * dst_transform.e

        # Convert mercator bounds to WGS84 for Leaflet
        transformer = Transformer.from_crs(
            PROJ4_WEB_MERCATOR, PROJ4_WGS84, always_xy=True
        )
        west, south = transformer.transform(mercator_left, mercator_bottom)
        east, north = transformer.transform(mercator_right, mercator_top)

        wgs84_bounds = {
            "west": west,
            "east": east,
            "south": south,
            "north": north,
        }

        mercator_bounds = {
            "left": mercator_left,
            "right": mercator_right,
            "bottom": mercator_bottom,
            "top": mercator_top,
        }

        # Compute coordinate arrays for index mapping
        # Create coordinate grids for destination pixels (pixel centers)
        dst_rows = np.arange(dst_height)
        dst_cols = np.arange(dst_width)
        dst_col_grid, dst_row_grid = np.meshgrid(dst_cols, dst_rows)

        # Convert destination pixel coordinates to mercator coordinates
        dst_x = dst_transform.c + (dst_col_grid + 0.5) * dst_transform.a
        dst_y = dst_transform.f + (dst_row_grid + 0.5) * dst_transform.e

        # Transform mercator coordinates to native CRS
        transformer_to_native = Transformer.from_crs(
            PROJ4_WEB_MERCATOR, native_crs.to_string(), always_xy=True
        )
        native_x, native_y = transformer_to_native.transform(
            dst_x.flatten(), dst_y.flatten()
        )
        native_x = native_x.reshape(dst_height, dst_width)
        native_y = native_y.reshape(dst_height, dst_width)

        # Convert native coordinates to source pixel indices
        # native_transform: Affine(a, b, c, d, e, f)
        # x = c + col * a + row * b
        # y = f + col * d + row * e
        # For non-rotated grids (b=d=0):
        # col = (x - c) / a
        # row = (y - f) / e
        inv_transform = ~native_transform
        src_col = inv_transform.a * native_x + inv_transform.b * native_y + inv_transform.c
        src_row = inv_transform.d * native_x + inv_transform.e * native_y + inv_transform.f

        # Round to nearest pixel and clip to valid range
        src_col_int = np.clip(np.round(src_col).astype(np.int32), 0, src_width - 1)
        src_row_int = np.clip(np.round(src_row).astype(np.int32), 0, src_height - 1)

        # Mark out-of-bounds pixels with -1
        out_of_bounds = (
            (src_col < -0.5) |
            (src_col >= src_width - 0.5) |
            (src_row < -0.5) |
            (src_row >= src_height - 0.5)
        )
        src_col_int[out_of_bounds] = -1
        src_row_int[out_of_bounds] = -1

        # Convert to int16 to save memory
        row_indices = src_row_int.astype(np.int16)
        col_indices = src_col_int.astype(np.int16)

        return TransformGrid(
            row_indices=row_indices,
            col_indices=col_indices,
            dst_shape=(dst_height, dst_width),
            wgs84_bounds=wgs84_bounds,
            source_name=source_name,
            version=CACHE_VERSION,
            src_shape=src_shape,
            mercator_bounds=mercator_bounds,
        )

    def _load_from_disk(self, local_path: Path) -> TransformGrid | None:
        """Load transform grid from local NPZ file.

        Returns None if file is invalid or version mismatch.

        Security: Uses allow_pickle=False to prevent arbitrary code execution
        from malicious NPZ files. All data is stored as plain numpy arrays.
        """
        try:
            # SECURITY: allow_pickle=False prevents RCE from malicious NPZ files
            with np.load(local_path, allow_pickle=False) as npz:
                # Check version
                version_arr = npz.get("version")
                if version_arr is None:
                    logger.debug(f"No version in cache file: {local_path}")
                    return None

                # Convert numpy array to string safely
                version = str(version_arr.item()) if version_arr.ndim == 0 else str(version_arr)
                if version != CACHE_VERSION:
                    logger.debug(f"Cache version mismatch: {version} != {CACHE_VERSION}")
                    return None

                # Load arrays and convert dict-like arrays back to dicts
                wgs84_arr = npz["wgs84_bounds"]
                mercator_arr = npz.get("mercator_bounds", np.array({}))

                # Handle dict conversion - stored as 0-d object arrays
                if wgs84_arr.ndim == 0:
                    wgs84_bounds = dict(wgs84_arr.item())
                else:
                    wgs84_bounds = {"west": 0, "east": 0, "south": 0, "north": 0}

                if mercator_arr.ndim == 0:
                    mercator_bounds = dict(mercator_arr.item())
                else:
                    mercator_bounds = {}

                source_arr = npz["source_name"]
                source_name = str(source_arr.item()) if source_arr.ndim == 0 else str(source_arr)

                return TransformGrid(
                    row_indices=npz["row_indices"],
                    col_indices=npz["col_indices"],
                    dst_shape=tuple(npz["dst_shape"]),
                    wgs84_bounds=wgs84_bounds,
                    source_name=source_name,
                    version=version,
                    src_shape=tuple(npz.get("src_shape", np.array((0, 0)))),
                    mercator_bounds=mercator_bounds,
                )
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Invalid cache file format {local_path}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to load transform cache from {local_path}: {e}")
            return None

    def _save_to_disk(self, local_path: Path, grid: TransformGrid):
        """Save transform grid to local NPZ file."""
        try:
            np.savez_compressed(
                local_path,
                row_indices=grid.row_indices,
                col_indices=grid.col_indices,
                dst_shape=np.array(grid.dst_shape),
                wgs84_bounds=np.array(grid.wgs84_bounds),
                source_name=np.array(grid.source_name),
                version=np.array(grid.version),
                src_shape=np.array(grid.src_shape),
                mercator_bounds=np.array(grid.mercator_bounds),
            )
            logger.debug(f"Saved transform cache to {local_path}")
        except Exception as e:
            logger.warning(f"Failed to save transform cache to {local_path}: {e}")

    def _try_load_from_s3(self, cache_key: str) -> TransformGrid | None:
        """Try to load transform grid from S3.

        Returns None if not found or download fails.

        Security: Uses secure temp file handling with try/finally cleanup
        and restrictive file permissions.
        """
        uploader = self._get_uploader()
        if not uploader:
            return None

        s3_key = self._get_s3_key(cache_key)
        tmp_path = None

        try:
            # Check if file exists first (avoids download attempt for missing files)
            uploader.s3_client.head_object(Bucket=uploader.bucket, Key=s3_key)

            # Create temp file in cache directory (not /tmp) with secure permissions
            with tempfile.NamedTemporaryFile(
                suffix=".npz",
                delete=False,
                dir=self.local_cache_dir,
                mode="wb",
            ) as tmp:
                tmp_path = Path(tmp.name)

            # Set restrictive permissions (owner read/write only)
            os.chmod(tmp_path, 0o600)

            # Download to temp file
            uploader.s3_client.download_file(uploader.bucket, s3_key, str(tmp_path))

            # Load from temp file
            grid = self._load_from_disk(tmp_path)

            return grid

        except uploader.s3_client.exceptions.ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                logger.debug(f"Transform cache not found in S3: {cache_key}")
            elif error_code == "403":
                logger.warning(f"Access denied to S3 transform cache: {cache_key}")
            else:
                logger.warning(f"S3 error loading transform cache: {error_code}")
            return None
        except Exception as e:
            logger.debug(f"Failed to load transform cache from S3: {e}")
            return None
        finally:
            # Always cleanup temp file
            if tmp_path is not None:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception as cleanup_err:
                    logger.debug(f"Failed to cleanup temp file {tmp_path}: {cleanup_err}")

    def _save_to_s3(self, cache_key: str, grid: TransformGrid):
        """Save transform grid to S3.

        Security: Uses secure temp file handling with try/finally cleanup.
        """
        uploader = self._get_uploader()
        if not uploader:
            return

        s3_key = self._get_s3_key(cache_key)
        tmp_path = None

        try:
            # Create temp file in cache directory with secure permissions
            with tempfile.NamedTemporaryFile(
                suffix=".npz",
                delete=False,
                dir=self.local_cache_dir,
                mode="wb",
            ) as tmp:
                tmp_path = Path(tmp.name)

            # Set restrictive permissions
            os.chmod(tmp_path, 0o600)

            # Save to temp file
            self._save_to_disk(tmp_path, grid)

            # Upload to S3
            uploader.s3_client.upload_file(
                str(tmp_path),
                uploader.bucket,
                s3_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )

            logger.debug(f"Uploaded transform cache to S3: {s3_key}")

        except Exception as e:
            # Sanitize error message to avoid leaking credentials
            logger.warning(f"Failed to upload transform cache to S3: {type(e).__name__}")
        finally:
            # Always cleanup temp file
            if tmp_path is not None:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass  # Ignore cleanup errors

    def precompute_for_source(
        self,
        source_name: str,
        upload_to_s3: bool = True,
    ) -> TransformGrid | None:
        """Precompute and cache transform grid for a source.

        This method fetches sample data from a source to determine its
        grid parameters, then computes and caches the transform grid.

        Args:
            source_name: Source identifier (e.g., 'dwd', 'shmu')
            upload_to_s3: Whether to upload to S3 after computing

        Returns:
            TransformGrid if successful, None otherwise
        """
        try:
            from ..config.sources import get_source_instance

            source = get_source_instance(source_name)
            if not source:
                logger.error(f"Unknown source: {source_name}")
                return None

            # Download a sample file to get grid parameters
            logger.info(f"Downloading sample data for {source_name}...")
            files = source.download_latest(count=1)
            if not files:
                logger.error(f"Could not download sample data for {source_name}")
                return None

            # Process to get projection info
            radar_data = source.process_to_array(files[0]["path"])
            projection_info = radar_data.get("projection")

            if not projection_info:
                logger.error(f"No projection info for {source_name}")
                return None

            # Extract grid parameters
            data = radar_data["data"]
            src_shape = data.shape

            # Get native CRS and transform
            proj_def = projection_info.get("proj_def")
            where_attrs = projection_info.get("where_attrs", {})

            if not proj_def:
                # WGS84 source (OMSZ, ARSO) — build native params from extent bounds
                # WGS84→WebMercator IS a valid reprojection that benefits from caching
                from rasterio.transform import from_bounds as _from_bounds

                extent = radar_data.get("extent", {})
                wgs84 = extent.get("wgs84", extent)
                if not all(k in wgs84 for k in ("west", "east", "south", "north")):
                    logger.warning(
                        f"{source_name} uses WGS84 but has no extent bounds, "
                        "cannot precompute transform grid"
                    )
                    return None

                from ..core.projections import get_crs_wgs84

                native_crs = get_crs_wgs84()
                native_transform = _from_bounds(
                    wgs84["west"], wgs84["south"],
                    wgs84["east"], wgs84["north"],
                    src_shape[1], src_shape[0],
                )
                native_bounds = (
                    wgs84["west"], wgs84["south"],
                    wgs84["east"], wgs84["north"],
                )
            else:
                # Projected source — build native transform from projection info
                from .reprojector import build_native_params_from_projection_info

                projection_info_dict = {
                    "proj_def": proj_def,
                    "where_attrs": where_attrs,
                }
                native_crs, native_transform, native_bounds = (
                    build_native_params_from_projection_info(src_shape, projection_info_dict)
                )

                if native_crs is None:
                    logger.error(f"Could not build native params for {source_name}")
                    return None

            # Compute transform grid
            grid = self.get_or_compute(
                source_name=source_name,
                src_shape=src_shape,
                native_crs=native_crs,
                native_transform=native_transform,
                native_bounds=native_bounds,
            )

            # Force S3 upload if requested
            if upload_to_s3 and self._get_uploader():
                cache_key = self._make_key(source_name, src_shape, native_bounds)
                self._save_to_s3(cache_key, grid)

            # Clean up sample file
            source.cleanup_temp_files()

            return grid

        except Exception as e:
            logger.error(f"Failed to precompute transform for {source_name}: {e}")
            return None

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics including memory, local, and S3 entries
        """
        stats = {
            "local_dir": str(self.local_cache_dir),
            "s3_enabled": self.s3_enabled and self._get_uploader() is not None,
            "s3_prefix": self.s3_prefix,
            "memory_cache_entries": len(self._memory_cache),
            "memory_cache_size_mb": 0.0,
            "local_entries": 0,
            "local_size_mb": 0.0,
            "s3_entries": 0,
            "sources": {},
        }

        # Memory cache stats
        for key, grid in self._memory_cache.items():
            stats["memory_cache_size_mb"] += grid.memory_size_mb()
            source = grid.source_name
            if source not in stats["sources"]:
                stats["sources"][source] = {"memory": False, "local": False, "s3": False}
            stats["sources"][source]["memory"] = True

        stats["memory_cache_size_mb"] = round(stats["memory_cache_size_mb"], 2)

        # Local cache stats
        if self.local_cache_dir.exists():
            for npz_file in self.local_cache_dir.glob("*.npz"):
                stats["local_entries"] += 1
                stats["local_size_mb"] += npz_file.stat().st_size / (1024 * 1024)

                # Extract source from filename
                parts = npz_file.stem.split("_")
                if parts:
                    source = parts[0]
                    if source not in stats["sources"]:
                        stats["sources"][source] = {"memory": False, "local": False, "s3": False}
                    stats["sources"][source]["local"] = True

        stats["local_size_mb"] = round(stats["local_size_mb"], 2)

        # S3 cache stats
        uploader = self._get_uploader()
        if uploader:
            try:
                paginator = uploader.s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(
                    Bucket=uploader.bucket, Prefix=self.s3_prefix
                ):
                    for obj in page.get("Contents", []):
                        if obj["Key"].endswith(".npz"):
                            stats["s3_entries"] += 1
                            # Extract source from key
                            filename = obj["Key"].split("/")[-1]
                            parts = filename.replace(".npz", "").split("_")
                            if parts:
                                source = parts[0]
                                if source not in stats["sources"]:
                                    stats["sources"][source] = {
                                        "memory": False,
                                        "local": False,
                                        "s3": False,
                                    }
                                stats["sources"][source]["s3"] = True
            except Exception as e:
                logger.warning(f"Failed to get S3 stats: {e}")

        return stats

    def clear_local(self) -> int:
        """Clear local transform cache.

        Returns:
            Number of files removed
        """
        removed = 0
        if self.local_cache_dir.exists():
            for npz_file in self.local_cache_dir.glob("*.npz"):
                try:
                    npz_file.unlink()
                    removed += 1
                except Exception as e:
                    logger.warning(f"Failed to remove {npz_file}: {e}")

        self._memory_cache.clear()
        logger.info(f"Cleared {removed} local transform cache entries")
        return removed

    def clear_s3(self) -> int:
        """Clear S3 transform cache.

        Returns:
            Number of files removed
        """
        uploader = self._get_uploader()
        if not uploader:
            return 0

        removed = 0
        try:
            paginator = uploader.s3_client.get_paginator("list_objects_v2")
            objects_to_delete = []

            for page in paginator.paginate(
                Bucket=uploader.bucket, Prefix=self.s3_prefix
            ):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".npz"):
                        objects_to_delete.append({"Key": obj["Key"]})

            if objects_to_delete:
                # Delete in batches of 1000 (S3 limit)
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i : i + 1000]
                    uploader.s3_client.delete_objects(
                        Bucket=uploader.bucket, Delete={"Objects": batch}
                    )
                    removed += len(batch)

            logger.info(f"Cleared {removed} S3 transform cache entries")

        except Exception as e:
            logger.warning(f"Failed to clear S3 transform cache: {e}")

        return removed

    def download_from_s3(self) -> int:
        """Download all transform grids from S3 to local cache.

        Used for warming local cache on pod startup.

        Returns:
            Number of files downloaded
        """
        uploader = self._get_uploader()
        if not uploader:
            return 0

        downloaded = 0
        try:
            paginator = uploader.s3_client.get_paginator("list_objects_v2")

            for page in paginator.paginate(
                Bucket=uploader.bucket, Prefix=self.s3_prefix
            ):
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith(".npz"):
                        continue

                    # Extract cache key from S3 key
                    filename = obj["Key"].split("/")[-1]
                    cache_key = filename.replace(".npz", "")
                    local_path = self._get_local_path(cache_key)

                    # Skip if already exists locally
                    if local_path.exists():
                        continue

                    try:
                        uploader.s3_client.download_file(
                            uploader.bucket, obj["Key"], str(local_path)
                        )
                        downloaded += 1
                        logger.debug(f"Downloaded transform cache: {filename}")
                    except Exception as e:
                        logger.warning(f"Failed to download {obj['Key']}: {e}")

            logger.info(f"Downloaded {downloaded} transform cache entries from S3")

        except Exception as e:
            logger.warning(f"Failed to download transform cache from S3: {e}")

        return downloaded


def fast_reproject(data: np.ndarray, grid: TransformGrid) -> np.ndarray:
    """Ultra-fast reprojection using precomputed index arrays.

    This function performs reprojection by simple array indexing, which is
    10-50x faster than runtime coordinate transformation.

    Args:
        data: 2D source array to reproject
        grid: Precomputed TransformGrid with index arrays

    Returns:
        Reprojected 2D array with NaN for invalid pixels
    """
    # Create output array filled with NaN
    output = np.full(grid.dst_shape, np.nan, dtype=np.float32)

    # Find valid pixels (where indices are >= 0)
    valid_mask = (grid.row_indices >= 0) & (grid.col_indices >= 0)

    # Perform reprojection by indexing
    # This is the fast path - direct numpy array indexing
    output[valid_mask] = data[
        grid.row_indices[valid_mask], grid.col_indices[valid_mask]
    ]

    return output
