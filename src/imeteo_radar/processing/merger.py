#!/usr/bin/env python3
"""
Radar Data Merger

Merges radar data from multiple sources (SHMU, DWD) into unified composites.
"""

from typing import Any

import cv2
import numpy as np

from ..core.base import lonlat_to_mercator
from ..core.logging import get_logger

logger = get_logger(__name__)


class RadarMerger:
    """Merges radar data from multiple sources"""

    def __init__(self):
        self.merge_strategies = {
            "average": self._average_merge,
            "priority": self._priority_merge,
            "weighted": self._weighted_merge,
            "max": self._max_merge,
        }

    def merge_sources(
        self,
        timestamp_data: dict[str, list[dict[str, Any]]],
        source_data: dict[str, dict[str, Any]],
        strategy: str = "average",
        target_resolution: tuple[int, int] | None = None,
    ) -> dict[str, Any] | None:
        """
        Merge radar data from multiple sources for a specific timestamp

        Args:
            timestamp_data: Data for specific timestamp from each source
            source_data: Complete source metadata and extent info
            strategy: Merging strategy ('average', 'priority', 'weighted', 'max')
            target_resolution: Target grid resolution (height, width)

        Returns:
            Merged radar data dictionary or None if merge fails
        """

        if len(timestamp_data) < 2:
            logger.warning("Need at least 2 sources to merge")
            return None

        logger.info(f"Merging {len(timestamp_data)} sources using '{strategy}' strategy")

        try:
            # Determine target grid and extent
            target_extent, target_shape = self._compute_target_grid(
                source_data, target_resolution
            )

            logger.debug(f"Target extent: {target_extent['wgs84']}")
            logger.debug(f"Target shape: {target_shape}")

            # Regrid all sources to target grid
            regridded_data = {}
            for source_name, files in timestamp_data.items():
                logger.info(f"Regridding {source_name} data...", extra={"source": source_name})

                # For now, use first file from each source (TODO: handle multiple products)
                file_data = files[0] if files else None
                if not file_data:
                    continue

                regridded = self._regrid_to_target(
                    file_data, target_extent, target_shape
                )

                if regridded is not None:
                    regridded_data[source_name] = regridded

            if len(regridded_data) < 2:
                logger.error("Failed to regrid enough sources for merging")
                return None

            # Store target coordinates for weighted merge
            self._last_target_extent = target_extent
            if "lons" in target_extent and "lats" in target_extent:
                self._last_target_coords = (
                    target_extent["lons"],
                    target_extent["lats"],
                )

            # Apply merging strategy
            merge_func = self.merge_strategies.get(strategy, self._average_merge)
            merged_data = merge_func(regridded_data)

            # Create merged result
            timestamp = list(timestamp_data.values())[0][0]["timestamp"]

            return {
                "data": merged_data,
                "coordinates": {
                    "lons": target_extent["lons"],
                    "lats": target_extent["lats"],
                },
                "metadata": {
                    "product": "MERGED",
                    "quantity": "DBZH",  # Assume reflectivity for now
                    "timestamp": timestamp,
                    "source": "MULTI",
                    "sources": list(regridded_data.keys()),
                    "merge_strategy": strategy,
                    "units": "dBZ",
                    "nodata_value": np.nan,
                },
                "extent": target_extent,
                "dimensions": target_shape,
                "timestamp": timestamp,
            }

        except Exception as e:
            logger.error(f"Merge failed: {e}")
            return None

    def _compute_target_grid(
        self,
        source_data: dict[str, dict[str, Any]],
        target_resolution: tuple[int, int] | None,
    ) -> tuple[dict[str, Any], tuple[int, int]]:
        """Compute target grid that encompasses all sources"""

        # Find combined extent
        all_extents = [data["extent"]["wgs84"] for data in source_data.values()]

        combined_west = min(extent["west"] for extent in all_extents)
        combined_east = max(extent["east"] for extent in all_extents)
        combined_south = min(extent["south"] for extent in all_extents)
        combined_north = max(extent["north"] for extent in all_extents)

        # Use target resolution or compute from highest resolution source
        if target_resolution:
            target_shape = target_resolution
        else:
            # Use resolution from source with most pixels
            max_pixels = 0
            best_shape = (1000, 1000)  # Default

            for data in source_data.values():
                shape = data["extent"].get("grid_size", [1000, 1000])
                pixels = shape[0] * shape[1]
                if pixels > max_pixels:
                    max_pixels = pixels
                    best_shape = tuple(shape)

            target_shape = best_shape

        # Create coordinate arrays
        lons = np.linspace(combined_west, combined_east, target_shape[1])
        lats = np.linspace(combined_north, combined_south, target_shape[0])

        # Convert to mercator
        x_min, y_min = lonlat_to_mercator(combined_west, combined_south)
        x_max, y_max = lonlat_to_mercator(combined_east, combined_north)

        target_extent = {
            "wgs84": {
                "west": combined_west,
                "east": combined_east,
                "south": combined_south,
                "north": combined_north,
            },
            "mercator": {
                "x_min": x_min,
                "x_max": x_max,
                "y_min": y_min,
                "y_max": y_max,
                "bounds": [x_min, y_min, x_max, y_max],
            },
            "lons": lons,
            "lats": lats,
        }

        return target_extent, target_shape

    def _regrid_to_target(
        self,
        file_data: dict[str, Any],
        target_extent: dict[str, Any],
        target_shape: tuple[int, int],
    ) -> np.ndarray | None:
        """Regrid source data to target grid"""

        try:
            source_data = file_data["data"]
            source_coords = file_data["coordinates"]

            # Use cv2.remap interpolation
            return self._regrid_data(
                source_data, source_coords, target_extent, target_shape
            )

        except Exception as e:
            logger.error(f"Regridding failed: {e}")
            return None

    def _regrid_data(
        self,
        source_data: np.ndarray,
        source_coords: dict[str, np.ndarray],
        target_extent: dict[str, np.ndarray],
        target_shape: tuple[int, int],
    ) -> np.ndarray | None:
        """Regrid source data to target grid using OpenCV remap."""
        logger.info(f"Regridding: {source_data.shape} → {target_shape}")

        # Handle invalid data
        valid_mask = np.isfinite(source_data)
        if not np.any(valid_mask):
            logger.warning("No valid data to regrid")
            return None

        # Get coordinate arrays
        source_lats = source_coords["lats"]
        source_lons = source_coords["lons"]
        target_lats = target_extent["lats"]
        target_lons = target_extent["lons"]

        # Ensure data is in correct orientation (ascending coordinates) only for 1D
        # Note: 2D coordinate arrays (DWD) skip orientation check - projection handles it
        if source_lats.ndim == 1:
            if source_lats[0] > source_lats[-1]:
                source_lats = source_lats[::-1]
                source_data = source_data[::-1, :]

            if source_lons[0] > source_lons[-1]:
                source_lons = source_lons[::-1]
                source_data = source_data[:, ::-1]

        # Create coordinate mapping for cv2.remap
        if source_lats.ndim == 2:
            # For 2D coordinates, use scipy's nearest neighbor search
            # This is slower but necessary for proper projection handling
            from scipy.spatial import cKDTree

            # Create source coordinate pairs
            source_points = np.column_stack(
                [source_lons.flatten(), source_lats.flatten()]
            )

            # Create target coordinate pairs
            target_lons_2d, target_lats_2d = np.meshgrid(target_lons, target_lats)
            target_points = np.column_stack(
                [target_lons_2d.flatten(), target_lats_2d.flatten()]
            )

            # Build KD-tree for fast nearest neighbor search
            tree = cKDTree(source_points)

            # Find nearest neighbors
            distances, indices = tree.query(target_points, k=1)

            # Convert flat indices to 2D indices
            source_shape = source_lats.shape
            source_y_idx = indices // source_shape[1]
            source_x_idx = indices % source_shape[1]

            # Create mapping arrays
            map_y = source_y_idx.reshape(target_shape).astype(np.float32)
            map_x = source_x_idx.reshape(target_shape).astype(np.float32)

        else:
            # For 1D coordinates, use the original linear interpolation
            # target_shape is (height, width) = (lat_count, lon_count)
            map_y, map_x = np.meshgrid(
                np.interp(target_lats, source_lats, np.arange(len(source_lats))),
                np.interp(target_lons, source_lons, np.arange(len(source_lons))),
                indexing="ij",
            )

        # Convert to float32 for cv2
        source_data_f32 = source_data.astype(np.float32)
        map_x_f32 = map_x.astype(np.float32) if map_x.dtype != np.float32 else map_x
        map_y_f32 = map_y.astype(np.float32) if map_y.dtype != np.float32 else map_y

        # Create validity mask before remapping
        valid_mask = np.isfinite(source_data_f32)

        # Use large negative value instead of NaN for border (cv2 doesn't handle NaN properly)
        INVALID_VALUE = -9999.0

        # Use cv2.remap for fast interpolation
        interpolated = cv2.remap(
            source_data_f32,
            map_x_f32,
            map_y_f32,
            interpolation=cv2.INTER_LINEAR,
            borderMode=cv2.BORDER_CONSTANT,
            borderValue=INVALID_VALUE,
        )

        # Also remap the validity mask to track which pixels should be valid
        valid_mask_f32 = valid_mask.astype(np.float32)
        interpolated_mask = cv2.remap(
            valid_mask_f32,
            map_x_f32,
            map_y_f32,
            interpolation=cv2.INTER_NEAREST,  # Use nearest neighbor for mask
            borderMode=cv2.BORDER_CONSTANT,
            borderValue=0.0,
        )

        # Apply proper masking - set pixels to NaN where:
        # 1. They were remapped from invalid source pixels
        # 2. They got the border fill value
        # 3. The interpolated mask indicates invalidity
        final_mask = (
            (interpolated_mask < 0.5)
            | (interpolated <= INVALID_VALUE)
            | (interpolated >= 1000)
        )
        interpolated[final_mask] = np.nan

        # Additional quality control - clip to valid meteorological range
        interpolated = np.clip(interpolated, -35, 85)

        logger.info(
            f"Regridded to shape {target_shape}, "
            f"valid pixels: {np.sum(np.isfinite(interpolated))}"
        )

        return interpolated.astype(np.float64)  # Convert back to expected dtype

    def _average_merge(self, regridded_data: dict[str, np.ndarray]) -> np.ndarray:
        """Improved average merge strategy with quality control and range clipping"""

        logger.info("Applying average merge strategy")

        # Stack all arrays
        arrays = list(regridded_data.values())
        stacked = np.stack(arrays, axis=0)

        # Quality control - remove outliers before averaging
        # Detect pixels where sources disagree too much (likely errors)
        with np.errstate(invalid="ignore", divide="ignore"):
            # Calculate standard deviation across sources for each pixel
            std_values = np.nanstd(stacked, axis=0)
            mean_values = np.nanmean(stacked, axis=0)

            # Identify pixels where standard deviation is suspiciously high
            # (more than 15 dBZ difference between sources indicates possible errors)
            outlier_mask = std_values > 15.0

            # For outlier pixels, use median instead of mean (more robust)
            merged = np.where(outlier_mask, np.nanmedian(stacked, axis=0), mean_values)

        # Post-processing quality control
        # 1. Clip to valid meteorological range for radar reflectivity
        merged = np.clip(merged, -32.0, 80.0)

        # 2. Remove isolated pixels (likely noise)
        # Simple 3x3 majority filter - if a pixel has fewer than 2 valid neighbors, set to NaN
        from scipy import ndimage

        valid_mask = ~np.isnan(merged)
        neighbor_count = ndimage.generic_filter(
            valid_mask.astype(float), np.sum, size=3, mode="constant", cval=0.0
        )
        # Keep pixels that have at least 2 valid neighbors (including themselves)
        isolated_mask = neighbor_count < 3
        merged[isolated_mask] = np.nan

        # 3. Final validation - ensure no extreme values
        extreme_mask = (merged < -35) | (merged > 85)
        merged[extreme_mask] = np.nan

        valid_pixels = np.sum(~np.isnan(merged))
        logger.info(f"Average merge: {valid_pixels} valid pixels")

        return merged

    def _priority_merge(self, regridded_data: dict[str, np.ndarray]) -> np.ndarray:
        """Priority merge - use first source, fill gaps with others"""

        logger.info("Applying priority merge strategy")

        sources = list(regridded_data.keys())
        logger.debug(f"Priority order: {' > '.join(sources)}")

        # Start with first source
        merged = regridded_data[sources[0]].copy()

        # Fill gaps with subsequent sources
        for source in sources[1:]:
            mask = np.isnan(merged)
            merged[mask] = regridded_data[source][mask]

        valid_pixels = np.sum(~np.isnan(merged))
        logger.info(f"Priority merge: {valid_pixels} valid pixels")

        return merged

    def _weighted_merge(self, regridded_data: dict[str, np.ndarray]) -> np.ndarray:
        """Weighted merge based on distance from radar centers and data quality"""

        logger.info("Applying weighted merge strategy")

        # Radar center coordinates (approximate)
        radar_centers = {
            "shmu": (19.15, 48.55),  # Bratislava, Slovakia (approximate)
            "dwd": (10.0, 51.5),  # Central Germany (approximate)
        }

        # Get target grid coordinates
        if hasattr(self, "_last_target_coords"):
            target_lons, target_lats = self._last_target_coords
        else:
            # Fallback - create coordinate grids for target extent
            target_extent = (
                self._last_target_extent
                if hasattr(self, "_last_target_extent")
                else {"west": 2.5, "east": 23.8, "south": 45.5, "north": 56.0}
            )
            target_shape = list(regridded_data.values())[0].shape

            lons = np.linspace(
                target_extent["west"], target_extent["east"], target_shape[1]
            )
            lats = np.linspace(
                target_extent["north"], target_extent["south"], target_shape[0]
            )
            target_lons, target_lats = np.meshgrid(lons, lats)

        # Calculate distance-based weights
        total_weights = np.zeros(target_lons.shape)
        weighted_sum = np.zeros(target_lons.shape)

        for source_name, data in regridded_data.items():
            if source_name in radar_centers:
                radar_lon, radar_lat = radar_centers[source_name]

                # Calculate distance from radar center (in degrees, approximately)
                distance = np.sqrt(
                    (target_lons - radar_lon) ** 2 + (target_lats - radar_lat) ** 2
                )

                # Inverse distance weighting (closer = higher weight)
                # Add small constant to avoid division by zero
                weights = 1.0 / (distance + 0.1)

                # Quality-based adjustment
                # Give higher weight to data that seems more reliable
                valid_mask = np.isfinite(data)

                # Reduce weight for extreme values (likely errors)
                extreme_mask = (data < -30) | (data > 75)
                weights = np.where(extreme_mask, weights * 0.1, weights)

                # Only use weights where data is valid
                masked_weights = np.where(valid_mask, weights, 0.0)
                masked_data = np.where(valid_mask, data, 0.0)

                weighted_sum += masked_data * masked_weights
                total_weights += masked_weights
            else:
                # If no position known, use equal weight
                valid_mask = np.isfinite(data)
                uniform_weight = 1.0

                masked_weights = np.where(valid_mask, uniform_weight, 0.0)
                masked_data = np.where(valid_mask, data, 0.0)

                weighted_sum += masked_data * masked_weights
                total_weights += masked_weights

        # Calculate weighted average
        with np.errstate(invalid="ignore", divide="ignore"):
            merged = np.divide(
                weighted_sum,
                total_weights,
                out=np.full_like(weighted_sum, np.nan),
                where=total_weights > 0,
            )

        # Apply quality control
        merged = np.clip(merged, -32.0, 80.0)

        # Remove isolated pixels
        from scipy import ndimage

        valid_mask = ~np.isnan(merged)
        neighbor_count = ndimage.generic_filter(
            valid_mask.astype(float), np.sum, size=3, mode="constant", cval=0.0
        )
        isolated_mask = neighbor_count < 3
        merged[isolated_mask] = np.nan

        valid_pixels = np.sum(~np.isnan(merged))
        logger.info(f"Weighted merge: {valid_pixels} valid pixels")

        return merged

    def _max_merge(self, regridded_data: dict[str, np.ndarray]) -> np.ndarray:
        """Maximum value merge - take highest reflectivity"""

        logger.info("Applying maximum merge strategy")

        # Stack all arrays
        arrays = list(regridded_data.values())
        stacked = np.stack(arrays, axis=0)

        # Calculate max ignoring NaN values
        with np.errstate(invalid="ignore"):
            merged = np.nanmax(stacked, axis=0)

        valid_pixels = np.sum(~np.isnan(merged))
        logger.info(f"Maximum merge: {valid_pixels} valid pixels")

        return merged
