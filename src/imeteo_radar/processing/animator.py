"""
Radar Data Animation Module

This module creates GIF animations from radar PNG sequences for SHMU, DWD, and merged data sources.
Supports multiple products per source and handles different timestamp formats.
"""

import logging
import re
from datetime import datetime
from pathlib import Path

try:
    from PIL import Image

    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    import sys
    print("Warning: PIL (Pillow) not available. Install with: pip install Pillow", file=sys.stderr)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RadarAnimator:
    """Creates GIF animations from radar PNG sequences."""

    SUPPORTED_SOURCES = ["shmu", "dwd", "merged"]

    # Timestamp patterns for different sources
    TIMESTAMP_PATTERNS = {
        "shmu": r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})",  # YYYYMMDDHHMMSS
        "dwd": r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})",  # YYYYMMDDHHMM
        "merged": r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})",  # YYYYMMDDHHMMSS
    }

    # Product patterns for each source
    PRODUCT_PATTERNS = {
        "shmu": ["zmax", "cappi2km"],
        "dwd": ["dmax"],
        "merged": ["merged"],
    }

    def __init__(self, fps: int = 12, loop: bool = True):
        """
        Initialize the RadarAnimator.

        Args:
            fps: Frames per second for GIF animation (default: 12)
            loop: Whether GIF should loop (default: True)
        """
        if not PIL_AVAILABLE:
            raise ImportError(
                "PIL (Pillow) is required. Install with: pip install Pillow"
            )

        self.fps = fps
        self.frame_duration = int(1000 / fps)  # Duration in milliseconds
        self.loop = loop

    def parse_timestamp(self, filename: str, source: str) -> datetime | None:
        """
        Parse timestamp from filename based on source format.

        Args:
            filename: The filename to parse
            source: The data source ('shmu', 'dwd', 'merged')

        Returns:
            datetime object or None if parsing fails
        """
        pattern = self.TIMESTAMP_PATTERNS.get(source)
        if not pattern:
            logger.error(f"Unknown source: {source}")
            return None

        match = re.search(pattern, filename)
        if not match:
            logger.warning(f"Could not parse timestamp from {filename}")
            return None

        try:
            if source == "dwd":
                # DWD format: YYYYMMDDHHMM
                year, month, day, hour, minute = match.groups()
                return datetime(int(year), int(month), int(day), int(hour), int(minute))
            else:
                # SHMU/Merged format: YYYYMMDDHHMMSS
                year, month, day, hour, minute, second = match.groups()
                return datetime(
                    int(year), int(month), int(day), int(hour), int(minute), int(second)
                )
        except ValueError as e:
            logger.error(f"Invalid timestamp in {filename}: {e}")
            return None

    def find_png_files(
        self, directory: Path, source: str, product: str = None
    ) -> list[tuple[Path, datetime]]:
        """
        Find PNG files for a specific source and optionally product.

        Args:
            directory: Directory to search for PNG files
            source: Data source ('shmu', 'dwd', 'merged')
            product: Specific product to filter (optional)

        Returns:
            List of (filepath, timestamp) tuples sorted by timestamp
        """
        if not directory.exists():
            logger.error(f"Directory does not exist: {directory}")
            return []

        png_files = []

        for file_path in directory.glob("*.png"):
            # Filter by product if specified
            if product and not file_path.name.startswith(product):
                continue

            timestamp = self.parse_timestamp(file_path.name, source)
            if timestamp:
                png_files.append((file_path, timestamp))
            else:
                logger.warning(
                    f"Skipping file with unparseable timestamp: {file_path.name}"
                )

        # Sort by timestamp
        png_files.sort(key=lambda x: x[1])
        return png_files

    def get_time_range_string(self, timestamps: list[datetime]) -> tuple[str, str, str]:
        """
        Get formatted time range strings for animation naming.

        Args:
            timestamps: List of datetime objects

        Returns:
            Tuple of (start_time_str, end_time_str, date_str)
        """
        if not timestamps:
            return "unknown", "unknown", "unknown"

        start_time = min(timestamps)
        end_time = max(timestamps)

        # Format: HHMM for times, YYYYMMDD for date
        start_str = start_time.strftime("%H%M")
        end_str = end_time.strftime("%H%M")
        date_str = start_time.strftime("%Y%m%d")

        return start_str, end_str, date_str

    def create_animation(
        self,
        png_files: list[tuple[Path, datetime]],
        output_path: Path,
        optimize: bool = True,
    ) -> bool:
        """
        Create GIF animation from PNG files.

        Args:
            png_files: List of (filepath, timestamp) tuples
            output_path: Output GIF file path
            optimize: Whether to optimize GIF size (default: True)

        Returns:
            True if successful, False otherwise
        """
        if not png_files:
            logger.error("No PNG files provided for animation")
            return False

        logger.info(f"Creating animation with {len(png_files)} frames")
        logger.info(f"Time range: {png_files[0][1]} to {png_files[-1][1]}")
        logger.info(f"Frame rate: {self.fps} fps ({self.frame_duration}ms per frame)")

        try:
            # Load images
            images = []
            for file_path, _timestamp in png_files:
                try:
                    img = Image.open(file_path)
                    # Convert to RGB if necessary (removes alpha channel for better compression)
                    if img.mode in ("RGBA", "LA", "P"):
                        img = img.convert("RGB")
                    images.append(img)
                    logger.debug(f"Loaded frame: {file_path.name}")
                except Exception as e:
                    logger.error(f"Failed to load image {file_path}: {e}")
                    continue

            if not images:
                logger.error("No images could be loaded")
                return False

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save as GIF animation
            images[0].save(
                output_path,
                save_all=True,
                append_images=images[1:],
                duration=self.frame_duration,
                loop=0 if self.loop else 1,
                optimize=optimize,
            )

            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Animation saved: {output_path}")
            logger.info(f"File size: {file_size_mb:.1f} MB")

            return True

        except Exception as e:
            logger.error(f"Failed to create animation: {e}")
            return False

    def create_source_animation(
        self, source_dir: Path, source: str, output_dir: Path, product: str = None
    ) -> dict[str, bool]:
        """
        Create animations for a specific source, handling multiple products if needed.

        Args:
            source_dir: Directory containing PNG files for the source
            source: Data source name ('shmu', 'dwd', 'merged')
            output_dir: Output directory for animations
            product: Specific product to animate (optional, animates all if None)

        Returns:
            Dictionary mapping animation filenames to success status
        """
        if source not in self.SUPPORTED_SOURCES:
            logger.error(f"Unsupported source: {source}")
            return {}

        results = {}

        # Determine products to animate
        if product:
            products = [product] if product in self.PRODUCT_PATTERNS[source] else []
        else:
            products = self.PRODUCT_PATTERNS[source]

        if not products:
            logger.warning(f"No products found for source {source}")
            # Try to animate all PNG files without product filtering
            products = [None]

        for prod in products:
            logger.info(f"Processing {source} - {prod or 'all products'}")

            # Find PNG files
            png_files = self.find_png_files(source_dir, source, prod)

            if not png_files:
                logger.warning(
                    f"No PNG files found for {source} - {prod or 'all products'}"
                )
                continue

            # Get time range for filename
            timestamps = [ts for _, ts in png_files]
            start_str, end_str, date_str = self.get_time_range_string(timestamps)

            # Generate output filename
            if prod:
                filename = f"{source}_{prod}_{date_str}_{start_str}_{end_str}.gif"
            else:
                filename = f"{source}_{date_str}_{start_str}_{end_str}.gif"

            output_path = output_dir / filename

            # Create animation
            success = self.create_animation(png_files, output_path)
            results[filename] = success

            if success:
                logger.info(f"✅ Created: {filename}")
            else:
                logger.error(f"❌ Failed: {filename}")

        return results

    def create_all_animations(
        self, data_dir: Path, output_dir: Path, sources: list[str] = None
    ) -> dict[str, dict[str, bool]]:
        """
        Create animations for all specified sources.

        Args:
            data_dir: Root directory containing source subdirectories
            output_dir: Output directory for animations
            sources: List of sources to process (default: all supported)

        Returns:
            Nested dictionary: {source: {animation_filename: success_status}}
        """
        if sources is None:
            sources = self.SUPPORTED_SOURCES

        results = {}

        for source in sources:
            if source not in self.SUPPORTED_SOURCES:
                logger.warning(f"Skipping unsupported source: {source}")
                continue

            source_dir = data_dir / source
            if not source_dir.exists():
                logger.warning(f"Source directory not found: {source_dir}")
                continue

            logger.info(f"Creating animations for {source.upper()}", extra={"operation": "animate"})
            source_results = self.create_source_animation(
                source_dir, source, output_dir
            )
            results[source] = source_results

        return results


def main():
    """Example usage of the RadarAnimator."""

    # Configuration
    data_dir = Path("outputs/production/latest_radar_data")
    output_dir = Path("outputs/production/animations")

    # Create animator (12 fps, ~83ms per frame)
    animator = RadarAnimator(fps=12, loop=True)

    # Create all animations
    results = animator.create_all_animations(data_dir, output_dir)

    # Print summary
    logger.info("Animation Summary:")

    total_success = 0
    total_attempts = 0

    for source, source_results in results.items():
        logger.info(f"{source.upper()}:")
        for filename, success in source_results.items():
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"  {filename}: {status}")
            total_attempts += 1
            if success:
                total_success += 1

    logger.info(
        f"Overall: {total_success}/{total_attempts} animations created successfully",
        extra={"count": total_success},
    )


if __name__ == "__main__":
    main()
