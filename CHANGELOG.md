# Changelog

All notable changes to iMeteo Radar project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.6.0] - 2026-02-05

### Added
- **Multi-format export support** with PNG and AVIF output formats
  - New `ExportConfig` dataclass for configuring export options
  - Renamed `PNGExporter` to `MultiFormatExporter` (backward-compatible alias maintained)
  - New `export_variants()` method generates all configured format/resolution combinations
  - AVIF support via Pillow >=10.0.0 with configurable quality (default: 50)

- **Multi-resolution export** for bandwidth-optimized variants
  - Resolution scaling with LANCZOS resampling for high-quality downscaling
  - Configurable target resolutions in meters (e.g., 1000m, 2000m, 3000m)
  - Output naming: `{timestamp}@{resolution}m.{format}` (e.g., `1738675200@1000m.avif`)

- **New CLI flags** for fetch and composite commands
  - `--resolutions`: Comma-separated list (e.g., `full,1000,2000`). Default: `full`
  - `--formats`: Output formats (`png`, `avif`, or both). Default: `png`
  - `--avif-quality`: AVIF quality 1-100. Default: 50 (optimized for radar palette images)

- **Auto content-type detection** in S3 uploader for AVIF files (`image/avif`)

- **S3-first caching for metadata** - Fresh containers automatically load from S3
  - Extent index (`extent_index.json`) - geographic bounds for composite
  - Coverage mask (`coverage_mask.png`) - alpha mask for radar coverage area
  - Transform grids synced bidirectionally (download missing, upload local-only)
  - New utility modules: `utils/extent_loader.py`, `utils/mask_loader.py`
  - Cached singleton `SpacesUploader` to avoid repeated initialization
  - New `download_metadata()` method with atomic temp file downloads

- **Docker-based composite loop** (`scripts/run_composite_docker_loop.sh`)
  - Simulates production pod-like behavior with ephemeral storage
  - Memory limit enforced at 1.5GB to match production constraints
  - Stage bucket safety check prevents accidental production writes
  - Monitors and reports peak memory usage per iteration

### Changed
- Pillow dependency updated to `>=10.0.0` for native AVIF support
- Default AVIF quality set to 50 (provides ~40-55% size reduction vs PNG for radar images)
- **python-dotenv** added as core dependency for .env file loading
- Simplified `.env.example` - removed verbose comments
- Updated `run_composite_loop.sh` defaults - PNG-only, full resolution, `--no-individual`

## [2.5.1] - 2026-02-04

### Changed
- **Modernized type annotations** to Python 3.10+ syntax across core modules
  - `Optional[X]` → `X | None`, `Dict` → `dict`, `List` → `list`, `Tuple` → `tuple`
  - `typing.Callable` → `collections.abc.Callable`

### Fixed
- **Exception chaining**: Added `from e` to all re-raised exceptions across 8 modules
  for proper traceback visibility
- **Warning stacklevel**: Added `stacklevel=2` to all `warnings.warn()` calls in
  `core/projection.py` so warnings point to the caller

### Removed
- Unused imports: `execute_parallel_downloads` (5 sources), `get_crs_web_mercator`,
  `timedelta`, `Optional`, `field`
- Unused variable `available_count` in composite CLI
- Applied Black formatting across all 24 source files

## [2.5.0] - 2026-02-03

### Added
- **Unified reprojection module** (`processing/reprojector.py`)
  - `reproject_to_web_mercator()`: Simple reprojection from extent bounds
  - `reproject_to_web_mercator_accurate()`: Uses calculate_default_transform for GeoTIFF-accurate bounds
  - `build_native_params_from_projection_info()`: Extract CRS/transform from ODIM_H5 metadata
  - Handles stereographic (DWD), mercator (SHMU/CHMI), azimuthal (IMGW), and WGS84 sources

- **Three-tier transform cache** (`processing/transform_cache.py`) for 10-50x faster reprojection
  - Tier 1: Memory (instant, same process)
  - Tier 2: Local disk (/tmp/radar-transforms) - persists in container
  - Tier 3: S3/DO Spaces - persists across pod restarts
  - Precomputes pixel-to-pixel index mappings (radar extents are static)
  - Uses int16 indices (~4 bytes/pixel) for memory efficiency
  - `fast_reproject()` function for numpy array indexing speedup

- **New `transform-cache` CLI command** for cache management
  - `--precompute`: Generate transform grids for sources
  - `--download-s3`: Warm local cache from S3
  - `--upload-s3`: Upload grids to S3 (use with --precompute)
  - `--clear-local` / `--clear-s3`: Clear cache tiers
  - `--stats`: Show cache statistics by tier and source
  - `--source`: Filter operations by source (default: all)

- **Composite loop helper script** (`scripts/run_composite_loop.sh`)
  - Runs composite generation every 2 minutes indefinitely (Ctrl+C to stop)
  - Useful for testing continuous operation and cache behavior

- **Projections utility module** (`core/projections.py`)
  - Centralized CRS constants and factory functions
  - `get_crs_web_mercator()`, `get_crs_wgs84()`, proj4 string constants
  - Eliminates duplicated CRS initialization across modules

### Changed
- **Compositor refactored** to use rasterio.warp.reproject instead of scipy interpolation
  - Fixes positioning errors from incorrect coordinate system handling
  - Proper handling of HDF5 pixel-center coordinates
  - Lazy-initialize CRS objects using proj4 strings

- **PNG exporter** now supports reprojection with caching
  - New `reproject` parameter (default True) enables Web Mercator reprojection
  - New `use_cached_transform` parameter (default True) uses precomputed grids
  - Returns reprojected WGS84 bounds in metadata for Leaflet overlays

- **All 6 sources updated** with projection info and accurate extents
  - Replace coordinates-based approach with projection info dictionaries
  - Extents now match validation GeoTIFFs exactly
  - DWD, SHMU, CHMI, OMSZ, IMGW, ARSO all updated

- **Fetch and composite commands** now enable reprojection for individual source exports
  - Pass radar_data with projection info to exporter
  - extent_index.json uses REPROJECTED bounds (matches PNG exactly)

- **Individual source output directories** are now siblings of composite directory
  - If composite is `./outputs/composite/`, DWD goes to `./outputs/germany/`, etc.

### Fixed
- **Coverage mask alignment**: Rewritten to reproject directly into target grid
  defined by extent_index.json bounds + data PNG dimensions (no intermediate grid)
  - Replace `_reproject_coverage_to_mercator` with `_reproject_coverage_to_target`
  - Load extent from extent_index.json instead of hardcoded SOURCE_EXTENTS
  - Composite mask extent computed as union of all source extents
  - Composite mask loads individual mask PNGs instead of re-downloading
- **OMSZ 32.5 dBZ pixels**: Recovered pixels masked by netCDF4 default `_FillValue`
  by disabling auto-masking and viewing int8 as uint8
- **Transform cache security**: Address critical security issues in transform cache

### Removed
- Dead code: `NODATA_VALUES` dict, `_resize_coverage_to_target` function

### Documentation
- Updated CLI examples with explicit --output paths
- Documented output directory structure

## [2.4.0] - 2026-01-29

### Added
- **Cache-aware fetching for single source commands** (`fetch` command)
  - New `--reprocess-count` argument (default: 6 = 30 minutes)
  - Full cache integration: `--cache-dir`, `--cache-ttl`, `--no-cache`, `--clear-cache`, `--no-cache-upload`
  - Multi-timestamp fetching instead of single latest timestamp
  - Skip redundant downloads (cached timestamps) and uploads (S3 existence check)
  - Mirrors the robust pattern already implemented in composite command

- **Shared CLI helpers module** (`utils/cli_helpers.py`)
  - `init_cache_from_args()`: Consistent cache initialization for both fetch and composite
  - `output_exists()`: Check local and S3 existence to avoid redundant processing/uploads

### Changed
- **Fetch command default behavior**: Now fetches 6 recent timestamps instead of just 1
  - Handles irregular provider uploads (data may be missing from latest)
  - Uses same `--reprocess-count` default (6 = 30 minutes) as composite command

- **Composite command refactored**: Uses shared CLI helpers for cache initialization and S3 checks

## [2.3.0] - 2026-01-28

### Added
- **Cache-aware downloading**: Skip downloads for timestamps already in cache
  - Query providers for available timestamps without downloading
  - Check cache before deciding what to download
  - ~90% reduction in redundant downloads per run
  - Enhanced logging: "DWD: 8 available, 7 in cache, 1 to download"

- **Dual-layer processed data cache** (`ProcessedDataCache`)
  - Layer 1: Local filesystem (/tmp/iradar-data) - fast, ephemeral
  - Layer 2: S3/DO Spaces - persistent across pod restarts
  - TTL-based expiration (60 minutes default)
  - Automatic cleanup of expired entries

- **S3 composite existence check**: Prevent regenerating composites after pod restart
  - Check S3 before processing if local file doesn't exist
  - `file_exists()` method in SpacesUploader

- **Skip reason tracking**: See why timestamps are excluded from processing
  - Categories: already_exists, insufficient_sources, processing_failed
  - Enhanced summary logging with exact skip reasons

- **Shared utility modules** for code deduplication:
  - `timestamps.py`: Timestamp generation, normalization, cache checking
  - `hdf5_utils.py`: Common HDF5 processing functions
  - `parallel_download.py`: Parallel download execution

### Changed
- **Refactored all 6 source implementations** to use cache-aware pattern
  - New `get_available_timestamps()` method (query without download)
  - New `download_timestamps()` method (download specific timestamps)
  - ARSO special handling (only provides latest timestamp)
  - ~150 lines of duplicate code removed

### Documentation
- Added cache-aware downloading mermaid diagrams
- Documented all 6 sources with correct specifications
- Added cache troubleshooting guide
- Added S3 composite existence check documentation

## [2.2.1] - 2026-01-28

### Added
- **Poland (IMGW) extent integration** in mask generation and extent metadata
  - IMGW geographic bounds now included in coverage mask calculations
  - Combined extent generation now includes Polish radar coverage

### Changed
- **Logging standardization**: Replaced 92+ print statements with structured logging across 26 files
  - 355 total logger statements (159 info, 68 debug, 56 warning, 66 error)
  - Standardized operation terminology: Finding, Checking, Downloading, Processing, Saving
  - Consistent source identification via `extra={"source": ...}`
  - New modules: `core/logging.py`, `core/alerts.py`, `core/retry.py`
- **Extent command output location**: Combined extent file now saves to `composite/extent_index.json` instead of `/tmp/` for better organization

## [2.2.0] - 2026-01-27

### Added
- **IMGW (Poland) radar data source**: 6th supported radar source
  - Data from danepubliczne.imgw.pl public API (no authentication required)
  - ODIM_H5 format with CMAX product (Column Maximum reflectivity)
  - 5-minute update intervals
  - Poland coverage: 13.0°-26.4°E, 48.1°-56.2°N
  - HEAD request availability checking (API listing lags behind file availability)
- CLI support: `imeteo-radar fetch --source imgw`
- Added IMGW to default composite sources
- Poland output directory: `/tmp/poland/`
- 54 unit tests covering all IMGW functionality

### Changed
- Registered IMGW in `SOURCE_REGISTRY` with 'poland' country/folder
- Updated extent and coverage-mask commands to include IMGW

## [2.1.1] - 2026-01-22

### Changed
- **Memory optimization**: Reduced peak memory usage by ~50%
  - Compositor: Tile-based processing (1000×1000 pixels) instead of full grid
  - Compositor: On-demand target point generation (saves 169 MB)
  - Projection: Return 1D coordinate arrays instead of 2D meshgrid (saves ~300 MB)
  - CLI: Add gc.collect() after individual PNG exports
  - Peak memory: ~1700 MB → ~850 MB (with `--no-individual` flag)

### Technical
- Modernized type hints: `Dict` → `dict`, `List` → `list`, `Optional[X]` → `X | None`
- Added ruff linter configuration (replaces flake8/isort/black)
- Added psutil to profiling optional dependencies
- Updated README with all 5 radar sources and new CLI options

## [2.1.0] - 2026-01-15

### Added
- **Coverage mask generation** (`coverage-mask` CLI command)
  - Generate static coverage masks for each radar source
  - Generate composite coverage mask combining all sources
  - Masks match exact dimensions of radar PNG output
  - Transparent = covered area, Gray = outside radar range
- New `src/imeteo_radar/processing/coverage_mask.py` module
  - Supports all radar sources: DWD, SHMU, CHMI, ARSO, OMSZ
  - Automatic dimension detection from existing radar PNG files
  - Web Mercator reprojection for composite mask generation

## [2.0.0] - 2026-01-14

### Changed
- **Major refactoring**: Centralized source configuration with new `SOURCE_REGISTRY`
  - New `src/imeteo_radar/config/sources.py` provides single source of truth
  - All 5 sources (DWD, SHMU, CHMI, ARSO, OMSZ) configured in one place
  - Dynamic source instantiation via `get_source_instance()`
  - Eliminates ~150 lines of duplicated configuration across CLI modules
- Moved `cleanup_temp_files()` method to `RadarSource` base class
  - Removes ~75 lines of identical code from DWD, SHMU, CHMI, ARSO sources
  - OMSZ retains custom override for directory cleanup logic
- Simplified `fetch_command()` in CLI from if/elif chains to registry lookup
- Simplified `extent_command()` to use registry-based iteration
- Consolidated `spaces_uploader.py` source folder mapping using registry

### Removed
- Dead commented precipitation colormap code from `exporter.py` (~30 lines)
- Duplicated source configuration mappings from `cli.py`, `cli_composite.py`

### Technical
- Total code reduction: ~335 lines of duplicated/dead code removed
- No functional changes to output - all existing behavior preserved
- All tests passing, code formatted with black/isort

## [1.5.0] - 2026-01-14

### Added
- Timestamp tolerance matching for composite generation
  - New `--timestamp-tolerance` argument (default: 2 minutes)
  - Finds data within time window instead of requiring exact timestamp matches
  - Better handling of sources with different update intervals
- ARSO fallback logic for composite command
  - New `--require-arso` flag to control fallback behavior
  - Auto-excludes ARSO when no timestamp match (ARSO only provides latest)
  - ARSO excluded from backload mode (no historical data available)
- Individual export paths for new sources
  - `/tmp/slovenia/` for ARSO radar images
  - `/tmp/hungary/` for OMSZ radar images
- OMSZ and ARSO added to default composite sources

### Fixed
- OMSZ timestamp format normalization (`YYYYMMDD_HHMM` → `YYYYMMDDHHMM00`)
- OMSZ nodata handling: raw value 0 (grey coverage mask) now transparent

## [1.4.0] - 2026-01-09

### Added
- OMSZ (Hungarian Meteorological Service) radar source support
  - `cmax` product (Column Maximum / ZMAX equivalent)
  - Coverage: Hungary + surroundings (13.5°-25.5°E, 44.0°-50.5°N)
  - Data from odp.met.hu open data portal (no authentication required)
  - netCDF format with int8→uint8 conversion for correct dBZ scaling
  - 5-minute update intervals, ~1km resolution (813×961 grid)
- CLI support: `imeteo-radar fetch --source omsz`
- netCDF4 dependency for OMSZ data processing

## [1.3.0] - 2026-01-09

### Added
- ARSO (Slovenian Environment Agency) radar source support
  - `zm` product (maximum reflectivity)
  - `rrg` product (ground rain rate)
  - Coverage: Slovenia (12.1°-17.4°E, 44.7°-47.4°N)
  - Uses proprietary SRD-3 format with custom parser
  - Lambert Conformal Conic projection support via pyproj
- CLI support: `imeteo-radar fetch --source arso`

## [1.2.2] - 2026-01-08

### Fixed
- CHMI source now correctly uploads to `czechia/` folder instead of `chmi/` in Spaces uploader

## [1.2.1] - 2026-01-05

### Added
- Individual source image export during composite generation
  - Each source (DWD, SHMU, CHMI) now exports to its native grid alongside the composite
  - Output directories: `/tmp/germany/`, `/tmp/slovakia/`, `/tmp/czechia/`
  - Same timestamp-based filenames as composite (`{unix_timestamp}.png`)
- `--no-individual` CLI flag for composite command to skip individual source images

## [1.2.0] - 2025-12-14

### Added
- CHMI (Czech Hydrometeorological Institute) radar source support
  - `maxz` product (maximum reflectivity)
  - Coverage: Czech Republic (12.0°-19.0°E, 48.5°-51.1°N)
- Composite radar generation from multiple sources
  - New `composite` CLI command: `imeteo-radar composite`
  - Merges DWD, SHMU, and CHMI using maximum reflectivity strategy
  - Reprojection to common Web Mercator grid
  - Configurable resolution (default: 500m)
- Radar data availability monitoring tools

### Changed
- Vectorized coordinate transformations (100x speedup)
- Updated Dockerfile for CHMI and composite support

### Fixed
- CHMI composite scaling, interpolation, and timestamp synchronization
- Output format consistency across sources

## [1.1.0] - 2025-11-20

### Added
- Time range filtering for backload functionality
  - `--from` and `--to` arguments for specific time ranges
  - Example: `--from "2024-09-25 10:00" --to "2024-09-25 16:00"`
- Docker deployment with automated CI/CD
  - GitHub Actions workflow for multi-platform builds
  - DockerHub integration (`lfranko/imeteo-radar`)
  - Docker Compose configuration for production

### Changed
- Memory optimization: reduced from 4.8GB to 669MB (86% improvement)
- Optimized PNG file size with indexed palette compression

### Fixed
- Docker workflow auth issues on PRs
- Various CLI bugs

## [1.0.0] - 2025-09-25

### Added
- Initial release with full radar data processing capabilities
- DWD (German Weather Service) radar source support
  - dmax product (maximum reflectivity composite)
  - LATEST endpoint for real-time data
  - Backload support for historical data
- SHMU (Slovak Hydrometeorological Institute) radar source support
  - zmax product (maximum reflectivity)
  - Real-time and historical data fetching
- CLI interface with `imeteo-radar` command
  - `fetch` command for downloading radar data
  - `extent` command for generating geographic extent information
  - Backload options with `--hours` or `--from/--to` time ranges
- High-quality PNG export
  - Transparent backgrounds for no-data areas
  - Official SHMU colorscale (-35 to 85 dBZ)
  - Proper nodata value handling (255 for uint8 data)
- Colorbar generation script
  - Multiple formats (vertical, horizontal)
  - Web-optimized variants (mobile, retina)
  - Transparent backgrounds for overlay use
- Geographic extent metadata
  - Automatic extent_index.json generation
  - WGS84 and Web Mercator coordinates
  - Grid size and resolution information

### Changed
- Renamed project from `radar-shmu` to `imeteo-radar`
- Switched from permanent storage to temporary file processing
  - Uses Python's tempfile module
  - No permanent raw HDF5 file storage
  - Automatic cleanup of temporary files
- Improved nodata handling
  - Fixed SHMU pink background issue
  - Proper transparency for values below minimum threshold
  - Correct handling of uint8 max value (255) as nodata

### Fixed
- CLI installation issues with proper entry points
- datetime.now() import error in DWD source
- PNGExporter title parameter error
- SHMU nodata visualization (pink areas now transparent)
- Timestamp extraction from LATEST files for accurate naming

### Technical Details
- Python 3.9+ support
- Dependencies: h5py, numpy, matplotlib, requests, pytz
- Projections: Stereographic (DWD), Web Mercator (SHMU)
- Data format: ODIM_H5 compliant radar data
- Output format: PNG with alpha channel for transparency

## [0.1.0] - 2025-09-24 (Pre-release)

### Added
- Initial project structure
- Basic radar data processing for SHMU
- HDF5 file handling
- Time-partitioned storage system