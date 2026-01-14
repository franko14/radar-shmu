# Changelog

All notable changes to iMeteo Radar project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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