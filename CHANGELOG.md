# Changelog

All notable changes to iMeteo Radar project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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