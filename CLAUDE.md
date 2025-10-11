# Claude Code Configuration

## Project Overview

iMeteo Radar is a multi-source weather radar data processing system that handles both DWD (German Weather Service) and SHMU (Slovak Hydrometeorological Institute) radar data. The system downloads ODIM_H5 format radar data, processes it with proper colorscaling and transparency, and exports high-quality PNG images suitable for web mapping applications.

## 🔧 Development Setup

### Prerequisites

```bash
python >= 3.9
```

### Installation

```bash
# Clone repository
git clone https://github.com/imeteo/imeteo-radar.git
cd imeteo-radar

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install -e ".[dev]"
```

### Project Structure

```
imeteo-radar/
├── src/imeteo_radar/        # Main package
│   ├── cli.py              # Command-line interface
│   ├── sources/            # Data source handlers
│   │   ├── dwd.py         # DWD radar source (tempfile-based)
│   │   └── shmu.py        # SHMU radar source (tempfile-based)
│   ├── processing/         # Data processing modules
│   │   └── exporter.py    # PNG export with transparency
│   ├── core/              # Core functionality
│   ├── config/            # Configuration
│   │   └── shmu_colormap.py  # Official SHMU colorscale
│   └── utils/             # Utilities
├── scripts/               # Utility scripts
│   └── generate_colorbar.py  # Colorbar generation
└── outputs/               # Generated files (PNG, JSON)
```

## ⚡ CLI Commands

The project provides the `imeteo-radar` CLI executable:

### Fetch Data
```bash
# Download latest DWD dmax data
imeteo-radar fetch --source dwd

# Download latest SHMU zmax data
imeteo-radar fetch --source shmu

# Download last 6 hours of data
imeteo-radar fetch --source dwd --backload --hours 6

# Download specific time range
imeteo-radar fetch --source shmu --backload --from "2024-09-25 10:00" --to "2024-09-25 16:00"

# Custom output directory
imeteo-radar fetch --source dwd --output /data/radar/

# Force update extent information
imeteo-radar fetch --source shmu --update-extent
```

### Generate Extent Information
```bash
# Generate extent files for all sources
imeteo-radar extent --source all

# Generate extent for specific source
imeteo-radar extent --source dwd
```

### Key Features
- **DWD Support**:
  - Uses LATEST endpoint for real-time data
  - dmax product (maximum reflectivity composite)
  - Coverage: Germany and surrounding areas
- **SHMU Support**:
  - zmax product (maximum reflectivity)
  - Coverage: Slovakia and surrounding areas
- **Data Processing**:
  - Temporary file processing (no permanent raw storage)
  - Transparent backgrounds for no-data areas
  - Official SHMU colorscale (-35 to 85 dBZ)
  - Proper nodata handling (255 for uint8)
- **Output**:
  - PNG format: `YYYY-MM-dd_HHMM.png`
  - Default: `/tmp/germany/` (DWD), `/tmp/slovakia/` (SHMU)
  - Automatic extent_index.json generation

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test
pytest tests/test_dwd.py
```

## 📊 Data Processing Pipeline

1. **Download**: Fetch HDF5 files from DWD/SHMU APIs to temp files
2. **Process**: Convert HDF5 to arrays with proper scaling
3. **Export**: Generate PNG images with transparency
4. **Cleanup**: Automatic removal of temporary HDF5 files

## 🎨 Visualization

- **Colormap**: Official SHMU colorscale (-35 to 85 dBZ)
- **PNG Export**: High-quality images with alpha channel
- **Transparency**: No-data areas are fully transparent
- **Colorbar Generation**: Standalone colorbars for web overlays
  ```bash
  python scripts/generate_colorbar.py --generate-all
  ```

## ⚠️ Important Notes

- **Data Format**: ODIM_H5 compliant HDF5 files
- **Time Resolution**: 5-minute intervals
- **Temporary Files**: Uses system temp directory, auto-cleanup
- **No Permanent Storage**: Raw HDF5 files are not kept
- **Projections**:
  - DWD: Stereographic projection
  - SHMU: Web Mercator (EPSG:3857)

## 🔍 Code Quality

The project uses strict code quality tools:
- **Black**: Code formatting (88 char line length)
- **isort**: Import sorting
- **mypy**: Static type checking
- **flake8**: Linting
- **pytest**: Testing

Run quality checks:
```bash
black src/
isort src/
mypy src/
flake8 src/
```

## 📦 Package Configuration

Defined in `pyproject.toml`:
- Package name: `imeteo-radar`
- Core dependencies: numpy, h5py, matplotlib, requests, PIL, opencv-python
- CLI entry point: `imeteo-radar`
- Version: 1.0.0

## 🚀 Production Deployment

For production use:
1. Install package: `pip install .`
2. Set up automated data downloads via cron/scheduler
3. Configure output directories with sufficient space
4. Note: Raw data uses temp files only, no cleanup needed

### Example Cron Setup
```bash
# Fetch latest DWD radar data every 5 minutes
*/5 * * * * imeteo-radar fetch --source dwd >> /var/log/radar-dwd.log 2>&1

# Fetch latest SHMU radar data every 5 minutes
*/5 * * * * imeteo-radar fetch --source shmu >> /var/log/radar-shmu.log 2>&1

# Daily backload of previous day (both sources)
0 1 * * * imeteo-radar fetch --source dwd --backload --hours 24 >> /var/log/radar-backload.log 2>&1
0 1 * * * imeteo-radar fetch --source shmu --backload --hours 24 >> /var/log/radar-backload.log 2>&1
```

## 🐳 Docker Deployment

### Pre-built Docker Image

The project is available as a pre-built Docker image on DockerHub: **`lfranko/imeteo-radar`**

**Quick start:**
```bash
# Pull the latest image
docker pull lfranko/imeteo-radar:latest

# Run a command
docker run --rm -v $(pwd)/outputs:/tmp lfranko/imeteo-radar:latest imeteo-radar fetch --source dwd
```

### Publishing to DockerHub

#### Manual Publishing (Local Development)

Use the provided script to build and push:
```bash
# Build and push to DockerHub
./scripts/docker-push.sh
```

This script will:
- Extract version from `pyproject.toml`
- Build the Docker image
- Tag with both `latest` and version-specific tags (e.g., `1.0.0`)
- Push to `lfranko/imeteo-radar` on DockerHub

**Prerequisites:**
- Docker Desktop running with logged-in account
- DockerHub repository created: `lfranko/imeteo-radar`

#### Automated CI/CD (GitHub Actions)

The repository includes a GitHub Actions workflow (`.github/workflows/docker-publish.yml`) that automatically:
- Builds on every push and pull request
- Pushes to DockerHub on pushes to `main` branch with `latest` tag
- Creates version-specific tags when git tags are pushed (e.g., `v1.0.0` → `1.0.0`)

**Setup GitHub secrets:**
1. Go to repository Settings → Secrets and variables → Actions
2. Add secrets:
   - `DOCKERHUB_USERNAME`: Your DockerHub username
   - `DOCKERHUB_TOKEN`: DockerHub access token (generate at hub.docker.com)

**Trigger automated builds:**
```bash
# Push to main triggers latest build
git push origin main

# Create version tag triggers versioned build
git tag v1.0.1
git push origin v1.0.1
```

### Team Usage

Team members can use the pre-built image without building locally. See **[DOCKER_DEPLOYMENT.md](DOCKER_DEPLOYMENT.md)** for comprehensive guide including:
- Quick start instructions
- Docker Compose examples
- Production deployment options
- Environment variable configuration
- Troubleshooting guide

**Simple team onboarding:**
```bash
# Team members only need Docker installed
docker pull lfranko/imeteo-radar:latest
docker run --rm lfranko/imeteo-radar:latest imeteo-radar --help
```

No Python, no dependencies, no build required! 🚀

## 🔗 Integration Examples

### Python API
```python
from imeteo_radar.sources.dwd import DWDRadarSource
from imeteo_radar.sources.shmu import SHMURadarSource
from imeteo_radar.processing.exporter import PNGExporter

# Initialize sources
dwd_source = DWDRadarSource()
shmu_source = SHMURadarSource()
exporter = PNGExporter()

# Download latest DWD
dwd_files = dwd_source.download_latest(count=1, products=['dmax'])

# Download latest SHMU
shmu_files = shmu_source.download_latest(count=1, products=['zmax'])

# Process and export
for file_info in dwd_files:
    data = dwd_source.process_to_array(file_info['path'])
    exporter.export_png(data, 'output.png')
```

### Web Integration
The exported PNG files can be directly used in web applications:
- Files are named with timestamps: `2024-09-25_14:30.png`
- Can be served as static files or overlays on web maps
- Compatible with Leaflet, OpenLayers, and other mapping libraries