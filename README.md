# iMeteo Radar

Weather radar data processor for DWD (German Weather Service) and SHMU (Slovak Hydrometeorological Institute) radar data. Provides high-quality radar imagery with transparent backgrounds and proper colorscaling.

## 🚀 Quick Start

```bash
# Install
pip install -e .

# Fetch latest DWD radar data (Germany)
imeteo-radar fetch --source dwd

# Fetch latest SHMU radar data (Slovakia)
imeteo-radar fetch --source shmu

# Fetch last 6 hours of data
imeteo-radar fetch --source dwd --backload --hours 6

# Specify custom output directory
imeteo-radar fetch --source shmu --output /path/to/output/

# Generate extent information
imeteo-radar extent --source all
```

## 📦 Installation

```bash
# Clone repository
git clone https://github.com/imeteo/imeteo-radar.git
cd imeteo-radar

# Install in editable mode (for development)
pip install -e .

# Or install directly
pip install .
```

## 🎯 Features

- **Multi-source support**: DWD (Germany) and SHMU (Slovakia)
- **Products**: DWD dmax and SHMU zmax (maximum reflectivity)
- **PNG export**: High-quality PNG images with transparent backgrounds
- **Proper nodata handling**: Transparent areas for no-data regions
- **SHMU colormap**: Official SHMU colorscale for consistent visualization
- **Backload support**: Fetch historical data with flexible time ranges
- **LATEST endpoint**: Optimized endpoint for most recent DWD data
- **Temporary file processing**: Uses system temp files, no permanent raw data storage
- **Extent information**: Automatic geographic extent metadata generation
- **Colorbar generation**: Standalone colorbars for web overlay use

## 📡 CLI Usage

### Generate Extent Information

```bash
# Generate extent information for all sources
imeteo-radar extent --source all

# Generate extent for specific source
imeteo-radar extent --source dwd

# Custom output directory
imeteo-radar extent --source shmu --output /data/extents/
```

Extent files are automatically created on first fetch in `/tmp/{country}/extent_index.json`

### Fetch Latest Data

```bash
# Download latest DWD radar data (Germany)
imeteo-radar fetch --source dwd
# Output: /tmp/germany/2024-09-25_1430.png

# Download latest SHMU radar data (Slovakia)
imeteo-radar fetch --source shmu
# Output: /tmp/slovakia/2024-09-25_1430.png
```

### Backload Historical Data

```bash
# Last 6 hours for DWD
imeteo-radar fetch --source dwd --backload --hours 6

# Last 3 hours for SHMU
imeteo-radar fetch --source shmu --backload --hours 3

# Specific time range
imeteo-radar fetch --source dwd --backload --from "2024-09-25 10:00" --to "2024-09-25 16:00"

# Custom output directory
imeteo-radar fetch --source shmu --output /data/radar/ --backload --hours 12

# Force update extent information
imeteo-radar fetch --source dwd --update-extent
```

## 🎨 Generate Colorbars

```bash
# Generate single colorbar
python scripts/generate_colorbar.py --output colorbar.png

# Generate all web variants (vertical, horizontal, mobile, retina)
python scripts/generate_colorbar.py --generate-all

# Custom colorbar
python scripts/generate_colorbar.py \
  --orientation horizontal \
  --width 5 --height 0.8 \
  --dpi 200
```

## 📁 Project Structure

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
├── pyproject.toml          # Package configuration
└── README.md              # This file
```

## 🛠️ Development

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Code formatting
black src/
isort src/
```

## 📊 Data Format

### Output Files

- **PNG Images**: `YYYY-MM-dd_HHMM.png` format with transparent backgrounds
- **Extent JSON**: `extent_index.json` with geographic bounds and projection info
- **Temporary Processing**: HDF5 files processed via tempfile, no permanent storage

### Data Specifications

- **DWD dmax**: Maximum reflectivity composite (Germany)
  - Coverage: ~45.7°N to 56.2°N, 1.5°E to 18.7°E
  - Resolution: 4800×4400 pixels
  - Projection: Stereographic

- **SHMU zmax**: Maximum reflectivity composite (Slovakia)
  - Coverage: 46.0°N to 50.7°N, 13.6°E to 23.8°E
  - Resolution: 1560×2270 pixels
  - Projection: Web Mercator (EPSG:3857)

- **Update Frequency**: 5-minute intervals
- **Colormap**: Official SHMU colorscale (-35 to 85 dBZ)
- **Nodata Handling**: Transparent pixels for areas without radar coverage

## 🔗 Data Sources

- **DWD OpenData**: https://opendata.dwd.de/weather/radar/composite/
- **SHMU OpenData**: https://opendata.shmu.sk/

## 📝 License

MIT

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.