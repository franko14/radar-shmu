# Claude Code Configuration

## Project Overview

This is a multi-source weather radar data processing system that handles SHMU (Slovak) and DWD (German) radar data. The system provides a complete pipeline from raw HDF5 radar data to JavaScript-ready JSON formats for web applications.

## 🔧 Development Setup

### Prerequisites

```bash
python >= 3.9
```

### Installation

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install -e ".[dev,performance]"
```

### Project Structure

```
radar-shmu/
├── src/radar_shmu/           # Main package
│   ├── cli.py               # Command-line interface
│   ├── sources/             # Data source handlers (SHMU, DWD)
│   ├── processing/          # Data processing modules
│   ├── core/                # Core functionality
│   ├── utils/               # Utilities and storage
│   └── config/              # Configuration and colormaps
├── scripts/                 # Production scripts
├── tests/                   # Test suite
├── config/                  # Configuration files
└── storage/                 # Time-partitioned data storage
```

## ⚡ CLI Commands

The project provides several CLI commands through the `radar-processor` executable:

### Download Data
```bash
# Download SHMU data (5 recent timestamps)
radar-processor download --sources shmu --count 5

# Download from multiple sources
radar-processor download --sources shmu dwd --count 3 --output outputs/
```

### Merge Data Sources
```bash
# Merge SHMU and DWD data for matching timestamps
radar-processor merge --output storage/merged/
```

### Create Animations
```bash
# Generate animated GIFs from time series data
radar-processor animate --input storage/merged/ --output animations/
```

## 📝 Production Scripts

Located in `scripts/` directory:

- `download_matching_data.py` - Download synchronized data from multiple sources
- `create_production_merged.py` - Generate production-ready merged datasets
- `generate_animations.py` - Create time-lapse animations
- `generate_production_indexes.py` - Build metadata indexes

### Example Usage
```bash
# Download matched data
PYTHONPATH=src ./.venv/bin/python scripts/download_matching_data.py --count 10 --output outputs/

# Create production merged data
PYTHONPATH=src ./.venv/bin/python scripts/create_production_merged.py
```

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test
PYTHONPATH=src ./.venv/bin/python tests/test_colormap.py
```

## 📊 Data Processing Pipeline

1. **Download**: Fetch HDF5 files from SHMU/DWD APIs
2. **Process**: Convert HDF5 to structured arrays with proper scaling
3. **Merge**: Combine multi-source data for overlapping regions
4. **Export**: Generate JSON/visualization outputs
5. **Store**: Time-partitioned storage system for efficient access

## 🎨 Visualization

The system includes built-in colormaps and visualization tools:
- SHMU standard colormap for reflectivity data
- Automatic precipitation rate estimation (Z-R relationship)
- PNG export with proper scaling and legends

## ⚠️ Important Notes

- Large file sizes: JSON outputs can be ~90MB each
- Memory usage: Processing requires significant RAM for large datasets
- API limits: Respect source API rate limits during downloads
- Coordinate systems: Data uses Mercator projection (+proj=merc +lon_0=18.7)

## 🔍 Code Quality

The project uses strict code quality tools:
- **Black**: Code formatting (88 char line length)
- **isort**: Import sorting
- **mypy**: Static type checking (strict mode)
- **flake8**: Linting
- **pytest**: Testing with 80% coverage requirement

Run quality checks:
```bash
black src/ tests/
isort src/ tests/
mypy src/
flake8 src/ tests/
```

## 📦 Package Configuration

Defined in `pyproject.toml`:
- Core dependencies: numpy, h5py, matplotlib, requests
- Optional performance: numba for acceleration
- Development tools: pytest, black, mypy, etc.
- CLI entry points: `radar-processor`, `radar-download`, etc.

## 🚀 Production Deployment

For production use:
1. Install with performance dependencies: `pip install ".[performance]"`
2. Configure time-partitioned storage (see STORAGE_STRATEGY.md)
3. Set up automated data downloads via cron/scheduler
4. Monitor disk usage and implement cleanup policies
5. Consider data compression (gzip) for web delivery

## 🔗 Integration Examples

### Python API
```python
from radar_shmu.sources import SHMUSource
from radar_shmu.processing import RadarProcessor

source = SHMUSource()
processor = RadarProcessor()

# Download and process
data = source.download_latest()
processed = processor.process(data)
```

### JavaScript/Web
```javascript
// Load processed JSON data
fetch('outputs/radar_merged_20250909120000.json')
  .then(response => response.json())
  .then(data => {
    // Use data.reflectivity, data.coordinates, etc.
  });
```