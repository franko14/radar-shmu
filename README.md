# iMeteo Radar

Multi-source weather radar data processor for Central Europe. Downloads, processes, and exports high-quality radar images from DWD (Germany), SHMU (Slovakia), CHMI (Czech Republic), OMSZ (Hungary), ARSO (Slovenia), and IMGW (Poland).

## Features

- **Six radar sources**: DWD, SHMU, CHMI, OMSZ, ARSO, IMGW with 5-minute updates
- **Composite generation**: Merge multiple sources using maximum reflectivity
- **Accurate reprojection**: rasterio-based reprojection with three-tier transform cache (10-50x speedup)
- **Coverage masks**: Static coverage mask generation for each source and composite
- **PNG export**: Transparent backgrounds, official SHMU colorscale (-35 to 85 dBZ)
- **Cache-aware fetching**: Skip redundant downloads with dual-layer processed data cache
- **Docker ready**: Pre-built image on DockerHub
- **Cloud storage**: Optional upload to S3-compatible storage

## Quick Start

### Python

```bash
# Install in development mode
pip install -e ".[dev]"

# Fetch latest radar data from individual sources
imeteo-radar fetch --source dwd --output ./outputs/germany
imeteo-radar fetch --source shmu --output ./outputs/slovakia
imeteo-radar fetch --source chmi --output ./outputs/czechia
imeteo-radar fetch --source omsz --output ./outputs/hungary
imeteo-radar fetch --source arso --output ./outputs/slovenia
imeteo-radar fetch --source imgw --output ./outputs/poland

# Generate composite from all sources (default: dwd,shmu,chmi,omsz,arso,imgw)
imeteo-radar composite --output ./outputs/composite

# Generate composite with memory optimization (no individual PNGs)
imeteo-radar composite --no-individual
```

### Docker

```bash
docker pull lfranko/imeteo-radar:latest

docker run --rm -v $(pwd)/outputs:/tmp lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd
```

## Usage Examples

```bash
# Backload last 6 hours
imeteo-radar fetch --source dwd --backload --hours 6

# Specific time range
imeteo-radar fetch --source shmu --backload \
  --from "2024-09-25 10:00" --to "2024-09-25 16:00"

# Custom output directory
imeteo-radar fetch --source dwd --output /data/radar/

# Composite with specific sources
imeteo-radar composite --sources dwd,shmu,chmi --output ./outputs/composite

# Composite with all 6 sources
imeteo-radar composite --sources dwd,shmu,chmi,omsz,arso,imgw

# Composite with custom resolution (default: 500m)
imeteo-radar composite --resolution 250

# Composite with timestamp tolerance (minutes between sources)
imeteo-radar composite --timestamp-tolerance 5

# Memory-optimized composite (skip individual source PNGs)
imeteo-radar composite --no-individual

# Generate extent metadata
imeteo-radar extent --source all

# Generate coverage mask
imeteo-radar coverage-mask --output /tmp/coverage/
```

## Output

- **PNG files**: `{unix_timestamp}.png` with transparency
- **Extent file**: `extent_index.json` for web mapping
- **Coverage mask**: `coverage_mask.png` for each source and composite
- **Directories** (configurable via `--output`):
  - `outputs/germany/` (DWD)
  - `outputs/slovakia/` (SHMU)
  - `outputs/czechia/` (CHMI)
  - `outputs/hungary/` (OMSZ)
  - `outputs/slovenia/` (ARSO)
  - `outputs/poland/` (IMGW)
  - `outputs/composite/` (merged)

## Data Sources

| Source | Product | Coverage | Resolution |
|--------|---------|----------|------------|
| DWD | dmax | Germany | ~1 km |
| SHMU | zmax | Slovakia | ~400 m |
| CHMI | maxz | Czech Republic | ~500 m |
| OMSZ | cmax | Hungary | ~500 m |
| ARSO | zm | Slovenia | ~1 km |
| IMGW | cmax | Poland | ~500 m |

## Documentation

| Document | Description |
|----------|-------------|
| [CLI Reference](docs/cli-reference.md) | Complete command documentation |
| [Data Flow](docs/data-flow.md) | Sequence diagrams and cache flows |
| [Deployment](docs/deployment.md) | Docker, Kubernetes, cloud storage |
| [Architecture](docs/architecture.md) | Technical deep-dive |
| [Development](docs/development.md) | Setup and contributing |
| [Monitoring](docs/monitoring.md) | Health checks and troubleshooting |

## Requirements

- Python 3.9+ (or Docker)
- Dependencies: numpy, scipy, h5py, pyproj, rasterio, matplotlib, requests, PIL, opencv-python, netCDF4

## License

MIT

## Contributing

Contributions welcome! See [docs/development.md](docs/development.md) for setup instructions.
