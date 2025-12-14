# iMeteo Radar

Multi-source weather radar data processor for Central Europe. Downloads, processes, and exports high-quality radar images from DWD (Germany), SHMU (Slovakia), and CHMI (Czech Republic).

## Features

- **Three radar sources**: DWD, SHMU, CHMI with 5-minute updates
- **Composite generation**: Merge multiple sources using maximum reflectivity
- **PNG export**: Transparent backgrounds, official SHMU colorscale (-35 to 85 dBZ)
- **Docker ready**: Pre-built image on DockerHub
- **Cloud storage**: Optional upload to S3-compatible storage

## Quick Start

### Python

```bash
pip install -e .

# Fetch latest radar data
imeteo-radar fetch --source dwd
imeteo-radar fetch --source shmu
imeteo-radar fetch --source chmi

# Generate composite from all sources
imeteo-radar composite
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
imeteo-radar composite --sources dwd,shmu

# Generate extent metadata
imeteo-radar extent --source all
```

## Output

- **PNG files**: `{unix_timestamp}.png` with transparency
- **Extent file**: `extent_index.json` for web mapping
- **Directories**:
  - `/tmp/germany/` (DWD)
  - `/tmp/slovakia/` (SHMU)
  - `/tmp/czechia/` (CHMI)
  - `/tmp/composite/` (merged)

## Data Sources

| Source | Product | Coverage | Resolution |
|--------|---------|----------|------------|
| DWD | dmax | Germany | ~1 km |
| SHMU | zmax | Slovakia | ~400 m |
| CHMI | maxz | Czech Republic | ~500 m |

## Documentation

| Document | Description |
|----------|-------------|
| [CLI Reference](docs/cli-reference.md) | Complete command documentation |
| [Deployment](docs/deployment.md) | Docker, Kubernetes, cloud storage |
| [Architecture](docs/architecture.md) | Technical deep-dive |
| [Development](docs/development.md) | Setup and contributing |
| [Monitoring](docs/monitoring.md) | Health checks and troubleshooting |

## Requirements

- Python 3.9+ (or Docker)
- Dependencies: numpy, h5py, matplotlib, requests, PIL, opencv-python

## License

MIT

## Contributing

Contributions welcome! See [docs/development.md](docs/development.md) for setup instructions.
