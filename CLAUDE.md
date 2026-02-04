# Claude Code Configuration

## Project Overview

iMeteo Radar is a multi-source weather radar data processing system that handles DWD (Germany), SHMU (Slovakia), CHMI (Czech Republic), OMSZ (Hungary), ARSO (Slovenia), and IMGW (Poland) radar data. The system downloads ODIM_H5/netCDF/SRD-3 format radar data, reprojects it to Web Mercator using rasterio, and exports PNG images suitable for web mapping applications. Supports composite radar images that merge data from multiple sources, with three-tier transform caching for fast reprojection.

## Project Structure

```mermaid
graph LR
    subgraph root["imeteo-radar/"]
        subgraph src["src/imeteo_radar/"]
            CLI["cli.py"]
            CLIC["cli_composite.py"]
            SRC["sources/"]
            PROC["processing/"]
            CORE["core/"]
            CONF["config/"]
            UTIL["utils/"]
        end
        SCRIPTS["scripts/"]
        TESTS["tests/"]
        DOCS["docs/"]
        OUT["outputs/"]
    end
```

## Key Commands

```bash
# Fetch from single source (saves to outputs/{source}/)
imeteo-radar fetch --source dwd --output ./outputs/germany
imeteo-radar fetch --source shmu --output ./outputs/slovakia
imeteo-radar fetch --source chmi --output ./outputs/czechia
imeteo-radar fetch --source imgw --output ./outputs/poland
imeteo-radar fetch --source omsz --output ./outputs/hungary
imeteo-radar fetch --source arso --output ./outputs/slovenia

# Generate composite
imeteo-radar composite --output ./outputs/composite

# Generate extent metadata
imeteo-radar extent --source all

# Generate coverage masks
imeteo-radar coverage-mask --output ./outputs

# Manage transform cache
imeteo-radar transform-cache --precompute
imeteo-radar transform-cache --stats

# Backload historical data
imeteo-radar fetch --source dwd --backload --hours 6 --output ./outputs/germany
```

## Output Directory Structure

```
outputs/
├── index.html          # Leaflet viewer (DO NOT DELETE)
├── germany/            # DWD radar data
├── slovakia/           # SHMU radar data
├── czechia/            # CHMI radar data
├── poland/             # IMGW radar data
├── hungary/            # OMSZ radar data
├── slovenia/           # ARSO radar data
└── composite/          # Merged composite images
```

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
imeteo-radar --help
```

## Documentation

| Topic | File |
|-------|------|
| CLI commands & options | [docs/cli-reference.md](docs/cli-reference.md) |
| Data flow & sequences | [docs/data-flow.md](docs/data-flow.md) |
| Docker, K8s, cloud | [docs/deployment.md](docs/deployment.md) |
| Architecture & pipeline | [docs/architecture.md](docs/architecture.md) |
| Development setup | [docs/development.md](docs/development.md) |
| Monitoring & debugging | [docs/monitoring.md](docs/monitoring.md) |

## Code Quality

```bash
black src/
isort src/
mypy src/
pytest
```
