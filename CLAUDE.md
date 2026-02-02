# Claude Code Configuration

## Project Overview

iMeteo Radar is a multi-source weather radar data processing system that handles DWD (Germany), SHMU (Slovakia), and CHMI (Czech Republic) radar data. The system downloads ODIM_H5 format radar data, processes it with proper colorscaling and transparency, and exports PNG images suitable for web mapping applications. Supports composite radar images that merge data from multiple sources.

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
