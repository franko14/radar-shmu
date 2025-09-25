# iMeteo Radar

Weather radar data processor for DWD (German Weather Service) and SHMU (Slovak Hydrometeorological Institute) radar data. Provides high-quality radar imagery with transparent backgrounds and proper colorscaling.

## 🚀 Quick Start

```bash
# Fetch latest radar data
imeteo-radar fetch --source dwd   # Germany
imeteo-radar fetch --source shmu  # Slovakia

# Backload historical data
imeteo-radar fetch --source dwd --backload --hours 6

# Docker
docker run --rm -v /tmp:/tmp imeteo-radar imeteo-radar fetch --source dwd
```

## 📦 Installation

```bash
pip install -e .  # Development
pip install .     # Production

# Docker
docker build -t imeteo-radar .
docker-compose build
```

## 🎯 Features

- **Multi-source**: DWD (Germany) and SHMU (Slovakia) radar data
- **PNG export**: Transparent backgrounds, official SHMU colorscale
- **Backload**: Historical data with flexible time ranges
- **Docker ready**: Production-ready containers with docker-compose

## 📡 Usage

```bash
# Backload historical data
imeteo-radar fetch --source dwd --backload --hours 6
imeteo-radar fetch --source dwd --backload --from "2024-09-25 10:00" --to "2024-09-25 16:00"

# Generate extent metadata
imeteo-radar extent --source all

# Generate colorbars
python scripts/generate_colorbar.py --generate-all
```

## 🐳 Docker

```bash
# Run single command
docker run --rm -v /tmp:/tmp imeteo-radar imeteo-radar fetch --source dwd

# Start production services (auto-fetch every 5 min)
docker-compose --profile production up -d

# Services: dwd-fetcher, shmu-fetcher, backloader, extent-generator
docker-compose logs -f dwd-fetcher
```

## 📊 Data Specifications

| Source | Coverage | Resolution | Output |
|--------|----------|------------|--------|
| **DWD** (Germany) | 45.7-56.2°N, 1.5-18.7°E | 4800×4400px | `/tmp/germany/YYYY-MM-dd_HHMM.png` |
| **SHMU** (Slovakia) | 46.0-50.7°N, 13.6-23.8°E | 1560×2270px | `/tmp/slovakia/YYYY-MM-dd_HHMM.png` |

- **Update frequency**: 5-minute intervals
- **Format**: PNG with transparency, SHMU colorscale (-35 to 85 dBZ)
- **Processing**: Temporary HDF5 files, no permanent raw storage

## 🔗 Data Sources

- **DWD OpenData**: https://opendata.dwd.de/weather/radar/composite/
- **SHMU OpenData**: https://opendata.shmu.sk/

## 📝 License

MIT

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.