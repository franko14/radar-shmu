# CLI Reference

Complete command-line interface documentation for `imeteo-radar`.

## Installation

```bash
# From source
pip install -e .

# From Docker
docker pull lfranko/imeteo-radar:latest
```

After installation, the `imeteo-radar` command is available.

---

## Commands Overview

| Command | Description |
|---------|-------------|
| `fetch` | Download and process radar data from a single source |
| `composite` | Generate merged radar images from multiple sources |
| `extent` | Generate geographic extent metadata files |

---

## fetch

Download radar data from DWD, SHMU, or CHMI and export as PNG images.

### Usage

```bash
imeteo-radar fetch [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--source` | `dwd\|shmu\|chmi` | `dwd` | Radar data source |
| `--output` | path | `/tmp/{country}/` | Output directory |
| `--backload` | flag | - | Enable historical data download |
| `--hours` | int | - | Hours to backload (requires `--backload`) |
| `--from` | string | - | Start time: `"YYYY-MM-DD HH:MM"` |
| `--to` | string | - | End time: `"YYYY-MM-DD HH:MM"` |
| `--update-extent` | flag | - | Force regenerate extent_index.json |
| `--disable-upload` | flag | - | Skip cloud storage upload |

### Default Output Directories

| Source | Directory | Country |
|--------|-----------|---------|
| `dwd` | `/tmp/germany/` | Germany |
| `shmu` | `/tmp/slovakia/` | Slovakia |
| `chmi` | `/tmp/czechia/` | Czech Republic |

### Examples

```bash
# Download latest from each source
imeteo-radar fetch --source dwd
imeteo-radar fetch --source shmu
imeteo-radar fetch --source chmi

# Custom output directory
imeteo-radar fetch --source dwd --output /data/radar/

# Backload last 6 hours
imeteo-radar fetch --source dwd --backload --hours 6

# Backload specific time range
imeteo-radar fetch --source shmu --backload \
  --from "2024-09-25 10:00" --to "2024-09-25 16:00"

# Local-only mode (no cloud upload)
imeteo-radar fetch --source dwd --disable-upload

# Force regenerate extent file
imeteo-radar fetch --source shmu --update-extent
```

### Output

- **PNG files**: `{unix_timestamp}.png` (e.g., `1728221400.png`)
- **Extent file**: `extent_index.json` (generated on first run)

---

## composite

Generate merged radar images from multiple sources using maximum reflectivity strategy with intelligent caching.

### Usage

```bash
imeteo-radar composite [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--sources` | string | `dwd,shmu,chmi,omsz,arso,imgw` | Comma-separated source list |
| `--output` | path | `/tmp/composite/` | Output directory |
| `--resolution` | float | `500` | Target resolution in meters |
| `--backload` | flag | - | Enable historical composite generation |
| `--hours` | int | - | Hours to backload |
| `--from` | string | - | Start time: `"YYYY-MM-DD HH:MM"` |
| `--to` | string | - | End time: `"YYYY-MM-DD HH:MM"` |
| `--update-extent` | flag | - | Force regenerate extent_index.json |
| `--disable-upload` | flag | - | Skip cloud storage upload |

### Examples

```bash
# Generate latest composite from all sources
imeteo-radar composite

# Select specific sources
imeteo-radar composite --sources dwd,shmu

# Higher resolution (1km instead of 500m)
imeteo-radar composite --resolution 1000

# Custom output directory
imeteo-radar composite --output /data/composite/

# Backload last 6 hours
imeteo-radar composite --backload --hours 6

# Backload specific time range
imeteo-radar composite --backload \
  --from "2024-11-10 10:00" --to "2024-11-10 12:00"

# Local-only mode (no cloud upload)
imeteo-radar composite --disable-upload

# Combined options
imeteo-radar composite \
  --sources dwd,shmu,chmi \
  --resolution 500 \
  --backload --hours 3
```

### How It Works

1. **Check cache** for already-processed timestamps
2. **Query providers** for available timestamps
3. **Download only new** timestamps (skip cached)
4. **Cache downloaded data** for future runs
5. **Match timestamps** across sources (5-minute tolerance)
6. **Reproject** all data to Web Mercator (EPSG:3857)
7. **Merge** using maximum reflectivity (highest dBZ wins)
8. **Export** as PNG with transparency

### Cache-Aware Downloading

The composite command uses intelligent caching to minimize downloads:

```
[15:42:23] 📡 DWD: 8 available, 7 in cache, 1 to download
[15:42:25] 📡 SHMU: 8 available, 7 in cache, 1 to download
[15:42:27] 📡 ARSO: 1 available, 1 in cache, 0 to download
```

- **First run**: Downloads all available timestamps
- **Subsequent runs**: Downloads only new timestamps
- **Efficiency**: ~90% reduction in network usage

### Skip Reasons

When timestamps are not processed, the reason is logged:

```
[15:42:36] 📡 Processed 1 composite(s), skipped 5
[15:42:36] 📡   Already exist (local/S3): 5 [20260128142000, 20260128141500, ...]
```

| Reason | Description |
|--------|-------------|
| `Already exist` | Composite PNG already generated |
| `Insufficient sources` | Less than 3/5 core sources available |
| `Processing failed` | Error during merge/export |

### Coverage

- **Combined extent**: ~2.5°E to 23.8°E, 45.5°N to 56°N
- **Region**: Central Europe (Germany, Czech Republic, Slovakia, and surrounding areas)

---

## extent

Generate geographic extent metadata files for web mapping integration.

### Usage

```bash
imeteo-radar extent [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--source` | `dwd\|shmu\|chmi\|all` | `all` | Source(s) to generate extent for |
| `--output` | path | `/tmp/{country}/` | Output directory |

### Examples

```bash
# Generate extent for all sources
imeteo-radar extent --source all

# Generate extent for single source
imeteo-radar extent --source dwd

# Custom output directory
imeteo-radar extent --source shmu --output /data/extents/
```

### Output Format

The `extent_index.json` file contains:

```json
{
  "metadata": {
    "title": "Radar Coverage Extent",
    "description": "Geographic extent and projection information",
    "version": "1.0",
    "generated": "2024-09-25T14:30:00Z",
    "coordinate_system": "WGS84 (EPSG:4326)"
  },
  "source": {
    "name": "DWD",
    "country": "Germany",
    "extent": {
      "wgs84": {
        "west": 1.5,
        "east": 18.7,
        "south": 45.7,
        "north": 56.2
      }
    },
    "projection": "Stereographic",
    "grid_size": [4800, 4400],
    "resolution_m": [1000, 1000]
  }
}
```

---

## Output Files

### PNG Images

- **Format**: 8-bit indexed PNG with alpha channel
- **Naming**: Unix timestamp (e.g., `1728221400.png`)
- **Colormap**: Official SHMU colorscale (-35 to 85 dBZ)
- **Transparency**: No-data areas are fully transparent
- **Compression**: Maximum PNG compression (level 9)

### Extent Index

- **File**: `extent_index.json`
- **Purpose**: Geographic metadata for web mapping
- **Generated**: Automatically on first run, or with `--update-extent`

---

## Data Sources

| Source | Product | Coverage | Resolution | Update |
|--------|---------|----------|------------|--------|
| **DWD** | dmax | Germany | ~1 km | 5 min |
| **SHMU** | zmax | Slovakia | ~400 m | 5 min |
| **CHMI** | maxz | Czech Republic | ~500 m | 5 min |
| **OMSZ** | cmax | Hungary | ~500 m | 5 min |
| **ARSO** | zm | Slovenia | ~1 km | 10-15 min |
| **IMGW** | cmax | Poland | ~500 m | 5 min |

### Data URLs

- **DWD**: https://opendata.dwd.de/weather/radar/composite/
- **SHMU**: https://opendata.shmu.sk/
- **CHMI**: https://opendata.chmi.cz/
- **OMSZ**: https://odp.met.hu/
- **ARSO**: https://vreme.arso.gov.si/
- **IMGW**: https://meteo.imgw.pl/

---

## Environment Variables

For cloud storage upload (optional):

| Variable | Description |
|----------|-------------|
| `DIGITALOCEAN_SPACES_KEY` | Access key |
| `DIGITALOCEAN_SPACES_SECRET` | Secret key |
| `DIGITALOCEAN_SPACES_ENDPOINT` | Endpoint URL |
| `DIGITALOCEAN_SPACES_REGION` | Region (e.g., `nyc3`) |
| `DIGITALOCEAN_SPACES_BUCKET` | Bucket name |
| `DIGITALOCEAN_SPACES_URL` | Public URL |

See [deployment.md](deployment.md) for detailed cloud storage setup.

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Error or interrupt |

---

## Help

```bash
# Show all commands
imeteo-radar --help

# Command-specific help
imeteo-radar fetch --help
imeteo-radar composite --help
imeteo-radar extent --help
```
