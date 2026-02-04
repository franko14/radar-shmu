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

## Global Options

These options apply to all commands:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--log-level` | `DEBUG\|INFO\|WARNING\|ERROR` | `INFO` | Logging verbosity |
| `--log-format` | `console\|json` | `console` | Log output format |
| `--log-file` | path | - | Log to file (in addition to console) |

---

## Commands Overview

| Command | Description |
|---------|-------------|
| `fetch` | Download and process radar data from a single source |
| `composite` | Generate merged radar images from multiple sources |
| `extent` | Generate geographic extent metadata files |
| `transform-cache` | Manage precomputed reprojection transform grids |
| `coverage-mask` | Generate coverage mask PNGs for sources |

---

## fetch

Download radar data from any supported source and export as PNG images with Web Mercator reprojection.

### Usage

```bash
imeteo-radar fetch [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--source` | `dwd\|shmu\|chmi\|arso\|omsz\|imgw` | `dwd` | Radar data source |
| `--output` | path | `/tmp/iradar/{country}/` | Output directory |
| `--backload` | flag | - | Enable historical data download |
| `--hours` | int | - | Hours to backload (requires `--backload`) |
| `--from` | string | - | Start time: `"YYYY-MM-DD HH:MM"` |
| `--to` | string | - | End time: `"YYYY-MM-DD HH:MM"` |
| `--update-extent` | flag | - | Force regenerate extent_index.json |
| `--disable-upload` | flag | - | Skip cloud storage upload |
| `--reprocess-count` | int | `6` | Number of recent timestamps to fetch (~30 min) |
| `--cache-dir` | path | `/tmp/iradar-data/data` | Directory for processed data cache |
| `--cache-ttl` | int | `60` | Cache TTL in minutes |
| `--no-cache` | flag | - | Disable caching entirely |
| `--no-cache-upload` | flag | - | Disable S3 cache sync (local cache only) |
| `--clear-cache` | flag | - | Clear cache before running |

### Default Output Directories

| Source | Directory | Country |
|--------|-----------|---------|
| `dwd` | `/tmp/iradar/germany/` | Germany |
| `shmu` | `/tmp/iradar/slovakia/` | Slovakia |
| `chmi` | `/tmp/iradar/czechia/` | Czech Republic |
| `omsz` | `/tmp/iradar/hungary/` | Hungary |
| `arso` | `/tmp/iradar/slovenia/` | Slovenia |
| `imgw` | `/tmp/iradar/poland/` | Poland |

### Examples

```bash
# Download latest from each source
imeteo-radar fetch --source dwd --output ./outputs/germany
imeteo-radar fetch --source shmu --output ./outputs/slovakia
imeteo-radar fetch --source chmi --output ./outputs/czechia
imeteo-radar fetch --source omsz --output ./outputs/hungary
imeteo-radar fetch --source arso --output ./outputs/slovenia
imeteo-radar fetch --source imgw --output ./outputs/poland

# Backload last 6 hours
imeteo-radar fetch --source dwd --backload --hours 6

# Backload specific time range
imeteo-radar fetch --source shmu --backload \
  --from "2024-09-25 10:00" --to "2024-09-25 16:00"

# Local-only mode (no cloud upload)
imeteo-radar fetch --source dwd --disable-upload

# Force regenerate extent file
imeteo-radar fetch --source shmu --update-extent

# Disable caching (always re-download)
imeteo-radar fetch --source dwd --no-cache
```

### Output

- **PNG files**: `{unix_timestamp}.png` (e.g., `1728221400.png`) — reprojected to Web Mercator
- **Extent file**: `extent_index.json` with reprojected WGS84 bounds (matches PNG pixels exactly)

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
| `--output` | path | `/tmp/iradar/composite/` | Output directory |
| `--resolution` | float | `500` | Target resolution in meters |
| `--backload` | flag | - | Enable historical composite generation |
| `--hours` | int | - | Hours to backload |
| `--from` | string | - | Start time: `"YYYY-MM-DD HH:MM"` |
| `--to` | string | - | End time: `"YYYY-MM-DD HH:MM"` |
| `--update-extent` | flag | - | Force regenerate extent_index.json |
| `--no-individual` | flag | - | Skip generating individual source PNGs |
| `--disable-upload` | flag | - | Skip cloud storage upload |
| `--timestamp-tolerance` | int | `2` | Timestamp matching tolerance in minutes |
| `--require-arso` | flag | - | Fail if ARSO data cannot be matched |
| `--min-core-sources` | int | `3` | Minimum core sources required for composite |
| `--max-data-age` | int | `30` | Maximum age of data in minutes (outage threshold) |
| `--reprocess-count` | int | `6` | Number of recent timestamps to reprocess |
| `--cache-dir` | path | `/tmp/iradar-data/data` | Directory for processed data cache |
| `--cache-ttl` | int | `60` | Cache TTL in minutes |
| `--no-cache` | flag | - | Disable caching entirely |
| `--no-cache-upload` | flag | - | Disable S3 cache sync (local cache only) |
| `--clear-cache` | flag | - | Clear cache before running |

### Examples

```bash
# Generate latest composite from all sources
imeteo-radar composite --output ./outputs/composite

# Select specific sources
imeteo-radar composite --sources dwd,shmu,chmi

# Higher resolution (1km instead of 500m)
imeteo-radar composite --resolution 1000

# Backload last 6 hours
imeteo-radar composite --backload --hours 6

# Skip individual source PNGs (saves memory)
imeteo-radar composite --no-individual

# Local-only mode (no cloud upload)
imeteo-radar composite --disable-upload

# Combined options
imeteo-radar composite \
  --sources dwd,shmu,chmi \
  --resolution 500 \
  --backload --hours 3 \
  --output ./outputs/composite
```

### How It Works

1. **Check cache** for already-processed timestamps
2. **Query providers** for available timestamps
3. **Download only new** timestamps (skip cached)
4. **Cache downloaded data** for future runs
5. **Match timestamps** across sources (configurable tolerance)
6. **Reproject** all data to Web Mercator (EPSG:3857) using rasterio
7. **Merge** using maximum reflectivity (highest dBZ wins)
8. **Export** as PNG with transparency

### Cache-Aware Downloading

Both `fetch` and `composite` commands use intelligent caching to minimize downloads:

```
DWD: 8 available, 7 in cache, 1 to download
SHMU: 8 available, 7 in cache, 1 to download
ARSO: 1 available, 1 in cache, 0 to download
```

- **First run**: Downloads all available timestamps
- **Subsequent runs**: Downloads only new timestamps
- **Efficiency**: ~90% reduction in network usage

### Skip Reasons

When timestamps are not processed, the reason is logged:

| Reason | Description |
|--------|-------------|
| `Already exist` | Composite PNG already generated (local or S3) |
| `Insufficient sources` | Less than `--min-core-sources` available |
| `Processing failed` | Error during merge/export |

### Coverage

- **Combined extent**: ~2.5°E to 26.4°E, 44.0°N to 56.2°N
- **Region**: Central Europe (Germany, Czech Republic, Slovakia, Hungary, Slovenia, Poland)

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
| `--source` | `dwd\|shmu\|chmi\|arso\|omsz\|imgw\|all` | `all` | Source(s) to generate extent for |
| `--output` | path | `/tmp/iradar/{country}/` | Output directory |

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
    "source": "dwd",
    "generated": "2024-09-25T14:30:00Z"
  },
  "wgs84": {
    "west": 1.5,
    "east": 18.7,
    "south": 45.7,
    "north": 56.2
  }
}
```

---

## transform-cache

Manage precomputed reprojection transform grids. These grids store pixel-to-pixel index mappings for 10-50x faster reprojection (radar extents are static).

### Usage

```bash
imeteo-radar transform-cache [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--precompute` | flag | - | Precompute transform grids for sources |
| `--download-s3` | flag | - | Download transform grids from S3 to local cache |
| `--upload-s3` | flag | - | Upload precomputed grids to S3 |
| `--clear-local` | flag | - | Clear local transform cache |
| `--clear-s3` | flag | - | Clear S3 transform cache |
| `--stats` | flag | - | Show transform cache statistics |
| `--source` | `dwd\|shmu\|chmi\|arso\|omsz\|imgw\|all` | `all` | Source to operate on |

### Examples

```bash
# Precompute grids for all sources
imeteo-radar transform-cache --precompute

# Precompute and upload to S3
imeteo-radar transform-cache --precompute --upload-s3

# Warm local cache from S3 (e.g., after pod restart)
imeteo-radar transform-cache --download-s3

# Show cache statistics
imeteo-radar transform-cache --stats

# Clear and rebuild
imeteo-radar transform-cache --clear-local --precompute

# Precompute for specific source
imeteo-radar transform-cache --precompute --source dwd
```

### Cache Tiers

| Tier | Location | Persistence | Speed |
|------|----------|-------------|-------|
| Memory | In-process | Session only | Instant |
| Local disk | `/tmp/iradar-data/grid/` | Container lifetime | Fast |
| S3/DO Spaces | Cloud storage | Permanent | Network |

---

## coverage-mask

Generate static coverage mask PNGs showing radar coverage areas. Masks are aligned with extent_index.json for pixel-perfect overlay.

### Usage

```bash
imeteo-radar coverage-mask [OPTIONS]
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--source` | `dwd\|shmu\|chmi\|arso\|omsz\|imgw\|all` | `all` | Source to generate mask for |
| `--composite` | flag | - | Generate composite coverage mask |
| `--output` | path | `/tmp/iradar` | Base output directory |
| `--resolution` | float | `500` | Resolution for composite mask in meters |

### Examples

```bash
# Generate masks for all sources
imeteo-radar coverage-mask --output ./outputs

# Generate mask for single source
imeteo-radar coverage-mask --source dwd --output ./outputs

# Generate composite mask (combining all sources)
imeteo-radar coverage-mask --composite --output ./outputs
```

### Output

- **Transparent pixels**: Inside radar coverage (where data can exist)
- **Gray pixels**: Outside radar coverage (beyond radar range)
- **File**: `coverage_mask.png` in each source output directory

---

## Output Files

### PNG Images

- **Format**: RGBA PNG with alpha channel
- **Naming**: Unix timestamp (e.g., `1728221400.png`)
- **Colormap**: Official SHMU colorscale (-35 to 85 dBZ)
- **Transparency**: No-data areas are fully transparent
- **Projection**: Web Mercator (EPSG:3857) — reprojected from native source projections

### Extent Index

- **File**: `extent_index.json`
- **Purpose**: Geographic metadata for web mapping (reprojected WGS84 bounds)
- **Generated**: Automatically on first run, or with `--update-extent`

### Coverage Mask

- **File**: `coverage_mask.png`
- **Purpose**: Static overlay showing radar coverage area
- **Generated**: Via `coverage-mask` command

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
- **IMGW**: https://danepubliczne.imgw.pl/

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
imeteo-radar transform-cache --help
imeteo-radar coverage-mask --help
```
