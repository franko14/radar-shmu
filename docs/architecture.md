# Architecture

Technical overview of the iMeteo Radar data processing system.

---

## Data Pipeline

```mermaid
flowchart TB
    subgraph sources["DATA SOURCES (APIs)"]
        DWD["DWD<br/>Germany"]
        SHMU["SHMU<br/>Slovakia"]
        CHMI["CHMI<br/>Czech Republic"]
    end

    subgraph download["DOWNLOAD"]
        D1["Parallel download (6 workers)"]
        D2["Temp files in system directory"]
        D3["Session caching"]
    end

    subgraph process["PROCESS (HDF5 → NumPy)"]
        P1["ODIM_H5 format parsing"]
        P2["Scaling: data = raw × gain + offset"]
        P3["NaN handling for nodata/undetect"]
    end

    sources --> download
    download --> process

    process --> SINGLE["SINGLE SOURCE<br/>(fetch command)"]
    process --> COMPOSITE["COMPOSITE<br/>(composite command)"]

    subgraph merge["COMPOSITE PROCESSING"]
        M1["Reproject to EPSG:3857"]
        M2["Interpolate to target grid"]
        M3["Merge (max reflectivity)"]
    end

    COMPOSITE --> merge

    SINGLE --> export
    merge --> export

    subgraph export["EXPORT (PNG)"]
        E1["uint8 LUT colormap"]
        E2["Alpha channel for no-data"]
        E3["Maximum PNG compression"]
    end

    subgraph output["OUTPUT"]
        O1["PNG: timestamp.png"]
        O2["Extent: extent_index.json"]
        O3["Optional: Cloud upload"]
    end

    export --> output
```

---

## Source Implementations

### DWD (German Weather Service)

| Property | Value |
|----------|-------|
| **Base URL** | https://opendata.dwd.de/weather/radar/composite |
| **Product** | dmax (maximum reflectivity) |
| **Format** | ODIM_H5 |
| **Projection** | Stereographic (DWD-specific) |
| **Grid size** | ~4800 × 4400 pixels |
| **Resolution** | ~1 km |
| **Coverage** | 1.5°-18.7°E, 45.7°-56.2°N |

**Implementation**: `src/imeteo_radar/sources/dwd.py`

### SHMU (Slovak Hydrometeorological Institute)

| Property | Value |
|----------|-------|
| **Base URL** | https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp |
| **Product** | zmax (maximum reflectivity) |
| **Format** | ODIM_H5 |
| **Projection** | Web Mercator (EPSG:3857) |
| **Grid size** | 1560 × 2270 pixels |
| **Resolution** | ~400 m |
| **Coverage** | 13.6°-23.8°E, 46.0°-50.7°N |

**Implementation**: `src/imeteo_radar/sources/shmu.py`

### CHMI (Czech Hydrometeorological Institute)

| Property | Value |
|----------|-------|
| **Base URL** | https://opendata.chmi.cz/meteorology/weather/radar/composite/maxz/hdf5 |
| **Product** | maxz (maximum reflectivity) |
| **Format** | ODIM_H5 |
| **Projection** | Web Mercator (EPSG:3857) |
| **Grid size** | Variable |
| **Resolution** | ~500 m |
| **Coverage** | 12°-19°E, 48.5°-51.1°N |

**Implementation**: `src/imeteo_radar/sources/chmi.py`

---

## Composite Generation

### Merging Strategy

The compositor uses **maximum reflectivity** merging:

```python
# For each grid cell, take the highest dBZ value
merged = np.fmax(source1, source2, source3)
```

This ensures:
- Precipitation is never underestimated
- Overlapping regions show strongest returns
- No-data areas from one source are filled by others

### Reprojection

All sources are reprojected to Web Mercator (EPSG:3857):

1. Extract source coordinates (1D arrays or 2D meshgrids)
2. Transform to Web Mercator using `pyproj`
3. Create `RegularGridInterpolator` for each source
4. Resample to target grid (nearest-neighbor)

**Implementation**: `src/imeteo_radar/processing/compositor.py`

### Target Grid

| Property | Default Value |
|----------|---------------|
| **Projection** | Web Mercator (EPSG:3857) |
| **Resolution** | 500 m |
| **Extent** | 2.5°-23.8°E, 45.5°-56°N |
| **Grid size** | ~3500 × 3500 pixels |

---

## Memory Optimizations

### Overview

The system was optimized from **4.8 GB** to **669 MB** (86% reduction).

### Key Optimizations

#### 1. uint8 LUT Colormap

**Problem**: Matplotlib creates 645 MB float64 RGBA arrays.

**Solution**: Pre-compute 256-entry uint8 lookup table.

```python
# Before: 886 MB peak
colors = colormap(normalize(data))  # float64 intermediate
rgba = (colors * 255).astype(np.uint8)

# After: 100 MB peak
indices = ((data - vmin) / (vmax - vmin) * 255).astype(np.uint8)
rgba = lut[indices]  # Direct lookup
```

**Savings**: ~786 MB per image

#### 2. Lazy Coordinate Generation

**Problem**: Creating 4800×4400 coordinate meshgrids (322 MB) that are never used after extent calculation.

**Solution**: Calculate extent from 4 corner coordinates only.

```python
# Before: 322 MB
lons_2d, lats_2d = np.meshgrid(lons, lats)
extent = {'west': lons_2d.min(), ...}

# After: ~200 bytes
corners = get_corners_from_hdf5()
extent = {'west': min(corners.lons), ...}
```

**Savings**: ~322 MB per image

#### 3. Memory Leak Fix

**Problem**: Matplotlib figure objects accumulating across files.

**Solution**: Explicit cleanup after each export.

```python
import matplotlib.pyplot as plt
import gc

# After export
plt.close('all')
gc.collect()
```

### Memory Usage

| Operation | Memory |
|-----------|--------|
| Single fetch | ~670 MB peak |
| 12-file backload | ~670 MB peak (constant) |
| Composite (3 sources) | ~1.2 GB peak |

---

## Colormap

### SHMU Official Scale

All sources use the official SHMU colorscale:

- **Range**: -35 to 85 dBZ
- **Resolution**: 1 dBZ steps
- **Colors**: 121 discrete colors

| dBZ Range | Color |
|-----------|-------|
| -35 to 0 | Black to dark blue |
| 0 to 20 | Blue to cyan |
| 20 to 40 | Green to yellow |
| 40 to 60 | Orange to red |
| 60 to 85 | Red to purple |

**Implementation**: `src/imeteo_radar/config/shmu_colormap.py`

---

## Projection Handling

### DWD Stereographic

DWD uses a custom stereographic projection defined in HDF5 `proj_def` attribute.

The `ProjectionHandler` class handles:
1. Parse projection definition from HDF5
2. Create transformer using `pyproj`
3. Convert corner coordinates to WGS84
4. Calculate extent bounds

**Implementation**: `src/imeteo_radar/core/projection.py`

### Web Mercator (SHMU/CHMI)

SHMU and CHMI data is already in Web Mercator (EPSG:3857).

Coordinate extraction:
1. Read corner coordinates from HDF5 `where` attributes
2. Create 1D linspace arrays for x and y
3. No reprojection needed for single-source export

---

## File Structure

```mermaid
graph LR
    subgraph src/imeteo_radar
        CLI[cli.py<br/>Main entry point]
        CLIC[cli_composite.py<br/>Composite command]

        subgraph sources/
            BASE1[base.py]
            DWD_S[dwd.py]
            SHMU_S[shmu.py]
            CHMI_S[chmi.py]
        end

        subgraph processing/
            EXP[exporter.py<br/>PNG export]
            COMP[compositor.py<br/>Multi-source merge]
        end

        subgraph core/
            BASE2[base.py<br/>Coord conversion]
            PROJ[projection.py<br/>Projections]
        end

        subgraph config/
            CMAP[shmu_colormap.py<br/>SHMU colorscale]
        end
    end

    CLI --> sources/
    CLI --> processing/
    CLIC --> processing/
    processing/ --> core/
    processing/ --> config/
```

---

## Performance Characteristics

### Download

- **Workers**: 6 concurrent
- **Session caching**: Prevents re-downloads
- **Timeout**: 30 seconds per file

### Processing

- **Time complexity**: O(n) where n = grid size
- **Memory**: Constant regardless of file count
- **Cleanup**: Automatic after each file

### Export

- **Method**: PIL with pre-computed LUT
- **Compression**: Maximum PNG level 9
- **Speed**: 4-10x faster than matplotlib

---

## Kubernetes Recommendations

Based on memory optimizations:

| Metric | Value |
|--------|-------|
| **Pod memory limit** | 1.2 GB |
| **Pods per 16 GB node** | 10-12 |
| **Estimated monthly savings** | ~50% vs unoptimized |

See [deployment.md](deployment.md) for detailed Kubernetes manifests.
