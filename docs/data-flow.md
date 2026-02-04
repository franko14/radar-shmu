# Data Flow

Sequence diagrams and flow visualizations for the iMeteo Radar processing pipeline.

---

## 1. Single Source Fetch

The `fetch` command downloads radar data from one source, reprojects it to Web Mercator, and exports PNG images.

```mermaid
sequenceDiagram
    participant User
    participant CLI as cli.py<br/>fetch_command
    participant Cache as ProcessedDataCache<br/>(local + S3)
    participant Source as RadarSource<br/>(e.g. DWD)
    participant Provider as Data Provider<br/>(e.g. opendata.dwd.de)
    participant Exporter as PNGExporter
    participant TCache as TransformCache<br/>(memory/disk/S3)
    participant Upload as SpacesUploader<br/>(S3)

    User->>CLI: imeteo-radar fetch --source dwd

    Note over CLI: Initialize components
    CLI->>Source: DWDRadarSource()
    CLI->>Exporter: PNGExporter(use_transform_cache=True)
    CLI->>Cache: init_cache_from_args()

    Note over CLI: Generate extent_index.json<br/>(first run or --update-extent)
    CLI->>Source: get_extent()
    Source-->>CLI: {wgs84: {west, east, south, north}}
    CLI->>CLI: save extent_index.json

    Note over CLI,Provider: Discover available timestamps
    CLI->>Source: get_available_timestamps(count=8)
    Source->>Provider: HTTP GET (listing)
    Provider-->>Source: [ts1, ts2, ts3, ...]
    Source-->>CLI: ["202501281010", "202501281005", ...]

    Note over CLI,Cache: Split into cached vs. new
    CLI->>Cache: get_available_timestamps("dwd", "dmax")
    Cache-->>CLI: ["202501281010"]

    Note over CLI: ts "202501281010" in cache → skip download<br/>ts "202501281005" not in cache → download

    Note over CLI,Provider: Download only new timestamps
    CLI->>Source: download_timestamps(["202501281005"])
    Source->>Provider: HTTP GET (HDF5 file)
    Provider-->>Source: ODIM_H5 binary
    Source-->>CLI: [{timestamp, path: "/tmp/dwd_xxx.h5"}]

    loop For each downloaded file
        Note over CLI: Parse timestamp → unix filename
        CLI->>CLI: output_exists? (local + S3 check)

        CLI->>Source: process_to_array("/tmp/dwd_xxx.h5")
        Source-->>CLI: radar_data {data, extent, projection}

        CLI->>Cache: put("dwd", ts, "dmax", radar_data)
        Note over Cache: Save .npz + .json locally<br/>Upload both to S3

        CLI->>Exporter: export_png_fast(radar_data, reproject=True)
        Note over Exporter,TCache: Reprojection (see diagram 4)
        Exporter->>TCache: get_or_compute("dwd", shape, projection)
        TCache-->>Exporter: TransformGrid (row/col indices)
        Exporter->>Exporter: fast_reproject(data, grid)
        Exporter->>Exporter: Apply uint8 LUT colormap
        Exporter->>Exporter: Save indexed PNG (palette mode)
        Exporter-->>CLI: output_path, metadata

        CLI->>Upload: upload_file(png_path, "dwd", filename)

        Note over CLI: del radar_data, gc.collect()
    end

    loop For each cached timestamp
        CLI->>CLI: output_exists? → skip if yes
        CLI->>Cache: get("dwd", ts, "dmax")
        Cache-->>CLI: radar_data (from .npz + .json)
        CLI->>Exporter: export_png_fast(radar_data, reproject=True)
        Exporter-->>CLI: output_path
        CLI->>Upload: upload_file(...)
    end

    CLI->>Source: cleanup_temp_files()
    CLI-->>User: Done (N processed, M skipped)
```

---

## 2. Composite Generation

The `composite` command fetches from all sources, matches timestamps across providers, and merges using maximum reflectivity.

```mermaid
sequenceDiagram
    participant User
    participant CLI as cli_composite.py
    participant Cache as ProcessedDataCache
    participant Sources as 6 RadarSources
    participant Providers as Data Providers
    participant Compositor as RadarCompositor
    participant Exporter as PNGExporter
    participant Upload as SpacesUploader

    User->>CLI: imeteo-radar composite

    Note over CLI: Initialize all 6 sources + cache

    rect rgb(240, 248, 255)
        Note over CLI,Providers: PHASE 1: Parallel data collection
        loop For each source (DWD, SHMU, CHMI, OMSZ, ARSO, IMGW)
            CLI->>Cache: get_available_timestamps(source)
            Cache-->>CLI: cached_set

            CLI->>Sources: get_available_timestamps(count=8)
            Sources->>Providers: HTTP GET
            Providers-->>Sources: available timestamps

            Note over CLI: Split: cached vs. to-download

            CLI->>Sources: download_timestamps(new_only)
            Sources->>Providers: HTTP GET (HDF5/netCDF)
            Providers-->>Sources: raw files

            loop For each downloaded file
                CLI->>Sources: process_to_array(file)
                Sources-->>CLI: radar_data
                CLI->>Cache: put(source, ts, product, radar_data)
            end

            Note over CLI: Store in timestamp_groups[ts][source]
        end
    end

    rect rgb(255, 248, 240)
        Note over CLI: PHASE 2: Outage detection
        CLI->>CLI: Check newest timestamp per source
        CLI->>CLI: Mark stale sources (age > 30 min)
        CLI->>CLI: Count available core sources
        Note over CLI: Need >= 3 of {DWD, SHMU, CHMI, OMSZ, IMGW}
    end

    rect rgb(240, 255, 240)
        Note over CLI: PHASE 3: Timestamp matching
        CLI->>CLI: Find common timestamps (±2 min tolerance)
        Note over CLI: Handle ARSO special case<br/>(only provides latest, retry without if no match)
        CLI-->>CLI: Up to 6 matched timestamps
    end

    rect rgb(255, 240, 255)
        Note over CLI,Upload: PHASE 4: Two-pass processing (per timestamp)

        Note over CLI: PASS 1 — Extract extents only (no large arrays)
        loop For each source in matched set
            alt From cache
                CLI->>Sources: get_extent()
            else From file
                CLI->>Sources: extract_extent_only(path)
            end
            CLI->>CLI: Store dimensions + extent
        end

        CLI->>Compositor: RadarCompositor(combined_extent, 500m)

        Note over CLI,Compositor: PASS 2 — Sequential load, merge, release
        loop For each source (one at a time)
            alt From cache
                CLI->>Cache: get(source, ts, product)
                Cache-->>CLI: radar_data
            else From file
                CLI->>Sources: process_to_array(file)
                Sources-->>CLI: radar_data
                CLI->>Cache: put(source, ts, product, radar_data)
            end

            opt Export individual source PNG
                CLI->>Exporter: export_png_fast(radar_data, reproject=True)
                CLI->>Upload: upload_file(source_png)
            end

            CLI->>Compositor: add_source(source, radar_data)
            Note over CLI: del radar_data, gc.collect()<br/>(keeps RAM constant)
        end

        CLI->>Compositor: get_composite()
        Compositor-->>CLI: merged_data (max reflectivity)
        CLI->>Exporter: export_png_fast(composite, reproject=False)
        Note over Exporter: Already in Web Mercator, no reprojection needed
        CLI->>Upload: upload_file(composite_png)
    end

    CLI->>CLI: Save extent_index.json
    CLI-->>User: Done (N composites, M skipped)
```

---

## 3. Dual-Layer Processed Data Cache

Caches parsed radar arrays (float32) and metadata to avoid re-downloading from providers.

```mermaid
sequenceDiagram
    participant Caller
    participant Cache as ProcessedDataCache
    participant Local as Local Filesystem<br/>/tmp/iradar-data/
    participant S3 as S3 / DO Spaces<br/>iradar-data/

    Note over Caller,S3: cache.put(source, timestamp, product, radar_data)

    Caller->>Cache: put("dwd", "202501281005", "dmax", radar_data)

    Cache->>Local: Save dwd_dmax_202501281005.npz<br/>(numpy compressed: data + coords)
    Cache->>Local: Save dwd_dmax_202501281005.json<br/>(extent, dimensions, cached_at)

    opt S3 enabled (not --no-cache-upload)
        Cache->>S3: Upload .npz
        Cache->>S3: Upload .json
    end

    Cache-->>Caller: OK

    Note over Caller,S3: cache.get(source, timestamp, product)

    Caller->>Cache: get("dwd", "202501281005", "dmax")

    Cache->>Local: Check .npz exists?
    alt Local hit
        Local-->>Cache: Load .npz + .json
        Note over Cache: Check TTL (default 60 min)
        alt Not expired
            Cache-->>Caller: radar_data
        else Expired
            Cache->>Local: Delete expired entry
            Cache->>S3: Check S3 for fresh copy
        end
    else Local miss
        Cache->>S3: Download .npz + .json
        alt S3 hit
            S3-->>Cache: Files downloaded
            Cache->>Local: Save to local (warm cache)
            Cache-->>Caller: radar_data
        else S3 miss
            Cache-->>Caller: None (cache miss)
        end
    end
```

### Cache filesystem layout

```mermaid
graph LR
    subgraph local["/tmp/iradar-data/"]
        subgraph dwd_dir["dwd/"]
            N1["dwd_dmax_202501281005.npz<br/><i>~2.5 MB (float32 array)</i>"]
            J1["dwd_dmax_202501281005.json<br/><i>~200 B (metadata)</i>"]
        end
        subgraph shmu_dir["shmu/"]
            N2["shmu_zmax_202501281005.npz"]
            J2["shmu_zmax_202501281005.json"]
        end
    end

    subgraph s3["S3: iradar-data/"]
        S1["dwd/dwd_dmax_202501281005.npz"]
        S2["dwd/dwd_dmax_202501281005.json"]
        S3x["shmu/shmu_zmax_202501281005.npz"]
        S4["shmu/shmu_zmax_202501281005.json"]
    end

    local -.->|"--no-cache-upload<br/>disables sync"| s3
```

---

## 4. Three-Tier Transform Cache

Precomputed pixel-to-pixel index mappings for 10-50x faster reprojection. Since radar source extents are static, these grids are computed once and reused indefinitely.

```mermaid
sequenceDiagram
    participant Exporter as PNGExporter
    participant TCache as TransformCache
    participant Mem as Tier 1: Memory<br/>(in-process dict)
    participant Disk as Tier 2: Local Disk<br/>/tmp/iradar-data/grid/
    participant S3 as Tier 3: S3<br/>transform-grids/

    Exporter->>TCache: get_or_compute("dwd", (1560,2270), projection_info)

    TCache->>Mem: Check key "dwd_1560x2270_v1"
    alt Memory hit (instant)
        Mem-->>TCache: TransformGrid
        TCache-->>Exporter: grid
    else Memory miss
        TCache->>Disk: Check dwd_1560x2270_v1.npz
        alt Disk hit (~10ms)
            Disk-->>TCache: Load .npz
            TCache->>Mem: Store in memory
            TCache-->>Exporter: grid
        else Disk miss
            TCache->>S3: Check transform-grids/dwd_1560x2270_v1.npz
            alt S3 hit (~1-3s)
                S3-->>TCache: Download .npz
                TCache->>Disk: Save locally
                TCache->>Mem: Store in memory
                TCache-->>Exporter: grid
            else S3 miss (first ever use)
                Note over TCache: Compute new grid (~5-15s)
                TCache->>TCache: calculate_default_transform()
                TCache->>TCache: Build int16 row/col index arrays
                TCache->>Mem: Store in memory
                TCache->>Disk: Save .npz locally
                TCache->>S3: Upload .npz
                TCache-->>Exporter: grid
            end
        end
    end

    Note over Exporter: Apply grid via fast_reproject()
    Exporter->>Exporter: output[valid] = data[row_idx[valid], col_idx[valid]]
    Note over Exporter: Pure numpy indexing — no coordinate math
```

### TransformGrid structure

```mermaid
graph TB
    subgraph grid["TransformGrid (~4 bytes/pixel)"]
        R["row_indices: int16[dst_h × dst_w]<br/><i>Source row for each output pixel</i>"]
        C["col_indices: int16[dst_h × dst_w]<br/><i>Source col for each output pixel</i>"]
        M["wgs84_bounds, mercator_bounds<br/>dst_shape, src_shape, version"]
    end

    subgraph usage["fast_reproject(data, grid)"]
        I["For output pixel (100, 200):<br/>src_row = row_indices[100,200] → 87<br/>src_col = col_indices[100,200] → 145"]
        O["output[100,200] = source_data[87, 145]"]
    end

    grid --> usage
```

---

## 5. Reprojection Pipeline

How source data in native projections gets reprojected to Web Mercator (EPSG:3857) for web map display.

```mermaid
sequenceDiagram
    participant Exporter as PNGExporter<br/>export_png_fast()
    participant Reproj as reprojector.py
    participant TCache as TransformCache
    participant Rasterio as rasterio.warp

    Note over Exporter: radar_data has projection_info

    Exporter->>Reproj: build_native_params_from_projection_info(shape, proj_info)
    Note over Reproj: Parse proj_def (PROJ4 string)<br/>+ where_attrs (HDF5 corner coords)<br/>→ Build affine transform from corners
    Reproj-->>Exporter: (native_crs, native_transform, native_bounds)

    alt Fast path: cached transform grid available
        Exporter->>TCache: get_or_compute(source, shape, crs, transform, bounds)
        TCache-->>Exporter: TransformGrid

        Exporter->>Exporter: fast_reproject(data, grid)
        Note over Exporter: output[mask] = data[rows[mask], cols[mask]]<br/>Pure array indexing: 10-50x faster
    else Slow path: no cached grid (fallback)
        Exporter->>Reproj: reproject_to_web_mercator_accurate(data, crs, transform, bounds)
        Reproj->>Rasterio: calculate_default_transform(src_crs → EPSG:3857)
        Rasterio-->>Reproj: dst_transform, dst_width, dst_height
        Reproj->>Rasterio: reproject(source, destination, Resampling.nearest)
        Note over Rasterio: Full GDAL coordinate math<br/>for every pixel
        Rasterio-->>Reproj: reprojected array + bounds
        Reproj-->>Exporter: (data_3857, wgs84_bounds)
    end

    Note over Exporter: Data is now in Web Mercator<br/>Shape changed (e.g. 1560×2270 → 3400×2800)
```

### Native projections per source

```mermaid
graph LR
    subgraph sources["Source Native Projections"]
        DWD["DWD<br/>Polar Stereographic"]
        SHMU["SHMU<br/>Web Mercator (3857)"]
        CHMI["CHMI<br/>Web Mercator (3857)"]
        IMGW["IMGW<br/>Azimuthal Equidistant"]
        OMSZ["OMSZ<br/>Geographic WGS84"]
        ARSO["ARSO<br/>Geographic WGS84"]
    end

    subgraph reproj["rasterio.warp.reproject"]
        R["Reproject to<br/>Web Mercator<br/>(EPSG:3857)"]
    end

    subgraph output["Output"]
        PNG["PNG ready for<br/>Leaflet / Mapbox"]
    end

    DWD -->|"needs full reproject"| R
    SHMU -->|"already 3857 (passthrough)"| R
    CHMI -->|"already 3857 (passthrough)"| R
    IMGW -->|"needs full reproject"| R
    OMSZ -->|"WGS84 → 3857"| R
    ARSO -->|"WGS84 → 3857"| R
    R --> PNG
```

---

## 6. PNG Export Pipeline

From raw float32 reflectivity array to optimized PNG file.

```mermaid
sequenceDiagram
    participant Caller
    participant Exporter as PNGExporter
    participant TCache as TransformCache
    participant PIL as Pillow

    Caller->>Exporter: export_png_fast(radar_data, reproject=True)

    Note over Exporter: Step 1: Reproject (optional)
    opt reproject=True and projection_info present
        Exporter->>TCache: get_or_compute(source, ...)
        TCache-->>Exporter: TransformGrid
        Exporter->>Exporter: fast_reproject(data, grid)
        Note over Exporter: (1560×2270) → (3400×2800)<br/>Extent updated to reprojected bounds
    end

    Note over Exporter: Step 2: uint8 LUT colormap
    Exporter->>Exporter: Normalize data to 0-255 range
    Exporter->>Exporter: indices = clip((data - vmin)/(vmax - vmin) * 255)
    Exporter->>Exporter: rgba = LUT[indices]
    Note over Exporter: LUT is 256×4 uint8 array (1 KB)<br/>vs. matplotlib float64 intermediate (~800 MB)

    Note over Exporter: Step 3: Transparency
    Exporter->>Exporter: rgba[~valid, alpha] = 0
    Note over Exporter: NaN pixels become fully transparent

    Note over Exporter: Step 4: Save optimized PNG
    Exporter->>PIL: Image.fromarray(rgba, "RGBA")
    Exporter->>PIL: convert("P", palette=ADAPTIVE, colors=256)
    Note over PIL: Indexed palette mode: ~75% smaller<br/>(SHMU colormap has ~24 discrete colors)
    Exporter->>PIL: save(path, optimize=True, compress_level=9)

    Exporter-->>Caller: (output_path, metadata)
    Note over Caller: metadata includes:<br/>wgs84 bounds (for Leaflet overlay),<br/>dimensions, reprojection flag
```

---

## 7. Extent Index Lifecycle

How `extent_index.json` is generated, stored, and consumed by downstream components.

```mermaid
sequenceDiagram
    participant Fetch as fetch command
    participant Composite as composite command
    participant Mask as coverage-mask<br/>command
    participant Leaflet as Web Map<br/>(Leaflet)

    Note over Fetch: Generated on first fetch<br/>or with --update-extent

    Fetch->>Fetch: source.get_extent()
    Fetch->>Fetch: Save outputs/germany/extent_index.json
    Note over Fetch: Contains reprojected WGS84 bounds<br/>matching PNG pixel grid exactly

    Note over Composite: Generated after first composite
    Composite->>Composite: Compute combined_extent from all sources
    Composite->>Composite: Save outputs/composite/extent_index.json

    Note over Mask: Reads extent_index.json to align masks

    Mask->>Mask: Load outputs/germany/extent_index.json
    Mask->>Mask: Get WGS84 bounds + target PNG dimensions
    Mask->>Mask: Reproject coverage directly into<br/>that exact pixel grid
    Mask->>Mask: Save outputs/germany/coverage_mask.png
    Note over Mask: Mask pixels align 1:1<br/>with data PNG pixels

    Note over Mask: Composite mask
    Mask->>Mask: Load each source extent_index.json
    Mask->>Mask: Compute union extent (all sources)
    Mask->>Mask: Load individual coverage_mask.png files
    Mask->>Mask: Map into union grid, OR-combine
    Mask->>Mask: Save outputs/composite/coverage_mask.png
    Mask->>Mask: Save coverage_mask_extent.json

    Note over Leaflet: Consumes extent_index.json
    Leaflet->>Leaflet: Read extent_index.json
    Leaflet->>Leaflet: L.imageOverlay(png_url, [[south,west],[north,east]])
    Note over Leaflet: Bounds from extent_index.json<br/>guarantee pixel-perfect positioning
```

### extent_index.json structure variants

```mermaid
graph TB
    subgraph fetch_format["Fetch command format"]
        F1["source.extent.wgs84<br/>{west, east, south, north}"]
        F2["source.grid_size<br/>[height, width]"]
        F3["source.projection<br/>stereographic | mercator | ..."]
    end

    subgraph composite_format["Composite command format"]
        C1["wgs84<br/>{west, east, south, north}"]
        C2["metadata.resolution_m<br/>500"]
        C3["metadata.sources<br/>[dwd, shmu, ...]"]
    end

    subgraph mask_reader["coverage_mask.py reader"]
        M1["_get_wgs84_from_extent_index()"]
        M2["Handles all 3 formats:<br/>top-level wgs84<br/>extent.wgs84<br/>source.extent"]
    end

    fetch_format --> mask_reader
    composite_format --> mask_reader
```

---

## 8. End-to-End: Full Composite Cycle

Complete flow from user command to web-ready output, showing all cache interactions.

```mermaid
flowchart TB
    START(["imeteo-radar composite<br/>--output ./outputs/composite"])

    subgraph init["Initialization"]
        I1["Create 6 RadarSources"]
        I2["Init ProcessedDataCache<br/>(local: /tmp/iradar-data, S3)"]
        I3["Init PNGExporter<br/>(transform cache enabled)"]
    end

    subgraph collect["Data Collection (per source)"]
        C1["Query provider for<br/>available timestamps"]
        C2{"Timestamp<br/>in cache?"}
        C3["Download HDF5<br/>from provider"]
        C4["process_to_array()"]
        C5["Cache radar_data<br/>(local + S3)"]
        C6["Use cached<br/>radar_data"]
    end

    subgraph match["Timestamp Matching"]
        M1["Detect outages<br/>(stale > 30 min)"]
        M2{"≥ 3 core<br/>sources?"}
        M3["Find common timestamps<br/>(±2 min tolerance)"]
        M4["Handle ARSO<br/>(latest only, optional)"]
        FAIL(["Abort: insufficient sources"])
    end

    subgraph process["Two-Pass Processing"]
        P1["PASS 1: Extract extents<br/>(no large arrays loaded)"]
        P2["Create RadarCompositor<br/>(combined_extent, 500m grid)"]

        subgraph pass2["PASS 2: Sequential (one source at a time)"]
            L1["Load radar_data<br/>(from cache or file)"]
            L2["Export individual PNG<br/>(with reprojection)"]
            L3["compositor.add_source()"]
            L4["del radar_data<br/>gc.collect()"]
        end

        P3["compositor.get_composite()<br/>(max reflectivity merge)"]
        P4["Export composite PNG<br/>(no reprojection needed)"]
        P5["Upload to S3"]
        P6["Save extent_index.json"]
    end

    START --> init
    I1 --> I2 --> I3

    I3 --> collect
    C1 --> C2
    C2 -->|"Yes"| C6
    C2 -->|"No"| C3
    C3 --> C4 --> C5
    C5 --> match
    C6 --> match

    M1 --> M2
    M2 -->|"No"| FAIL
    M2 -->|"Yes"| M3
    M3 --> M4

    M4 --> process
    P1 --> P2
    P2 --> pass2
    L1 --> L2 --> L3 --> L4
    L4 -->|"next source"| L1

    pass2 --> P3
    P3 --> P4 --> P5 --> P6
```

---

## 9. Cache Decision Matrix

Summary of when each cache tier is consulted during different operations.

```mermaid
graph TB
    subgraph operation["Operation"]
        OP1["fetch command"]
        OP2["composite command"]
        OP3["Export PNG<br/>(reprojection)"]
        OP4["coverage-mask<br/>command"]
    end

    subgraph data_cache["Processed Data Cache"]
        DC["ProcessedDataCache<br/>(avoids re-downloading)"]
        DC1["Local: /tmp/iradar-data/data/<br/>TTL: 60 min"]
        DC2["S3: iradar-data/<br/>Persistent"]
    end

    subgraph transform_cache["Transform Cache"]
        TC["TransformCache<br/>(avoids recomputing projections)"]
        TC1["Memory: in-process dict<br/>Session lifetime"]
        TC2["Local: /tmp/iradar-data/grid/<br/>Container lifetime"]
        TC3["S3: iradar-data/grid/<br/>Permanent"]
    end

    subgraph none["No cache (reads extent_index.json)"]
        EI["extent_index.json<br/>+ existing data PNGs"]
    end

    OP1 --> DC
    OP2 --> DC
    OP1 --> TC
    OP2 --> TC
    OP3 --> TC
    OP4 --> EI

    DC --- DC1
    DC --- DC2
    TC --- TC1
    TC --- TC2
    TC --- TC3

    style DC fill:#e3f2fd
    style TC fill:#fff3e0
    style EI fill:#e8f5e9
```

| Operation | Processed Data Cache | Transform Cache | extent_index.json |
|-----------|---------------------|-----------------|-------------------|
| `fetch` | Read + Write | Read (for reprojection) | Write (first run) |
| `composite` | Read + Write | Read (for individual PNGs) | Write (first run) |
| `coverage-mask` | Not used | Not used | Read (for alignment) |
| `transform-cache --precompute` | Not used | Write (all 3 tiers) | Not used |
| `transform-cache --stats` | Not used | Read (all 3 tiers) | Not used |
