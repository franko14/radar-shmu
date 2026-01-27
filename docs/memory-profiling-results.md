# Memory Profiling Results

This document captures the memory profiling analysis performed on the iMeteo Radar processing system.

## Baseline Profiling Summary

| Workload | Peak RSS | Current K8s Limit | Status |
|----------|----------|-------------------|--------|
| DWD single | 728 MB | 512Mi | ⚠️ Over |
| DWD backload 1h | 1137 MB | 1Gi | ⚠️ Over |
| Composite latest | 3522 MB | 1.2Gi | ❌ 3× over |
| Composite backload 1h | 3888 MB | 1.2Gi | ❌ 3× over |

## Problem Statement

Composite operations peak at **3.5-3.9 GB RSS**, far exceeding the current 1.2Gi K8s limit. The actual data size is approximately 550 MB, but heap fragmentation causes a **6.3× memory multiplier**.

## Root Causes Identified

### 1. Target Grid Recreated Per Source (HIGH IMPACT)

**Location:** `compositor.py:193-194`

```python
target_xx, target_yy = np.meshgrid(self.target_x, self.target_y)  # 64 MB
target_points = np.column_stack([target_yy.ravel(), target_xx.ravel()])  # 64 MB
```

Called for EACH of 5 sources = **640 MB wasted** (should be allocated once).

### 2. Coordinate Cache Never Cleared (MEDIUM IMPACT)

**Location:** `compositor.py:47, 122-134`

```python
self.coordinate_cache = {}  # Persists entire session, never cleared
```

Holds 70-100 MB of coordinate arrays indefinitely.

### 3. Too Many Timestamps Downloaded (HIGH IMPACT)

**Location:** `cli_composite.py:158`

```python
files = source.download_latest(count=10, products=[product])
```

Downloads 10 timestamps × 5 sources = 50 HDF5 files (~250 MB) before finding common timestamp.

### 4. Float64 Used Where Float32 Suffices (LOW IMPACT)

**Location:** `compositor.py:159-160`, `projection.py:156-159`

Mercator transformations produce float64 arrays, wasting ~50% memory on coordinates.

### 5. No Explicit Cleanup in _process_latest() (MEDIUM IMPACT)

**Location:** `cli_composite.py:298`

Unlike `_process_backload()`, doesn't delete `sources_data` or call `gc.collect()`.

## Implemented Optimizations

### Phase 1: Pre-allocate Target Grid ✅
- Moved target meshgrid creation to `__init__` in `RadarCompositor`
- Reuse `self.target_xx`, `self.target_yy`, `self.target_points` in `add_source()`
- **Expected savings:** ~500 MB

### Phase 2: Clear Coordinate Cache ✅
- Added `clear_cache()` method to `RadarCompositor`
- Called after composite creation in `create_composite()`
- **Expected savings:** 70-100 MB

### Phase 3: Reduce Downloaded Timestamps ✅
- Changed `count=10` to `count=4` in `_process_latest()`
- **Expected savings:** 150-200 MB

### Phase 4: Add Explicit Cleanup ✅
- Added cleanup in `_process_latest()` - deletes `sources_data` and calls `gc.collect()`
- Added early temp file deletion after `process_to_array()` in both functions
- **Expected savings:** 200-300 MB

### Phase 5: Use Float32 for Coordinates ✅
- Cast `source_x_2d` and `source_y_2d` to float32 after Mercator conversion
- **Expected savings:** ~50 MB

## Post-Optimization Results

| Metric | Before | After (Measured) | Change |
|--------|--------|------------------|--------|
| Composite latest peak | 3522 MB | 3260-3521 MB | -0.03% to -7.4% |
| Composite average | ~2000 MB | 1281-1766 MB | -12% to -36% |

### Why Savings Were Less Than Expected

1. **All sources loaded simultaneously**: The architecture requires all radar_data to be in `sources_data` for individual source exports BEFORE composite creation. This means ~200-500 MB per source (especially DWD with 21M pixels) stays in memory simultaneously.

2. **Memory fragmentation**: Python/NumPy memory fragmentation means `gc.collect()` doesn't always return memory to the OS, especially on macOS/Linux.

3. **Overlapping allocations**: Peak memory occurs when source data, interpolated arrays, and composite grid all exist simultaneously during merging.

4. **Download count reduction**: Reducing from 10 to 4 timestamps saves disk I/O but not peak memory since files are downloaded to temp then immediately processed.

## Remaining Bottleneck

The main memory hog is holding all 5 source radar arrays in `sources_data` simultaneously:
- DWD: ~84 MB (4800×4400 float32)
- SHMU: ~14 MB (1560×2270 float32)
- CHMI: ~0.9 MB (378×598 float32)
- OMSZ: ~3 MB (813×961 float32)
- ARSO: ~0.5 MB (301×401 float32)

Plus coordinate arrays, interpolation temporaries, and composite grid (~88 MB).

## Future Optimization Opportunities

1. **Stream processing**: Process and merge each source individually, deleting radar_data immediately after merge (requires skipping individual exports or changing export order).

2. **Lazy loading**: Load source data on-demand during composite creation rather than pre-loading all.

3. **Memory-mapped files**: Use numpy memmap for large arrays to reduce RSS.

4. **Chunked processing**: Process the composite in spatial chunks rather than all at once.

## Recommended K8s Limits

Based on profiling:

| Workload | Memory Request | Memory Limit |
|----------|---------------|--------------|
| Single source fetch | 768Mi | 1024Mi |
| Composite latest | 1536Mi | 4608Mi |
| Composite backload | 2048Mi | 5120Mi |

## Profiling Commands

```bash
# Profile composite latest
python scripts/profile_memory_rss.py --json /tmp/composite-profile.json composite

# Profile composite backload
python scripts/profile_memory_rss.py --json /tmp/composite-backload.json composite --backload --hours 1

# Profile single source
python scripts/profile_memory_rss.py --json /tmp/dwd-profile.json fetch --source dwd
```
