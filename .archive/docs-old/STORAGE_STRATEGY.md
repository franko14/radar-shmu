# Radar Data Storage Architecture

## Technical Overview

This document describes the time-partitioned storage architecture for the radar data processing system, designed to handle high-frequency meteorological data from multiple European radar sources with optimal performance and scalability.

## Architecture Principles

### 1. Time-Partitioned Storage Design
- **Structure**: `storage/{source}/{YYYY}/{MM}/{DD}/{HH}/`
- **Purpose**: Hierarchical organization enabling O(1) time-based queries
- **Benefits**: Efficient data retrieval, automatic cleanup, scalable to years of data

### 2. Separation of Concerns
- **Raw Data**: Time-partitioned HDF5 files in `storage/`
- **Processed Outputs**: Generated assets in `outputs/`
- **Source Code**: Clean package structure in `src/radar_shmu/`
- **Automation**: Production scripts in `scripts/`
- **Configuration**: System settings in `config/`

## Directory Structure

### Raw Data Storage: `./storage/`
**Implementation**: `TimePartitionedStorage` class in `src/radar_shmu/utils/storage.py`
**Structure**: `storage/{source}/{YYYY}/{MM}/{DD}/{HH}/{source}_{product}_{timestamp}.hdf`
**Current Status**: 50 HDF files, ~11MB total

```
storage/
├── shmu/2025/09/09/07/
│   ├── shmu_zmax_20250909071500.hdf
│   ├── shmu_cappi2km_20250909071500.hdf
│   ├── shmu_zmax_20250909072000.hdf
│   └── shmu_cappi2km_20250909072000.hdf
└── dwd/2025/09/09/07/
    └── dwd_dmax_20250909_0715.hdf
```

**Key Features**:
- **Time-based partitioning**: Direct path construction from timestamp
- **Metadata storage**: Optional JSON metadata alongside HDF files
- **Automatic cleanup**: Configurable retention policies
- **Migration support**: Automated migration from flat cache structures

### Processed Outputs: `./outputs/`
**Purpose**: Generated visualizations, animations, and web-ready data
**Current Status**: 0B (empty, outputs generated on-demand)

```
outputs/
├── png/                    # Radar visualizations
├── animations/             # Time-series GIF animations
├── json/                   # Frontend-ready JSON exports
├── matched_data/           # Synchronized multi-source datasets
└── merged/                 # Composite radar data
```

### Python Package: `./src/radar_shmu/`
**Implementation**: Modern Python package with PEP 517/518 compliance
**Size**: 340K source code
**Structure**: Modular architecture with clear separation of concerns

```
src/radar_shmu/
├── sources/                # Data source implementations
│   ├── base.py            # Abstract base classes
│   ├── shmu.py            # SHMU implementation
│   └── dwd.py             # DWD implementation
├── processing/            # Data processing modules
│   ├── merger.py          # Multi-source data merging
│   ├── exporter.py        # Visualization export
│   └── animator.py        # Animation generation
├── utils/                 # Core utilities
│   └── storage.py         # Time-partitioned storage
├── config/                # Configuration management
│   └── shmu_colormap.py   # Official SHMU colormap
└── cli.py                 # Command-line interface
```

### Production Scripts: `./scripts/`
**Purpose**: Automated production workflows
**Size**: 36K automation scripts

```
scripts/
├── download_matching_data.py      # Multi-source synchronization
├── create_production_merged.py    # Advanced data merging
├── generate_animations.py         # Animation creation
└── generate_production_indexes.py # Metadata indexing
```

### Configuration: `./config/`
**Purpose**: Centralized system configuration
**Size**: 4.0K configuration data

```
config/
└── extent_index.json              # Geographic coverage definitions
```

**Configuration includes**:
- **Geographic extents**: WGS84 and Mercator projections
- **Grid specifications**: Resolution and dimensions
- **Radar networks**: Site locations and coverage areas
- **Overlap regions**: Multi-source coverage areas

### Test Suite: `./tests/`
**Purpose**: Comprehensive quality assurance
**Size**: 48K test code

```
tests/
├── test_sources.py                # Data source functionality
├── test_integration.py            # End-to-end workflows
└── test_colormap.py              # Visualization validation
```

## Time-Partitioned Storage Implementation

### Core Class: `TimePartitionedStorage`
**Location**: `src/radar_shmu/utils/storage.py`
**Key Methods**:

```python
# Path generation
get_partition_path(timestamp, source) -> Path

# File operations
store_file(file_path, timestamp, source, product, metadata=None) -> Path
get_files(source, start_time=None, end_time=None, product=None) -> List[Dict]
get_latest_files(source, count=10, product=None) -> List[Dict]

# Maintenance operations
cleanup_old_files(source, keep_days=30) -> int
get_storage_stats() -> Dict[str, Any]

# Migration utilities
migrate_existing_data(old_cache_dir, source)
```

### Performance Characteristics

#### Storage Metrics (Current System)
- **Total files**: 50 HDF files
- **Storage size**: ~11MB
- **Average file size**: ~220KB per file
- **Query performance**: O(1) path-based access
- **Cleanup efficiency**: Automatic empty directory removal

#### Time Complexity
- **File retrieval**: O(1) for specific timestamp
- **Range queries**: O(n) where n = files in time range
- **Latest files**: O(n log n) for sorting by timestamp
- **Cleanup operations**: O(n) for files older than retention

### Geographic Coverage Configuration

**SHMU Coverage**:
- **Extent**: 13.6°-23.8°E, 46.0°-50.7°N
- **Grid**: 2270 × 1560 pixels
- **Resolution**: ~330m × 480m
- **Projection**: EPSG:3857 (Web Mercator)
- **Radar sites**: CZSKA, SKJAV, SKKOJ, SKLAZ, SKKUB

**DWD Coverage**:
- **Extent**: 3.0°-17.0°E, 47.0°-56.0°N
- **Grid**: 4800 × 4400 pixels
- **Resolution**: ~1km × 1km
- **Projection**: EPSG:3857 (Web Mercator)

**Overlap Region**:
- **Extent**: 13.6°-17.0°E, 47.0°-50.7°N
- **Countries**: Czech Republic, Eastern Austria, Southern Germany

## Storage Operations

### Data Ingestion
```python
from radar_shmu.utils.storage import TimePartitionedStorage

storage = TimePartitionedStorage("storage")

# Store downloaded file
stored_path = storage.store_file(
    file_path="temp_file.hdf",
    timestamp="20250909081500",
    source="shmu",
    product="zmax"
)
```

### Time-based Queries
```python
# Get files for specific time range
files = storage.get_files(
    source="shmu",
    start_time="20250909060000",
    end_time="20250909120000",
    product="zmax"
)

# Get latest files
latest = storage.get_latest_files("shmu", count=24)
```

### Storage Management
```python
# Get storage statistics
stats = storage.get_storage_stats()
print(f"Total files: {stats['total_files']}")
print(f"Storage size: {stats['total_size']/1024/1024:.1f} MB")

# Cleanup old data (keep 30 days)
deleted_count = storage.cleanup_old_files("shmu", keep_days=30)
print(f"Deleted {deleted_count} old files")
```

### Migration from Legacy Systems
```python
# Migrate from old flat cache structure
storage.migrate_existing_data("old_cache_shmu", "shmu")
```

## Performance Optimizations

### Achieved Performance Improvements
Based on `performance_analyzer.py` findings:

1. **Parallel Processing**: ThreadPoolExecutor implementation
   - **Impact**: 4-6x faster downloads
   - **Implementation**: Concurrent file downloads with 6 workers

2. **Storage Optimization**: Time-partitioned structure
   - **Impact**: O(1) timestamp-based access
   - **Benefit**: Eliminates linear cache searches

3. **Memory Efficiency**: Optimized data structures
   - **Impact**: 50-70% memory reduction
   - **Method**: NumPy arrays instead of Python lists

4. **I/O Optimization**: Direct file operations
   - **Impact**: Reduced file system overhead
   - **Method**: Efficient path construction and batch operations

### Identified Optimization Opportunities

1. **cv2.remap for interpolation**: 5-10x speedup over scipy
2. **PIL for PNG export**: 2-3x speedup over matplotlib
3. **Indexed cache metadata**: O(1) cache lookups
4. **Vectorized operations**: Combined NumPy operations
5. **Async downloads**: Further parallelization potential

## Production Commands

### Storage Management
```bash
# Check storage status
python -c "
from radar_shmu.utils.storage import TimePartitionedStorage
print(TimePartitionedStorage('storage').get_storage_stats())
"

# Download and store data
PYTHONPATH=src python -m radar_shmu.sources.shmu

# Run production pipeline
PYTHONPATH=src python scripts/download_matching_data.py --count 24
```

### Quality Assurance
```bash
# Run all tests
python -m pytest tests/ -v --cov=src --cov-fail-under=80

# Validate storage integrity
python -c "
import h5py
from pathlib import Path
for f in Path('storage').glob('**/*.hdf'):
    try:
        with h5py.File(f, 'r') as hf:
            pass
        print(f'✅ {f.name}')
    except Exception as e:
        print(f'❌ {f.name}: {e}')
"

# Performance analysis
python performance_analyzer.py
```

### Configuration Validation
```bash
# Validate configuration
cat config/extent_index.json | python -m json.tool

# Test geographic extents
python -c "
import json
config = json.load(open('config/extent_index.json'))
for source, data in config['sources'].items():
    extent = data['extent']['wgs84']
    print(f'{source}: {extent}')
"
```

## Benefits and Characteristics

### Performance Benefits
- **Fast queries**: O(1) timestamp-based file access
- **Efficient cleanup**: Automatic removal of old partitions
- **Scalable**: Handles years of data without performance degradation
- **Memory efficient**: Minimal metadata overhead

### Operational Benefits
- **Automated maintenance**: Self-cleaning storage with configurable retention
- **Migration support**: Seamless transition from legacy cache systems
- **Monitoring**: Built-in storage statistics and health checks
- **Error resilience**: Graceful handling of corrupt or missing files

### Development Benefits
- **Type safety**: Full type hints with mypy compatibility
- **Testing**: Comprehensive test coverage for storage operations
- **Documentation**: Clear API documentation and examples
- **Modularity**: Pluggable storage backend for different requirements

## Integration Points

### Web Frontend Integration
```python
# Export frontend-ready data structure
{
    "product": "MAX",
    "timestamp": "20250909081500",
    "dimensions": [1560, 2270],
    "extent": [13.6, 23.8, 46.0, 50.7],
    "data": "base64_encoded_array",
    "coordinates": {"lons": [...], "lats": [...]},
    "data_range": [-20.0, 60.0],
    "units": "dBZ"
}
```

### Production Pipeline Integration
```python
# Automated data processing workflow
from radar_shmu.utils.storage import TimePartitionedStorage
from radar_shmu.sources.shmu import SHMURadarSource

storage = TimePartitionedStorage()
shmu = SHMURadarSource()

# Download latest data
files = shmu.download_latest(count=12)

# Process to storage
for file_info in files:
    processed = shmu.process_to_array(file_info['path'])
    # Generated outputs go to outputs/
```

### Monitoring and Alerting
```python
# Storage health monitoring
stats = storage.get_storage_stats()
if stats['total_size'] > 100 * 1024 * 1024:  # 100MB threshold
    cleanup_count = storage.cleanup_old_files("shmu", keep_days=7)
    print(f"🧹 Cleaned up {cleanup_count} old files")
```

This time-partitioned storage architecture provides scalable, efficient data management for high-frequency meteorological radar data with production-ready automation and monitoring capabilities.