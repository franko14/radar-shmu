# SHMU Radar Data Processor - MVP

Minimal Viable Product for processing Slovak Hydrometeorological Institute (SHMU) radar data and preparing it for JavaScript frontend consumption.

## 🚀 Quick Start

```bash
# Run the MVP processor
python radar_processor.py

# Create visualizations  
python simple_demo.py
```

## 📁 MVP File Structure

```
radar/
├── README.md                    # This file
├── DOCUMENTATION.md             # Complete documentation
├── radar_processor.py           # Main MVP processor
├── simple_demo.py               # Simple visualization demo
├── sample_data.json             # Data structure examples
├── processed/                   # Processed radar data
│   ├── frontend_index.json     # Frontend integration index
│   ├── radar_*.json            # Individual product data  
│   └── simple_*.png            # Sample visualizations
├── data/                        # Downloaded HDF5 files (temporary)
└── archive/                     # Non-MVP analysis files
```

## 🎯 Key Features

### 1. Automatic Data Processing
- Downloads SHMU radar data from official API
- Processes HDF5 files with proper scaling and coordinate transformation
- Converts to JavaScript-ready JSON format

### 2. Multiple Radar Products
- **ZMAX**: Column maximum reflectivity for precipitation intensity
- **CAPPI 2km**: Horizontal reflectivity at 2km altitude  
- **Precipitation**: Direct 1-hour accumulated precipitation

### 3. Frontend Integration
- Clean JSON data structure optimized for web consumption
- Coordinate arrays for mapping libraries
- Metadata and projection information included

## 📊 Sample Data Structure

```json
{
  "product_name": {"name": "ZMAX", "description": "Column Maximum Reflectivity"},
  "timestamp": "20250904014500", 
  "extent": [13.6, 23.8, 46.0, 50.7],
  "data": [[...], [...], ...],
  "coordinates": {
    "lons": [...],
    "lats": [...]
  },
  "units": "dBZ",
  "data_range": [-24.47, 95.50]
}
```

## 🔧 Usage Examples

### Process New Data
```python
from radar_processor import SHMURadarProcessor

processor = SHMURadarProcessor()

# Process specific timestamp
results = processor.process_multiple_products("20250904014500")

# Access processed data
zmax_data = results['PABV']  # Maximum reflectivity
cappi_data = results['PANV'] # CAPPI 2km  
precip_data = results['PASV'] # Precipitation
```

### Precipitation Rate Estimation
```python
# Automatic Z-R conversion for reflectivity products
if 'precipitation_rate' in zmax_data:
    precip_rate = zmax_data['precipitation_rate']['data']
    print(f"Max precipitation: {max(precip_rate)} mm/h")
```

## 🌐 JavaScript Integration

### Load Data
```javascript
fetch('processed/radar_pabv_20250904014500.json')
  .then(response => response.json())
  .then(radarData => {
    // Use radarData.data for visualization
    // Use radarData.coordinates for mapping
  });
```

### Map Integration (Leaflet.js example)
```javascript
// Create heatmap from radar data
const heatmapData = [];
for (let i = 0; i < radarData.dimensions[0]; i++) {
  for (let j = 0; j < radarData.dimensions[1]; j++) {
    const value = radarData.data[i][j];
    if (value !== null) {
      heatmapData.push([
        radarData.coordinates.lats[i],
        radarData.coordinates.lons[j], 
        value
      ]);
    }
  }
}

const heatLayer = L.heatLayer(heatmapData).addTo(map);
```

## 📈 Data Specifications

- **Coverage**: Slovakia and surrounding areas (13.6°-23.8°E, 46.0°-50.7°N)
- **Resolution**: ~330m × 480m (2270 × 1560 pixels)
- **Projection**: Mercator (+proj=merc +lon_0=18.7)
- **Update Frequency**: Every 5 minutes (operational)
- **Data Source**: Multi-radar composite (5 radar stations)

## ⚡ Performance Notes

- JSON files are large (~90MB each for reflectivity products)
- Consider implementing data compression (gzip) for production
- Spatial downsampling recommended for web display
- Processed data includes coordinate arrays for efficient mapping

## 🔗 API Information

**Base URL**: `https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp/`

**Products Available**:
- `zmax/` - Maximum reflectivity (ZMAX)
- `cappi2km/` - CAPPI 2km reflectivity  
- `pac01/` - 1-hour accumulated precipitation
- `etop/` - Echo top heights

**URL Format**: `{base_url}/{product}/{yyyymmdd}/T_{TYPE}22_C_LZIB_{yyyymmddHHMMSS}.hdf`

## 📝 Complete Documentation

See [DOCUMENTATION.md](DOCUMENTATION.md) for:
- Complete API documentation
- Advanced usage examples  
- Technical specifications
- Best practices for production deployment

## 🏗️ Next Steps

1. **Optimize for Production**:
   - Implement data compression
   - Add spatial downsampling options
   - Create tiled data structure for large datasets

2. **Enhanced Features**:
   - Real-time data streaming
   - Animation support for time series
   - Quality control indicators

3. **Frontend Improvements**:
   - WebGL-based rendering for performance
   - Custom color scales
   - Interactive controls for data exploration

---

*This MVP provides a complete workflow from SHMU radar data to JavaScript-ready formats, enabling rapid development of web-based weather visualization applications.*