# SHMU Radar Data Processing Documentation

## Overview

This project processes Slovak Hydrometeorological Institute (SHMU) radar data using PyArt and prepares it for JavaScript frontend consumption. The data follows the ODIM_H5 standard and provides various radar products including reflectivity, precipitation estimates, and echo top heights.

## Data Sources

**Format Documentation**: [ODIM_H5_v2.4.pdf](https://www.eumetnet.eu/wp-content/uploads/2021/07/ODIM_H5_v2.4.pdf)

**API Endpoint** (HTTP only - no SSL):
```
https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp/{type}/{yyyymmdd}/T_{composite_type}22_C_LZIB_{yyyymmddHHMMSS}.hdf
```

### Product Types

| Type | Composite Type | Slovak Name | English Description |
|------|---------------|-------------|-------------------|
| `zmax` | `PABV` | Maximálna odrazivosť v stĺpci | Column Maximum Reflectivity (ZMAX) |
| `cappi2km` | `PANV` | Odrazivosť vo výške 2 km | Reflectivity at 2km altitude (CAPPI 2km) |
| `etop` | `PADV` | Horná hranica odrazivosti | Echo Top Height |
| `pac01` | `PASV` | 1h kumulovaný odhad zrážok | 1-hour Accumulated Precipitation |

### Example URLs

For timestamp `20250904014500`:

```bash
# ZMAX (Maximum Reflectivity)
https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp/zmax/20250904/T_PABV22_C_LZIB_20250904014500.hdf

# CAPPI 2km
https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp/cappi2km/20250904/T_PANV22_C_LZIB_20250904014500.hdf

# Echo Top Height
https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp/etop/20250904/T_PADV22_C_LZIB_20250904014500.hdf

# 1h Accumulated Precipitation
https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp/pac01/20250904/T_PASV22_C_LZIB_20250904014500.hdf
```

## Technical Specifications

- **Grid Size**: 2270 × 1560 pixels
- **Geographic Coverage**: Slovakia and surrounding areas
- **Projection**: Mercator (+proj=merc +lon_0=18.7 +lat_0=0 +lon_ts=0 +lat_ts=48.43 +ellps=sphere)
- **Coordinate Range**: 13.6°-23.8°E, 46.0°-50.7°N
- **Resolution**: ~330m × 480m
- **Radar Network**: Multi-radar composite (CZSKA, SKJAV, SKKOJ, SKLAZ, SKKUB)
- **Data Format**: HDF5 with ODIM_H5 structure

## PyArt Integration

### Installation Requirements

```bash
pip install pyart h5py numpy matplotlib cartopy
```

### Minimal Working Example

```python
import numpy as np
import h5py
import json
from radar_processor import SHMURadarProcessor

# Initialize processor
processor = SHMURadarProcessor()

# Process SHMU radar file
radar_data = processor.process_for_frontend('T_PABV22_C_LZIB_20250904014500.hdf')

# Output JavaScript-ready data
with open('radar_data.json', 'w') as f:
    json.dump(radar_data, f)

print(f"Processed {radar_data['product']} data")
print(f"Grid size: {radar_data['dimensions']}")
print(f"Data range: {radar_data['data_range']}")
```

## Data Processing Workflow

### 1. Data Download
```python
import requests
from datetime import datetime

def download_shmu_data(product_type, timestamp):
    """Download SHMU radar data for specified product and timestamp"""
    base_url = "https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp"
    date_str = timestamp[:8]  # YYYYMMDD
    
    type_mapping = {
        'PABV': 'zmax',
        'PANV': 'cappi2km', 
        'PADV': 'etop',
        'PASV': 'pac01'
    }
    
    url = f"{base_url}/{type_mapping[product_type]}/{date_str}/T_{product_type}22_C_LZIB_{timestamp}.hdf"
    
    response = requests.get(url, verify=False)  # No SSL verification
    response.raise_for_status()
    
    filename = f"T_{product_type}22_C_LZIB_{timestamp}.hdf"
    with open(filename, 'wb') as f:
        f.write(response.content)
    
    return filename
```

### 2. Data Processing for Frontend

```python
def process_radar_for_web(hdf_filepath):
    """Process HDF5 radar data for web frontend consumption"""
    
    with h5py.File(hdf_filepath, 'r') as f:
        # Read raw data
        data = f['dataset1/data1/data'][:]
        what_attrs = dict(f['dataset1/what'].attrs)
        where_attrs = dict(f['where'].attrs)
        
        # Apply scaling
        gain = what_attrs.get('gain', 1.0)
        offset = what_attrs.get('offset', 0.0)
        nodata = what_attrs.get('nodata', -32768)
        
        scaled_data = data.astype(np.float32) * gain + offset
        scaled_data[data == 0] = np.nan  # Handle nodata values
        
        # Create coordinate arrays
        ll_lon, ll_lat = where_attrs['LL_lon'], where_attrs['LL_lat']
        ur_lon, ur_lat = where_attrs['UR_lon'], where_attrs['UR_lat']
        
        lons = np.linspace(ll_lon, ur_lon, data.shape[1])
        lats = np.linspace(ur_lat, ll_lat, data.shape[0])  # Flipped for correct orientation
        
        # Prepare frontend-ready data structure
        return {
            'product': what_attrs.get('product', b'').decode(),
            'quantity': what_attrs.get('quantity', b'').decode(),
            'timestamp': what_attrs.get('startdate', b'').decode() + what_attrs.get('starttime', b'').decode(),
            'dimensions': data.shape,
            'projection': where_attrs.get('projdef', b'').decode(),
            'extent': [ll_lon, ur_lon, ll_lat, ur_lat],
            'data': scaled_data.tolist(),  # Convert to JSON-serializable format
            'coordinates': {
                'lons': lons.tolist(),
                'lats': lats.tolist()
            },
            'data_range': [float(np.nanmin(scaled_data)), float(np.nanmax(scaled_data))],
            'units': 'dBZ' if 'DBZH' in what_attrs.get('quantity', b'').decode() else 'mm',
            'nodata_value': float(nodata)
        }
```

### 3. Precipitation Rate Estimation

For reflectivity products (ZMAX, CAPPI), estimate precipitation using Z-R relationship:

```python
def estimate_precipitation_rate(dbz_data):
    """Convert reflectivity (dBZ) to precipitation rate (mm/h) using Marshall-Palmer"""
    
    # Convert dBZ to linear reflectivity factor Z
    z_linear = 10.0 ** (dbz_data / 10.0)
    
    # Apply Z-R relationship: Z = 200 * R^1.6
    # Therefore: R = (Z/200)^(1/1.6)
    precip_rate = np.power(z_linear / 200.0, 1.0/1.6)
    
    # Handle invalid values
    precip_rate[np.isnan(dbz_data) | (dbz_data < -10)] = 0
    
    return precip_rate
```

## JavaScript Frontend Integration

### Data Structure for Frontend

```javascript
// Radar data structure received from Python processing
const radarData = {
    product: "MAX",           // ZMAX, CAPPI, ETOP, RR
    quantity: "DBZH",         // DBZH, HGHT, ACRR
    timestamp: "20250904014500",
    dimensions: [1560, 2270],
    projection: "+proj=merc +lon_0=18.7...",
    extent: [13.6, 23.8, 46.0, 50.7],  // [lon_min, lon_max, lat_min, lat_max]
    data: [[...], [...], ...],          // 2D array of radar values
    coordinates: {
        lons: [...],                     // Longitude array
        lats: [...]                      // Latitude array
    },
    data_range: [-20.0, 60.0],
    units: "dBZ",
    nodata_value: -32768
};
```

### Example: Leaflet.js Integration

```javascript
// Create heatmap layer from radar data
function createRadarLayer(radarData) {
    const heatmapData = [];
    
    for (let i = 0; i < radarData.dimensions[0]; i++) {
        for (let j = 0; j < radarData.dimensions[1]; j++) {
            const value = radarData.data[i][j];
            if (value !== null && !isNaN(value)) {
                heatmapData.push([
                    radarData.coordinates.lats[i],
                    radarData.coordinates.lons[j],
                    value
                ]);
            }
        }
    }
    
    return L.heatLayer(heatmapData, {
        radius: 2,
        blur: 1,
        maxZoom: 10
    });
}
```

## Usage Examples

### Complete Processing Pipeline

```python
from radar_processor import SHMURadarProcessor
import json

def main():
    # Initialize processor
    processor = SHMURadarProcessor()
    
    # Process different radar products
    products = ['PABV', 'PANV', 'PASV']  # ZMAX, CAPPI 2km, Precipitation
    timestamp = '20250904014500'
    
    results = {}
    
    for product in products:
        # Download data (implement download_shmu_data function)
        filename = f"T_{product}22_C_LZIB_{timestamp}.hdf"
        
        # Process for frontend
        radar_data = processor.process_for_frontend(filename)
        
        # Estimate precipitation if reflectivity data
        if radar_data['quantity'] == 'DBZH':
            precip_data = estimate_precipitation_rate(np.array(radar_data['data']))
            radar_data['precipitation_rate'] = precip_data.tolist()
        
        results[product] = radar_data
        
        # Save to JSON for frontend
        with open(f'radar_{product.lower()}_{timestamp}.json', 'w') as f:
            json.dump(radar_data, f, indent=2)
    
    print(f"Processed {len(results)} radar products")
    return results

if __name__ == "__main__":
    main()
```

## Best Practices

### Performance Optimization
- **Data Compression**: Use gzip compression for JSON files
- **Spatial Downsampling**: Reduce grid resolution for web display
- **Value Quantization**: Round values to reduce file size
- **Chunking**: Split large datasets into tiles

### Data Quality
- **Nodata Handling**: Properly mask invalid values
- **Coordinate Validation**: Ensure proper lat/lon ordering  
- **Unit Conversion**: Provide data in appropriate units for visualization
- **Metadata Preservation**: Keep essential radar metadata

### Frontend Integration
- **Coordinate System**: Convert to Web Mercator (EPSG:3857) if needed
- **Color Mapping**: Provide appropriate color scales for each product
- **Animation**: Structure data for time series visualization
- **Caching**: Implement client-side caching for frequently accessed data

## Limitations

- **Individual Radar Data**: Only composite products available, no single radar data
- **CAPPI Altitude**: Altitude level not explicitly stored (assumed 2km)
- **PyArt Compatibility**: Requires custom reader due to SHMU-specific format variations
- **SSL Certificate**: SHMU API requires SSL verification to be disabled

## File Structure

```
radar/
├── DOCUMENTATION.md             # This documentation
├── radar_processor.py           # Main processing script
├── demo_visualization.py        # Map visualization examples
├── sample_data.json            # Sample processed radar data
└── data/                       # Raw HDF5 files (temporary)
```