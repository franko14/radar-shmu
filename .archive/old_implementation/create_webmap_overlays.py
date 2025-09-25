#!/usr/bin/env python3
"""
Create Web Mercator PNG overlays for web mapping with transparent no-data.
Generates PNG files and extent definition for web mapping applications.
"""

import os
import json
import glob
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

from radar_processor import SHMURadarProcessor  
from shmu_colormap import get_shmu_colormap

def lonlat_to_mercator(lon, lat):
    """Convert WGS84 coordinates to Web Mercator (EPSG:3857)"""
    import math
    x = lon * 20037508.34 / 180.0
    y = math.log(math.tan((90.0 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
    y = y * 20037508.34 / 180.0
    return x, y

def get_shmu_extent():
    """Get the extent information for SHMU radar data"""
    # SHMU radar coverage bounds in WGS84
    wgs84 = {
        'west': 13.6,   # lon_min
        'east': 23.8,   # lon_max  
        'south': 46.0,  # lat_min
        'north': 50.7   # lat_max
    }
    
    # Convert to Web Mercator
    x_min, y_min = lonlat_to_mercator(wgs84['west'], wgs84['south'])
    x_max, y_max = lonlat_to_mercator(wgs84['east'], wgs84['north'])
    
    return {
        'wgs84': wgs84,
        'mercator': {
            'x_min': x_min,
            'x_max': x_max, 
            'y_min': y_min,
            'y_max': y_max,
            'bounds': [x_min, x_max, y_min, y_max]  # [west, east, south, north]
        },
        'projection': 'EPSG:3857'
    }

def create_transparent_png(radar_data, product_type, output_path, mercator_bounds):
    """Create PNG with transparent no-data areas for web overlay"""
    
    # Get data array and handle no-data values
    data = np.array(radar_data['data'])
    
    # Create mask for no-data values (typically NaN or very low values)
    if product_type in ['ZMAX', 'CAPPI 2km']:
        # For reflectivity, values below -30 dBZ are typically no-data
        nodata_mask = np.isnan(data) | (data < -30)
        colormap, norm = get_shmu_colormap()
    elif product_type == 'Echo Top':
        # For echo top, values below 0.5 km are typically no-data
        nodata_mask = np.isnan(data) | (data < 0.5)
        colormap = plt.cm.viridis
        norm = plt.Normalize(vmin=0, vmax=20)
    elif product_type == 'Precipitation':
        # For precipitation, negative values are no-data
        nodata_mask = np.isnan(data) | (data < 0)
        colormap = plt.cm.Blues
        norm = plt.Normalize(vmin=0, vmax=50)
    else:
        nodata_mask = np.isnan(data)
        colormap = plt.cm.viridis
        norm = None
    
    # Create figure matching the data dimensions
    fig_height = 8
    fig_width = fig_height * (mercator_bounds[1] - mercator_bounds[0]) / (mercator_bounds[3] - mercator_bounds[2])
    
    fig, ax = plt.subplots(figsize=(fig_width, fig_height), dpi=150)
    
    # Plot the data
    if norm is not None:
        im = ax.imshow(
            data,
            extent=mercator_bounds,  # [x_min, x_max, y_min, y_max] 
            origin='upper',
            cmap=colormap,
            norm=norm,
            interpolation='nearest'
        )
    else:
        im = ax.imshow(
            data,
            extent=mercator_bounds,
            origin='upper',
            cmap=colormap,
            interpolation='nearest'
        )
    
    # Make no-data areas transparent
    im.set_array(np.ma.masked_array(data, mask=nodata_mask))
    
    # Set exact bounds
    ax.set_xlim(mercator_bounds[0], mercator_bounds[1])
    ax.set_ylim(mercator_bounds[2], mercator_bounds[3])
    
    # Remove axes for clean overlay
    ax.set_xticks([])
    ax.set_yticks([])
    ax.axis('off')
    
    # Save as PNG with transparency
    plt.savefig(
        output_path,
        dpi=150,
        bbox_inches='tight',
        transparent=True,
        pad_inches=0
    )
    
    plt.close()

def create_extent_definition(extent_info, output_dir):
    """Create extent definition file for web mapping"""
    
    extent_file = os.path.join(output_dir, 'radar_extent.json')
    
    extent_def = {
        'name': 'SHMU Slovakia Radar Coverage',
        'projection': extent_info['projection'],
        'wgs84_bounds': {
            'west': extent_info['wgs84']['west'],
            'east': extent_info['wgs84']['east'], 
            'south': extent_info['wgs84']['south'],
            'north': extent_info['wgs84']['north']
        },
        'mercator_bounds': {
            'x_min': extent_info['mercator']['x_min'],
            'x_max': extent_info['mercator']['x_max'],
            'y_min': extent_info['mercator']['y_min'], 
            'y_max': extent_info['mercator']['y_max'],
            'bounds_array': extent_info['mercator']['bounds']
        },
        'usage': {
            'description': 'Use these bounds to position PNG overlays in web maps',
            'leaflet_example': {
                'bounds': [
                    [extent_info['wgs84']['south'], extent_info['wgs84']['west']], 
                    [extent_info['wgs84']['north'], extent_info['wgs84']['east']]
                ],
                'code': "L.imageOverlay('radar.png', bounds).addTo(map);"
            },
            'openlayers_example': {
                'extent': extent_info['mercator']['bounds'],
                'code': "new ol.layer.Image({source: new ol.source.ImageStatic({url: 'radar.png', extent: extent})})"
            }
        },
        'grid_info': {
            'pixel_width': (extent_info['mercator']['x_max'] - extent_info['mercator']['x_min']) / 2270,
            'pixel_height': (extent_info['mercator']['y_max'] - extent_info['mercator']['y_min']) / 1560,
            'grid_size': [1560, 2270],  # [height, width]
            'resolution_meters': 'Approximately 500m per pixel'
        }
    }
    
    with open(extent_file, 'w') as f:
        json.dump(extent_def, f, indent=2)
    
    return extent_file

def main():
    """Create Web Mercator PNG overlays for web mapping"""
    print("Creating Web Mercator PNG Overlays")
    print("=" * 50)
    
    # Get extent information
    extent_info = get_shmu_extent()
    mercator_bounds = extent_info['mercator']['bounds']
    
    print(f"SHMU Radar Extent:")
    print(f"  WGS84: {extent_info['wgs84']['west']:.1f}°W to {extent_info['wgs84']['east']:.1f}°E, {extent_info['wgs84']['south']:.1f}°S to {extent_info['wgs84']['north']:.1f}°N")
    print(f"  Mercator: X={mercator_bounds[0]:.0f} to {mercator_bounds[1]:.0f}, Y={mercator_bounds[2]:.0f} to {mercator_bounds[3]:.0f}")
    
    # Initialize processor
    processor = SHMURadarProcessor()
    
    # Find latest HDF files
    hdf_dir = 'processed/latest_shmu_data'
    hdf_files = glob.glob(f"{hdf_dir}/*.hdf")
    
    if not hdf_files:
        print("No HDF files found!")
        return
    
    # Create output directory
    output_dir = 'processed/webmap_overlays'
    os.makedirs(output_dir, exist_ok=True)
    
    # Create extent definition file
    extent_file = create_extent_definition(extent_info, output_dir)
    print(f"✓ Created extent file: {os.path.basename(extent_file)}")
    
    # Process latest timestamp for each product type  
    latest_files = {}
    for hdf_file in hdf_files:
        filename = os.path.basename(hdf_file)
        timestamp = filename.split('_')[1].replace('.hdf', '')
        
        if 'zmax_' in filename:
            product_key = 'zmax'
        elif 'cappi2km_' in filename:
            product_key = 'cappi2km'
        elif 'etop_' in filename:
            product_key = 'etop'
        elif 'pac01_' in filename:
            product_key = 'pac01'
        else:
            continue
            
        if product_key not in latest_files or timestamp > latest_files[product_key][1]:
            latest_files[product_key] = (hdf_file, timestamp)
    
    processed_overlays = []
    
    print(f"\nProcessing {len(latest_files)} product types:")
    for product_key, (hdf_file, timestamp) in latest_files.items():
        filename = os.path.basename(hdf_file)
        print(f"\n  Processing: {filename}")
        
        try:
            # Process HDF
            radar_data = processor.process_for_frontend(hdf_file)
            
            # Map product types
            product_types = {
                'zmax': 'ZMAX',
                'cappi2km': 'CAPPI 2km', 
                'etop': 'Echo Top',
                'pac01': 'Precipitation'
            }
            product_type = product_types.get(product_key, product_key)
            
            # Create PNG
            png_filename = f"{product_key}_{timestamp}.png"
            png_path = os.path.join(output_dir, png_filename)
            
            create_transparent_png(radar_data, product_type, png_path, mercator_bounds)
            
            print(f"    ✓ Created: {png_filename}")
            
            processed_overlays.append({
                'product': product_type,
                'filename': png_filename,
                'timestamp': timestamp,
                'data_range': radar_data['data_range'],
                'units': radar_data['units']
            })
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
    
    # Save overlay summary
    summary = {
        'created': datetime.now().isoformat(),
        'extent_file': os.path.basename(extent_file),
        'projection': 'EPSG:3857',
        'bounds': mercator_bounds,
        'overlays': processed_overlays
    }
    
    summary_file = os.path.join(output_dir, 'overlay_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n" + "=" * 50)
    print(f"Web Mapping Overlays Created!")
    print(f"  Directory: {output_dir}/")
    print(f"  Extent file: {os.path.basename(extent_file)}")
    print(f"  PNG overlays: {len(processed_overlays)}")
    print(f"  Summary: {os.path.basename(summary_file)}")
    print(f"\nWeb Mercator Bounds: {mercator_bounds}")
    print(f"Ready for web mapping applications! 🗺️")

if __name__ == "__main__":
    main()