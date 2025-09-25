#!/usr/bin/env python3
"""
Process all downloaded SHMU HDF files to create Web Mercator PNG overlays.
Creates transparent PNG files ready for web mapping with extent definition.
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

def get_mercator_bounds():
    """Get Web Mercator bounds for SHMU radar coverage"""
    # SHMU coverage in WGS84
    west, east = 13.6, 23.8
    south, north = 46.0, 50.7
    
    # Convert to Web Mercator
    x_min, y_min = lonlat_to_mercator(west, south)
    x_max, y_max = lonlat_to_mercator(east, north)
    
    return [x_min, x_max, y_min, y_max]

def create_transparent_overlay(radar_data, product_type, output_path, bounds):
    """Create PNG overlay with transparent no-data for web mapping"""
    
    # Get data and create no-data mask
    data = np.array(radar_data['data'])
    
    if product_type in ['ZMAX', 'CAPPI 2km']:
        # Reflectivity products - values below -30 dBZ are no-data
        nodata_mask = np.isnan(data) | (data < -30)
        colormap, norm = get_shmu_colormap()
    elif product_type == 'Echo Top':
        # Echo top - values below 0.5 km are no-data  
        nodata_mask = np.isnan(data) | (data < 0.5)
        colormap = plt.cm.viridis
        norm = plt.Normalize(vmin=0, vmax=20)
    elif product_type == 'Precipitation':
        # Precipitation - negative values are no-data
        nodata_mask = np.isnan(data) | (data < 0)
        colormap = plt.cm.Blues  
        norm = plt.Normalize(vmin=0, vmax=50)
    else:
        nodata_mask = np.isnan(data)
        colormap = plt.cm.viridis
        norm = None
    
    # Create figure
    fig_height = 8
    fig_width = fig_height * (bounds[1] - bounds[0]) / (bounds[3] - bounds[2])
    
    fig, ax = plt.subplots(figsize=(fig_width, fig_height), dpi=150)
    
    # Plot with transparency for no-data
    masked_data = np.ma.masked_array(data, mask=nodata_mask)
    
    if norm is not None:
        im = ax.imshow(
            masked_data,
            extent=bounds,
            origin='upper',
            cmap=colormap,
            norm=norm,
            interpolation='nearest'
        )
    else:
        im = ax.imshow(
            masked_data,
            extent=bounds,
            origin='upper', 
            cmap=colormap,
            interpolation='nearest'
        )
    
    # Set bounds and remove axes
    ax.set_xlim(bounds[0], bounds[1])
    ax.set_ylim(bounds[2], bounds[3])
    ax.set_xticks([])
    ax.set_yticks([])
    ax.axis('off')
    
    # Save with transparency
    plt.savefig(
        output_path,
        dpi=150,
        bbox_inches='tight',
        transparent=True,
        pad_inches=0
    )
    
    plt.close()

def main():
    """Process all downloaded HDF files to create web map overlays"""
    print("Creating All Web Mapping Overlays")
    print("=" * 50)
    
    # Get Web Mercator bounds
    bounds = get_mercator_bounds()
    print(f"Web Mercator bounds: {bounds}")
    
    # Initialize processor
    processor = SHMURadarProcessor()
    
    # Find all HDF files
    hdf_dir = 'processed/latest_shmu_data'
    hdf_files = sorted(glob.glob(f"{hdf_dir}/*.hdf"))
    
    if not hdf_files:
        print("No HDF files found!")
        return
    
    # Create output directory
    output_dir = 'processed/all_webmap_overlays'
    os.makedirs(output_dir, exist_ok=True)
    
    # Create extent file once
    extent_info = {
        'name': 'SHMU Slovakia Radar Coverage',
        'projection': 'EPSG:3857',
        'bounds': bounds,
        'wgs84_coverage': {
            'west': 13.6, 'east': 23.8,
            'south': 46.0, 'north': 50.7
        },
        'grid_size': [1560, 2270],
        'pixel_resolution_meters': 500,
        'usage': {
            'leaflet': {
                'bounds': [[46.0, 13.6], [50.7, 23.8]],
                'example': "L.imageOverlay('radar.png', bounds).addTo(map);"
            },
            'openlayers': {
                'extent': bounds,
                'example': "new ol.layer.Image({source: new ol.source.ImageStatic({url: 'radar.png', extent: extent})})"
            }
        }
    }
    
    extent_file = os.path.join(output_dir, 'extent_definition.json')
    with open(extent_file, 'w') as f:
        json.dump(extent_info, f, indent=2)
    
    print(f"✓ Created: {os.path.basename(extent_file)}")
    
    # Process all HDF files
    processed_overlays = []
    failed_files = []
    
    print(f"\nProcessing {len(hdf_files)} HDF files...")
    
    for hdf_file in hdf_files:
        filename = os.path.basename(hdf_file)
        
        # Parse filename to get product type and timestamp
        parts = filename.replace('.hdf', '').split('_')
        if len(parts) >= 2:
            product_key = parts[0]
            timestamp = parts[1]
        else:
            print(f"Skipping invalid filename: {filename}")
            continue
        
        # Map product types
        product_mapping = {
            'zmax': 'ZMAX',
            'cappi2km': 'CAPPI 2km',
            'etop': 'Echo Top', 
            'pac01': 'Precipitation'
        }
        
        product_type = product_mapping.get(product_key)
        if not product_type:
            print(f"Unknown product type: {product_key}")
            continue
        
        print(f"  {filename} -> ", end="", flush=True)
        
        try:
            # Process HDF file
            radar_data = processor.process_for_frontend(hdf_file)
            
            # Create PNG filename
            png_filename = f"{product_key}_{timestamp}.png"
            png_path = os.path.join(output_dir, png_filename)
            
            # Create overlay
            create_transparent_overlay(radar_data, product_type, png_path, bounds)
            
            print(f"✓ {png_filename}")
            
            processed_overlays.append({
                'product': product_type,
                'product_key': product_key,
                'timestamp': timestamp,
                'filename': png_filename,
                'data_range': radar_data['data_range'],
                'units': radar_data['units']
            })
            
        except Exception as e:
            print(f"✗ Error: {e}")
            failed_files.append({'file': filename, 'error': str(e)})
    
    # Create summary file
    summary = {
        'created': datetime.now().isoformat(),
        'total_processed': len(processed_overlays),
        'total_failed': len(failed_files),
        'extent_file': 'extent_definition.json',
        'bounds': bounds,
        'overlays': processed_overlays
    }
    
    if failed_files:
        summary['failed_files'] = failed_files
    
    summary_file = os.path.join(output_dir, 'overlay_catalog.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n" + "=" * 50)
    print(f"All Web Map Overlays Created!")
    print(f"  Directory: {output_dir}/")
    print(f"  Successfully processed: {len(processed_overlays)}")
    if failed_files:
        print(f"  Failed: {len(failed_files)}")
    print(f"  Extent definition: extent_definition.json")
    print(f"  Overlay catalog: overlay_catalog.json")
    print(f"\n📍 Web Mercator Bounds: {bounds}")
    print(f"🗺️  Ready for web mapping!")

if __name__ == "__main__":
    main()