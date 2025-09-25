#!/usr/bin/env python3
"""
Process SHMU HDF files to create properly georeferenced PNG overlays in Web Mercator.
Creates PNG files with exact extent bounds compatible with web mapping libraries.
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

def get_mercator_extent():
    """Get the Web Mercator extent for SHMU radar data"""
    # SHMU radar coverage bounds in WGS84
    wgs84_bounds = {
        'lon_min': 13.6,
        'lon_max': 23.8, 
        'lat_min': 46.0,
        'lat_max': 50.7
    }
    
    # Convert to Web Mercator
    x_min, y_min = lonlat_to_mercator(wgs84_bounds['lon_min'], wgs84_bounds['lat_min'])
    x_max, y_max = lonlat_to_mercator(wgs84_bounds['lon_max'], wgs84_bounds['lat_max'])
    
    return [x_min, x_max, y_min, y_max]

def create_georeferenced_png(radar_data, product_type, output_path, mercator_extent):
    """Create a properly georeferenced PNG in Web Mercator projection"""
    
    # Determine colormap and scaling based on product type
    if product_type in ['ZMAX', 'CAPPI 2km']:
        colormap, norm = get_shmu_colormap()
        alpha = 0.8
    elif product_type == 'Echo Top':
        colormap = plt.cm.viridis
        norm = plt.Normalize(vmin=0, vmax=20)
        alpha = 0.7
    elif product_type == 'Precipitation':
        colormap = plt.cm.Blues
        norm = plt.Normalize(vmin=0, vmax=50)
        alpha = 0.8
    else:
        raise ValueError(f"Unknown product type: {product_type}")
    
    # Create figure with exact pixel dimensions for Web Mercator
    # Standard web map tile: 256x256 pixels, but we'll use higher resolution
    fig_width = 10  # inches
    fig_height = 10 * (mercator_extent[3] - mercator_extent[2]) / (mercator_extent[1] - mercator_extent[0])
    
    fig, ax = plt.subplots(figsize=(fig_width, fig_height), dpi=150)
    
    # Plot radar data with Web Mercator extent
    if norm is not None:
        im = ax.imshow(
            radar_data['data'],
            extent=mercator_extent,  # [x_min, x_max, y_min, y_max]
            origin='upper',
            cmap=colormap,
            norm=norm,
            alpha=alpha,
            interpolation='nearest'
        )
    else:
        im = ax.imshow(
            radar_data['data'],
            extent=mercator_extent,
            origin='upper', 
            cmap=colormap,
            alpha=alpha,
            interpolation='nearest'
        )
    
    # Set exact extent for Web Mercator
    ax.set_xlim(mercator_extent[0], mercator_extent[1])
    ax.set_ylim(mercator_extent[2], mercator_extent[3])
    
    # Remove all axes and padding for clean overlay
    ax.set_xticks([])
    ax.set_yticks([])
    ax.axis('off')
    
    # Save as transparent PNG with no padding
    plt.savefig(
        output_path,
        dpi=150,
        bbox_inches='tight',
        transparent=True,
        pad_inches=0,
        facecolor='none',
        edgecolor='none'
    )
    
    plt.close()

def create_world_file(png_path, mercator_extent, grid_size):
    """Create a world file (.pgw) for proper georeferencing"""
    world_file_path = png_path.replace('.png', '.pgw')
    
    # Calculate pixel size in Web Mercator units
    pixel_width = (mercator_extent[1] - mercator_extent[0]) / grid_size[1]  # width
    pixel_height = -(mercator_extent[3] - mercator_extent[2]) / grid_size[0]  # height (negative for image orientation)
    
    # World file format:
    # Line 1: pixel width in map units
    # Line 2: rotation (0)
    # Line 3: rotation (0) 
    # Line 4: pixel height in map units (negative)
    # Line 5: x coordinate of upper left pixel center
    # Line 6: y coordinate of upper left pixel center
    
    with open(world_file_path, 'w') as f:
        f.write(f"{pixel_width}\n")      # pixel width
        f.write("0.0\n")                 # rotation
        f.write("0.0\n")                 # rotation
        f.write(f"{pixel_height}\n")     # pixel height (negative)
        f.write(f"{mercator_extent[0] + pixel_width/2}\n")  # x of upper-left center
        f.write(f"{mercator_extent[3] + pixel_height/2}\n") # y of upper-left center
    
    return world_file_path

def process_mercator_overlays():
    """Process HDF files to create Web Mercator compatible PNG overlays"""
    print("Creating Web Mercator PNG Overlays")
    print("=" * 50)
    
    # Initialize processor
    processor = SHMURadarProcessor()
    
    # Get Web Mercator extent
    mercator_extent = get_mercator_extent()
    print(f"Web Mercator extent: {mercator_extent}")
    print(f"  X: {mercator_extent[0]:.2f} to {mercator_extent[1]:.2f}")
    print(f"  Y: {mercator_extent[2]:.2f} to {mercator_extent[3]:.2f}")
    
    # Find HDF files to process
    hdf_dir = 'processed/latest_shmu_data'
    hdf_files = glob.glob(f"{hdf_dir}/*.hdf")
    
    if not hdf_files:
        print("No HDF files found to process!")
        return
    
    # Create output directory
    output_dir = 'processed/mercator_overlays'
    os.makedirs(output_dir, exist_ok=True)
    
    processed_files = []
    
    # Process a subset for testing (e.g., latest timestamp of each product type)
    latest_files = {}
    for hdf_file in hdf_files:
        filename = os.path.basename(hdf_file)
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
            
        timestamp = filename.split('_')[1].replace('.hdf', '')
        if product_key not in latest_files or timestamp > latest_files[product_key][1]:
            latest_files[product_key] = (hdf_file, timestamp)
    
    print(f"\nProcessing latest files for each product type:")
    for product_key, (hdf_file, timestamp) in latest_files.items():
        filename = os.path.basename(hdf_file)
        print(f"\nProcessing: {filename}")
        
        try:
            # Process HDF file
            radar_data = processor.process_for_frontend(hdf_file)
            
            # Determine product type
            if product_key == 'zmax':
                product_type = 'ZMAX'
            elif product_key == 'cappi2km':
                product_type = 'CAPPI 2km'
            elif product_key == 'etop':
                product_type = 'Echo Top'
            elif product_key == 'pac01':
                product_type = 'Precipitation'
            
            # Create PNG filename
            png_filename = f"{product_key}_{timestamp}_mercator.png"
            png_path = os.path.join(output_dir, png_filename)
            
            # Create georeferenced PNG
            create_georeferenced_png(radar_data, product_type, png_path, mercator_extent)
            
            # Create world file for georeferencing
            world_file = create_world_file(png_path, mercator_extent, radar_data['dimensions'])
            
            print(f"✓ Created: {png_filename}")
            print(f"✓ Created: {os.path.basename(world_file)}")
            
            processed_files.append({
                'product_type': product_type,
                'product_key': product_key,
                'timestamp': timestamp,
                'png_file': png_filename,
                'world_file': os.path.basename(world_file),
                'mercator_extent': mercator_extent,
                'grid_size': radar_data['dimensions'],
                'data_range': radar_data['data_range'],
                'units': radar_data['units']
            })
            
        except Exception as e:
            print(f"✗ Error processing {filename}: {e}")
            continue
    
    # Save processing results with extent information
    extent_info = {
        'processing_time': datetime.now().isoformat(),
        'projection': 'EPSG:3857 (Web Mercator)',
        'mercator_extent': {
            'bounds': mercator_extent,
            'x_min': mercator_extent[0],
            'x_max': mercator_extent[1], 
            'y_min': mercator_extent[2],
            'y_max': mercator_extent[3]
        },
        'wgs84_bounds': {
            'lon_min': 13.6,
            'lon_max': 23.8,
            'lat_min': 46.0, 
            'lat_max': 50.7
        },
        'usage': {
            'description': 'PNG overlays for web mapping applications',
            'extent_format': '[x_min, x_max, y_min, y_max] in Web Mercator meters',
            'example_leaflet': 'L.imageOverlay(url, [[lat_min, lon_min], [lat_max, lon_max]])',
            'example_openlayers': 'extent: [x_min, y_min, x_max, y_max]'
        },
        'processed_files': processed_files
    }
    
    results_file = os.path.join(output_dir, 'mercator_overlay_info.json')
    with open(results_file, 'w') as f:
        json.dump(extent_info, f, indent=2)
    
    print(f"\n" + "=" * 50)
    print(f"Web Mercator Processing Complete!")
    print(f"  Processed: {len(processed_files)} files")
    print(f"  Output directory: {output_dir}/")
    print(f"  Extent info: {results_file}")
    print(f"\nWeb Mercator Extent for overlay positioning:")
    print(f"  Bounds: {mercator_extent}")
    print(f"  Use these bounds in your web mapping application!")

if __name__ == "__main__":
    process_mercator_overlays()