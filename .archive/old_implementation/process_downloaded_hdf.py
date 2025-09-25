#!/usr/bin/env python3
"""
Process downloaded SHMU HDF files to generate transparent PNG overlays.
Creates visualizations using the official SHMU colorscale.
"""

import os
import json
import glob
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

from radar_processor import SHMURadarProcessor
from shmu_colormap import get_shmu_colormap

def process_hdf_files():
    """Process all downloaded HDF files to create PNG overlays"""
    print("Processing downloaded HDF files...")
    print("=" * 40)
    
    # Initialize processor
    processor = SHMURadarProcessor()
    
    # Find all downloaded HDF files
    hdf_dir = 'processed/shmu_hdf_data'
    hdf_files = glob.glob(f"{hdf_dir}/*.hdf")
    
    if not hdf_files:
        print("No HDF files found to process!")
        return
    
    print(f"Found {len(hdf_files)} HDF files to process")
    
    # Create output directory for PNGs
    png_dir = 'processed/shmu_png_overlays'
    os.makedirs(png_dir, exist_ok=True)
    
    processed_files = []
    
    for hdf_file in hdf_files:
        filename = os.path.basename(hdf_file)
        print(f"\nProcessing: {filename}")
        
        try:
            # Process the HDF file
            radar_data = processor.process_for_frontend(hdf_file)
            
            # Determine product type from filename
            if 'zmax_' in filename:
                product_type = 'ZMAX'
                colormap, norm = get_shmu_colormap()
                units = 'dBZ'
                vmin, vmax = -35, 85
            elif 'cappi2km_' in filename:
                product_type = 'CAPPI 2km'
                colormap, norm = get_shmu_colormap()
                units = 'dBZ'
                vmin, vmax = -35, 85
            elif 'etop_' in filename:
                product_type = 'Echo Top'
                colormap = plt.cm.viridis  # Use viridis for echo top
                norm = None  # Use default normalization
                units = 'km'
                vmin, vmax = 0, 20
            elif 'pac01_' in filename:
                product_type = 'Precipitation'
                colormap = plt.cm.Blues  # Use Blues colormap for precipitation
                norm = None  # Use default normalization
                units = 'mm'
                vmin, vmax = 0, 50
            else:
                print(f"Unknown product type in {filename}")
                continue
            
            # Create visualization
            fig, ax = plt.subplots(figsize=(15, 20), dpi=150)
            
            # Load extent metadata if available
            extent_file = 'processed/radar_extent_metadata.json'
            if os.path.exists(extent_file):
                with open(extent_file, 'r') as f:
                    extent_metadata = json.load(f)
                mercator_extent = extent_metadata['coordinate_systems']['mercator']['bounds']
            else:
                # Fallback to WGS84 extent (less ideal for overlay)
                mercator_extent = radar_data['extent']
            
            # Plot radar data
            if norm is not None:
                # Use custom normalization (for SHMU reflectivity products)
                im = ax.imshow(
                    radar_data['data'], 
                    extent=mercator_extent,
                    origin='upper',
                    cmap=colormap,
                    norm=norm,
                    alpha=0.8,  # Make slightly transparent
                    interpolation='nearest'
                )
            else:
                # Use standard min/max normalization
                im = ax.imshow(
                    radar_data['data'], 
                    extent=mercator_extent,
                    origin='upper',
                    cmap=colormap,
                    vmin=vmin,
                    vmax=vmax,
                    alpha=0.8,  # Make slightly transparent
                    interpolation='nearest'
                )
            
            # Set map extent and projection
            ax.set_xlim(mercator_extent[0], mercator_extent[1])
            ax.set_ylim(mercator_extent[2], mercator_extent[3])
            
            # Remove axes for clean overlay
            ax.set_xticks([])
            ax.set_yticks([])
            ax.axis('off')
            
            # Save as transparent PNG
            png_filename = filename.replace('.hdf', '.png')
            png_path = os.path.join(png_dir, png_filename)
            
            plt.savefig(
                png_path, 
                dpi=150, 
                bbox_inches='tight', 
                transparent=True,
                pad_inches=0
            )
            
            plt.close()
            
            print(f"✓ Created: {png_filename}")
            
            # Store processed file info
            processed_files.append({
                'hdf_file': filename,
                'png_file': png_filename,
                'product_type': product_type,
                'timestamp': radar_data['timestamp'],
                'units': units,
                'data_range': radar_data['data_range'],
                'extent_mercator': mercator_extent
            })
            
        except Exception as e:
            print(f"✗ Error processing {filename}: {e}")
            continue
    
    # Save processing results
    results_file = os.path.join(png_dir, 'processing_results.json')
    with open(results_file, 'w') as f:
        json.dump({
            'processed_files': len(processed_files),
            'output_directory': png_dir,
            'files': processed_files
        }, f, indent=2)
    
    print(f"\n" + "=" * 40)
    print(f"Processing Summary:")
    print(f"  Processed: {len(processed_files)}/{len(hdf_files)} files")
    print(f"  PNG files saved in: {png_dir}/")
    print(f"  Results saved: {results_file}")

if __name__ == "__main__":
    process_hdf_files()