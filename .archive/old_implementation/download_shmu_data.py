#!/usr/bin/env python3
"""
Download SHMU radar HDF data for multiple product types and timestamps.
Downloads HDF files for: zmax, cappi2km, etop, pac01
"""

import requests
import json
import os
from datetime import datetime, timedelta

def generate_timestamps(count=20):
    """Generate last 'count' timestamps in 5-minute intervals"""
    # Start from yesterday at 14:35 when we know data exists
    now = datetime.now() - timedelta(days=1)
    current = now.replace(hour=14, minute=35, second=0, microsecond=0)
    
    timestamps = []
    for i in range(count):
        timestamp = current - timedelta(minutes=5 * i)
        timestamps.append(timestamp.strftime("%Y%m%d%H%M"))
    
    return sorted(timestamps)  # Return in chronological order

def get_shmu_product_urls(timestamp):
    """Get SHMU product URLs for a given timestamp"""
    base_url = "https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp"
    
    # Format date for URL (YYYYMMDD)
    date_str = timestamp[:8]  # YYYYMMDD
    
    # Map product types to composite types
    type_mapping = {
        'zmax': 'PABV',
        'cappi2km': 'PANV',
        'etop': 'PADV', 
        'pac01': 'PASV'
    }
    
    products = {}
    for product_type, composite_type in type_mapping.items():
        products[product_type] = f"{base_url}/{product_type}/{date_str}/T_{composite_type}22_C_LZIB_{timestamp}.hdf"
    
    return products

def download_shmu_hdf(url, save_path):
    """Download SHMU radar HDF file"""
    try:
        print(f"Downloading: {url}")
        response = requests.get(url, timeout=10, verify=False)  # No SSL verification per docs
        response.raise_for_status()
        
        # Save the HDF file
        with open(save_path, 'wb') as f:
            f.write(response.content)
        
        print(f"✓ Saved: {save_path}")
        return True
        
    except Exception as e:
        print(f"✗ Failed to download {url}: {e}")
        return False

def create_metadata_json(timestamps, products):
    """Create metadata JSON for all downloaded products"""
    
    # Load extent metadata
    extent_file = 'processed/radar_extent_metadata.json'
    if os.path.exists(extent_file):
        with open(extent_file, 'r') as f:
            extent_metadata = json.load(f)
    else:
        print("Warning: No extent metadata found. Run utils/extract_extent.py first.")
        extent_metadata = None
    
    metadata = {
        'generated': datetime.now().isoformat(),
        'data_source': 'SHMU Slovakia',
        'file_format': 'transparent PNG',
        'projection': 'EPSG:3857',
        'product_types': {
            'zmax': 'Maximum reflectivity',
            'cappi2km': 'CAPPI at 2km altitude', 
            'etop': 'Echo top height',
            'pac01': 'Precipitation accumulation'
        },
        'timestamps': timestamps,
        'total_images': len(timestamps) * len(products),
        'extent': extent_metadata['coordinate_systems'] if extent_metadata else None,
        'grid_size': extent_metadata['grid_properties']['size'] if extent_metadata else None,
        'usage': 'Overlay transparent PNGs on web maps using Mercator extent'
    }
    
    return metadata

def main():
    """Download SHMU radar data for multiple timestamps and products"""
    print("SHMU Radar Data Downloader")
    print("=" * 40)
    
    # Create output directory
    output_dir = 'processed/shmu_hdf_data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate timestamps (last 20 timestamps in 5-minute intervals)
    timestamps = generate_timestamps(20)
    print(f"Generated {len(timestamps)} timestamps")
    print(f"Time range: {timestamps[0]} to {timestamps[-1]}")
    
    # Product types to download
    products = ['zmax', 'cappi2km', 'etop', 'pac01']
    
    successful_downloads = 0
    total_attempts = 0
    
    # Download data for each timestamp and product
    for timestamp in timestamps:
        print(f"\nProcessing timestamp: {timestamp}")
        
        urls = get_shmu_product_urls(timestamp)
        
        for product in products:
            url = urls[product]
            save_path = f"{output_dir}/{product}_{timestamp}.hdf"
            
            total_attempts += 1
            if download_shmu_hdf(url, save_path):
                successful_downloads += 1
    
    print(f"\n" + "="*50)
    print(f"Download Summary:")
    print(f"  Successful: {successful_downloads}/{total_attempts}")
    print(f"  Success rate: {successful_downloads/total_attempts*100:.1f}%")
    
    # Create metadata
    metadata = create_metadata_json(timestamps, products)
    metadata_file = f"{output_dir}/metadata.json"
    
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"✓ Metadata saved: {metadata_file}")
    
    # Create index of available files
    available_files = []
    for timestamp in timestamps:
        for product in products:
            file_path = f"{output_dir}/{product}_{timestamp}.hdf"
            if os.path.exists(file_path):
                available_files.append({
                    'product': product,
                    'timestamp': timestamp,
                    'filename': f"{product}_{timestamp}.hdf",
                    'datetime': f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]} {timestamp[8:10]}:{timestamp[10:12]}"
                })
    
    index_file = f"{output_dir}/file_index.json"
    with open(index_file, 'w') as f:
        json.dump({
            'total_files': len(available_files),
            'products': products,
            'timestamps': timestamps,
            'files': available_files
        }, f, indent=2)
    
    print(f"✓ File index saved: {index_file}")
    print(f"\n🎉 Downloaded {len(available_files)} radar overlay images!")
    print(f"Files saved in: {output_dir}/")

if __name__ == "__main__":
    main()