#!/usr/bin/env python3
"""
Download SHMU radar HDF data for known working timestamps.
Downloads HDF files for: zmax, cappi2km, etop, pac01
"""

import requests
import json
import os
from datetime import datetime

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

def main():
    """Download SHMU radar data for known working timestamps"""
    print("SHMU Radar Data Downloader - Known Timestamps")
    print("=" * 50)
    
    # Create output directory
    output_dir = 'processed/shmu_hdf_data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Known working timestamps
    known_timestamps = [
        '20250905153500',
        '20250904143500',  # Yesterday
        '20250904014500'   # Example from docs
    ]
    
    # Product types to download
    products = ['zmax', 'cappi2km', 'etop', 'pac01']
    
    successful_downloads = 0
    total_attempts = 0
    
    # Download data for each timestamp and product
    for timestamp in known_timestamps:
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
    
    # Create index of available files
    available_files = []
    for timestamp in known_timestamps:
        for product in products:
            file_path = f"{output_dir}/{product}_{timestamp}.hdf"
            if os.path.exists(file_path):
                available_files.append({
                    'product': product,
                    'timestamp': timestamp,
                    'filename': f"{product}_{timestamp}.hdf",
                    'datetime': f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]} {timestamp[8:10]}:{timestamp[10:12]}"
                })
    
    index_file = f"{output_dir}/available_files.json"
    with open(index_file, 'w') as f:
        json.dump({
            'total_files': len(available_files),
            'products': products,
            'timestamps': known_timestamps,
            'files': available_files
        }, f, indent=2)
    
    print(f"✓ File index saved: {index_file}")
    print(f"\n🎉 Downloaded {len(available_files)} radar HDF files!")
    print(f"Files saved in: {output_dir}/")

if __name__ == "__main__":
    main()