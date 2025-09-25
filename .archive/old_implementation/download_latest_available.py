#!/usr/bin/env python3
"""
Download last 10 available SHMU radar timestamps and process them.
Searches backwards from current time to find available data.
"""

import requests
import json
import os
from datetime import datetime, timedelta

def check_timestamp_availability(timestamp, product_type='zmax'):
    """Check if a timestamp has data available by testing one product type"""
    base_url = "https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp"
    date_str = timestamp[:8]  # YYYYMMDD
    
    # Use ZMAX (PABV) as test product
    url = f"{base_url}/{product_type}/{date_str}/T_PABV22_C_LZIB_{timestamp}.hdf"
    
    try:
        response = requests.head(url, timeout=5, verify=False)
        return response.status_code == 200
    except:
        return False

def find_available_timestamps(max_count=10, max_hours_back=48):
    """Find the last N available timestamps by searching backwards"""
    print(f"Searching for last {max_count} available timestamps...")
    print("=" * 50)
    
    available_timestamps = []
    current_time = datetime.now()
    
    # Search backwards in 5-minute intervals
    for minutes_back in range(0, max_hours_back * 60, 5):
        if len(available_timestamps) >= max_count:
            break
            
        # Calculate timestamp
        check_time = current_time - timedelta(minutes=minutes_back)
        # Round down to nearest 5 minutes
        check_time = check_time.replace(minute=(check_time.minute // 5) * 5, second=0, microsecond=0)
        timestamp = check_time.strftime("%Y%m%d%H%M%S")
        
        # Skip if already checked
        if timestamp in available_timestamps:
            continue
            
        print(f"Checking {timestamp}... ", end="", flush=True)
        
        if check_timestamp_availability(timestamp):
            available_timestamps.append(timestamp)
            print(f"✓ Available ({len(available_timestamps)}/{max_count})")
        else:
            print("✗ Not available")
    
    print(f"\nFound {len(available_timestamps)} available timestamps:")
    for i, ts in enumerate(available_timestamps, 1):
        dt = datetime.strptime(ts, "%Y%m%d%H%M%S")
        print(f"  {i:2d}. {ts} - {dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    return available_timestamps

def download_shmu_hdf(url, save_path):
    """Download SHMU radar HDF file"""
    try:
        response = requests.get(url, timeout=10, verify=False)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            f.write(response.content)
        
        return True
    except Exception as e:
        print(f"✗ Failed: {e}")
        return False

def download_timestamps(timestamps):
    """Download all products for the given timestamps"""
    print(f"\nDownloading data for {len(timestamps)} timestamps...")
    print("=" * 50)
    
    # Create output directory
    output_dir = 'processed/latest_shmu_data'
    os.makedirs(output_dir, exist_ok=True)
    
    base_url = "https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp"
    
    # Product types to download
    product_mapping = {
        'zmax': 'PABV',
        'cappi2km': 'PANV',
        'etop': 'PADV', 
        'pac01': 'PASV'
    }
    
    successful_downloads = 0
    total_attempts = 0
    downloaded_files = []
    
    for timestamp in timestamps:
        print(f"\nProcessing timestamp: {timestamp}")
        date_str = timestamp[:8]
        
        for product_type, composite_type in product_mapping.items():
            url = f"{base_url}/{product_type}/{date_str}/T_{composite_type}22_C_LZIB_{timestamp}.hdf"
            save_path = f"{output_dir}/{product_type}_{timestamp}.hdf"
            
            print(f"  {product_type:10s} ... ", end="", flush=True)
            
            total_attempts += 1
            if download_shmu_hdf(url, save_path):
                successful_downloads += 1
                downloaded_files.append(save_path)
                print("✓")
            else:
                print("✗")
    
    # Save download results
    results = {
        'download_time': datetime.now().isoformat(),
        'timestamps': timestamps,
        'total_attempts': total_attempts,
        'successful_downloads': successful_downloads,
        'success_rate': f"{successful_downloads/total_attempts*100:.1f}%",
        'downloaded_files': downloaded_files
    }
    
    results_file = f"{output_dir}/download_results.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n" + "=" * 50)
    print(f"Download Summary:")
    print(f"  Successful: {successful_downloads}/{total_attempts}")
    print(f"  Success rate: {successful_downloads/total_attempts*100:.1f}%")
    print(f"  Results saved: {results_file}")
    
    return downloaded_files

def main():
    """Find and download the last 10 available timestamps"""
    print("SHMU Latest Available Data Downloader")
    print("=" * 50)
    
    # Find available timestamps
    available_timestamps = find_available_timestamps(max_count=10)
    
    if not available_timestamps:
        print("No available timestamps found!")
        return
    
    # Download the data
    downloaded_files = download_timestamps(available_timestamps)
    
    print(f"\n🎉 Downloaded {len(downloaded_files)} radar files!")
    print(f"Files saved in: processed/latest_shmu_data/")

if __name__ == "__main__":
    main()