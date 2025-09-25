#!/usr/bin/env python3
"""
Download Matching Timestamps Script

This script finds and downloads radar data from both SHMU and DWD 
for the same timestamps to enable meaningful comparison.
"""

import sys
import os
import requests
import re
from datetime import datetime, timedelta
from pathlib import Path
import argparse

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def clear_cache():
    """Clear existing cached data"""
    cache_dirs = [
        "processed/shmu_hdf_data",
        "processed/dwd_hdf_data"
    ]
    
    for cache_dir in cache_dirs:
        cache_path = Path(cache_dir)
        if cache_path.exists():
            print(f"🧹 Clearing cache: {cache_dir}")
            for file in cache_path.glob("*"):
                file.unlink()

def get_dwd_available_timestamps(product='dmax', max_count=50):
    """Get available timestamps from DWD server"""
    
    url = f"https://opendata.dwd.de/weather/radar/composite/{product}/"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        # Extract timestamps from HTML
        pattern = rf'composite_{product}_(\d{{8}}_\d{{4}})-hd5'
        matches = re.findall(pattern, response.text)
        
        # Sort newest first and limit count
        timestamps = sorted(set(matches), reverse=True)[:max_count]
        
        print(f"✅ Found {len(timestamps)} DWD timestamps")
        return timestamps
        
    except Exception as e:
        print(f"❌ Failed to get DWD timestamps: {e}")
        return []

def convert_dwd_to_shmu_format(dwd_timestamp):
    """Convert DWD timestamp format to SHMU format"""
    # DWD: 20250909_0735 -> SHMU: 20250909073500
    date_part, time_part = dwd_timestamp.split('_')
    return f"{date_part}{time_part}00"

def check_shmu_available(timestamp):
    """Check if SHMU data is available for timestamp"""
    products = ['zmax', 'cappi2km']
    base_url = "https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp"
    
    product_mapping = {
        'zmax': 'PABV',
        'cappi2km': 'PANV'
    }
    
    for product in products:
        composite_type = product_mapping[product]
        date_str = timestamp[:8]
        url = f"{base_url}/{product}/{date_str}/T_{composite_type}22_C_LZIB_{timestamp}.hdf"
        
        try:
            response = requests.head(url, timeout=5, verify=False)
            if response.status_code == 200:
                return True
        except:
            continue
    
    return False

def find_matching_timestamps(count=5):
    """Find timestamps where both SHMU and DWD have data"""
    
    print(f"🔍 Looking for {count} matching timestamps...")
    
    dwd_timestamps = get_dwd_available_timestamps()
    if not dwd_timestamps:
        return []
    
    matching = []
    
    for dwd_ts in dwd_timestamps:
        if len(matching) >= count:
            break
            
        # Convert to SHMU format
        shmu_ts = convert_dwd_to_shmu_format(dwd_ts)
        
        print(f"🔍 Checking: DWD {dwd_ts} -> SHMU {shmu_ts}")
        
        # Check if SHMU has data for this timestamp
        if check_shmu_available(shmu_ts):
            matching.append({
                'dwd': dwd_ts,
                'shmu': shmu_ts,
                'datetime': datetime.strptime(shmu_ts, '%Y%m%d%H%M%S')
            })
            print(f"✅ Match found: {dwd_ts} <-> {shmu_ts}")
    
    return matching

def download_matching_data(matching_timestamps, output_dir="outputs/matched_comparison"):
    """Download data for matching timestamps"""
    
    if not matching_timestamps:
        print("❌ No matching timestamps found")
        return False
    
    print(f"\n📥 Downloading data for {len(matching_timestamps)} matching timestamps...")
    
    # Import radar sources
    from radar_shmu.sources.shmu import SHMURadarSource
    from radar_shmu.sources.dwd import DWDRadarSource
    from radar_shmu.processing.merger import RadarMerger
    
    # Download SHMU data for matching timestamps
    shmu_source = SHMURadarSource()
    dwd_source = DWDRadarSource()
    
    # Clear cache first
    clear_cache()
    
    print("📡 Downloading SHMU data...")
    shmu_products = ['zmax', 'cappi2km']
    shmu_files = []
    
    for match in matching_timestamps:
        shmu_ts = match['shmu']
        for product in shmu_products:
            try:
                # This will check cache first, then download
                files = shmu_source.download_latest(count=1, products=[product])
                shmu_files.extend(files)
            except Exception as e:
                print(f"⚠️  SHMU download failed for {shmu_ts}: {e}")
    
    print("📡 Downloading DWD data...")
    dwd_files = []
    
    for match in matching_timestamps:
        try:
            files = dwd_source.download_latest(count=1, products=['dmax'])
            dwd_files.extend(files)
        except Exception as e:
            print(f"⚠️  DWD download failed: {e}")
    
    print(f"✅ Downloaded {len(shmu_files)} SHMU files, {len(dwd_files)} DWD files")
    
    # Process the downloaded data
    merger = RadarMerger()
    
    print(f"🔄 Processing matched data...")
    try:
        processor.process(
            source_names=['shmu', 'dwd'],
            count=len(matching_timestamps),
            merge_data=True
        )
        print(f"✅ Processing complete! Check {output_dir}")
        return True
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Download matching radar data timestamps')
    parser.add_argument('--count', type=int, default=3, 
                       help='Number of matching timestamps to find (default: 3)')
    parser.add_argument('--output', type=str, default='outputs/matched_comparison',
                       help='Output directory for processed data')
    parser.add_argument('--clear-cache', action='store_true',
                       help='Clear existing cache before downloading')
    
    args = parser.parse_args()
    
    print("🎯 Matching Timestamp Downloader")
    print("=" * 40)
    
    if args.clear_cache:
        clear_cache()
    
    # Find matching timestamps
    matching = find_matching_timestamps(args.count)
    
    if not matching:
        print("❌ No matching timestamps found between SHMU and DWD")
        return False
    
    print(f"\n📋 Found {len(matching)} matching timestamp pairs:")
    for match in matching:
        dt = match['datetime']
        print(f"  {match['dwd']} <-> {match['shmu']} ({dt.strftime('%Y-%m-%d %H:%M:%S')} UTC)")
    
    # Download and process
    success = download_matching_data(matching, args.output)
    
    if success:
        print(f"\n🎉 Success! Check {args.output} for synchronized radar data")
        print("📊 Both SHMU and DWD data are from the same timestamps for comparison")
    else:
        print("\n❌ Failed to download matching data")
    
    return success

if __name__ == "__main__":
    main()