#!/usr/bin/env python3
"""
Create Production Merged Radar Products

Uses existing downloaded SHMU and DWD data to create merged radar products
for all synchronized timestamps in the production directory.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import os
import re
import json
from datetime import datetime
from radar_shmu.sources.shmu import SHMURadarSource
from radar_shmu.sources.dwd import DWDRadarSource
from radar_shmu.processing.merger import RadarMerger
from radar_shmu.processing.exporter import PNGExporter

def create_production_merged():
    """Create merged products from existing production data"""
    
    print("🔀 Creating Production Merged Products")
    print("=" * 50)
    
    # Initialize components
    shmu = SHMURadarSource()
    dwd = DWDRadarSource()
    merger = RadarMerger()
    exporter = PNGExporter()
    
    # Paths
    production_dir = Path("outputs/production/latest_radar_data")
    shmu_dir = production_dir / "shmu"
    dwd_dir = production_dir / "dwd"
    merged_dir = production_dir / "merged"
    
    # Ensure merged directory exists
    merged_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all SHMU timestamps
    shmu_files = {}  # timestamp -> {product -> file_path}
    for png_file in shmu_dir.glob("*.png"):
        # Parse: {product}_{timestamp}.png
        match = re.match(r'([^_]+)_(\d{14})\.png', png_file.name)
        if match:
            product, timestamp = match.groups()
            if timestamp not in shmu_files:
                shmu_files[timestamp] = {}
            shmu_files[timestamp][product] = png_file
    
    # Find all DWD timestamps  
    dwd_files = {}  # timestamp -> {product -> file_path}
    for png_file in dwd_dir.glob("*.png"):
        # Parse: {product}_{timestamp}.png
        match = re.match(r'([^_]+)_(\d+)\.png', png_file.name)
        if match:
            product, timestamp = match.groups()
            if timestamp not in dwd_files:
                dwd_files[timestamp] = {}
            dwd_files[timestamp][product] = png_file
    
    print(f"📊 Found {len(shmu_files)} SHMU timestamps, {len(dwd_files)} DWD timestamps")
    
    # Find synchronized timestamps (convert formats)
    synchronized_pairs = []
    
    for shmu_ts in shmu_files.keys():
        # Convert SHMU format (YYYYMMDDHHMM00) to DWD format (YYYYMMDDHHMM)  
        if len(shmu_ts) == 14:
            dwd_ts = shmu_ts[:12]  # Remove last two digits (seconds)
            if dwd_ts in dwd_files:
                synchronized_pairs.append((shmu_ts, dwd_ts))
    
    print(f"🎯 Found {len(synchronized_pairs)} synchronized timestamp pairs")
    
    if not synchronized_pairs:
        print("⚠️  No synchronized timestamps found - cannot create merged products")
        return
    
    # Process each synchronized timestamp
    strategies = ['max', 'average', 'priority']
    processed_count = 0
    
    for shmu_ts, dwd_ts in synchronized_pairs:
        print(f"\n🔄 Processing {shmu_ts} <-> {dwd_ts}")
        
        try:
            # Load SHMU data (use zmax for merging)
            shmu_data = None
            if 'zmax' in shmu_files[shmu_ts]:
                # We need to load the actual HDF file, not the PNG
                # Find corresponding HDF file
                hdf_pattern = f"*{shmu_ts}.hdf"
                hdf_files = list(Path("processed/shmu_hdf_data").glob(hdf_pattern))
                if hdf_files:
                    shmu_hdf = hdf_files[0]  # Take first matching file
                    shmu_data = shmu.process_to_array(str(shmu_hdf))
                    shmu_data['timestamp'] = shmu_ts
            
            # Load DWD data
            dwd_data = None
            if 'dmax' in dwd_files[dwd_ts]:
                # Find corresponding HDF file - DWD format: composite_dmax_YYYYMMDD_HHMM.hd5
                hdf_pattern = f"composite_dmax_{dwd_ts[:8]}_{dwd_ts[8:]}.hd5"
                hdf_files = list(Path("processed/dwd_hdf_data").glob(hdf_pattern))
                if hdf_files:
                    dwd_hdf = hdf_files[0]
                    dwd_data = dwd.process_to_array(str(dwd_hdf))
                    dwd_data['timestamp'] = shmu_ts  # Use consistent timestamp
            
            if not shmu_data or not dwd_data:
                print(f"⚠️  Missing data files for {shmu_ts}")
                continue
            
            # Create timestamp data structure for merger
            timestamp_data = {
                'shmu': [shmu_data],
                'dwd': [dwd_data]
            }
            
            source_data = {
                'shmu': {
                    'extent': shmu.get_extent(),
                    'processed_data': [shmu_data]
                },
                'dwd': {
                    'extent': dwd.get_extent(),
                    'processed_data': [dwd_data]
                }
            }
            
            # Create merged products with different strategies
            for strategy in strategies:
                print(f"   🔀 Creating {strategy} merge...")
                
                try:
                    merged_data = merger.merge_sources(
                        timestamp_data, source_data, strategy=strategy
                    )
                    
                    if merged_data:
                        # Export merged PNG
                        output_path = merged_dir / f"merged_{strategy}_{shmu_ts}.png"
                        png_path, metadata = exporter.export_png(
                            merged_data, output_path, merged_data['extent']
                        )
                        
                        print(f"   ✅ {strategy}: {png_path}")
                        
                    else:
                        print(f"   ❌ {strategy} merge failed")
                        
                except Exception as e:
                    print(f"   ❌ {strategy} error: {e}")
            
            processed_count += 1
            
        except Exception as e:
            print(f"❌ Failed to process {shmu_ts}: {e}")
    
    print(f"\n🎉 Merged products complete!")
    print(f"📊 Processed {processed_count} synchronized timestamps")
    print(f"📁 Output directory: {merged_dir}")
    
    # Update indexes
    print(f"\n📋 Updating production indexes...")
    os.system("./.venv/bin/python scripts/generate_production_indexes.py")

if __name__ == "__main__":
    create_production_merged()