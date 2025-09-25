#!/usr/bin/env python3
"""
Test script to specifically demonstrate the merged radar product functionality.
This will manually create radar data from both sources with matching timestamps.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import numpy as np
from datetime import datetime
from radar_sources import SHMURadarSource, DWDRadarSource
from radar_sources.merger import RadarMerger
from radar_sources.exporter import PNGExporter

def test_merge():
    """Test the merge functionality with current data"""
    
    print("🧪 Testing Radar Merge Functionality")
    print("=" * 50)
    
    # Initialize sources
    shmu = SHMURadarSource()
    dwd = DWDRadarSource()
    merger = RadarMerger()
    exporter = PNGExporter()
    
    # Download latest data from each source
    print("📡 Downloading SHMU data...")
    shmu_files = shmu.download_latest(count=1, products=['zmax'])
    if not shmu_files:
        print("❌ No SHMU data available")
        return
        
    print("📡 Downloading DWD data...")
    dwd_files = dwd.download_latest(count=1, products=['dmax'])
    if not dwd_files:
        print("❌ No DWD data available")
        return
    
    # Process the data
    print("🔄 Processing SHMU data...")
    shmu_data = shmu.process_to_array(shmu_files[0]['path'])
    shmu_data.update(shmu_files[0])
    
    print("🔄 Processing DWD data...")
    dwd_data = dwd.process_to_array(dwd_files[0]['path'])
    dwd_data.update(dwd_files[0])
    
    print(f"📊 SHMU timestamp: {shmu_data['timestamp']}")
    print(f"📊 DWD timestamp: {dwd_data['timestamp']}")
    print(f"📊 SHMU shape: {shmu_data['dimensions']}")
    print(f"📊 DWD shape: {dwd_data['dimensions']}")
    print(f"📊 SHMU extent: {shmu_data['extent']['wgs84']}")
    print(f"📊 DWD extent: {dwd_data['extent']['wgs84']}")
    
    # Force same timestamp for merge testing
    common_timestamp = shmu_data['timestamp'][:12] + "00"  # Round to nearest 10 minutes
    print(f"🕐 Using common timestamp for merge: {common_timestamp}")
    
    shmu_data['timestamp'] = common_timestamp
    dwd_data['timestamp'] = common_timestamp
    
    # Create fake timestamp data structure for merger
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
    
    # Test different merge strategies
    strategies = ['average', 'max', 'priority']
    output_dir = Path("outputs/production/merge_test")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for strategy in strategies:
        print(f"\n🔀 Testing {strategy} merge strategy...")
        
        try:
            merged_data = merger.merge_sources(
                timestamp_data, source_data, strategy=strategy
            )
            
            if merged_data:
                # Export merged PNG
                output_path = output_dir / f"merged_{strategy}_{common_timestamp}.png"
                png_path, metadata = exporter.export_png(
                    merged_data, output_path, merged_data['extent']
                )
                
                print(f"✅ Merged PNG exported: {png_path}")
                print(f"📐 Merged shape: {merged_data['dimensions']}")
                print(f"📊 Valid pixels: {np.sum(~np.isnan(merged_data['data']))}")
                
            else:
                print(f"❌ {strategy} merge failed")
                
        except Exception as e:
            print(f"❌ {strategy} merge error: {e}")
    
    # Also export individual sources for comparison
    print("\n📊 Exporting individual sources for comparison...")
    
    shmu_path = output_dir / f"shmu_zmax_{common_timestamp}.png"
    shmu_png, _ = exporter.export_png(shmu_data, shmu_path, source_data['shmu']['extent'])
    print(f"✅ SHMU exported: {shmu_png}")
    
    dwd_path = output_dir / f"dwd_dmax_{common_timestamp}.png"
    dwd_png, _ = exporter.export_png(dwd_data, dwd_path, source_data['dwd']['extent'])
    print(f"✅ DWD exported: {dwd_png}")
    
    print(f"\n🎉 Merge test complete!")
    print(f"📁 Check {output_dir} for results")
    print(f"🔍 Compare individual vs merged products")

if __name__ == "__main__":
    test_merge()