#!/usr/bin/env python3
"""
Generate Production Indexes for Frontend Consumption

Creates comprehensive index files for radar data in production directory.
"""

import json
import os
from pathlib import Path
from datetime import datetime
import glob

def generate_production_indexes():
    """Generate comprehensive indexes for production data"""
    
    print("📋 Generating Production Indexes")
    print("=" * 50)
    
    production_dir = Path("outputs/production")
    
    # Main index structure
    main_index = {
        "generated_at": datetime.now().isoformat(),
        "radar_sources": ["shmu", "dwd", "merged"],
        "data_structure": {
            "latest_radar_data": "Current operational radar data",
            "extent_reference": "config/extent_index.json"
        },
        "directories": {},
        "latest_timestamps": {},
        "available_products": {
            "shmu": ["zmax", "cappi2km"],
            "dwd": ["dmax"],
            "merged": ["average", "max", "priority"]
        }
    }
    
    # Scan latest_radar_data directory
    latest_dir = production_dir / "latest_radar_data"
    if latest_dir.exists():
        print(f"📊 Scanning {latest_dir}")
        
        # Process each source directory
        for source_dir in ["shmu", "dwd", "merged"]:
            source_path = latest_dir / source_dir
            
            if source_path.exists():
                print(f"   Indexing {source_dir}...")
                
                # Find PNG files
                png_files = list(source_path.glob("*.png"))
                
                # Extract timestamps
                timestamps = []
                products = set()
                
                for png_file in png_files:
                    filename = png_file.name
                    # Extract timestamp pattern: {product}_{timestamp}.png
                    if '_' in filename:
                        parts = filename.replace('.png', '').split('_')
                        if len(parts) >= 2:
                            product = parts[0]
                            timestamp = '_'.join(parts[1:])
                            timestamps.append(timestamp)
                            products.add(product)
                
                # Sort timestamps (newest first)
                timestamps.sort(reverse=True)
                
                main_index["directories"][source_dir] = {
                    "file_count": len(png_files),
                    "products": list(products),
                    "timestamp_count": len(set(timestamps)),
                    "latest_timestamp": timestamps[0] if timestamps else None,
                    "files": [f.name for f in png_files]
                }
                
                if timestamps:
                    main_index["latest_timestamps"][source_dir] = timestamps[0]
        
        # Process metadata files
        metadata_files = list(latest_dir.glob("metadata_*.json"))
        if metadata_files:
            main_index["metadata"] = {
                "file_count": len(metadata_files),
                "files": [f.name for f in metadata_files]
            }
    
    # Write main index
    main_index_path = production_dir / "index.json"
    with open(main_index_path, 'w') as f:
        json.dump(main_index, f, indent=2)
    
    print(f"✅ Main index: {main_index_path}")
    
    # Generate frontend-ready index
    frontend_index = {
        "version": "1.0",
        "generated_at": datetime.now().isoformat(),
        "radar_data": {
            "sources": list(main_index["latest_timestamps"].keys()),
            "latest_timestamps": main_index["latest_timestamps"],
            "available_products": main_index["available_products"]
        },
        "endpoints": {
            "shmu_latest": f"outputs/production/latest_radar_data/shmu/",
            "dwd_latest": f"outputs/production/latest_radar_data/dwd/",
            "merged_latest": f"outputs/production/latest_radar_data/merged/",
            "extents": "config/extent_index.json"
        },
        "file_patterns": {
            "shmu": "{product}_{timestamp}.png",
            "dwd": "{product}_{timestamp}.png", 
            "merged": "merged_{strategy}_{timestamp}.png"
        }
    }
    
    frontend_index_path = production_dir / "frontend_index.json"
    with open(frontend_index_path, 'w') as f:
        json.dump(frontend_index, f, indent=2)
    
    print(f"✅ Frontend index: {frontend_index_path}")
    
    # Generate per-source detailed indexes
    for source in ["shmu", "dwd", "merged"]:
        source_dir = latest_dir / source
        if source_dir.exists():
            source_index = generate_source_index(source, source_dir)
            source_index_path = production_dir / f"{source}_index.json"
            
            with open(source_index_path, 'w') as f:
                json.dump(source_index, f, indent=2)
            
            print(f"✅ {source} index: {source_index_path}")
    
    print(f"\n📋 Summary:")
    print(f"   Main index: {main_index['directories']}")
    print(f"   Latest timestamps: {main_index['latest_timestamps']}")
    print(f"   Total files indexed: {sum(dir_info['file_count'] for dir_info in main_index['directories'].values())}")

def generate_source_index(source_name: str, source_dir: Path):
    """Generate detailed index for a specific source"""
    
    png_files = list(source_dir.glob("*.png"))
    
    # Group by timestamp and product
    timeline = {}
    products = set()
    
    for png_file in png_files:
        filename = png_file.name
        
        # Parse filename based on source
        if source_name == "merged":
            # merged_{strategy}_{timestamp}.png
            parts = filename.replace('.png', '').split('_')
            if len(parts) >= 3:
                product = parts[1]  # strategy
                timestamp = '_'.join(parts[2:])
        else:
            # {product}_{timestamp}.png
            parts = filename.replace('.png', '').split('_')
            if len(parts) >= 2:
                product = parts[0]
                timestamp = '_'.join(parts[1:])
        
        if 'timestamp' in locals():
            products.add(product)
            
            if timestamp not in timeline:
                timeline[timestamp] = {}
            
            # Get file info
            file_stat = png_file.stat()
            timeline[timestamp][product] = {
                "filename": filename,
                "size_bytes": file_stat.st_size,
                "modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            }
    
    # Sort timeline by timestamp (newest first)
    sorted_timeline = dict(sorted(timeline.items(), reverse=True))
    
    return {
        "source": source_name,
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "total_files": len(png_files),
            "products": list(products),
            "timestamp_count": len(timeline),
            "latest_timestamp": list(sorted_timeline.keys())[0] if sorted_timeline else None
        },
        "timeline": sorted_timeline
    }

if __name__ == "__main__":
    generate_production_indexes()