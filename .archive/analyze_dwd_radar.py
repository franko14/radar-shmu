#!/usr/bin/env python3
"""
Analyze DWD radar HDF5 files to find CAPPI 2km equivalent
"""

import h5py
import numpy as np
import sys
from pathlib import Path

def analyze_hdf5_structure(filepath):
    """Analyze the structure and metadata of an HDF5 radar file"""
    
    print(f"\n{'='*60}")
    print(f"ANALYZING: {filepath}")
    print(f"{'='*60}")
    
    try:
        with h5py.File(filepath, 'r') as f:
            print(f"File size: {filepath.stat().st_size / 1024:.1f} KB")
            
            def print_structure(name, obj):
                """Recursively print HDF5 structure"""
                indent = "  " * name.count('/')
                if isinstance(obj, h5py.Group):
                    print(f"{indent}{name}/ (Group)")
                    # Print group attributes
                    if len(obj.attrs) > 0:
                        for attr_name, attr_value in obj.attrs.items():
                            if isinstance(attr_value, bytes):
                                attr_value = attr_value.decode('utf-8', errors='ignore')
                            print(f"{indent}  @{attr_name}: {attr_value}")
                elif isinstance(obj, h5py.Dataset):
                    print(f"{indent}{name} (Dataset) - Shape: {obj.shape}, Dtype: {obj.dtype}")
                    # Print dataset attributes
                    if len(obj.attrs) > 0:
                        for attr_name, attr_value in obj.attrs.items():
                            if isinstance(attr_value, bytes):
                                attr_value = attr_value.decode('utf-8', errors='ignore')
                            print(f"{indent}  @{attr_name}: {attr_value}")
                    
                    # Show some data statistics for numeric datasets
                    if obj.dtype.kind in ['i', 'f', 'u']:  # integer, float, unsigned
                        try:
                            data = obj[:]
                            valid_data = data[data != obj.attrs.get('nodata', -32768)]
                            if len(valid_data) > 0:
                                print(f"{indent}  Data range: {np.min(valid_data):.2f} to {np.max(valid_data):.2f}")
                                print(f"{indent}  Valid pixels: {len(valid_data)}/{data.size} ({100*len(valid_data)/data.size:.1f}%)")
                        except Exception as e:
                            print(f"{indent}  Could not analyze data: {e}")
            
            print("\nHDF5 STRUCTURE:")
            print("-" * 40)
            f.visititems(print_structure)
            
            # Look for specific radar-related information
            print(f"\nSPECIFIC RADAR ANALYSIS:")
            print("-" * 40)
            
            # Check root attributes
            if len(f.attrs) > 0:
                print("Root attributes:")
                for attr_name, attr_value in f.attrs.items():
                    if isinstance(attr_value, bytes):
                        attr_value = attr_value.decode('utf-8', errors='ignore')
                    print(f"  {attr_name}: {attr_value}")
            
            # Look for what and where groups (ODIM standard)
            if 'what' in f:
                print("\nProduct information ('what' group):")
                what_group = f['what']
                for attr_name, attr_value in what_group.attrs.items():
                    if isinstance(attr_value, bytes):
                        attr_value = attr_value.decode('utf-8', errors='ignore')
                    print(f"  {attr_name}: {attr_value}")
            
            if 'where' in f:
                print("\nGeographic information ('where' group):")
                where_group = f['where']
                for attr_name, attr_value in where_group.attrs.items():
                    if isinstance(attr_value, bytes):
                        attr_value = attr_value.decode('utf-8', errors='ignore')
                    print(f"  {attr_name}: {attr_value}")
            
            # Look for dataset information
            dataset_count = 0
            for key in f.keys():
                if key.startswith('dataset'):
                    dataset_count += 1
                    dataset_group = f[key]
                    print(f"\n{key.upper()} information:")
                    
                    if 'what' in dataset_group:
                        what_attrs = dataset_group['what'].attrs
                        for attr_name, attr_value in what_attrs.items():
                            if isinstance(attr_value, bytes):
                                attr_value = attr_value.decode('utf-8', errors='ignore')
                            print(f"  {attr_name}: {attr_value}")
                    
                    if 'where' in dataset_group:
                        where_attrs = dataset_group['where'].attrs
                        for attr_name, attr_value in where_attrs.items():
                            if isinstance(attr_value, bytes):
                                attr_value = attr_value.decode('utf-8', errors='ignore')
                            print(f"  {attr_name}: {attr_value}")
            
            print(f"\nTotal datasets found: {dataset_count}")
            
    except Exception as e:
        print(f"ERROR analyzing {filepath}: {e}")

def main():
    """Analyze all DWD radar files in the current directory"""
    
    current_dir = Path('.')
    dwd_files = list(current_dir.glob('composite_*.hd5'))
    
    if not dwd_files:
        print("No DWD composite files found in current directory")
        return
    
    print(f"Found {len(dwd_files)} DWD radar files to analyze")
    
    for filepath in sorted(dwd_files):
        analyze_hdf5_structure(filepath)
    
    print(f"\n{'='*60}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()