#!/usr/bin/env python3
"""
SHMU (Slovak Hydrometeorological Institute) Radar Source

Handles downloading and processing of SHMU radar data in ODIM_H5 format.
"""

import os
import h5py
import numpy as np
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..core.base import RadarSource, RadarData, lonlat_to_mercator
from ..utils.storage import TimePartitionedStorage

class SHMURadarSource(RadarSource):
    """SHMU Radar data source implementation"""
    
    def __init__(self):
        super().__init__("shmu")
        self.base_url = "https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp"
        
        # SHMU product mapping
        self.product_mapping = {
            'zmax': 'PABV',      # Maximum reflectivity
            'cappi2km': 'PANV',  # CAPPI at 2km
            'etop': 'PADV',      # Echo top height
            'pac01': 'PASV'      # 1h accumulated precipitation
        }
        
        # Product metadata
        self.product_info = {
            'zmax': {
                'name': 'Maximum Reflectivity (ZMAX)',
                'units': 'dBZ',
                'description': 'Column maximum reflectivity'
            },
            'cappi2km': {
                'name': 'CAPPI 2km',
                'units': 'dBZ', 
                'description': 'Reflectivity at 2km altitude'
            },
            'etop': {
                'name': 'Echo Top Height',
                'units': 'km',
                'description': 'Echo top height'
            },
            'pac01': {
                'name': '1h Precipitation',
                'units': 'mm',
                'description': '1-hour accumulated precipitation'
            }
        }
        
        # Time-partitioned storage (new optimized storage)
        self.storage = TimePartitionedStorage("storage")
        
        # Legacy cache directory (for migration)
        self.cache_dir = Path("processed/shmu_hdf_data")
        self._migrated = False  # Track if migration completed
        
    def get_available_products(self) -> List[str]:
        """Get list of available SHMU radar products"""
        return list(self.product_mapping.keys())
        
    def get_product_metadata(self, product: str) -> Dict[str, Any]:
        """Get metadata for a specific SHMU product"""
        if product in self.product_info:
            return {
                'product': product,
                'source': self.name,
                **self.product_info[product]
            }
        return super().get_product_metadata(product)
        
    def _generate_timestamps(self, count: int) -> List[str]:
        """Generate recent timestamps to search for available data"""
        timestamps = []
        import pytz
        current_time = datetime.now(pytz.UTC)  # Use UTC time
        
        # Start from current time and work backwards in 5-minute intervals
        # SHMU may have slight delay but should be much more recent than 2 hours
        for minutes_back in range(0, count * 30, 5):  # Start from now, go back up to count*30 minutes
            check_time = current_time - timedelta(minutes=minutes_back)
            # Round down to nearest 5 minutes
            check_time = check_time.replace(
                minute=(check_time.minute // 5) * 5, 
                second=0, 
                microsecond=0
            )
            timestamp = check_time.strftime("%Y%m%d%H%M%S")
            
            if timestamp not in timestamps:
                timestamps.append(timestamp)
                
            if len(timestamps) >= count * 4:  # Get extra to account for missing data
                break
                
        return timestamps
        
    def _check_timestamp_availability(self, timestamp: str, product: str) -> bool:
        """Check if data is available for a specific timestamp and product"""
        url = self._get_product_url(timestamp, product)
        try:
            response = requests.head(url, timeout=5, verify=False)
            return response.status_code == 200
        except:
            return False
            
    def _get_product_url(self, timestamp: str, product: str) -> str:
        """Generate URL for SHMU product"""
        if product not in self.product_mapping:
            raise ValueError(f"Unknown product: {product}")
            
        composite_type = self.product_mapping[product]
        date_str = timestamp[:8]  # YYYYMMDD
        
        return (f"{self.base_url}/{product}/{date_str}/"
                f"T_{composite_type}22_C_LZIB_{timestamp}.hdf")
    
    def _download_single_file(self, timestamp: str, product: str) -> Dict[str, Any]:
        """Download a single radar file (for parallel processing)"""
        if product not in self.product_mapping:
            return {'error': f"Unknown product: {product}", 'timestamp': timestamp, 'product': product, 'success': False}
        
        try:
            self._ensure_storage_ready()
            
            # Check if file exists in new storage first
            existing_files = self.storage.get_files(
                source="shmu", 
                start_time=timestamp, 
                end_time=timestamp, 
                product=product
            )
            
            if existing_files:
                existing_file = existing_files[0]
                return {
                    'timestamp': timestamp,
                    'product': product,
                    'path': existing_file['path'],
                    'url': self._get_product_url(timestamp, product),
                    'cached': True,
                    'success': True
                }
            
            # Download to temporary location
            url = self._get_product_url(timestamp, product)
            filename = f"T_{self.product_mapping[product]}22_C_LZIB_{timestamp}.hdf"
            temp_filepath = Path(f"/tmp/{filename}")
            
            # Download
            response = requests.get(url, timeout=30, verify=False)
            response.raise_for_status()
            
            # Save temporarily
            with open(temp_filepath, 'wb') as f:
                f.write(response.content)
            
            # Store in time-partitioned storage
            stored_path = self.storage.store_file(
                temp_filepath, timestamp, "shmu", product,
                metadata={
                    'url': url,
                    'downloaded_at': datetime.now().isoformat(),
                    'composite_type': self.product_mapping[product],
                    'file_size': temp_filepath.stat().st_size
                }
            )
            
            # Clean up temp file
            temp_filepath.unlink()
                
            return {
                'timestamp': timestamp,
                'product': product, 
                'path': str(stored_path),
                'url': url,
                'cached': False,
                'success': True
            }
            
        except Exception as e:
            return {'error': str(e), 'timestamp': timestamp, 'product': product, 'success': False}
        
    def download_latest(self, count: int, products: List[str] = None) -> List[Dict[str, Any]]:
        """Download latest SHMU radar data"""
        
        if products is None:
            products = ['zmax', 'cappi2km']  # Default products
            
        print(f"🔍 Finding last {count} available SHMU timestamps...")
        
        # First check for cached files
        cached_files = []
        for file_path in self.cache_dir.glob("*.hdf"):
            # Parse timestamp from filename: T_PABV22_C_LZIB_20250905153500.hdf
            filename = file_path.name
            if '_' in filename and filename.endswith('.hdf'):
                try:
                    timestamp_part = filename.split('_')[-1].replace('.hdf', '')
                    if len(timestamp_part) == 14 and timestamp_part.isdigit():
                        cached_files.append((timestamp_part, file_path))
                except:
                    continue
        
        # Sort by timestamp (newest first) and extract unique timestamps
        cached_files.sort(key=lambda x: x[0], reverse=True)
        cached_timestamps = []
        for timestamp, _ in cached_files:
            if timestamp not in cached_timestamps:
                cached_timestamps.append(timestamp)
                if len(cached_timestamps) >= count:
                    break
        
        # Strategy 1: Check for current timestamps online first
        print(f"🌐 Checking SHMU server for current timestamps...")
        test_timestamps = self._generate_timestamps(count * 4)  # Generate more candidates
        available_timestamps = []
        
        for timestamp in test_timestamps:
            if len(available_timestamps) >= count:
                break
                
            # Test with zmax (most reliable product)
            if self._check_timestamp_availability(timestamp, 'zmax'):
                available_timestamps.append(timestamp)
                print(f"✅ Found current: {timestamp}")
        
        # Strategy 2: Add cached timestamps if we need more
        if len(available_timestamps) < count and cached_timestamps:
            print(f"📁 Adding cached timestamps...")
            for cached_ts in cached_timestamps:
                if cached_ts not in available_timestamps:
                    available_timestamps.append(cached_ts)
                    print(f"✅ Found cached: {cached_ts}")
                    if len(available_timestamps) >= count:
                        break
                        
        if not available_timestamps:
            print("❌ No available timestamps found")
            return []
            
        print(f"📥 Downloading {len(available_timestamps)} timestamps × {len(products)} products...")
        
        # Create download tasks
        download_tasks = []
        for timestamp in available_timestamps:
            for product in products:
                download_tasks.append((timestamp, product))
        
        print(f"📥 Starting parallel downloads ({len(download_tasks)} files, max 6 concurrent)...")
        
        # Execute downloads in parallel
        downloaded_files = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            # Submit all download tasks
            future_to_task = {
                executor.submit(self._download_single_file, timestamp, product): (timestamp, product)
                for timestamp, product in download_tasks
            }
            
            # Process completed downloads
            for future in as_completed(future_to_task):
                timestamp, product = future_to_task[future]
                try:
                    result = future.result()
                    if result['success']:
                        downloaded_files.append(result)
                        if result['cached']:
                            print(f"📁 Using cached: {product} {timestamp}")
                        else:
                            print(f"✅ Downloaded: {product} {timestamp}")
                    else:
                        print(f"❌ Failed {product} {timestamp}: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    print(f"❌ Exception {product} {timestamp}: {e}")
                    
        print(f"📋 SHMU: Downloaded {len(downloaded_files)} files ({len(download_tasks)-len(downloaded_files)} failed)")
        return downloaded_files
        
    def process_to_array(self, file_path: str) -> Dict[str, Any]:
        """Process SHMU HDF5 file to array with metadata"""
        
        try:
            with h5py.File(file_path, 'r') as f:
                # Read raw data
                data = f['dataset1/data1/data'][:]
                
                # Get attributes
                what_attrs = dict(f['dataset1/what'].attrs)
                where_attrs = dict(f['where'].attrs)
                
                # Decode byte strings
                for attr_dict in [what_attrs, where_attrs]:
                    for key, value in attr_dict.items():
                        if isinstance(value, bytes):
                            attr_dict[key] = value.decode('utf-8')
                
                # Apply scaling
                gain = what_attrs.get('gain', 1.0)
                offset = what_attrs.get('offset', 0.0)
                nodata = what_attrs.get('nodata', -32768)
                undetect = what_attrs.get('undetect', 0)
                
                # Scale data
                scaled_data = data.astype(np.float32) * gain + offset
                
                # Handle special values
                scaled_data[data == nodata] = np.nan
                scaled_data[data == undetect] = np.nan
                
                # Create coordinate arrays
                ll_lon = float(where_attrs['LL_lon'])
                ll_lat = float(where_attrs['LL_lat'])
                ur_lon = float(where_attrs['UR_lon'])
                ur_lat = float(where_attrs['UR_lat'])
                
                lons = np.linspace(ll_lon, ur_lon, data.shape[1])
                lats = np.linspace(ur_lat, ll_lat, data.shape[0])  # Note: flipped
                
                # Extract metadata
                product = what_attrs.get('product', 'UNKNOWN')
                quantity = what_attrs.get('quantity', 'UNKNOWN')
                start_date = what_attrs.get('startdate', '')
                start_time = what_attrs.get('starttime', '')
                timestamp = start_date + start_time
                
                return {
                    'data': scaled_data,
                    'coordinates': {
                        'lons': lons,
                        'lats': lats
                    },
                    'metadata': {
                        'product': product,
                        'quantity': quantity, 
                        'timestamp': timestamp,
                        'source': 'SHMU',
                        'units': self._get_units(quantity),
                        'nodata_value': np.nan,
                        'gain': gain,
                        'offset': offset
                    },
                    'extent': {
                        'wgs84': {
                            'west': ll_lon,
                            'east': ur_lon,
                            'south': ll_lat, 
                            'north': ur_lat
                        }
                    },
                    'dimensions': data.shape,
                    'timestamp': timestamp[:14]  # YYYYMMDDHHMMSS format
                }
                
        except Exception as e:
            raise RuntimeError(f"Failed to process SHMU file {file_path}: {e}")
            
    def _get_units(self, quantity: str) -> str:
        """Get units for a quantity"""
        units_map = {
            'DBZH': 'dBZ',
            'HGHT': 'km', 
            'ACRR': 'mm',
            'TH': 'dBZ'
        }
        return units_map.get(quantity, 'unknown')
        
    def get_extent(self) -> Dict[str, Any]:
        """Get SHMU radar coverage extent"""
        
        # SHMU radar coverage (approximate)
        wgs84 = {
            'west': 13.6,
            'east': 23.8,
            'south': 46.0,
            'north': 50.7
        }
        
        # Convert to Web Mercator
        x_min, y_min = lonlat_to_mercator(wgs84['west'], wgs84['south'])
        x_max, y_max = lonlat_to_mercator(wgs84['east'], wgs84['north'])
        
        return {
            'wgs84': wgs84,
            'mercator': {
                'x_min': x_min,
                'x_max': x_max,
                'y_min': y_min, 
                'y_max': y_max,
                'bounds': [x_min, y_min, x_max, y_max]  # [xmin, ymin, xmax, ymax]
            },
            'projection': 'EPSG:3857',
            'grid_size': [1560, 2270],  # [height, width]
            'resolution_m': [480, 330]  # [y_res, x_res] approximately
        }
        
    def _migrate_legacy_cache(self):
        """Migrate existing cache files to time-partitioned storage"""
        if self._migrated or not self.cache_dir.exists():
            return
            
        print(f"📦 Migrating SHMU cache from {self.cache_dir} to time-partitioned storage...")
        self.storage.migrate_existing_data(self.cache_dir, "shmu")
        self._migrated = True
        
    def _ensure_storage_ready(self):
        """Ensure storage is ready (migrate if needed)"""
        if not self._migrated and self.cache_dir.exists():
            self._migrate_legacy_cache()