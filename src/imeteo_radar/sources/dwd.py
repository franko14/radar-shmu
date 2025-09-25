#!/usr/bin/env python3
"""
DWD (German Weather Service) Radar Source

Handles downloading and processing of DWD radar composite data.
"""

import h5py
import numpy as np
import requests
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..core.base import RadarSource, lonlat_to_mercator
from ..core.projection import projection_handler

class DWDRadarSource(RadarSource):
    """DWD Radar data source implementation"""
    
    def __init__(self):
        super().__init__("dwd")
        self.base_url = "https://opendata.dwd.de/weather/radar/composite"

        # DWD product mapping (to be verified)
        self.product_mapping = {
            'dmax': 'dmax',      # Maximum reflectivity (equivalent to SHMU's ZMAX)
            'pg': 'pg',          # Possible CAPPI product (to be verified)
            'hg': 'hg',          # Possible CAPPI product (to be verified)
            'hx': 'hx'           # Possible CAPPI product (to be verified)
        }
        
        # Product metadata (preliminary - will be updated after analysis)
        self.product_info = {
            'dmax': {
                'name': 'Maximum Reflectivity',
                'units': 'dBZ',
                'description': 'Column maximum reflectivity'
            },
            'pg': {
                'name': 'PG Composite',
                'units': 'unknown',
                'description': 'DWD composite product PG'
            },
            'hg': {
                'name': 'HG Composite',
                'units': 'unknown',
                'description': 'DWD composite product HG'
            },
            'hx': {
                'name': 'HX Composite',
                'units': 'unknown',
                'description': 'DWD composite product HX'
            }
        }

        # Temporary files for current session
        self.temp_files = {}
        
    def get_available_products(self) -> List[str]:
        """Get list of available DWD radar products"""
        return list(self.product_mapping.keys())
        
    def get_product_metadata(self, product: str) -> Dict[str, Any]:
        """Get metadata for a specific DWD product"""
        if product in self.product_info:
            return {
                'product': product,
                'source': self.name,
                **self.product_info[product]
            }
        return super().get_product_metadata(product)
        
    def _get_available_timestamps_from_server(self, product: str) -> List[str]:
        """Get actually available timestamps by parsing DWD directory listing"""
        
        directory_url = f"{self.base_url}/{product}/"
        try:
            print(f"🌐 Fetching DWD directory: {directory_url}")
            response = requests.get(directory_url, timeout=15)
            response.raise_for_status()
            
            # Parse HTML directory listing to extract timestamps
            import re
            pattern = rf'composite_{product}_(\d{{8}}_\d{{4}})-hd5'
            matches = re.findall(pattern, response.text)
            
            if matches:
                # Sort by timestamp (newest first)
                timestamps = sorted(set(matches), reverse=True)
                print(f"✅ Found {len(timestamps)} available timestamps: {timestamps[:3]}...")
                return timestamps
            else:
                print(f"❌ No timestamp patterns found in directory listing")
                return []
                
        except Exception as e:
            print(f"⚠️  Could not parse directory listing: {e}")
            return []

    def _generate_timestamps(self, count: int) -> List[str]:
        """Generate recent timestamps with timezone-aware approach"""
        timestamps = []
        import pytz
        
        # Use UTC time and account for ~15 minute processing delay
        current_time = datetime.now(pytz.UTC) - timedelta(minutes=15)
        
        # Convert to German time (UTC+1 in winter, UTC+2 in summer)
        # For simplicity, using UTC+1 (adjust for DST if needed)
        german_time = current_time + timedelta(hours=1)
        
        # DWD updates every 5 minutes, search backwards
        for minutes_back in range(0, count * 60, 5):
            check_time = german_time - timedelta(minutes=minutes_back)
            # Round down to nearest 5 minutes
            check_time = check_time.replace(
                minute=(check_time.minute // 5) * 5, 
                second=0, 
                microsecond=0
            )
            # DWD uses YYYYMMDD_HHMM format
            timestamp = check_time.strftime("%Y%m%d_%H%M")
            
            if timestamp not in timestamps:
                timestamps.append(timestamp)
                
            if len(timestamps) >= count * 3:
                break
                
        return timestamps
        
    def _check_timestamp_availability(self, timestamp: str, product: str) -> bool:
        """Check if DWD data is available with robust error handling"""
        url = self._get_product_url(timestamp, product)
        
        # Try HEAD request first
        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                return True
        except:
            pass
        
        # Fallback to GET request with range header to minimize data transfer
        try:
            headers = {'Range': 'bytes=0-1024'}  # Just get first 1KB
            response = requests.get(url, headers=headers, timeout=10)
            return response.status_code in [200, 206]  # 206 = Partial Content
        except:
            return False
            
    def _get_product_url(self, timestamp: str, product: str) -> str:
        """Generate URL for DWD product"""
        if product not in self.product_mapping:
            raise ValueError(f"Unknown product: {product}")

        # Special case for latest data
        if timestamp == "LATEST":
            return f"{self.base_url}/{product}/composite_{product}_LATEST-hd5"

        # DWD URL format: composite_{product}_{YYYYMMDD_HHMM}-hd5
        return f"{self.base_url}/{product}/composite_{product}_{timestamp}-hd5"
    
    def _download_single_file(self, timestamp: str, product: str) -> Dict[str, Any]:
        """Download a single DWD radar file (for parallel processing)"""
        if product not in self.product_mapping:
            return {'error': f"Unknown product: {product}", 'timestamp': timestamp, 'product': product, 'success': False}
        
        try:
            url = self._get_product_url(timestamp, product)
            # Download to temporary file
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Create a proper temporary file
            with tempfile.NamedTemporaryFile(suffix=f'_dwd_{product}_{timestamp}.hdf', delete=False) as temp_file:
                temp_file.write(response.content)
                temp_path = Path(temp_file.name)

            # Handle LATEST timestamp specially - extract actual timestamp from HDF5 file
            if timestamp == "LATEST":
                # Extract actual timestamp from the downloaded HDF5 file
                try:
                    with h5py.File(temp_path, 'r') as f:
                        # Try to get timestamp from ODIM_H5 metadata
                        if 'what' in f and 'date' in f['what'].attrs and 'time' in f['what'].attrs:
                            date_str = f['what'].attrs['date']
                            time_str = f['what'].attrs['time']
                            if isinstance(date_str, bytes):
                                date_str = date_str.decode('utf-8')
                            if isinstance(time_str, bytes):
                                time_str = time_str.decode('utf-8')
                            # Convert YYYYMMDD and HHMMSS to our format
                            normalized_timestamp = f"{date_str}{time_str[:4]}00"
                        else:
                            # Fallback to current time if metadata not found
                            normalized_timestamp = datetime.now().strftime("%Y%m%d%H%M00")
                except Exception as e:
                    print(f"⚠️  Could not extract timestamp from LATEST file: {e}")
                    normalized_timestamp = datetime.now().strftime("%Y%m%d%H%M00")
            else:
                normalized_timestamp = timestamp.replace('_', '') + '00'  # Convert to YYYYMMDDHHMM00 (14-digit)

            # Check if we already have this file in current session
            cache_key = f"{product}_{normalized_timestamp}"
            if cache_key in self.temp_files and os.path.exists(self.temp_files[cache_key]):
                # File already downloaded in this session
                temp_path.unlink()  # Remove the duplicate
                return {
                    'timestamp': normalized_timestamp,
                    'product': product,
                    'path': self.temp_files[cache_key],
                    'url': url,
                    'cached': True,
                    'success': True
                }

            # Store temp file path for this session
            self.temp_files[cache_key] = str(temp_path)

            return {
                'timestamp': normalized_timestamp,
                'product': product,
                'path': str(temp_path),
                'url': url,
                'cached': False,
                'success': True
            }
            
        except Exception as e:
            return {'error': str(e), 'timestamp': timestamp, 'product': product, 'success': False}
        
    def download_latest(self, count: int = 1, products: List[str] = None, use_latest: bool = True) -> List[Dict[str, Any]]:
        """Download latest DWD radar data"""

        if products is None:
            products = ['dmax']  # Default to maximum reflectivity

        # If only fetching one file and use_latest is True, use LATEST endpoint
        if count == 1 and use_latest:
            print(f"📡 Using LATEST endpoint for most recent data...")
            downloaded_files = []
            for product in products:
                result = self._download_single_file("LATEST", product)
                if result['success']:
                    downloaded_files.append(result)
                    print(f"✅ Downloaded latest {product} data")
                else:
                    print(f"❌ Failed to download latest {product}: {result.get('error', 'Unknown error')}")
            return downloaded_files
            
        print(f"🔍 Finding last {count} available DWD timestamps...")
        
        # Strategy 1: Try parsing directory listing first (most reliable)
        available_timestamps = []
        for product in products:
            server_timestamps = self._get_available_timestamps_from_server(product)
            if server_timestamps:
                available_timestamps = server_timestamps[:count]
                print(f"✅ Using server directory listing")
                break  # Use first working product for timestamp discovery
        
        # Strategy 2: Fallback to generated timestamps if directory parsing fails
        if not available_timestamps:
            print("📡 Fallback to timestamp generation...")
            test_timestamps = self._generate_timestamps(count * 4)  # Generate more candidates
            
            for timestamp in test_timestamps:
                if len(available_timestamps) >= count:
                    break
                    
                if self._check_timestamp_availability(timestamp, 'dmax'):
                    available_timestamps.append(timestamp)
                    print(f"✅ Found: {timestamp}")
        
        # Remove duplicates and limit to requested count
        available_timestamps = list(dict.fromkeys(available_timestamps))[:count]
        
        if not available_timestamps:
            print("❌ No available DWD timestamps found")
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
                    
        print(f"📋 DWD: Downloaded {len(downloaded_files)} files ({len(download_tasks)-len(downloaded_files)} failed)")
        return downloaded_files
        
    def process_to_array(self, file_path: str) -> Dict[str, Any]:
        """Process DWD HDF5 file to array with metadata"""
        
        try:
            with h5py.File(file_path, 'r') as f:
                # DWD files might have different structure than SHMU
                # Need to explore the file structure first
                
                print(f"🔍 Analyzing DWD file structure: {file_path}")
                self._print_hdf5_structure(f)
                
                # Try to find the main dataset
                data = None
                main_dataset_paths = [
                    'dataset1/data1/data',  # ODIM_H5 standard
                    'dataset/data',         # Alternative
                    'data',                 # Simple structure
                    'precipitation',        # Possible name
                    'reflectivity'          # Possible name
                ]
                
                for path in main_dataset_paths:
                    try:
                        data = f[path][:]
                        print(f"✅ Found data at: {path}")
                        break
                    except KeyError:
                        continue
                        
                if data is None:
                    # Try to find any large array
                    data = self._find_main_dataset(f)
                    
                if data is None:
                    raise ValueError("Could not find main data array in DWD file")
                
                # Get metadata attributes
                metadata = self._extract_dwd_metadata(f, file_path)
                
                # Get scaling attributes from data1/what
                data_what_attrs = {}
                if 'dataset1/data1/what' in f:
                    data_what_attrs = dict(f['dataset1/data1/what'].attrs)
                
                gain = data_what_attrs.get('gain', 1.0)
                offset = data_what_attrs.get('offset', 0.0)
                nodata = data_what_attrs.get('nodata', 65535)
                undetect = data_what_attrs.get('undetect', 0)
                
                print(f"📊 DWD scaling - gain: {gain}, offset: {offset}, nodata: {nodata}, undetect: {undetect}")
                
                # Apply proper scaling like SHMU does: gain * data + offset
                scaled_data = data.astype(np.float32) * gain + offset
                
                # Handle special values
                scaled_data[data == nodata] = np.nan
                scaled_data[data == undetect] = np.nan
                
                print(f"✅ Scaled data range: {np.nanmin(scaled_data):.2f} to {np.nanmax(scaled_data):.2f}")
                
                # Get geographic information from where attributes
                where_attrs = {}
                if 'where' in f:
                    where_attrs = dict(f['where'].attrs)
                elif 'dataset1/where' in f:
                    where_attrs = dict(f['dataset1/where'].attrs)
                
                # Get projection definition
                proj_def = None
                try:
                    if 'where' in f and 'projdef' in f['where'].attrs:
                        proj_def = f['where'].attrs['projdef']
                    elif 'dataset1/where' in f and 'projdef' in f['dataset1/where'].attrs:
                        proj_def = f['dataset1/where'].attrs['projdef']
                    
                    if isinstance(proj_def, bytes):
                        proj_def = proj_def.decode('utf-8')
                        
                    print(f"🗺️ DWD projection: {proj_def}")
                except:
                    print("⚠️ No projection definition found - using corner approximation")
                
                # Create proper coordinate arrays using projection handling
                try:
                    lons, lats = projection_handler.create_dwd_coordinates(
                        data.shape, where_attrs, proj_def
                    )
                    print(f"📊 DWD bounds: W={np.nanmin(lons):.2f}, E={np.nanmax(lons):.2f}, "
                          f"N={np.nanmax(lats):.2f}, S={np.nanmin(lats):.2f}")
                    
                except Exception as e:
                    print(f"⚠️ Projection handling failed: {e}")
                    # Fallback to default bounds
                    west_lon, south_lat = 3.0, 47.0
                    east_lon, north_lat = 17.0, 56.0
                    lons = np.linspace(west_lon, east_lon, data.shape[1])
                    lats = np.linspace(north_lat, south_lat, data.shape[0])
                    lons, lats = np.meshgrid(lons, lats)
                
                timestamp = self._extract_timestamp_from_path(file_path)
                
                return {
                    'data': scaled_data,
                    'coordinates': {
                        'lons': lons,
                        'lats': lats
                    },
                    'metadata': {
                        'product': metadata.get('product', 'UNKNOWN'),
                        'quantity': metadata.get('quantity', 'UNKNOWN'),
                        'timestamp': timestamp,
                        'source': 'DWD',
                        'units': metadata.get('units', 'unknown'),
                        'nodata_value': np.nan
                    },
                    'extent': {
                        'wgs84': {
                            'west': np.nanmin(lons),
                            'east': np.nanmax(lons),
                            'south': np.nanmin(lats),
                            'north': np.nanmax(lats)
                        }
                    },
                    'dimensions': data.shape,
                    'timestamp': timestamp
                }
                
        except Exception as e:
            raise RuntimeError(f"Failed to process DWD file {file_path}: {e}")
            
        
    def _extract_dwd_metadata(self, hdf_file, file_path: str) -> Dict[str, Any]:
        """Extract metadata from DWD HDF5 file"""
        metadata = {}
        
        # Try to find standard ODIM_H5 attributes
        try:
            if 'what' in hdf_file:
                what_attrs = dict(hdf_file['what'].attrs)
                for key, value in what_attrs.items():
                    if isinstance(value, bytes):
                        metadata[key] = value.decode('utf-8')
                    else:
                        metadata[key] = value
        except:
            pass
            
        # Extract product from filename
        filename = Path(file_path).name
        if 'dmax' in filename:
            metadata['product'] = 'dmax'
            metadata['quantity'] = 'DBZH'
            metadata['units'] = 'dBZ'
        elif 'pg' in filename:
            metadata['product'] = 'pg'
            metadata['units'] = 'unknown'
        elif 'hg' in filename:
            metadata['product'] = 'hg' 
            metadata['units'] = 'unknown'
            
        return metadata
        
    def _extract_timestamp_from_path(self, file_path: str) -> str:
        """Extract timestamp from DWD file path and normalize to 14-digit format"""
        filename = Path(file_path).name
        # Extract YYYYMMDD_HHMM pattern
        import re
        match = re.search(r'(\d{8})_(\d{4})', filename)
        if match:
            date_part = match.group(1)
            time_part = match.group(2)
            return f"{date_part}{time_part}00"  # Normalize to 14-digit: YYYYMMDDHHMM00
        return "unknown"
        
    def get_extent(self) -> Dict[str, Any]:
        """Get DWD radar coverage extent"""
        
        # DWD radar coverage (actual data bounds - larger than Germany proper)
        wgs84 = {
            'west': 2.5,   # Extended to cover actual data range
            'east': 18.0,  # Extended to cover actual data range  
            'south': 45.5, # Extended to cover actual data range
            'north': 56.0
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
            'grid_size': [900, 900],  # DWD standard grid
            'resolution_m': [1000, 1000]  # 1km resolution
        }
    
    def _print_hdf5_structure(self, hdf_file, level=0, max_items=10):
        """Helper method to print HDF5 file structure for debugging"""
        try:
            items = list(hdf_file.items())[:max_items]
            for key, value in items:
                indent = "  " * level
                if hasattr(value, 'keys'):  # It's a group
                    print(f"{indent}{key}/ (group)")
                    if level < 2:  # Limit depth to avoid spam
                        self._print_hdf5_structure(value, level + 1, max_items)
                else:  # It's a dataset
                    print(f"{indent}{key} (dataset): shape={getattr(value, 'shape', '?')}")
        except Exception as e:
            print(f"  Error printing structure: {e}")

    def cleanup_temp_files(self):
        """Clean up all temporary files created during this session"""
        cleaned_count = 0
        for cache_key, file_path in list(self.temp_files.items()):
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
                    cleaned_count += 1
                del self.temp_files[cache_key]
            except Exception as e:
                print(f"⚠️  Could not delete temp file {file_path}: {e}")

        if cleaned_count > 0:
            print(f"🧹 Cleaned up {cleaned_count} temporary DWD files")
        return cleaned_count