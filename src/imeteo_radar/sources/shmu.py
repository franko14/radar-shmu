#!/usr/bin/env python3
"""
SHMU (Slovak Hydrometeorological Institute) Radar Source

Handles downloading and processing of SHMU radar data in ODIM_H5 format.
"""

import os
import h5py
import numpy as np
import requests
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..core.base import RadarSource, RadarData, lonlat_to_mercator

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
        
        # Track temporary files for cleanup
        self.temp_files = {}
        
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

    def _filter_timestamps_by_range(self, timestamps: List[str],
                                    start_time: datetime, end_time: datetime) -> List[str]:
        """Filter timestamps to only include those within the specified time range

        Args:
            timestamps: List of timestamp strings in format YYYYMMDDHHMM00 (14 digits)
            start_time: Start of time range (timezone-aware datetime)
            end_time: End of time range (timezone-aware datetime)

        Returns:
            Filtered list of timestamps within the range
        """
        import pytz

        filtered = []
        for ts in timestamps:
            try:
                # Parse SHMU timestamp format: YYYYMMDDHHMM00 (14 digits)
                ts_dt = datetime.strptime(ts[:12], "%Y%m%d%H%M")
                # Make timezone aware (SHMU uses UTC)
                ts_dt = pytz.UTC.localize(ts_dt)

                # Check if timestamp is within range
                if start_time <= ts_dt <= end_time:
                    filtered.append(ts)
            except ValueError:
                # Skip timestamps that can't be parsed
                continue

        return filtered

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
            # Check if we've already downloaded this file in this session
            cache_key = f"{timestamp}_{product}"
            if cache_key in self.temp_files:
                return {
                    'timestamp': timestamp,
                    'product': product,
                    'path': self.temp_files[cache_key],
                    'url': self._get_product_url(timestamp, product),
                    'cached': True,
                    'success': True
                }

            # Download to temporary file
            url = self._get_product_url(timestamp, product)

            # Create a proper temporary file
            with tempfile.NamedTemporaryFile(suffix=f'_shmu_{product}_{timestamp}.hdf', delete=False) as temp_file:
                # Download directly to temp file
                response = requests.get(url, timeout=30, verify=False)
                response.raise_for_status()

                temp_file.write(response.content)
                temp_path = Path(temp_file.name)

            # Track the temporary file
            self.temp_files[cache_key] = str(temp_path)

            return {
                'timestamp': timestamp,
                'product': product,
                'path': str(temp_path),
                'url': url,
                'cached': False,
                'success': True
            }

        except Exception as e:
            return {'error': str(e), 'timestamp': timestamp, 'product': product, 'success': False}
        
    def download_latest(self, count: int, products: List[str] = None,
                       start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Download latest SHMU radar data

        Args:
            count: Maximum number of timestamps to download
            products: List of products to download (default: ['zmax', 'cappi2km'])
            start_time: Optional start time for filtering (timezone-aware datetime)
            end_time: Optional end time for filtering (timezone-aware datetime)
        """

        if products is None:
            products = ['zmax', 'cappi2km']  # Default products

        print(f"🔍 Finding last {count} available SHMU timestamps...")

        # Strategy: Check for current timestamps online
        print(f"🌐 Checking SHMU server for current timestamps...")

        # Generate more timestamps if we're filtering by time range
        multiplier = 8 if (start_time and end_time) else 4
        test_timestamps = self._generate_timestamps(count * multiplier)

        # Filter by time range if specified
        if start_time and end_time:
            test_timestamps = self._filter_timestamps_by_range(test_timestamps, start_time, end_time)
            print(f"📅 Filtered timestamps to range: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")

        available_timestamps = []

        for timestamp in test_timestamps:
            if len(available_timestamps) >= count:
                break

            # Test with zmax (most reliable product)
            if self._check_timestamp_availability(timestamp, 'zmax'):
                available_timestamps.append(timestamp)
                print(f"✅ Found current: {timestamp}")

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
                # For SHMU uint8 data, 255 is the actual nodata marker (max uint8 value)
                # The nodata attribute value of -1 is impossible for uint8
                if data.dtype == np.uint8:
                    scaled_data[data == 255] = np.nan  # 255 is nodata for uint8
                else:
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
            print(f"🧹 Cleaned up {cleaned_count} temporary SHMU files")
        return cleaned_count

