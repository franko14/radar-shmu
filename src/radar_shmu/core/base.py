#!/usr/bin/env python3
"""
Base classes for radar data sources
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Tuple
import numpy as np

class RadarSource(ABC):
    """Abstract base class for radar data sources"""
    
    def __init__(self, name: str):
        self.name = name
        self.cache_dir = f"processed/{name}_data"
        
    @abstractmethod
    def download_latest(self, count: int, products: List[str] = None) -> List[Dict[str, Any]]:
        """
        Download latest available radar data files
        
        Args:
            count: Number of timestamps to download
            products: List of product types to download
            
        Returns:
            List of downloaded file information dictionaries
        """
        pass
        
    @abstractmethod
    def process_to_array(self, file_path: str) -> Dict[str, Any]:
        """
        Process radar file to numpy array with metadata
        
        Args:
            file_path: Path to radar data file
            
        Returns:
            Dictionary with processed data, coordinates, and metadata
        """
        pass
        
    @abstractmethod
    def get_extent(self) -> Dict[str, Any]:
        """
        Get geographic extent information for this radar source
        
        Returns:
            Dictionary with extent information in various projections
        """
        pass
        
    @abstractmethod
    def get_available_products(self) -> List[str]:
        """
        Get list of available radar products for this source
        
        Returns:
            List of product identifiers
        """
        pass
        
    def get_product_metadata(self, product: str) -> Dict[str, Any]:
        """
        Get metadata for a specific product
        
        Args:
            product: Product identifier
            
        Returns:
            Dictionary with product metadata
        """
        return {
            'product': product,
            'source': self.name,
            'units': 'unknown',
            'description': 'No description available'
        }

class RadarData:
    """Container for processed radar data"""
    
    def __init__(self, 
                 data: np.ndarray,
                 coordinates: Dict[str, np.ndarray],
                 metadata: Dict[str, Any],
                 extent: Dict[str, Any]):
        self.data = data
        self.coordinates = coordinates
        self.metadata = metadata  
        self.extent = extent
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'data': self.data.tolist() if hasattr(self.data, 'tolist') else self.data,
            'coordinates': {
                key: arr.tolist() if hasattr(arr, 'tolist') else arr
                for key, arr in self.coordinates.items()
            },
            'metadata': self.metadata,
            'extent': self.extent
        }

def lonlat_to_mercator(lon: float, lat: float) -> Tuple[float, float]:
    """Convert WGS84 coordinates to Web Mercator (EPSG:3857)"""
    import math
    x = lon * 20037508.34 / 180.0
    y = math.log(math.tan((90.0 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
    y = y * 20037508.34 / 180.0
    return x, y

def mercator_to_lonlat(x: float, y: float) -> Tuple[float, float]:
    """Convert Web Mercator (EPSG:3857) to WGS84 coordinates"""
    import math
    lon = x / 20037508.34 * 180.0
    lat = math.atan(math.exp(y / 20037508.34 * math.pi / 180.0)) * 360.0 / math.pi - 90.0
    return lon, lat