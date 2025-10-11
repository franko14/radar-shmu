#!/usr/bin/env python3
"""
Projection utilities for radar data coordinate transformations.

Handles conversion between different coordinate systems including:
- Stereographic projection (used by DWD)
- Mercator projection (used by SHMU) 
- Web Mercator (EPSG:3857) for visualization
- WGS84 geographic coordinates (EPSG:4326)
"""

import numpy as np
from typing import Tuple, Dict, Any, Optional
import warnings

try:
    from pyproj import CRS, Transformer
    PYPROJ_AVAILABLE = True
except ImportError:
    PYPROJ_AVAILABLE = False
    warnings.warn("pyproj not available - projection handling will be limited")


class ProjectionHandler:
    """Handles coordinate transformations for radar data"""
    
    def __init__(self):
        self.transformers = {}  # Cache transformers for efficiency
        
    def create_transformer(self, src_crs: str, dst_crs: str) -> Optional['Transformer']:
        """Create and cache a coordinate transformer"""
        if not PYPROJ_AVAILABLE:
            warnings.warn("pyproj not available - cannot create transformer")
            return None
            
        key = f"{src_crs}_to_{dst_crs}"
        if key not in self.transformers:
            try:
                src = CRS.from_string(src_crs)
                dst = CRS.from_string(dst_crs)
                self.transformers[key] = Transformer.from_crs(
                    src, dst, always_xy=True
                )
            except Exception as e:
                warnings.warn(f"Failed to create transformer {key}: {e}")
                return None
                
        return self.transformers[key]
    
    def parse_proj_string(self, proj_def: str) -> Optional[str]:
        """Parse projection definition string and normalize it"""
        if not proj_def:
            return None
            
        # Clean up the projection string
        proj_def = proj_def.strip()
        
        # Handle byte strings
        if isinstance(proj_def, bytes):
            proj_def = proj_def.decode('utf-8')
            
        return proj_def
    
    def create_dwd_coordinates(self, shape: Tuple[int, int], 
                             where_attrs: Dict[str, Any], 
                             proj_def: Optional[str] = None) -> Tuple[np.ndarray, np.ndarray]:
        """
        Create proper coordinate arrays for DWD stereographic data
        
        Args:
            shape: Data shape (ny, nx) 
            where_attrs: DWD where attributes with UL/UR/LL/LR coordinates
            proj_def: Projection definition string from HDF5
            
        Returns:
            Tuple of (longitudes, latitudes) arrays in WGS84
        """
        
        if not PYPROJ_AVAILABLE or not proj_def:
            # Fallback to corner averaging (less accurate)
            return self._fallback_dwd_coordinates(shape, where_attrs)
            
        # Parse projection definition
        proj_clean = self.parse_proj_string(proj_def)
        if not proj_clean:
            return self._fallback_dwd_coordinates(shape, where_attrs)
            
        try:
            # Create transformer from DWD projection to WGS84
            transformer = self.create_transformer(proj_clean, "EPSG:4326")
            if not transformer:
                return self._fallback_dwd_coordinates(shape, where_attrs)
                
            # Get grid parameters from where attributes
            xsize = shape[1]  # nx
            ysize = shape[0]  # ny
            
            # DWD typically uses these attributes for stereographic grid
            LL_x = float(where_attrs.get('LL_x', 0))  # Lower left X in projection coords
            LL_y = float(where_attrs.get('LL_y', 0))  # Lower left Y in projection coords  
            UR_x = float(where_attrs.get('UR_x', 0))  # Upper right X
            UR_y = float(where_attrs.get('UR_y', 0))  # Upper right Y
            
            # If projection coordinates not available, estimate from corner lat/lon
            if LL_x == 0 and LL_y == 0:
                return self._estimate_from_corners(shape, where_attrs, transformer)
                
            # Create regular grid in projection coordinates
            x_coords = np.linspace(LL_x, UR_x, xsize)
            y_coords = np.linspace(UR_y, LL_y, ysize)  # Note: Y decreases from top to bottom
            
            X, Y = np.meshgrid(x_coords, y_coords)
            
            # Transform to WGS84
            lons, lats = transformer.transform(X.flatten(), Y.flatten())
            lons = lons.reshape(shape)
            lats = lats.reshape(shape)
            
            return lons, lats
            
        except Exception as e:
            warnings.warn(f"DWD coordinate creation failed: {e}")
            return self._fallback_dwd_coordinates(shape, where_attrs)
    
    def _estimate_from_corners(self, shape: Tuple[int, int], 
                             where_attrs: Dict[str, Any],
                             transformer: 'Transformer') -> Tuple[np.ndarray, np.ndarray]:
        """Estimate projection coordinates from corner lat/lon"""
        
        # Get corner coordinates in WGS84
        ul_lon = float(where_attrs.get('UL_lon', 0))
        ul_lat = float(where_attrs.get('UL_lat', 0))
        ur_lon = float(where_attrs.get('UR_lon', 0))
        ur_lat = float(where_attrs.get('UR_lat', 0))
        ll_lon = float(where_attrs.get('LL_lon', 0))
        ll_lat = float(where_attrs.get('LL_lat', 0))
        lr_lon = float(where_attrs.get('LR_lon', 0))
        lr_lat = float(where_attrs.get('LR_lat', 0))
        
        # Transform corners to projection coordinates
        ul_x, ul_y = transformer.transform(ul_lon, ul_lat, direction='INVERSE')
        ur_x, ur_y = transformer.transform(ur_lon, ur_lat, direction='INVERSE')
        ll_x, ll_y = transformer.transform(ll_lon, ll_lat, direction='INVERSE')
        lr_x, lr_y = transformer.transform(lr_lon, lr_lat, direction='INVERSE')
        
        # Create grid in projection coordinates
        ny, nx = shape
        
        # Linear interpolation between corners (better than simple averaging)
        x_left = np.linspace(ll_x, ul_x, ny)
        x_right = np.linspace(lr_x, ur_x, ny)
        y_bottom = np.linspace(ll_y, lr_y, nx)
        y_top = np.linspace(ul_y, ur_y, nx)
        
        # Create 2D coordinate arrays
        lons = np.zeros(shape)
        lats = np.zeros(shape)
        
        for i in range(ny):
            x_coords = np.linspace(x_left[i], x_right[i], nx)
            y_coord = np.linspace(y_top[0], y_bottom[0], ny)[i]  # Approximate
            
            # Transform back to WGS84
            row_lons, row_lats = transformer.transform(x_coords, np.full(nx, y_coord))
            lons[i, :] = row_lons
            lats[i, :] = row_lats
            
        return lons, lats
    
    def calculate_dwd_extent(self, where_attrs: Dict[str, Any],
                           proj_def: Optional[str] = None) -> Dict[str, float]:
        """
        Calculate DWD extent bounds from corner coordinates only.

        MEMORY OPTIMIZATION: Instead of creating full 2D coordinate meshgrids (322 MB),
        we only transform the 4 corner points to get extent bounds. This saves ~322 MB
        of memory per file processed.

        Args:
            where_attrs: DWD where attributes with corner coordinates
            proj_def: Projection definition string from HDF5

        Returns:
            Dictionary with 'west', 'east', 'south', 'north' bounds in WGS84
        """

        # Extract corner coordinates from attributes
        ul_lon = float(where_attrs.get('UL_lon', 1.46))
        ul_lat = float(where_attrs.get('UL_lat', 55.86))
        ur_lon = float(where_attrs.get('UR_lon', 18.73))
        ur_lat = float(where_attrs.get('UR_lat', 55.85))
        ll_lon = float(where_attrs.get('LL_lon', 3.57))
        ll_lat = float(where_attrs.get('LL_lat', 45.70))
        lr_lon = float(where_attrs.get('LR_lon', 16.58))
        lr_lat = float(where_attrs.get('LR_lat', 45.68))

        # Calculate extent from corners (min/max of all 4 corner points)
        all_lons = [ul_lon, ur_lon, ll_lon, lr_lon]
        all_lats = [ul_lat, ur_lat, ll_lat, lr_lat]

        return {
            'west': min(all_lons),
            'east': max(all_lons),
            'south': min(all_lats),
            'north': max(all_lats)
        }

    def _fallback_dwd_coordinates(self, shape: Tuple[int, int],
                                where_attrs: Dict[str, Any]) -> Tuple[np.ndarray, np.ndarray]:
        """Fallback coordinate creation using corner averaging (less accurate)"""

        warnings.warn("Using fallback coordinate creation - accuracy reduced")

        # Extract corner coordinates
        ul_lon = float(where_attrs.get('UL_lon', 1.46))
        ul_lat = float(where_attrs.get('UL_lat', 55.86))
        ur_lon = float(where_attrs.get('UR_lon', 18.73))
        ur_lat = float(where_attrs.get('UR_lat', 55.85))
        ll_lon = float(where_attrs.get('LL_lon', 3.57))
        ll_lat = float(where_attrs.get('LL_lat', 45.70))
        lr_lon = float(where_attrs.get('LR_lon', 16.58))
        lr_lat = float(where_attrs.get('LR_lat', 45.68))

        # Average corners (original flawed method)
        west_lon = (ul_lon + ll_lon) / 2
        east_lon = (ur_lon + lr_lon) / 2
        north_lat = (ul_lat + ur_lat) / 2
        south_lat = (ll_lat + lr_lat) / 2

        # Create linear arrays
        lons = np.linspace(west_lon, east_lon, shape[1])
        lats = np.linspace(north_lat, south_lat, shape[0])

        # Create 2D grids
        lon_grid, lat_grid = np.meshgrid(lons, lats)

        return lon_grid, lat_grid
    
    def transform_coordinates(self, x: np.ndarray, y: np.ndarray, 
                            src_proj: str, dst_proj: str) -> Tuple[np.ndarray, np.ndarray]:
        """Transform coordinates between projections"""
        
        if not PYPROJ_AVAILABLE:
            warnings.warn("pyproj not available - returning original coordinates")
            return x, y
            
        transformer = self.create_transformer(src_proj, dst_proj)
        if not transformer:
            return x, y
            
        try:
            return transformer.transform(x, y)
        except Exception as e:
            warnings.warn(f"Coordinate transformation failed: {e}")
            return x, y


# Global instance for easy access
projection_handler = ProjectionHandler()