#!/usr/bin/env python3
"""
Simple visualization demo for SHMU radar data without cartopy
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import json
import os
import requests
from PIL import Image
from shmu_colormap import get_shmu_colormap, get_dbz_range

def load_extent_metadata():
    """Load pre-extracted extent metadata"""
    extent_file = 'processed/radar_extent_metadata.json'
    if os.path.exists(extent_file):
        with open(extent_file, 'r') as f:
            return json.load(f)
    else:
        print(f"Extent metadata not found: {extent_file}")
        print("Run extract_extent.py first to generate extent metadata")
        return None

def create_simple_radar_plot(radar_data, save_path):
    """Create simple 2D plot of radar data"""
    
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    # Get data
    data = np.array(radar_data['data'])
    lons = np.array(radar_data['coordinates']['lons'])
    lats = np.array(radar_data['coordinates']['lats'])
    
    # Create meshgrid
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    
    # Mask invalid data
    data_masked = np.ma.masked_invalid(data)
    
    # Plot based on data type and units
    if radar_data.get('units') == 'dBZ':
        # DISCRETE SHMU colorscale with exact 1 dBZ intervals
        # Clean discrete steps with BoundaryNorm - no grey lines or artifacts
        
        # Get the discrete SHMU colorscale
        cmap, norm = get_shmu_colormap()
        min_dbz, max_dbz = get_dbz_range()
        
        # Mask data below minimum range (no data)
        data_plot = np.ma.masked_where(data_masked < min_dbz, data_masked)
        
        # Use 'nearest' interpolation with discrete colorscale to prevent artifacts
        im = ax.pcolormesh(lon_grid, lat_grid, data_plot, cmap=cmap, norm=norm, shading='nearest')
    elif radar_data['data_type'] == 'precipitation':
        # Use discrete levels for precipitation
        levels = [0.1, 0.5, 1, 2, 5, 10, 20]
        data_precip = np.ma.masked_where(data_masked <= 0.1, data_masked)
        im = ax.contourf(lon_grid, lat_grid, data_precip, levels=levels, cmap='Blues')
    else:
        # Continuous data
        im = ax.pcolormesh(lon_grid, lat_grid, data_masked, shading='auto', cmap='viridis')
    
    # Colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label(f"{radar_data['units']}", rotation=270, labelpad=15)
    
    # Labels and title
    ax.set_xlabel('Longitude (°E)')
    ax.set_ylabel('Latitude (°N)')
    
    product_name = radar_data['product_name']['name']
    timestamp = radar_data['timestamp']
    time_str = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]} {timestamp[8:10]}:{timestamp[10:12]}"
    
    ax.set_title(f"SHMU {product_name} - {time_str} UTC")
    
    # Data range info
    data_range = radar_data['data_range']
    ax.text(0.02, 0.98, f"Range: {data_range[0]:.1f} to {data_range[1]:.1f} {radar_data['units']}", 
            transform=ax.transAxes, verticalalignment='top', 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight', transparent=True)
    plt.close(fig)
    
    print(f"✓ Simple plot saved: {save_path}")

def create_shmu_overlay_plot(radar_data, timestamp, save_path):
    """Create PANV plot with SHMU official radar overlay"""
    
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    # Get data for coordinate reference
    lons = np.array(radar_data['coordinates']['lons'])
    lats = np.array(radar_data['coordinates']['lats'])
    
    try:
        # Download SHMU official radar image
        shmu_url = f"https://www.shmu.sk/data/dataradary/data.cappi2km/cappi.2km.{timestamp[:8]}.{timestamp[8:12]}.0.png"
        print(f"Downloading SHMU image: {shmu_url}")
        
        response = requests.get(shmu_url, timeout=10)
        response.raise_for_status()
        
        # Load and display the SHMU image
        from io import BytesIO
        img = Image.open(BytesIO(response.content))
        
        # Display the SHMU image as overlay
        # Map to the same coordinate extent as our data
        extent = (float(lons.min()), float(lons.max()), float(lats.min()), float(lats.max()))
        ax.imshow(img, extent=extent, aspect='auto', alpha=0.8, interpolation='bilinear')
        
        # Labels and title
        ax.set_xlabel('Longitude (°E)')
        ax.set_ylabel('Latitude (°N)')
        
        product_name = radar_data['product_name']['name']
        time_str = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]} {timestamp[8:10]}:{timestamp[10:12]}"
        
        ax.set_title(f"SHMU {product_name} (Official) - {time_str} UTC")
        
        # Add note about source
        ax.text(0.02, 0.02, "Source: SHMU Slovakia Official Radar", 
                transform=ax.transAxes, fontsize=8,
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        print(f"✓ SHMU overlay plot saved: {save_path}")
        
    except Exception as e:
        print(f"✗ Failed to download SHMU image: {e}")
        print(f"URL attempted: {shmu_url}")
        
        # Fallback: create regular plot
        create_simple_radar_plot(radar_data, save_path)
        return
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight', transparent=True)
    plt.close(fig)

def create_simple_radar_plot_no_colorbar(radar_data, save_path):
    """Create simple 2D plot of radar data without colorbar"""
    
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    # Get data
    data = np.array(radar_data['data'])
    lons = np.array(radar_data['coordinates']['lons'])
    lats = np.array(radar_data['coordinates']['lats'])
    
    # Create meshgrid
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    
    # Mask invalid data
    data_masked = np.ma.masked_invalid(data)
    
    # Plot based on data type and units
    if radar_data.get('units') == 'dBZ':
        # DISCRETE SHMU colorscale with exact 1 dBZ intervals
        # Clean discrete steps with BoundaryNorm - no grey lines or artifacts
        
        # Get the discrete SHMU colorscale
        cmap, norm = get_shmu_colormap()
        min_dbz, max_dbz = get_dbz_range()
        
        # Mask data below minimum range (no data)
        data_plot = np.ma.masked_where(data_masked < min_dbz, data_masked)
        
        # Use 'nearest' interpolation with discrete colorscale to prevent artifacts
        im = ax.pcolormesh(lon_grid, lat_grid, data_plot, cmap=cmap, norm=norm, shading='nearest')
    elif radar_data['data_type'] == 'precipitation':
        # Use discrete levels for precipitation
        levels = [0.1, 0.5, 1, 2, 5, 10, 20]
        data_precip = np.ma.masked_where(data_masked <= 0.1, data_masked)
        im = ax.contourf(lon_grid, lat_grid, data_precip, levels=levels, cmap='Blues')
    else:
        # Continuous data
        im = ax.pcolormesh(lon_grid, lat_grid, data_masked, shading='auto', cmap='viridis')
    
    # NO COLORBAR - this is the key difference
    
    # Labels and title
    ax.set_xlabel('Longitude (°E)')
    ax.set_ylabel('Latitude (°N)')
    
    product_name = radar_data['product_name']['name']
    timestamp = radar_data['timestamp']
    time_str = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]} {timestamp[8:10]}:{timestamp[10:12]}"
    
    ax.set_title(f"SHMU {product_name} - {time_str} UTC")
    
    # Data range info (without colorbar reference)
    data_range = radar_data['data_range']
    ax.text(0.02, 0.98, f"Range: {data_range[0]:.1f} to {data_range[1]:.1f} {radar_data['units']}", 
            transform=ax.transAxes, verticalalignment='top', 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight', transparent=True)
    plt.close(fig)
    
    print(f"✓ Simple plot (no colorbar) saved: {save_path}")

def create_sample_frontend_data():
    """Create sample data in various formats for frontend"""
    
    # Load processed JSON
    timestamp = "20250905153500"
    combined_file = f"processed/radar_all_{timestamp}.json"
    
    if not os.path.exists(combined_file):
        print("No processed data found. Run radar_processor.py first.")
        return
    
    with open(combined_file, 'r') as f:
        all_data = json.load(f)
    
    # Load extent metadata (extracted once from HDF files)
    extent_metadata = load_extent_metadata()
    if not extent_metadata:
        return
    
    # Create sample formats for frontend
    frontend_samples = {}
    
    for product_type, radar_data in all_data.items():
        
        # Structure for transparent PNG overlay usage
        simple_data = {
            'name': radar_data['product_name']['name'],
            'type': radar_data['data_type'],
            'timestamp': radar_data['timestamp'],
            'extent': extent_metadata['coordinate_systems'],  # WGS84 and Mercator info
            'grid_size': extent_metadata['grid_properties']['size'],
            'png_file': f"simple_{product_type.lower()}_{timestamp}.png",
            'units': radar_data['units'],
            'data_range': radar_data['data_range'],
            'transparent': True,  # PNG files are transparent
            'dpi': 150
        }
        
        frontend_samples[product_type] = simple_data
        
        # Create simple visualization
        if product_type.lower() == 'panv':
            # Create three versions for PANV (CAPPI 2km)
            # 1. Regular plot with colorbar
            plot_path = f"processed/simple_{product_type.lower()}_{timestamp}.png"
            create_simple_radar_plot(radar_data, plot_path)
            
            # 2. SHMU overlay version
            plot_path_shmu = f"processed/simple_{product_type.lower()}_{timestamp}_SHMU.png"
            create_shmu_overlay_plot(radar_data, timestamp, plot_path_shmu)
            
            # 3. No colorbar version (matches layout)
            plot_path_no_cbar = f"processed/simple_{product_type.lower()}_{timestamp}_no_colorbar.png"
            create_simple_radar_plot_no_colorbar(radar_data, plot_path_no_cbar)
        else:
            plot_path = f"processed/simple_{product_type.lower()}_{timestamp}.png"
            create_simple_radar_plot(radar_data, plot_path)
    
    # Save frontend index
    with open("processed/frontend_index.json", 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'products': frontend_samples,
            'api_info': {
                'coordinate_system': 'WGS84',
                'mercator_projection': 'EPSG:3857',
                'data_source': 'SHMU Slovakia',
                'file_format': 'transparent PNG',
                'usage': 'Overlay on web maps using Mercator extent'
            }
        }, f, indent=2)
    
    print("✓ Frontend index saved: processed/frontend_index.json")
    
    return frontend_samples

def main():
    """Main demo function"""
    print("Simple SHMU Radar Visualization Demo")
    print("=" * 40)
    
    # Create frontend samples
    samples = create_sample_frontend_data()
    
    if samples:
        print(f"\n✅ Created {len(samples)} product visualizations")
        print("\nGenerated files:")
        print("- Transparent PNG overlays for web mapping")
        print("- SHMU official overlay for PANV (PNG with _SHMU suffix)")
        print("- No-colorbar version for PANV (PNG with _no_colorbar suffix)")  
        print("- Mercator extent metadata (JSON)")
        print("- Ready for overlay on web maps (EPSG:3857)")
        
        print(f"\nProducts processed:")
        for product_type, info in samples.items():
            if product_type.lower() == 'panv':
                print(f"  - {product_type}: {info['name']} ({info['type']}, {info['units']}) [3 versions: regular, SHMU overlay, no colorbar]")
            else:
                print(f"  - {product_type}: {info['name']} ({info['type']}, {info['units']})")

if __name__ == "__main__":
    main()