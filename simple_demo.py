#!/usr/bin/env python3
"""
Simple visualization demo for SHMU radar data without cartopy
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import json
import os

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
    
    # Plot
    if radar_data['data_type'] == 'precipitation':
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
    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    
    print(f"✓ Simple plot saved: {save_path}")

def create_sample_frontend_data():
    """Create sample data in various formats for frontend"""
    
    # Load processed JSON
    timestamp = "20250904014500"
    combined_file = f"processed/radar_all_{timestamp}.json"
    
    if not os.path.exists(combined_file):
        print("No processed data found. Run radar_processor.py first.")
        return
    
    with open(combined_file, 'r') as f:
        all_data = json.load(f)
    
    # Create sample formats for frontend
    frontend_samples = {}
    
    for product_type, radar_data in all_data.items():
        # Simplified structure for JavaScript
        simple_data = {
            'name': radar_data['product_name']['name'],
            'type': radar_data['data_type'],
            'timestamp': radar_data['timestamp'],
            'extent': radar_data['extent'],  # [lon_min, lon_max, lat_min, lat_max]
            'grid_size': radar_data['dimensions'],
            'data_url': f"radar_{product_type.lower()}_{timestamp}.json",
            'units': radar_data['units'],
            'data_range': radar_data['data_range']
        }
        
        frontend_samples[product_type] = simple_data
        
        # Create simple visualization
        plot_path = f"processed/simple_{product_type.lower()}_{timestamp}.png"
        create_simple_radar_plot(radar_data, plot_path)
    
    # Save frontend index
    with open("processed/frontend_index.json", 'w') as f:
        json.dump({
            'timestamp': timestamp,
            'products': frontend_samples,
            'api_info': {
                'projection': 'Mercator',
                'coordinate_system': 'WGS84',
                'data_source': 'SHMU Slovakia'
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
        print("- Simple visualization plots (PNG)")
        print("- Frontend index (JSON)")
        print("- Individual product data (JSON)")
        
        print(f"\nProducts processed:")
        for product_type, info in samples.items():
            print(f"  - {info['name']} ({info['type']}, {info['units']})")

if __name__ == "__main__":
    main()