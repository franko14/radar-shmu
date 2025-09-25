#!/usr/bin/env python3
"""
SHMU Colormap Validation Tests

This module tests the SHMU colormap to ensure it provides discrete 1 dBZ increments
and is used correctly throughout the system.
"""

import sys
import os
import numpy as np
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def test_shmu_colormap_availability():
    """Test that SHMU colormap is available and importable"""
    try:
        from shmu_colormap import get_shmu_colormap, get_dbz_range
        print("✅ SHMU colormap imports successfully")
        assert True  # Import succeeded
    except ImportError as e:
        print(f"❌ SHMU colormap import failed: {e}")
        assert False, f"SHMU colormap import failed: {e}"

def test_discrete_dbz_increments():
    """Test that colormap provides discrete 1 dBZ increments"""
    from shmu_colormap import get_shmu_colormap, get_dbz_range
    
    cmap, norm = get_shmu_colormap()
    min_dbz, max_dbz = get_dbz_range()
    
    print(f"📊 Testing dBZ range: {min_dbz} to {max_dbz}")
    
    # Check that range is correct
    assert min_dbz == -35 and max_dbz == 85, f"Wrong dBZ range: expected -35 to 85, got {min_dbz} to {max_dbz}"
    
    # Check number of colors (should be 121 colors for -35 to 85)
    expected_colors = (max_dbz - min_dbz) + 1
    assert cmap.N == expected_colors, f"Wrong number of colors: expected {expected_colors}, got {cmap.N}"
    
    print(f"✅ Colormap has {cmap.N} discrete colors for 1 dBZ increments")
    
    # Check boundaries (should be at half-dBZ values for discrete steps)
    if hasattr(norm, 'boundaries'):
        boundaries = norm.boundaries
        expected_boundaries = np.arange(min_dbz - 0.5, max_dbz + 1, 1.0)
        
        assert np.allclose(boundaries, expected_boundaries, atol=1e-6), "Boundaries don't match expected 1 dBZ discrete steps"
        
        print("✅ Boundaries are correctly set for 1 dBZ discrete steps")
    else:
        assert False, "Norm doesn't have boundaries - might not be discrete"
    
    print(f"✅ Colormap has {cmap.N} discrete colors for 1 dBZ increments")

def test_colormap_consistency():
    """Test that colormap returns consistent colors for same dBZ values"""
    from shmu_colormap import get_color_for_dbz
    
    # Test key dBZ values
    test_values = [-35, -20, 0, 20, 40, 60, 85]
    
    print("🎨 Testing color consistency...")
    
    for dbz in test_values:
        color1 = get_color_for_dbz(dbz)
        color2 = get_color_for_dbz(dbz)
        
        assert np.allclose(color1, color2, atol=1e-6), f"Inconsistent colors for {dbz} dBZ"
    
    # Test that adjacent dBZ values give different colors
    for dbz in range(-30, 80, 10):
        color1 = get_color_for_dbz(dbz)
        color2 = get_color_for_dbz(dbz + 1)
        
        assert not np.allclose(color1[:3], color2[:3], atol=1e-6), f"Same colors for adjacent dBZ values: {dbz} and {dbz+1}"
    
    print("✅ Colors are consistent and discrete")

def test_exporter_uses_shmu_colormap():
    """Test that the PNG exporter uses SHMU colormap exclusively"""
    
    try:
        from radar_sources.exporter import PNGExporter
        
        exporter = PNGExporter()
        
        # Check that reflectivity_shmu colormap exists
        assert 'reflectivity_shmu' in exporter.colormaps, "Exporter doesn't have reflectivity_shmu colormap"
        
        # Check that it uses the correct colormap
        cmap_info = exporter.colormaps['reflectivity_shmu']
        assert cmap_info['units'] == 'dBZ', f"Wrong units in SHMU colormap: {cmap_info['units']}"
        assert cmap_info['range'] == [-35, 85], f"Wrong range in SHMU colormap: {cmap_info['range']}"
        
        print("✅ PNG Exporter correctly uses SHMU colormap")
        
    except ImportError as e:
        assert False, f"Failed to import PNG exporter: {e}"
    except Exception as e:
        assert False, f"Error testing exporter: {e}"

def test_no_fallback_colormaps():
    """Test that there are no fallback colormaps, only SHMU"""
    
    # Test that importing with missing shmu_colormap fails properly
    print("🔒 Testing that no fallbacks exist...")
    
    try:
        from radar_sources.exporter import PNGExporter
        exporter = PNGExporter()
        
        # Should only have reflectivity_shmu and possibly precipitation
        allowed_colormaps = {'reflectivity_shmu', 'precipitation'}
        
        for cmap_name in exporter.colormaps.keys():
            assert cmap_name in allowed_colormaps, f"Unexpected colormap found: {cmap_name}"
        
        print("✅ No forbidden fallback colormaps found")
        
    except Exception as e:
        assert False, f"Error checking fallback colormaps: {e}"

def generate_colormap_sample():
    """Generate a sample showing the discrete colormap"""
    try:
        import matplotlib.pyplot as plt
        from shmu_colormap import get_shmu_colormap, get_dbz_range
        
        cmap, norm = get_shmu_colormap()
        min_dbz, max_dbz = get_dbz_range()
        
        # Create sample data
        dbz_values = np.arange(min_dbz, max_dbz + 1, 1)
        data = np.tile(dbz_values, (10, 1))
        
        fig, ax = plt.subplots(figsize=(12, 2))
        
        # Use exact same parameters as radar processing
        im = ax.pcolormesh(
            np.arange(len(dbz_values) + 1), 
            np.arange(11), 
            data, 
            cmap=cmap, 
            norm=norm, 
            shading='nearest'  # Critical: ensures discrete colors
        )
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax, orientation='horizontal', shrink=0.8)
        cbar.set_label('dBZ (SHMU Colorscale)', fontsize=10)
        
        ax.set_title('SHMU Discrete Colorscale - 1 dBZ Increments')
        ax.set_ylabel('Sample')
        ax.set_xlabel('dBZ Value')
        
        # Set x-ticks to show key dBZ values
        tick_positions = np.arange(0, len(dbz_values), 10)
        tick_labels = [str(dbz_values[i]) for i in tick_positions]
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels)
        
        plt.tight_layout()
        plt.savefig('tests/shmu_colormap_sample.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        print("✅ Generated colormap sample: tests/shmu_colormap_sample.png")
        
    except ImportError:
        print("⚠️  Matplotlib not available for sample generation")
        # Not critical, just skip
    except Exception as e:
        assert False, f"Failed to generate sample: {e}"

def main():
    """Run all colormap validation tests"""
    
    print("🎨 SHMU Colormap Validation Tests")
    print("=" * 50)
    
    tests = [
        ("SHMU Colormap Availability", test_shmu_colormap_availability),
        ("Discrete 1 dBZ Increments", test_discrete_dbz_increments),
        ("Colormap Consistency", test_colormap_consistency),
        ("Exporter Uses SHMU Colormap", test_exporter_uses_shmu_colormap),
        ("No Fallback Colormaps", test_no_fallback_colormaps),
        ("Generate Sample", generate_colormap_sample)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        print("-" * 30)
        
        try:
            result = test_func()
            results.append((test_name, result))
            
            if result:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
                
        except Exception as e:
            print(f"💥 {test_name}: ERROR - {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    print(f"Passed: {passed}/{total} ({passed/total*100:.1f}%)")
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status} - {test_name}")
    
    if passed == total:
        print("\n🎉 All colormap tests passed!")
        print("✅ SHMU colormap is correctly implemented with discrete 1 dBZ steps")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        print("❌ Colormap implementation needs fixes")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)