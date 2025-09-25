#!/usr/bin/env python3
"""
Test script for Multi-Source Radar Processor

This script tests the basic functionality and helps identify DWD CAPPI equivalent.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test if all required modules can be imported"""
    print("🧪 Testing imports...")
    
    try:
        import numpy as np
        print("✅ numpy")
    except ImportError:
        print("❌ numpy - install with: pip install numpy")
        return False
        
    try:
        import h5py
        print("✅ h5py")
    except ImportError:
        print("❌ h5py - install with: pip install h5py")
        return False
        
    try:
        import matplotlib
        print("✅ matplotlib")
    except ImportError:
        print("❌ matplotlib - install with: pip install matplotlib")
        return False
        
    try:
        import requests
        print("✅ requests")
    except ImportError:
        print("❌ requests - install with: pip install requests")
        return False
        
    try:
        import scipy
        print("✅ scipy")
    except ImportError:
        print("❌ scipy - install with: pip install scipy")
        return False
        
    try:
        from radar_sources import SHMURadarSource, DWDRadarSource
        print("✅ radar_sources")
    except ImportError as e:
        print(f"❌ radar_sources - {e}")
        return False
        
    return True

def test_shmu_basic():
    """Test SHMU source basic functionality"""
    print("\n🇸🇰 Testing SHMU source...")
    
    try:
        from radar_sources import SHMURadarSource
        
        shmu = SHMURadarSource()
        print(f"✅ SHMU initialized")
        print(f"📋 Available products: {shmu.get_available_products()}")
        
        extent = shmu.get_extent()
        print(f"📍 Coverage: {extent['wgs84']}")
        
        return True
        
    except Exception as e:
        print(f"❌ SHMU test failed: {e}")
        return False

def test_dwd_basic():
    """Test DWD source basic functionality"""
    print("\n🇩🇪 Testing DWD source...")
    
    try:
        from radar_sources import DWDRadarSource
        
        dwd = DWDRadarSource()
        print(f"✅ DWD initialized")
        print(f"📋 Available products: {dwd.get_available_products()}")
        
        extent = dwd.get_extent()
        print(f"📍 Coverage: {extent['wgs84']}")
        
        return True
        
    except Exception as e:
        print(f"❌ DWD test failed: {e}")
        return False

def test_download_sample():
    """Test downloading sample data"""
    print("\n⬇️  Testing sample downloads...")
    
    try:
        from radar_sources import SHMURadarSource, DWDRadarSource
        
        # Test SHMU
        print("📡 Testing SHMU download...")
        shmu = SHMURadarSource()
        shmu_files = shmu.download_latest(count=1, products=['zmax'])
        
        if shmu_files:
            print(f"✅ SHMU: Downloaded {len(shmu_files)} files")
            
            # Try to process one file
            sample_file = shmu_files[0]
            processed = shmu.process_to_array(sample_file['path'])
            print(f"📊 SHMU data shape: {processed['dimensions']}")
            print(f"📊 SHMU data range: {np.nanmin(processed['data']):.1f} - {np.nanmax(processed['data']):.1f}")
        else:
            print("⚠️  No SHMU files downloaded")
            
        # Test DWD
        print("📡 Testing DWD download...")
        dwd = DWDRadarSource()
        dwd_files = dwd.download_latest(count=1, products=['dmax'])
        
        if dwd_files:
            print(f"✅ DWD: Downloaded {len(dwd_files)} files")
            
            # Try to process one file
            sample_file = dwd_files[0]
            processed = dwd.process_to_array(sample_file['path'])
            print(f"📊 DWD data shape: {processed['dimensions']}")
            print(f"📊 DWD data range: {np.nanmin(processed['data']):.1f} - {np.nanmax(processed['data']):.1f}")
        else:
            print("⚠️  No DWD files downloaded")
            
        return True
        
    except Exception as e:
        print(f"❌ Download test failed: {e}")
        return False

def analyze_dwd_products():
    """Analyze DWD products to find CAPPI 2km equivalent"""
    print("\n🔬 Analyzing DWD products for CAPPI 2km equivalent...")
    
    try:
        from radar_sources import DWDRadarSource
        
        dwd = DWDRadarSource()
        
        # Test each potential CAPPI product
        test_products = ['pg', 'hg', 'hx']
        
        for product in test_products:
            print(f"\n🧪 Testing product: {product}")
            try:
                dwd.analyze_product_metadata(product, sample_count=1)
            except Exception as e:
                print(f"❌ Failed to analyze {product}: {e}")
                
        return True
        
    except Exception as e:
        print(f"❌ DWD analysis failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Multi-Source Radar Processor Test Suite")
    print("=" * 50)
    
    # Test imports
    if not test_imports():
        print("\n❌ Import test failed - install missing dependencies")
        return False
        
    # Test basic functionality  
    shmu_ok = test_shmu_basic()
    dwd_ok = test_dwd_basic()
    
    if not (shmu_ok and dwd_ok):
        print("\n❌ Basic tests failed")
        return False
    
    # Test downloads
    print("\n" + "="*30)
    
    import numpy as np  # Need this for the download test
    
    if not test_download_sample():
        print("\n⚠️  Download test failed - check network connection")
        
    # Analyze DWD products
    print("\n" + "="*30)
    analyze_dwd_products()
    
    print("\n✅ Test suite completed!")
    print("\nNext steps:")
    print("1. Review DWD product analysis results")
    print("2. Identify which product is CAPPI 2km equivalent") 
    print("3. Run full processor: python radar_multi_source.py --sources all --count 3")
    
    return True

if __name__ == "__main__":
    main()