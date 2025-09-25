#!/usr/bin/env python3
"""
End-to-End Test Suite for Multi-Source Radar Processor

Tests three scenarios with hierarchical output naming:
1. DWD (Germany) only - using dmax product 
2. SHMU (Slovakia) only - using zmax and cappi2km products
3. Combined - both sources with merging

All outputs use shmu_colormap.py as the single source of colorscale.
"""

import sys
import os
import subprocess
from pathlib import Path
from datetime import datetime
import json

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def run_command(cmd: str, description: str) -> tuple[bool, str]:
    """Run a command and return success status and output"""
    print(f"🔄 {description}")
    print(f"📝 Command: {cmd}")
    
    try:
        result = subprocess.run(
            cmd.split(), 
            capture_output=True, 
            text=True, 
            timeout=300,  # 5 minute timeout
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
            return True, result.stdout
        else:
            print(f"❌ {description} - FAILED")
            print(f"Error: {result.stderr}")
            return False, result.stderr
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {description} - TIMEOUT")
        return False, "Command timed out"
    except Exception as e:
        print(f"💥 {description} - EXCEPTION: {e}")
        return False, str(e)

def verify_shmu_colormap_integration():
    """Verify that shmu_colormap.py is properly integrated as single source"""
    print("\n🎨 Verifying SHMU colormap integration...")
    
    try:
        from shmu_colormap import get_shmu_colormap, get_dbz_range
        
        # Test colormap functions
        colormap, norm = get_shmu_colormap()
        min_dbz, max_dbz = get_dbz_range()
        
        print(f"✅ SHMU colormap loaded successfully")
        print(f"📊 dBZ range: {min_dbz} to {max_dbz}")
        print(f"🎨 Colormap: {colormap.N} colors")
        
        return True
        
    except ImportError as e:
        print(f"❌ Failed to import shmu_colormap: {e}")
        return False
    except Exception as e:
        print(f"❌ Error testing SHMU colormap: {e}")
        return False

def analyze_output_structure(output_dir: Path, test_name: str) -> dict:
    """Analyze the generated output structure"""
    print(f"\n📂 Analyzing output structure for {test_name}")
    
    analysis = {
        'test_name': test_name,
        'output_dir': str(output_dir),
        'png_files': [],
        'json_files': [],
        'total_size_mb': 0,
        'has_transparency': False,
        'uses_shmu_colorscale': False
    }
    
    if not output_dir.exists():
        print(f"⚠️  Output directory does not exist: {output_dir}")
        return analysis
    
    # Find all PNG files
    png_files = list(output_dir.rglob("*.png"))
    json_files = list(output_dir.rglob("*.json"))
    
    analysis['png_files'] = [str(f.relative_to(output_dir)) for f in png_files]
    analysis['json_files'] = [str(f.relative_to(output_dir)) for f in json_files]
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in output_dir.rglob("*") if f.is_file())
    analysis['total_size_mb'] = round(total_size / (1024 * 1024), 2)
    
    # Check PNG properties (if PIL is available)
    if png_files:
        try:
            from PIL import Image
            sample_png = png_files[0]
            with Image.open(sample_png) as img:
                analysis['has_transparency'] = img.mode in ('RGBA', 'LA') or 'transparency' in img.info
                analysis['image_mode'] = img.mode
                analysis['image_size'] = img.size
        except ImportError:
            print("⚠️  PIL not available - skipping PNG analysis")
        except Exception as e:
            print(f"⚠️  PNG analysis failed: {e}")
    
    # Check if SHMU colorscale is used (check metadata)
    if json_files:
        try:
            sample_json = json_files[0]
            with open(sample_json, 'r') as f:
                metadata = json.load(f)
            
            # Look for SHMU colorscale references
            metadata_str = json.dumps(metadata).lower()
            analysis['uses_shmu_colorscale'] = (
                'shmu' in metadata_str and 
                ('colormap' in metadata_str or 'colorscale' in metadata_str)
            )
        except Exception as e:
            print(f"⚠️  Metadata analysis failed: {e}")
    
    print(f"📊 Found {len(png_files)} PNG files, {len(json_files)} JSON files")
    print(f"💾 Total size: {analysis['total_size_mb']} MB")
    print(f"🎨 Transparency: {'Yes' if analysis['has_transparency'] else 'No/Unknown'}")
    print(f"🎨 SHMU colorscale: {'Yes' if analysis['uses_shmu_colorscale'] else 'No/Unknown'}")
    
    return analysis

def main():
    """Run the complete end-to-end test suite"""
    
    print("🚀 Multi-Source Radar Processor - End-to-End Test Suite")
    print("=" * 80)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Verify SHMU colormap integration first
    if not verify_shmu_colormap_integration():
        print("\n❌ SHMU colormap verification failed - aborting tests")
        return False
    
    # Find .venv python path
    venv_python = Path('.venv/bin/python')
    if not venv_python.exists():
        print("❌ .venv/bin/python not found")
        return False
    
    # Test configuration
    tests = [
        {
            'name': 'DWD Germany Only',
            'description': 'Test DWD radar data processing with dmax product',
            'command': f'{venv_python} radar_multi_source.py --sources dwd --count 2 --output outputs/test_dwd_only',
            'output_dir': Path('outputs/test_dwd_only'),
            'expected_files': ['dwd/dmax_*.png', 'metadata_*.json']
        },
        {
            'name': 'SHMU Slovakia Only', 
            'description': 'Test SHMU radar data processing with zmax and cappi2km',
            'command': f'{venv_python} radar_multi_source.py --sources shmu --count 2 --output outputs/test_shmu_only',
            'output_dir': Path('outputs/test_shmu_only'),
            'expected_files': ['shmu/zmax_*.png', 'shmu/cappi2km_*.png', 'metadata_*.json']
        },
        {
            'name': 'Combined Multi-Source',
            'description': 'Test combined SHMU+DWD processing with merging',
            'command': f'{venv_python} radar_multi_source.py --sources all --count 2 --merge --output outputs/test_combined',
            'output_dir': Path('outputs/test_combined'),
            'expected_files': ['shmu/zmax_*.png', 'dwd/dmax_*.png', 'merged/merged_*.png', 'metadata_*.json']
        }
    ]
    
    results = []
    
    print(f"\n📋 Running {len(tests)} test scenarios...")
    
    # Run each test
    for i, test in enumerate(tests, 1):
        print(f"\n" + "="*60)
        print(f"🧪 Test {i}/{len(tests)}: {test['name']}")
        print(f"📝 {test['description']}")
        print("="*60)
        
        # Clean output directory
        if test['output_dir'].exists():
            print(f"🧹 Cleaning existing output: {test['output_dir']}")
            import shutil
            shutil.rmtree(test['output_dir'])
        
        # Run the test command
        success, output = run_command(test['command'], f"Running {test['name']}")
        
        # Analyze results
        analysis = analyze_output_structure(test['output_dir'], test['name'])
        
        # Store results
        result = {
            'test': test,
            'success': success,
            'output': output[:1000] if output else "",  # Truncate long outputs
            'analysis': analysis
        }
        results.append(result)
        
        # Quick verification
        if success and analysis['png_files']:
            print(f"✅ Test completed successfully with {len(analysis['png_files'])} PNG outputs")
        else:
            print(f"⚠️  Test completed with issues")
    
    # Generate comprehensive report
    print("\n" + "="*80)
    print("📊 FINAL TEST REPORT")
    print("="*80)
    
    total_tests = len(results)
    successful_tests = sum(1 for r in results if r['success'])
    
    print(f"📈 Overall Success Rate: {successful_tests}/{total_tests} ({successful_tests/total_tests*100:.1f}%)")
    
    # Hierarchical output structure verification
    print(f"\n📁 Hierarchical Output Structure:")
    
    all_outputs = Path('outputs')
    if all_outputs.exists():
        for test_dir in sorted(all_outputs.iterdir()):
            if test_dir.is_dir():
                print(f"📂 {test_dir.name}/")
                for file_path in sorted(test_dir.rglob("*")):
                    if file_path.is_file():
                        rel_path = file_path.relative_to(test_dir)
                        size_kb = file_path.stat().st_size / 1024
                        print(f"   📄 {rel_path} ({size_kb:.1f}KB)")
    
    # Detailed test results
    for i, result in enumerate(results, 1):
        test = result['test']
        analysis = result['analysis']
        
        print(f"\n🧪 Test {i}: {test['name']}")
        print(f"   Status: {'✅ PASSED' if result['success'] else '❌ FAILED'}")
        print(f"   PNG Files: {len(analysis['png_files'])}")
        print(f"   JSON Files: {len(analysis['json_files'])}")
        print(f"   Total Size: {analysis['total_size_mb']} MB")
        print(f"   Transparency: {'✅ Yes' if analysis['has_transparency'] else '❓ Unknown'}")
        print(f"   SHMU Colorscale: {'✅ Yes' if analysis['uses_shmu_colorscale'] else '❓ Unknown'}")
        
        if analysis['png_files']:
            print(f"   Generated Files:")
            for png_file in analysis['png_files'][:5]:  # Show first 5
                print(f"     - {png_file}")
            if len(analysis['png_files']) > 5:
                print(f"     ... and {len(analysis['png_files']) - 5} more")
    
    # Save detailed report
    report_path = Path('outputs/test_report.json')
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_tests': total_tests,
                'successful_tests': successful_tests,
                'success_rate': successful_tests/total_tests*100
            },
            'results': results
        }, f, indent=2, default=str)
    
    print(f"\n📋 Detailed report saved: {report_path}")
    
    # Final recommendations
    print(f"\n💡 Recommendations:")
    
    if successful_tests == total_tests:
        print("✅ All tests passed! System is ready for production use.")
        print("🎨 Verify that all PNG outputs use the SHMU colorscale consistently.")
        print("🔗 Integrate outputs with your web mapping application.")
    else:
        print("⚠️  Some tests failed. Review error messages above.")
        print("🔧 Check network connectivity for data downloads.")
        print("🗂️  Verify output directory permissions.")
    
    print(f"\n⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return successful_tests == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)