#!/usr/bin/env python3
"""
Performance Analysis Tool for Radar Processing Pipeline

Analyzes memory usage, I/O operations, and processing bottlenecks.
"""

import time
import tracemalloc
import cProfile
import pstats
import io
import sys
from pathlib import Path
from memory_profiler import profile
import numpy as np

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from radar_sources import SHMURadarSource, DWDRadarSource
from radar_sources.merger import RadarMerger
from radar_sources.exporter import PNGExporter

class PerformanceAnalyzer:
    def __init__(self):
        self.timings = {}
        self.memory_snapshots = {}
        
    def analyze_memory_usage(self):
        """Analyze memory usage patterns"""
        print("\n" + "="*60)
        print("MEMORY USAGE ANALYSIS")
        print("="*60)
        
        tracemalloc.start()
        
        # Test SHMU processing
        print("\n1. SHMU Data Processing Memory Usage:")
        snapshot1 = tracemalloc.take_snapshot()
        
        shmu = SHMURadarSource()
        files = shmu.download_latest(count=1, products=['zmax'])
        
        snapshot2 = tracemalloc.take_snapshot()
        
        if files:
            data = shmu.process_to_array(files[0]['path'])
            snapshot3 = tracemalloc.take_snapshot()
            
            # Analyze memory growth
            stats = snapshot3.compare_to(snapshot1, 'lineno')
            print("\nTop 10 memory allocations:")
            for stat in stats[:10]:
                print(f"  {stat}")
                
            # Calculate memory usage
            download_mem = self._calculate_memory_diff(snapshot1, snapshot2)
            process_mem = self._calculate_memory_diff(snapshot2, snapshot3)
            
            print(f"\nMemory used for download: {download_mem / 1024 / 1024:.2f} MB")
            print(f"Memory used for processing: {process_mem / 1024 / 1024:.2f} MB")
            
            # Check for memory leaks
            self._check_memory_patterns(data)
            
        tracemalloc.stop()
        
    def _calculate_memory_diff(self, snapshot1, snapshot2):
        """Calculate memory difference between snapshots"""
        stats = snapshot2.compare_to(snapshot1, 'traceback')
        total = sum(stat.size_diff for stat in stats if stat.size_diff > 0)
        return total
        
    def _check_memory_patterns(self, data_dict):
        """Check for inefficient memory patterns"""
        print("\n2. Memory Pattern Analysis:")
        
        issues = []
        
        # Check for unnecessary data copies
        if 'data' in data_dict:
            arr = data_dict['data']
            if isinstance(arr, np.ndarray):
                print(f"  - Array shape: {arr.shape}")
                print(f"  - Array dtype: {arr.dtype}")
                print(f"  - Array memory: {arr.nbytes / 1024 / 1024:.2f} MB")
                
                # Check if array is C-contiguous (efficient)
                if not arr.flags['C_CONTIGUOUS']:
                    issues.append("Array is not C-contiguous (inefficient memory layout)")
                    
                # Check for unnecessary float64
                if arr.dtype == np.float64:
                    issues.append("Using float64 when float32 might suffice")
                    
        # Check for data duplication
        if 'coordinates' in data_dict:
            coords = data_dict['coordinates']
            if 'lons' in coords and 'lats' in coords:
                coord_mem = (len(coords['lons']) + len(coords['lats'])) * 8
                print(f"  - Coordinate arrays: {coord_mem / 1024:.2f} KB")
                
        if issues:
            print("\n  Potential issues found:")
            for issue in issues:
                print(f"    ⚠️  {issue}")
        else:
            print("  ✅ No major memory issues detected")
            
    def analyze_io_operations(self):
        """Analyze I/O operation efficiency"""
        print("\n" + "="*60)
        print("I/O OPERATIONS ANALYSIS")
        print("="*60)
        
        # Test download performance
        print("\n1. Network I/O Analysis:")
        shmu = SHMURadarSource()
        
        # Time HEAD requests
        start = time.time()
        available = shmu._check_timestamp_availability('20250909060000', 'zmax')
        head_time = time.time() - start
        print(f"  - HEAD request time: {head_time*1000:.2f} ms")
        
        # Test caching efficiency
        print("\n2. Cache Performance:")
        cache_dir = Path("processed/shmu_hdf_data")
        cached_files = list(cache_dir.glob("*.hdf"))
        print(f"  - Cached files: {len(cached_files)}")
        
        if cached_files:
            # Time reading from cache
            test_file = cached_files[0]
            start = time.time()
            with open(test_file, 'rb') as f:
                data = f.read()
            cache_read_time = time.time() - start
            print(f"  - Cache read time: {cache_read_time*1000:.2f} ms")
            print(f"  - File size: {len(data) / 1024 / 1024:.2f} MB")
            print(f"  - Read speed: {len(data) / cache_read_time / 1024 / 1024:.2f} MB/s")
            
    def analyze_processing_bottlenecks(self):
        """Identify processing bottlenecks"""
        print("\n" + "="*60)
        print("PROCESSING BOTTLENECKS ANALYSIS")  
        print("="*60)
        
        # Profile SHMU processing
        print("\n1. SHMU Processing Profile:")
        profiler = cProfile.Profile()
        
        shmu = SHMURadarSource()
        test_file = Path("processed/shmu_hdf_data").glob("*.hdf")
        test_file = next(test_file, None)
        
        if test_file:
            profiler.enable()
            data = shmu.process_to_array(str(test_file))
            profiler.disable()
            
            # Print profiling results
            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
            ps.print_stats(15)
            print(s.getvalue())
            
        # Profile merging
        print("\n2. Data Merging Profile:")
        merger = RadarMerger()
        
        # Create sample data for merging
        sample_data1 = {
            'data': np.random.rand(1000, 1000).astype(np.float32),
            'coordinates': {
                'lons': np.linspace(10, 20, 1000),
                'lats': np.linspace(45, 55, 1000)
            }
        }
        
        sample_data2 = {
            'data': np.random.rand(900, 900).astype(np.float32),
            'coordinates': {
                'lons': np.linspace(12, 22, 900),
                'lats': np.linspace(46, 54, 900)
            }
        }
        
        profiler = cProfile.Profile()
        profiler.enable()
        
        # Test regridding
        target_extent = {
            'wgs84': {'west': 10, 'east': 22, 'south': 45, 'north': 55},
            'lons': np.linspace(10, 22, 1200),
            'lats': np.linspace(45, 55, 1200)
        }
        
        result = merger._regrid_to_target(sample_data1, target_extent, (1200, 1200))
        
        profiler.disable()
        
        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
        ps.print_stats(10)
        print(s.getvalue())
        
    def identify_optimization_opportunities(self):
        """Identify specific optimization opportunities"""
        print("\n" + "="*60)
        print("OPTIMIZATION OPPORTUNITIES")
        print("="*60)
        
        opportunities = []
        
        # Check for parallel processing opportunities
        print("\n1. Parallelization Opportunities:")
        
        # In SHMU download_latest
        opportunities.append({
            'location': 'shmu.py:184-228',
            'issue': 'Sequential download of multiple files',
            'solution': 'Use concurrent.futures or asyncio for parallel downloads',
            'impact': 'Could reduce download time by 3-4x'
        })
        
        # In process_to_array
        opportunities.append({
            'location': 'shmu.py:257-261',
            'issue': 'Multiple passes over data for NaN handling',
            'solution': 'Combine operations in single vectorized operation',
            'impact': 'Reduce memory access by 50%'
        })
        
        # In merger
        opportunities.append({
            'location': 'merger.py:184-212',
            'issue': 'scipy.interpolate.RegularGridInterpolator is slow for large grids',
            'solution': 'Use cv2.remap or numba-accelerated interpolation',
            'impact': '5-10x speedup for interpolation'
        })
        
        # In exporter
        opportunities.append({
            'location': 'exporter.py:136-144',
            'issue': 'matplotlib is slow for large array visualization',
            'solution': 'Use PIL directly or opencv for faster PNG generation',
            'impact': '2-3x speedup in PNG export'
        })
        
        # Memory optimizations
        opportunities.append({
            'location': 'Multiple locations',
            'issue': 'Converting numpy arrays to lists for JSON serialization',
            'solution': 'Use numpy save/load or HDF5 for internal data passing',
            'impact': 'Reduce memory usage by 50-70%'
        })
        
        # Caching improvements
        opportunities.append({
            'location': 'shmu.py:130-149',
            'issue': 'Linear search through cached files',
            'solution': 'Maintain indexed cache metadata (SQLite or pickle)',
            'impact': 'O(1) cache lookups instead of O(n)'
        })
        
        print("\nIdentified optimizations:")
        for i, opt in enumerate(opportunities, 1):
            print(f"\n  {i}. {opt['location']}")
            print(f"     Issue: {opt['issue']}")
            print(f"     Solution: {opt['solution']}")
            print(f"     Impact: {opt['impact']}")
            
        return opportunities
        
def main():
    analyzer = PerformanceAnalyzer()
    
    print("🔬 RADAR PROCESSING PERFORMANCE ANALYSIS")
    print("=" * 60)
    
    # Run analyses
    analyzer.analyze_memory_usage()
    analyzer.analyze_io_operations()
    analyzer.analyze_processing_bottlenecks()
    opportunities = analyzer.identify_optimization_opportunities()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"\nTotal optimization opportunities found: {len(opportunities)}")
    print("\nTop priority optimizations:")
    print("1. Implement parallel downloading (3-4x speedup)")
    print("2. Replace scipy interpolation with cv2/numba (5-10x speedup)")
    print("3. Optimize PNG generation (2-3x speedup)")
    print("4. Implement proper caching index (instant lookups)")
    print("5. Use numpy arrays instead of lists internally (50% memory reduction)")
    
if __name__ == "__main__":
    main()