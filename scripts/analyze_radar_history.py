#!/usr/bin/env python3
"""
Historical Radar Data Availability Analysis

Analyzes the last week of radar data from DWD, SHMU, and CHMI sources to detect:
- Generation timing and delays
- Data gaps and batch uploads
- Downtime periods
- Composite generation impact
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set
from collections import defaultdict
import pytz

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

# Import source classes
# Note: This requires all dependencies (h5py, requests, etc.) to be installed
try:
    from imeteo_radar.sources.dwd import DWDRadarSource
    from imeteo_radar.sources.shmu import SHMURadarSource
    from imeteo_radar.sources.chmi import CHMIRadarSource
except ImportError as e:
    print(f"ERROR: Failed to import radar sources: {e}")
    print("\nThis script requires the imeteo-radar package to be installed.")
    print("Please run: pip install -e .")
    print("Or use Docker: scripts/run_analysis_docker.sh")
    sys.exit(1)


class RadarHistoryAnalyzer:
    """Analyzes historical radar data availability"""

    def __init__(self, days: int = 7):
        self.days = days
        self.sources = {
            'dwd': (DWDRadarSource(), 'dmax'),
            'shmu': (SHMURadarSource(), 'zmax'),
            'chmi': (CHMIRadarSource(), 'maxz')
        }
        self.utc = pytz.UTC

    def get_available_timestamps(self, source_name: str) -> List[Dict]:
        """Get all available timestamps for a source without downloading files"""
        source, product = self.sources[source_name]

        print(f"Querying {source_name.upper()} timestamps (last {self.days} days)...")

        # Calculate hours to query based on days
        hours = self.days * 24

        # Use download_latest with large count to get metadata only
        # We'll use count=hours*12 to get all 5-minute intervals
        count = hours * 12

        try:
            files = source.download_latest(count=count, products=[product])

            # Extract timestamps and metadata
            timestamps = []
            for file_info in files:
                timestamp_str = file_info['timestamp']
                dt = datetime.strptime(timestamp_str[:14], "%Y%m%d%H%M%S")
                dt_utc = self.utc.localize(dt)

                timestamps.append({
                    'timestamp': timestamp_str,
                    'datetime': dt_utc,
                    'unix': int(dt_utc.timestamp())
                })

            return timestamps
        except Exception as e:
            print(f"  ERROR: Failed to query {source_name}: {e}")
            return []

    def analyze_source(self, source_name: str, timestamps: List[Dict]) -> Dict:
        """Analyze a single source's timestamp data"""
        if not timestamps:
            return {
                'error': 'No data available',
                'count': 0
            }

        # Sort by datetime
        timestamps = sorted(timestamps, key=lambda x: x['datetime'])

        # Calculate intervals between consecutive timestamps
        intervals = []
        gaps = []

        for i in range(1, len(timestamps)):
            prev_dt = timestamps[i-1]['datetime']
            curr_dt = timestamps[i]['datetime']
            interval = (curr_dt - prev_dt).total_seconds() / 60  # minutes
            intervals.append(interval)

            # Detect gaps (more than 10 minutes)
            if interval > 10:
                gaps.append({
                    'start': prev_dt,
                    'end': curr_dt,
                    'duration_minutes': interval
                })

        # Calculate statistics
        total_time = (timestamps[-1]['datetime'] - timestamps[0]['datetime']).total_seconds() / 3600  # hours
        expected_count = int(total_time * 12)  # 12 images per hour (5-minute intervals)
        actual_count = len(timestamps)
        uptime_pct = (actual_count / expected_count * 100) if expected_count > 0 else 0

        # Detect batch uploads (multiple timestamps appearing at once)
        # This would show as sudden availability of old data
        batch_uploads = []

        # Average interval
        avg_interval = sum(intervals) / len(intervals) if intervals else 0

        # Most recent timestamp
        most_recent = timestamps[-1]['datetime']
        oldest = timestamps[0]['datetime']

        return {
            'count': actual_count,
            'expected_count': expected_count,
            'uptime_pct': uptime_pct,
            'time_range': {
                'oldest': oldest,
                'most_recent': most_recent,
                'total_hours': total_time
            },
            'intervals': {
                'average_minutes': avg_interval,
                'min_minutes': min(intervals) if intervals else 0,
                'max_minutes': max(intervals) if intervals else 0
            },
            'gaps': gaps,
            'batch_uploads': batch_uploads  # TODO: implement detection
        }

    def find_common_timestamps(self, all_timestamps: Dict[str, List[Dict]]) -> Dict:
        """Find timestamps that are available across all sources"""
        # Convert to sets of unix timestamps
        timestamp_sets = {}
        for source_name, timestamps in all_timestamps.items():
            if timestamps:
                timestamp_sets[source_name] = set(t['unix'] for t in timestamps)

        if len(timestamp_sets) < 3:
            return {
                'error': 'Not all sources have data',
                'available_sources': list(timestamp_sets.keys())
            }

        # Find intersection
        common = timestamp_sets['dwd'] & timestamp_sets['shmu'] & timestamp_sets['chmi']

        # Calculate composite generation potential
        total_possible = len(timestamp_sets['dwd'])
        composite_count = len(common)
        composite_pct = (composite_count / total_possible * 100) if total_possible > 0 else 0

        return {
            'total_common': composite_count,
            'total_possible': total_possible,
            'composite_pct': composite_pct,
            'common_timestamps': sorted(list(common))
        }

    def generate_report(self, all_timestamps: Dict[str, List[Dict]],
                       analyses: Dict[str, Dict],
                       composite_analysis: Dict) -> str:
        """Generate human-readable report"""
        report = []
        report.append("=" * 80)
        report.append(f"RADAR DATA AVAILABILITY REPORT - Last {self.days} Days")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now(self.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        report.append("")

        # Per-source analysis
        for source_name in ['dwd', 'shmu', 'chmi']:
            analysis = analyses[source_name]
            report.append("-" * 80)
            report.append(f"{source_name.upper()} Analysis")
            report.append("-" * 80)

            if 'error' in analysis:
                report.append(f"ERROR: {analysis['error']}")
                report.append("")
                continue

            # Basic statistics
            report.append(f"Total images: {analysis['count']} (expected: {analysis['expected_count']})")
            report.append(f"Uptime: {analysis['uptime_pct']:.2f}%")
            report.append(f"Time range: {analysis['time_range']['oldest'].strftime('%Y-%m-%d %H:%M')} to "
                         f"{analysis['time_range']['most_recent'].strftime('%Y-%m-%d %H:%M')} UTC")
            report.append(f"Total coverage: {analysis['time_range']['total_hours']:.1f} hours")
            report.append("")

            # Interval statistics
            report.append(f"Generation intervals:")
            report.append(f"  Average: {analysis['intervals']['average_minutes']:.1f} minutes")
            report.append(f"  Min: {analysis['intervals']['min_minutes']:.1f} minutes")
            report.append(f"  Max: {analysis['intervals']['max_minutes']:.1f} minutes")
            report.append("")

            # Gaps
            if analysis['gaps']:
                report.append(f"Detected gaps (>{10} minutes): {len(analysis['gaps'])}")
                # Show top 10 longest gaps
                sorted_gaps = sorted(analysis['gaps'], key=lambda x: x['duration_minutes'], reverse=True)
                for i, gap in enumerate(sorted_gaps[:10], 1):
                    report.append(f"  {i}. {gap['duration_minutes']:.0f} min gap: "
                                 f"{gap['start'].strftime('%Y-%m-%d %H:%M')} to "
                                 f"{gap['end'].strftime('%Y-%m-%d %H:%M')}")
                if len(analysis['gaps']) > 10:
                    report.append(f"  ... and {len(analysis['gaps']) - 10} more gaps")
            else:
                report.append("No significant gaps detected!")

            report.append("")

        # Composite analysis
        report.append("-" * 80)
        report.append("COMPOSITE GENERATION ANALYSIS")
        report.append("-" * 80)

        if 'error' in composite_analysis:
            report.append(f"ERROR: {composite_analysis['error']}")
            report.append(f"Available sources: {', '.join(composite_analysis.get('available_sources', []))}")
        else:
            report.append(f"Common timestamps across all sources: {composite_analysis['total_common']}")
            report.append(f"Total DWD timestamps: {composite_analysis['total_possible']}")
            report.append(f"Composite generation potential: {composite_analysis['composite_pct']:.2f}%")
            report.append("")
            report.append(f"This means that {composite_analysis['composite_pct']:.1f}% of the time, "
                         f"all three sources have matching timestamps.")
            report.append(f"The remaining {100 - composite_analysis['composite_pct']:.1f}% of the time, "
                         f"at least one source is missing data.")

        report.append("")
        report.append("=" * 80)

        return "\n".join(report)

    def run(self) -> str:
        """Run the complete analysis"""
        print("Starting historical radar data analysis...")
        print("")

        # Query all sources
        all_timestamps = {}
        for source_name in ['dwd', 'shmu', 'chmi']:
            all_timestamps[source_name] = self.get_available_timestamps(source_name)
            print(f"  Found {len(all_timestamps[source_name])} timestamps for {source_name.upper()}")

        print("")
        print("Analyzing data...")

        # Analyze each source
        analyses = {}
        for source_name in ['dwd', 'shmu', 'chmi']:
            analyses[source_name] = self.analyze_source(source_name, all_timestamps[source_name])

        # Analyze composite potential
        composite_analysis = self.find_common_timestamps(all_timestamps)

        # Generate report
        report = self.generate_report(all_timestamps, analyses, composite_analysis)

        return report


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Analyze historical radar data availability'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days to analyze (default: 7)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path (default: print to stdout)'
    )

    args = parser.parse_args()

    # Run analysis
    analyzer = RadarHistoryAnalyzer(days=args.days)
    report = analyzer.run()

    # Output
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report)
        print(f"\nReport saved to: {output_path}")
    else:
        print("\n")
        print(report)


if __name__ == "__main__":
    main()
