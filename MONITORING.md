# Radar Data Availability Monitoring

This document describes the monitoring tools for analyzing radar data availability across DWD, SHMU, and CHMI sources.

## Overview

The monitoring system provides historical analysis of radar data availability, helping identify:
- **Generation timing and delays**: When images are generated relative to the observation time
- **Data gaps**: Periods where no data is available
- **Batch uploads**: Retroactive data appearing later
- **Downtime periods**: Extended outages (>30 minutes)
- **Composite generation impact**: How often all three sources have matching timestamps

## Tools

### 1. Historical Data Analyzer

**Script**: `scripts/analyze_radar_history.py`

Analyzes the last N days of radar data and generates a comprehensive availability report.

#### Features

- Queries metadata from all three sources (DWD, SHMU, CHMI) without downloading files
- Calculates uptime percentage and generation intervals
- Detects gaps (>10 minutes between timestamps)
- Identifies common timestamps across all sources
- Calculates composite generation potential

#### Usage

**Direct execution** (requires dependencies):
```bash
python3 scripts/analyze_radar_history.py --days 7
python3 scripts/analyze_radar_history.py --days 7 --output reports/availability.txt
```

**Docker execution** (recommended):
```bash
./scripts/run_analysis_docker.sh 7  # Analyze last 7 days
```

#### Dependencies

- `requests` - HTTP requests
- `pytz` - Timezone handling
- `h5py` - HDF5 file parsing (from source classes)
- `numpy` - Array operations (from source classes)
- All dependencies from `imeteo-radar` package

**Installation**:
```bash
pip install -e .  # Install package with all dependencies
```

or use Docker which has everything pre-installed.

### 2. Wrapper Scripts

#### `generate_availability_report.sh`

Bash wrapper that runs the analysis and saves output to `reports/` directory.

```bash
./scripts/generate_availability_report.sh
```

**What it does**:
- Creates `reports/` directory
- Runs analysis for last 7 days
- Saves report with timestamp: `radar_availability_YYYYMMDD_HHMMSS.txt`
- Shows preview of last 20 lines

#### `run_analysis_docker.sh`

Docker-based execution for environments without Python dependencies.

```bash
./scripts/run_analysis_docker.sh 7  # Analyze 7 days
./scripts/run_analysis_docker.sh 1  # Analyze 1 day
```

**What it does**:
- Runs analysis in Docker container
- Mounts project directories (read-only)
- Saves reports to `reports/` directory

## Report Format

The generated report includes:

### Per-Source Analysis (DWD, SHMU, CHMI)

```
--------------------------------------------------------------------------------
DWD Analysis
--------------------------------------------------------------------------------
Total images: 2016 (expected: 2016)
Uptime: 100.00%
Time range: 2025-11-04 10:00 to 2025-11-11 10:00 UTC
Total coverage: 168.0 hours

Generation intervals:
  Average: 5.0 minutes
  Min: 5.0 minutes
  Max: 15.0 minutes

Detected gaps (>10 minutes): 3
  1. 45 min gap: 2025-11-08 14:20 to 2025-11-08 15:05
  2. 30 min gap: 2025-11-09 08:10 to 2025-11-09 08:40
  3. 20 min gap: 2025-11-10 22:35 to 2025-11-10 22:55
```

### Composite Generation Analysis

```
--------------------------------------------------------------------------------
COMPOSITE GENERATION ANALYSIS
--------------------------------------------------------------------------------
Common timestamps across all sources: 1850
Total DWD timestamps: 2016
Composite generation potential: 91.77%

This means that 91.8% of the time, all three sources have matching timestamps.
The remaining 8.2% of the time, at least one source is missing data.
```

## Use Cases

### 1. Weekly Health Check

Run analysis every Monday to review the previous week:

```bash
# Manual
./scripts/run_analysis_docker.sh 7

# Cron (every Monday at 9 AM)
0 9 * * 1 cd /path/to/radar-shmu && ./scripts/run_analysis_docker.sh 7 >> /var/log/radar-monitoring.log 2>&1
```

### 2. Post-Incident Analysis

After noticing missing data, analyze the affected period:

```bash
# Analyze specific number of days
python3 scripts/analyze_radar_history.py --days 3 --output reports/incident_analysis.txt
```

### 3. Performance Baseline

Establish baseline metrics for alerting:

```bash
# Analyze last 30 days
./scripts/run_analysis_docker.sh 30

# Review report to establish:
# - Expected uptime percentage
# - Typical gap frequency
# - Normal composite generation rate
```

### 4. Source Comparison

Compare reliability across DWD, SHMU, and CHMI:

```bash
# Run analysis and compare per-source statistics
python3 scripts/analyze_radar_history.py --days 7
```

Look at:
- Uptime percentages
- Average intervals
- Number of gaps
- Gap durations

## Understanding the Metrics

### Uptime Percentage

```
Uptime: 95.5%
```

- **100%**: No missing data, all expected timestamps present
- **90-99%**: Occasional gaps, generally reliable
- **80-89%**: Frequent gaps, investigate further
- **<80%**: Significant issues, source may be down

### Generation Intervals

```
Average: 5.2 minutes
Min: 5.0 minutes
Max: 15.0 minutes
```

- **Expected**: ~5.0 minutes (standard radar update frequency)
- **Acceptable**: 5.0-6.0 minutes (slight delays)
- **Warning**: 6.0-10.0 minutes (significant delays)
- **Critical**: >10.0 minutes (potential issues)

### Gaps

```
Detected gaps (>10 minutes): 5
  1. 120 min gap: 2025-11-08 14:00 to 2025-11-08 16:00
```

- **Minor gaps**: 10-30 minutes (temporary connectivity issues)
- **Major gaps**: 30-120 minutes (service interruption)
- **Downtime**: >120 minutes (extended outage)

### Composite Generation Potential

```
Composite generation potential: 85.5%
```

- **>95%**: Excellent - almost always possible to generate composite
- **90-95%**: Good - occasional mismatches
- **80-90%**: Fair - frequent mismatches affecting composite
- **<80%**: Poor - one or more sources frequently unavailable

## Troubleshooting

### "No module named 'cv2'" Error

**Solution**: Use Docker execution which has all dependencies:
```bash
./scripts/run_analysis_docker.sh 7
```

Or install opencv:
```bash
pip install opencv-python
```

### "Permission denied" Docker Error

**Solution**: Ensure Docker Desktop is running and logged in.

Or use direct Python execution:
```bash
pip install -e .
python3 scripts/analyze_radar_history.py --days 7
```

### "No data available" for a Source

**Possible causes**:
1. Network connectivity issues
2. Source API temporarily unavailable
3. Directory structure changed (needs code update)

**Solution**: Check source URL manually and verify API is responding.

### Analysis Takes Too Long

**Normal timing**: 1-3 minutes for 7 days (depends on network speed)

**If longer**:
- Reduce number of days: `--days 1`
- Check network connectivity
- Use Docker which may be faster

## Future Enhancements

### Real-time Monitoring (Optional)

For continuous monitoring, a daemon process could be implemented:

```python
# scripts/monitor_radar_live.py (not yet implemented)
# - Runs every 5 minutes
# - Checks for new data
# - Alerts if data is missing
# - Logs metrics to database
```

This would be useful if analysis shows frequent issues requiring immediate attention.

### Alerting System

Integration with monitoring systems:
- Prometheus metrics export
- Grafana dashboards
- Email/Slack alerts for downtime
- Threshold-based notifications

### Historical Database

Store metrics in database for long-term trend analysis:
- SQLite or PostgreSQL
- Time-series data
- Query historical uptime
- Generate trend reports

## Architecture Notes

### Why Metadata Queries?

The analyzer queries only metadata (timestamps, file listings) without downloading actual HDF5 files. This makes analysis:
- **Fast**: No large file downloads
- **Efficient**: Minimal bandwidth usage
- **Scalable**: Can analyze months of data quickly

### Source Class Reuse

The analyzer reuses the existing `DWDRadarSource`, `SHMURadarSource`, and `CHMIRadarSource` classes:
- **Consistency**: Same logic as production downloads
- **Maintainability**: Single source of truth
- **Reliability**: Well-tested code

### Docker Execution

Docker provides:
- **Consistency**: Same environment as production
- **Portability**: Works on any system with Docker
- **Simplicity**: No dependency management

## Example Workflow

### Weekly Monitoring Routine

1. **Run Analysis**
   ```bash
   ./scripts/run_analysis_docker.sh 7
   ```

2. **Review Report**
   ```bash
   cat reports/radar_availability_*.txt | tail -50
   ```

3. **Check Key Metrics**
   - DWD uptime: Should be >95%
   - SHMU uptime: Should be >95%
   - CHMI uptime: Should be >90% (newer source, may be less stable)
   - Composite potential: Should be >90%

4. **Investigate Issues**
   - If uptime <90%: Check source APIs
   - If gaps >2 hours: Review gap timestamps
   - If composite <85%: Identify which source is missing

5. **Archive Report**
   ```bash
   mv reports/radar_availability_*.txt reports/archive/
   ```

---

**Generated**: 2025-11-11
**Version**: 1.2.0
**Author**: Radar Processing Team
