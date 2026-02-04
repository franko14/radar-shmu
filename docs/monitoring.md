# Monitoring Guide

Monitor radar data availability, detect issues, and troubleshoot problems.

---

## Data Availability Analysis

### Historical Analyzer

Analyze radar data availability over time without downloading files.

```bash
# Analyze last 7 days
python3 scripts/analyze_radar_history.py --days 7

# Save report to file
python3 scripts/analyze_radar_history.py --days 7 --output reports/availability.txt

# Using Docker
./scripts/run_analysis_docker.sh 7
```

### Report Output

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

--------------------------------------------------------------------------------
COMPOSITE GENERATION ANALYSIS
--------------------------------------------------------------------------------
Common timestamps across all sources: 1850
Total DWD timestamps: 2016
Composite generation potential: 91.77%
```

---

## Understanding Metrics

### Uptime Percentage

| Uptime | Status |
|--------|--------|
| 100% | Perfect - all expected timestamps present |
| 90-99% | Good - occasional gaps |
| 80-89% | Warning - frequent gaps, investigate |
| <80% | Critical - significant issues |

### Generation Intervals

| Interval | Status |
|----------|--------|
| ~5.0 min | Normal (expected update frequency) |
| 5-6 min | Acceptable (slight delays) |
| 6-10 min | Warning (significant delays) |
| >10 min | Gap detected |

### Composite Generation Potential

| Potential | Status |
|-----------|--------|
| >95% | Excellent |
| 90-95% | Good |
| 80-90% | Fair - one source often missing |
| <80% | Poor - frequent mismatches |

---

## Health Checks

### Docker Containers

```bash
# Check running containers
docker ps

# View logs
docker logs -f dwd-fetcher

# Check last 100 lines
docker logs --tail 100 dwd-fetcher

# Execute command in container
docker exec dwd-fetcher ls /tmp/iradar/germany/
```

### Kubernetes

```bash
# Check CronJobs
kubectl get cronjobs

# Check recent Jobs
kubectl get jobs

# View logs
kubectl logs job/radar-dwd-fetcher-xxxxx

# Describe CronJob
kubectl describe cronjob radar-dwd-fetcher
```

### Output Verification

```bash
# Check recent files
ls -la /tmp/iradar/germany/ | tail -10

# Count files from last hour
find /tmp/iradar/germany/ -mmin -60 -name "*.png" | wc -l

# Verify extent file
cat /tmp/iradar-data/extent/dwd/extent_index.json | jq .

# Check file sizes
du -sh /tmp/iradar/germany/
```

---

## Troubleshooting

### No Data Downloaded

**Symptoms**: Empty output directory, no PNG files

**Check**:
1. Network connectivity
2. Source API availability:
   - DWD: https://opendata.dwd.de/weather/radar/composite/
   - SHMU: https://opendata.shmu.sk/
   - CHMI: https://opendata.chmi.cz/

```bash
# Test connectivity
curl -I https://opendata.dwd.de/weather/radar/composite/
curl -I https://opendata.shmu.sk/
curl -I https://opendata.chmi.cz/
```

### Upload Failures

**Symptoms**: Files generated locally but not uploaded

**Check**:
```bash
# Verify credentials loaded
echo $DIGITALOCEAN_SPACES_KEY

# Test connection
aws s3 ls s3://$DIGITALOCEAN_SPACES_BUCKET/ \
  --endpoint-url $DIGITALOCEAN_SPACES_ENDPOINT
```

**Common errors**:

| Error | Cause | Solution |
|-------|-------|----------|
| Missing env vars | Credentials not loaded | `set -a; source .env; set +a` |
| 403 Forbidden | Invalid keys | Verify in DO Console |
| 404 Not Found | Wrong bucket | Check bucket name and region |

### Memory Issues

**Symptoms**: OOM kills, crashes during processing

**Check**:
```bash
# Monitor memory
docker stats

# Check Kubernetes events
kubectl get events --sort-by='.lastTimestamp'
```

**Solutions**:
- Single fetch: Set limit to 512Mi
- Composite: Set limit to 1.2Gi
- Run memory profiler: `python3 scripts/profile_memory.py`

### Permission Denied

**Symptoms**: Can't write to output directory

**Fix**:
```bash
# Fix permissions
chmod -R 777 ./outputs

# Or run with user mapping
docker run --rm --user $(id -u):$(id -g) ...
```

### Container Exits Immediately

**Symptoms**: Container starts and stops

**Check**:
```bash
# View exit logs
docker logs dwd-fetcher

# Check exit code
docker inspect dwd-fetcher --format='{{.State.ExitCode}}'
```

**Common causes**:
- No command specified
- Command finished (expected for one-time jobs)
- Missing dependencies

### Cache Issues

**Symptoms**: Data re-downloaded every run despite caching

**Check**:
```bash
# Verify cache directory exists
ls -la ~/.cache/imeteo-radar/

# Check cache contents
ls -la ~/.cache/imeteo-radar/*.npz | head -20

# Check cache age
find ~/.cache/imeteo-radar/ -name "*.npz" -mmin -60 | wc -l
```

**Common causes**:

| Issue | Cause | Solution |
|-------|-------|----------|
| Cache miss | Timestamp format mismatch | Check logs for timestamp normalization |
| Re-downloading | Cache directory not persisted | Mount volume in Docker/K8s |
| Stale data | Cache not updated | Clear cache: `rm -rf ~/.cache/imeteo-radar/` |
| Disk full | Too many cached files | Automatic cleanup after 2 hours |

**Docker volume mount for cache persistence**:
```bash
docker run -v imeteo-cache:/root/.cache/imeteo-radar imeteo-radar composite
```

**Kubernetes PVC for cache**:
```yaml
volumes:
  - name: cache
    persistentVolumeClaim:
      claimName: imeteo-cache
volumeMounts:
  - name: cache
    mountPath: /root/.cache/imeteo-radar
```

---

## Scheduled Monitoring

### Weekly Health Check (Cron)

```bash
# Every Monday at 9 AM
0 9 * * 1 cd /path/to/radar-shmu && ./scripts/run_analysis_docker.sh 7 >> /var/log/radar-monitoring.log 2>&1
```

### Post-Incident Analysis

```bash
# Analyze affected period
python3 scripts/analyze_radar_history.py --days 3 --output reports/incident.txt

# Review gaps
grep "gap" reports/incident.txt
```

### Baseline Metrics

Run 30-day analysis to establish baselines:

```bash
./scripts/run_analysis_docker.sh 30

# Expected baselines:
# - DWD uptime: >99%
# - SHMU uptime: >95%
# - CHMI uptime: >90%
# - Composite potential: >90%
```

### Cache Efficiency Monitoring

Monitor cache hit rates over time:

```bash
# Create monitoring script
cat > /tmp/composite_monitor.sh << 'EOF'
#!/bin/bash
LOG_DIR="/tmp/composite_logs"
mkdir -p "$LOG_DIR"

for i in {1..8}; do
    echo "--- Run $i at $(date) ---"
    imeteo-radar composite --disable-upload 2>&1 | tee "$LOG_DIR/run_$i.log"
    sleep 240
done

# Analyze results
echo "=== Cache Efficiency ==="
for f in "$LOG_DIR"/run_*.log; do
    downloaded=$(grep -c "to download$" "$f" 2>/dev/null || echo "0")
    cached=$(grep -c "in cache" "$f" 2>/dev/null || echo "0")
    echo "$(basename $f): $cached cached, $downloaded downloaded"
done
EOF
chmod +x /tmp/composite_monitor.sh
```

**Expected metrics**:

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| Cache hit rate | >85% | 70-85% | <70% |
| Downloads per run | 1-2/source | 3-4/source | >5/source |
| Composites per run | 1 | 0-2 | >3 (indicates gaps) |

### Composite Generation Analysis

```bash
# Count composites generated over 30 minutes
grep "Processed.*composite" /tmp/composite_logs/*.log | \
  awk -F'Processed ' '{print $2}' | \
  awk '{sum+=$1} END {print "Total composites:", sum}'

# Count skipped timestamps by reason
grep "Already exist" /tmp/composite_logs/*.log | wc -l
grep "Insufficient sources" /tmp/composite_logs/*.log | wc -l
```

---

## Log Analysis

### Application Logs

Look for these patterns:

```
✅ Downloaded 12 files          # Success
☁️  Uploaded to Spaces          # Upload success
⚠️  Spaces not configured       # Upload disabled
❌ Failed to download           # Download error
⚠️  Failed to initialize        # Connection error
```

### Common Log Entries

| Log | Meaning |
|-----|---------|
| `📡 Fetching DWD...` | Starting fetch |
| `⏰ Backload period...` | Time range for backload |
| `📥 Downloading...` | Download in progress |
| `💾 Saved: /tmp/...` | Local save success |
| `☁️ Uploaded to Spaces` | Cloud upload success |
| `🗑️ Cleaned up X files` | Old file cleanup |

### Cache-Related Log Entries

| Log | Meaning |
|-----|---------|
| `📡 DWD: 8 available, 7 in cache, 1 to download` | Cache hit - only 1 new file |
| `📡 ARSO: 1 available, 1 in cache, 0 to download` | Full cache hit - no download needed |
| `📡 Cached: dwd_dmax_202601281430.npz` | New data cached |
| `📡 Timestamps available for matching` | Summary of available timestamps |
| `📡 Processed 1 composite(s), skipped 5` | Composites generated vs skipped |
| `📡   Already exist (local/S3): 5` | Skip reason: already generated |
| `📡   Insufficient sources: 1` | Skip reason: not enough data |

### Cache Efficiency Interpretation

| Pattern | Status |
|---------|--------|
| `X available, X-1 in cache, 1 to download` | Normal - only newest timestamp downloaded |
| `X available, X in cache, 0 to download` | Full cache hit - source hasn't updated |
| `X available, 0 in cache, X to download` | Cold cache or cache cleared |
| `0 available` | Source offline or network issue |

### S3 Composite Existence Check

The composite command checks both local and S3 before processing:

1. **Local check** (fast) - `output_dir/{timestamp}.png`
2. **S3 check** (if local miss) - `iradar/composite/{timestamp}.png`

This prevents regenerating composites after pod restarts in Kubernetes deployments where local storage is ephemeral.

```
# Log when composite exists in S3 but not locally
[15:42:29] 📡 Processed 0 composite(s), skipped 6
[15:42:29] 📡   Already exist (local/S3): 6 [...]
```

---

## Source-Specific Issues

### DWD (Germany)

- Uses LATEST endpoint - check if updated
- Stereographic projection may cause coordinate issues
- SSL certificate usually valid
- Timestamp format: `YYYYMMDD_HHMM` (with underscore)

### SHMU (Slovakia)

- SSL verification disabled (may have cert issues)
- 5-minute update intervals
- Coverage limited to Slovakia region
- Timestamp format: `YYYYMMDDHHMM`

### CHMI (Czech Republic)

- Newer source, may be less stable
- Check directory structure hasn't changed
- Data path differs from SHMU/DWD
- Timestamp format: `YYYYMMDDHHMMSS` (includes seconds)

### OMSZ (Hungary)

- Uses opendata portal at https://odp.met.hu/
- **netCDF format** (zipped), not HDF5
- Timestamp format: `YYYYMMDD_HHMM` (with underscore)
- Data scaling: `dBZ = raw / 2 - 32`
- Products: cmax (refl2D), pscappi (refl2D_pscappi)

### ARSO (Slovenia)

- **Special case**: Only provides latest timestamp (no archive)
- **SRD-3 format** (proprietary Slovenian Radar Data format)
- Lambert Conformal Conic projection (SIRAD)
- Update frequency: 10-15 minutes (slower than others)
- Cache shows "1 available" at most
- Base URL: https://meteo.arso.gov.si/uploads/probase/www/observ/radar

### IMGW (Poland)

- Uses IMGW public data portal at https://danepubliczne.imgw.pl/
- ODIM_H5 format (HDF5)
- Timestamp format: `YYYYMMDDHHMMSS` (14 digits)
- Files served via HVD path (not POLCOMP API)
- 5-minute update intervals, ~10 minute delay

---

## Getting Help

If issues persist:

1. Check application logs
2. Run availability analysis
3. Test source APIs manually
4. Open issue: https://github.com/imeteo/imeteo-radar/issues

Include in issue:
- Error logs
- Availability report
- Docker/K8s version
- Environment (local/cloud)
