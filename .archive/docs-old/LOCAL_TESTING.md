# Local Testing Guide - Radar Scheduler

## Overview

This guide helps you run the radar image generation pipeline locally for testing before production deployment.

The scheduler runs every 5 minutes and generates:
- **3 individual radar images**: DWD (Germany), SHMU (Slovakia), CHMI (Czechia)
- **1 composite image**: Merged from all three sources with timestamp synchronization

## Quick Start

### 1. Build Docker Image

```bash
docker build -t imeteo-radar:test .
```

### 2. Start the Scheduler

```bash
./scripts/run-radar-scheduler.sh
```

The scheduler will:
- Run immediately on start
- Generate all 4 images (3 individual + 1 composite)
- Wait 5 minutes
- Repeat indefinitely

**To stop**: Press `Ctrl+C`

## Output Structure

All images are stored in `test-output/`:

```
test-output/
├── germany/       # DWD radar images (Unix timestamp.png)
├── slovakia/      # SHMU radar images (Unix timestamp.png)
├── czechia/       # CHMI radar images (Unix timestamp.png)
└── composite/     # Composite images (Unix timestamp.png)
```

Each image filename is a **Unix timestamp** (e.g., `1762796100.png`), representing the actual data timestamp in UTC.

## What to Monitor During Testing

### 1. Timestamp Synchronization
Check that composite images are only created when all sources have matching timestamps:

```bash
# Check latest composite log output
# Should see: "✅ Found common timestamp: YYYYMMDDHHMMSS"
```

If you see `❌ No common timestamp found`, it means:
- Sources have different update schedules at that moment
- The composite will be skipped for this run
- This is **expected behavior** - it will succeed on the next run

### 2. Data Quality
Verify image quality by checking:
- **No triangular artifacts** in composite images
- **Correct dBZ range**: -35 to +85 (shown in logs)
- **Realistic coverage**: 5-10% typical (not 100%)

### 3. File Count
Monitor image accumulation:

```bash
# Count images generated
ls -1 test-output/germany/*.png | wc -l
ls -1 test-output/slovakia/*.png | wc -l
ls -1 test-output/czechia/*.png | wc -l
ls -1 test-output/composite/*.png | wc -l
```

After 1 hour (12 runs):
- Individual sources: ~12 images each
- Composite: ~8-12 images (depending on timestamp alignment)

## Manual Testing

### Test Individual Sources

```bash
# Test DWD
docker run --rm -v $(pwd)/test-output/germany:/tmp/germany imeteo-radar:test \
  imeteo-radar fetch --source dwd

# Test SHMU
docker run --rm -v $(pwd)/test-output/slovakia:/tmp/slovakia imeteo-radar:test \
  imeteo-radar fetch --source shmu

# Test CHMI
docker run --rm -v $(pwd)/test-output/czechia:/tmp/czechia imeteo-radar:test \
  imeteo-radar fetch --source chmi
```

### Test Composite

```bash
docker run --rm -v $(pwd)/test-output/composite:/tmp/composite imeteo-radar:test \
  imeteo-radar composite --sources dwd,shmu,chmi --output /tmp/composite
```

## Expected Behavior

### Success Scenario
```
[2025-11-10 18:50:00] Starting radar generation...
📡 Generating individual radar images...
✅ DWD complete
✅ SHMU complete
✅ CHMI complete
🎨 Generating composite...
✅ Found common timestamp: 20251110173500
✅ Composite complete
📊 Statistics:
  Germany:   15 images
  Slovakia:  15 images
  Czechia:   15 images
  Composite: 12 images
```

### Partial Success (No Common Timestamp)
```
[2025-11-10 18:55:00] Starting radar generation...
📡 Generating individual radar images...
✅ DWD complete
✅ SHMU complete
✅ CHMI complete
🎨 Generating composite...
❌ No common timestamp found across all sources
   Each source has different timestamps - try again in a few minutes
⚠️  Composite generation failed (may be due to no common timestamp)
```

This is **normal** - sources don't always update simultaneously. The next run will likely succeed.

## Troubleshooting

### Issue: Docker permission denied
**Solution**: Restart Docker Desktop

### Issue: No images generated
**Solution**:
```bash
# Check Docker image exists
docker image inspect imeteo-radar:test

# Rebuild if needed
docker build -t imeteo-radar:test .
```

### Issue: Composite always fails
**Symptoms**: Never finds common timestamp

**Possible causes**:
1. Network issues connecting to data sources
2. One source is temporarily offline
3. Sources have mismatched time intervals

**Check individual sources**:
```bash
# Run fetch commands manually and check timestamps in output
docker run --rm imeteo-radar:test imeteo-radar fetch --source dwd
docker run --rm imeteo-radar:test imeteo-radar fetch --source shmu
docker run --rm imeteo-radar:test imeteo-radar fetch --source chmi
```

## Testing Checklist

Before considering test successful:

- [ ] Scheduler runs for at least 1 hour (12 cycles)
- [ ] Individual sources generate images consistently
- [ ] Composite succeeds at least 8 out of 12 times
- [ ] No triangular artifacts in composite images
- [ ] Data ranges are correct (-35 to +85 dBZ)
- [ ] Unix timestamp filenames match data timestamps
- [ ] Coverage percentages are realistic (5-10%)

## Next Steps

Once local testing is successful:
1. Review the PR: https://github.com/franko14/radar-shmu/pull/4
2. Merge to `development` branch
3. Deploy to production with proper cron scheduling
4. Monitor production for 24 hours

## Production Deployment

For production, use system cron instead of this script:

```bash
# Add to crontab
*/5 * * * * /path/to/production-radar-runner.sh >> /var/log/radar-scheduler.log 2>&1
```

See `DOCKER_DEPLOYMENT.md` for full production setup guide.
