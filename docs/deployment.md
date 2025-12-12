# Deployment Guide

Deploy iMeteo Radar using Docker, Docker Compose, Kubernetes, or cron jobs.

---

## Docker

### Quick Start

```bash
# Pull latest image
docker pull lfranko/imeteo-radar:latest

# Run single command
docker run --rm \
  -v $(pwd)/outputs:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd
```

### Image Tags

| Tag | Description |
|-----|-------------|
| `latest` | Latest stable from main branch |
| `1.2.0` | Specific version |
| `1.2` | Minor version (receives patches) |

### Volume Mounts

Mount output directories to persist generated images:

```bash
# All outputs to single directory
docker run --rm -v /data/radar:/tmp lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd

# Source-specific directories
docker run --rm -v /data/germany:/tmp/germany lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd
```

### Environment Variables

```bash
# With environment variables
docker run --rm \
  -e TZ=Europe/Berlin \
  -e DIGITALOCEAN_SPACES_KEY=xxx \
  -e DIGITALOCEAN_SPACES_SECRET=xxx \
  -v /data/radar:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd

# Using .env file
docker run --rm --env-file .env -v /data/radar:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd
```

---

## Docker Compose

### Basic Setup

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  # Manual command execution
  imeteo-radar:
    image: lfranko/imeteo-radar:latest
    container_name: imeteo-radar
    volumes:
      - ./outputs:/tmp
    environment:
      - TZ=Europe/Berlin

  # Automated DWD fetcher (every 5 minutes)
  dwd-fetcher:
    image: lfranko/imeteo-radar:latest
    container_name: dwd-fetcher
    command: sh -c "while true; do imeteo-radar fetch --source dwd; sleep 300; done"
    volumes:
      - ./outputs/germany:/tmp/germany
    environment:
      - TZ=Europe/Berlin
    restart: unless-stopped

  # Automated SHMU fetcher
  shmu-fetcher:
    image: lfranko/imeteo-radar:latest
    container_name: shmu-fetcher
    command: sh -c "while true; do imeteo-radar fetch --source shmu; sleep 300; done"
    volumes:
      - ./outputs/slovakia:/tmp/slovakia
    environment:
      - TZ=Europe/Bratislava
    restart: unless-stopped

  # Automated CHMI fetcher
  chmi-fetcher:
    image: lfranko/imeteo-radar:latest
    container_name: chmi-fetcher
    command: sh -c "while true; do imeteo-radar fetch --source chmi; sleep 300; done"
    volumes:
      - ./outputs/czechia:/tmp/czechia
    environment:
      - TZ=Europe/Prague
    restart: unless-stopped

  # Composite generator (every 5 minutes)
  composite-generator:
    image: lfranko/imeteo-radar:latest
    container_name: composite-generator
    command: sh -c "while true; do imeteo-radar composite; sleep 300; done"
    volumes:
      - ./outputs/composite:/tmp/composite
    environment:
      - TZ=Europe/Berlin
    restart: unless-stopped
```

### Commands

```bash
# Start all services
docker-compose up -d

# Start specific service
docker-compose up -d dwd-fetcher

# View logs
docker-compose logs -f dwd-fetcher

# Stop all services
docker-compose down

# Restart services
docker-compose restart
```

### Production Profile

For the full production setup with profiles:

```bash
# Use production profile
docker-compose --profile production up -d
```

---

## Kubernetes

### CronJob (Recommended)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: radar-dwd-fetcher
spec:
  schedule: "*/5 * * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: radar-fetcher
            image: lfranko/imeteo-radar:latest
            command: ["imeteo-radar", "fetch", "--source", "dwd"]
            envFrom:
            - secretRef:
                name: radar-credentials
            volumeMounts:
            - name: radar-output
              mountPath: /tmp/germany
            resources:
              requests:
                memory: "256Mi"
                cpu: "250m"
              limits:
                memory: "1Gi"
                cpu: "500m"
          volumes:
          - name: radar-output
            persistentVolumeClaim:
              claimName: radar-output-pvc
          restartPolicy: Never
```

### Secret

```bash
kubectl create secret generic radar-credentials \
  --from-literal=DIGITALOCEAN_SPACES_KEY='your-key' \
  --from-literal=DIGITALOCEAN_SPACES_SECRET='your-secret' \
  --from-literal=DIGITALOCEAN_SPACES_ENDPOINT='https://nyc3.digitaloceanspaces.com' \
  --from-literal=DIGITALOCEAN_SPACES_REGION='nyc3' \
  --from-literal=DIGITALOCEAN_SPACES_BUCKET='your-bucket' \
  --from-literal=DIGITALOCEAN_SPACES_URL='https://your-bucket.nyc3.digitaloceanspaces.com'
```

### One-Time Job (Backload)

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: radar-backload
spec:
  template:
    spec:
      containers:
      - name: radar-backload
        image: lfranko/imeteo-radar:latest
        command: ["imeteo-radar", "fetch", "--source", "dwd", "--backload", "--hours", "24"]
        envFrom:
        - secretRef:
            name: radar-credentials
      restartPolicy: Never
```

### Resource Recommendations

| Workload | Memory Request | Memory Limit | CPU Request | CPU Limit |
|----------|---------------|--------------|-------------|-----------|
| Single fetch | 256Mi | 512Mi | 250m | 500m |
| Composite | 512Mi | 1.2Gi | 500m | 1000m |
| Backload | 512Mi | 1Gi | 250m | 500m |

---

## Cron Jobs (Non-Docker)

For systems without Docker:

```bash
# /etc/crontab or crontab -e

# Fetch latest every 5 minutes
*/5 * * * * /usr/local/bin/imeteo-radar fetch --source dwd >> /var/log/radar-dwd.log 2>&1
*/5 * * * * /usr/local/bin/imeteo-radar fetch --source shmu >> /var/log/radar-shmu.log 2>&1
*/5 * * * * /usr/local/bin/imeteo-radar fetch --source chmi >> /var/log/radar-chmi.log 2>&1

# Generate composite every 5 minutes
*/5 * * * * /usr/local/bin/imeteo-radar composite >> /var/log/radar-composite.log 2>&1

# Daily backload at 1 AM
0 1 * * * /usr/local/bin/imeteo-radar fetch --source dwd --backload --hours 24 >> /var/log/radar-backload.log 2>&1
```

---

## Cloud Storage (DigitalOcean Spaces)

### Setup

1. **Create a Space** at [DigitalOcean Spaces](https://cloud.digitalocean.com/spaces)
2. **Generate API keys** at [API Tokens](https://cloud.digitalocean.com/account/api/spaces)

### Configuration

Create `.env` file:

```bash
DIGITALOCEAN_SPACES_KEY=DO00XXXXXXXXXXXXX
DIGITALOCEAN_SPACES_SECRET=your-secret-key
DIGITALOCEAN_SPACES_ENDPOINT=https://nyc3.digitaloceanspaces.com
DIGITALOCEAN_SPACES_REGION=nyc3
DIGITALOCEAN_SPACES_BUCKET=my-radar-data
DIGITALOCEAN_SPACES_URL=https://my-radar-data.nyc3.digitaloceanspaces.com
```

Load environment:

```bash
set -a; source .env; set +a
```

### Upload Path Structure

Files are uploaded to:

```
s3://your-bucket/iradar/germany/{timestamp}.png
s3://your-bucket/iradar/slovakia/{timestamp}.png
s3://your-bucket/iradar/czechia/{timestamp}.png
s3://your-bucket/iradar/composite/{timestamp}.png
```

### Verify Uploads

```bash
# Using AWS CLI
aws s3 ls s3://$DIGITALOCEAN_SPACES_BUCKET/iradar/ \
  --endpoint-url $DIGITALOCEAN_SPACES_ENDPOINT

# Or check in browser (if public)
open $DIGITALOCEAN_SPACES_URL/iradar/germany/
```

---

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `TZ` | No | Timezone (e.g., `Europe/Berlin`) |
| `DIGITALOCEAN_SPACES_KEY` | For upload | Spaces access key |
| `DIGITALOCEAN_SPACES_SECRET` | For upload | Spaces secret key |
| `DIGITALOCEAN_SPACES_ENDPOINT` | For upload | Endpoint URL |
| `DIGITALOCEAN_SPACES_REGION` | For upload | Region code |
| `DIGITALOCEAN_SPACES_BUCKET` | For upload | Bucket name |
| `DIGITALOCEAN_SPACES_URL` | For upload | Public URL |

---

## Troubleshooting

### Permission Denied

```bash
# Fix volume permissions
chmod -R 777 ./outputs

# Or run with user mapping
docker run --rm --user $(id -u):$(id -g) ...
```

### No Data Downloaded

- Check network connectivity
- Verify source APIs are online:
  - DWD: https://opendata.dwd.de/weather/radar/composite/
  - SHMU: https://opendata.shmu.sk/
  - CHMI: https://opendata.chmi.cz/

### Upload Not Working

```bash
# Check credentials are loaded
echo $DIGITALOCEAN_SPACES_KEY

# Test connection
aws s3 ls s3://$DIGITALOCEAN_SPACES_BUCKET/ \
  --endpoint-url $DIGITALOCEAN_SPACES_ENDPOINT
```

### Memory Issues

- Single fetch: 512MB limit
- Composite: 1.2GB limit
- Backload 24h: 1GB limit

See [architecture.md](architecture.md) for optimization details.

---

## Health Checks

### Docker

```bash
docker ps
docker logs -f dwd-fetcher
docker exec dwd-fetcher ls /tmp/germany/
```

### Kubernetes

```bash
kubectl get cronjobs
kubectl get jobs
kubectl logs job/radar-dwd-fetcher-xxxxx
```

### Output Verification

```bash
# Check recent files
ls -la /tmp/germany/ | tail -10

# Verify extent file
cat /tmp/germany/extent_index.json | jq .
```
