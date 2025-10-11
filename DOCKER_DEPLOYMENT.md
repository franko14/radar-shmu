# Docker Deployment Guide

This guide explains how to use the pre-built `lfranko/imeteo-radar` Docker image from DockerHub.

## 📦 Quick Start

### Pull the Latest Image

```bash
docker pull lfranko/imeteo-radar:latest
```

### Run Your First Command

```bash
# Fetch latest DWD radar data
docker run --rm -v $(pwd)/outputs:/tmp lfranko/imeteo-radar:latest imeteo-radar fetch --source dwd

# Fetch latest SHMU radar data
docker run --rm -v $(pwd)/outputs:/tmp lfranko/imeteo-radar:latest imeteo-radar fetch --source shmu
```

## 🎯 Available Image Tags

| Tag | Description | Use Case |
|-----|-------------|----------|
| `latest` | Latest stable version from main branch | Production use |
| `1.0.0` | Specific version release | Version pinning |
| `1.0` | Minor version (auto-updates patches) | Stable with patches |

**Example:**
```bash
# Pull specific version
docker pull lfranko/imeteo-radar:1.0.0

# Pull latest patch of version 1.0
docker pull lfranko/imeteo-radar:1.0
```

## 🚀 Usage Examples

### Single Command Execution

```bash
# Fetch latest DWD data (Germany)
docker run --rm \
  -v $(pwd)/radar-output:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd

# Fetch latest SHMU data (Slovakia)
docker run --rm \
  -v $(pwd)/radar-output:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source shmu

# Backload last 6 hours
docker run --rm \
  -v $(pwd)/radar-output:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd --backload --hours 6

# Generate extent files
docker run --rm \
  -v $(pwd)/radar-output:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar extent --source all
```

### Using Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  # Manual execution service
  imeteo-radar:
    image: lfranko/imeteo-radar:latest
    container_name: imeteo-radar
    volumes:
      - ./radar-output:/tmp
    environment:
      - TZ=Europe/Berlin

  # Automated DWD fetcher (every 5 minutes)
  dwd-fetcher:
    image: lfranko/imeteo-radar:latest
    container_name: dwd-fetcher
    command: sh -c "while true; do imeteo-radar fetch --source dwd; sleep 300; done"
    volumes:
      - ./radar-output/germany:/tmp/germany
    environment:
      - TZ=Europe/Berlin
    restart: unless-stopped

  # Automated SHMU fetcher (every 5 minutes)
  shmu-fetcher:
    image: lfranko/imeteo-radar:latest
    container_name: shmu-fetcher
    command: sh -c "while true; do imeteo-radar fetch --source shmu; sleep 300; done"
    volumes:
      - ./radar-output/slovakia:/tmp/slovakia
    environment:
      - TZ=Europe/Bratislava
    restart: unless-stopped
```

**Run services:**
```bash
# Start all services
docker-compose up -d

# Start only DWD fetcher
docker-compose up -d dwd-fetcher

# View logs
docker-compose logs -f dwd-fetcher

# Stop services
docker-compose down
```

## 🔧 Environment Variables

Configure these environment variables for cloud storage integration:

```yaml
environment:
  - TZ=Europe/Berlin                                    # Timezone
  - DIGITALOCEAN_SPACES_KEY=${DO_SPACES_KEY}           # DigitalOcean Spaces access key
  - DIGITALOCEAN_SPACES_SECRET=${DO_SPACES_SECRET}     # DigitalOcean Spaces secret
  - DIGITALOCEAN_SPACES_ENDPOINT=${DO_SPACES_ENDPOINT} # Endpoint URL
  - DIGITALOCEAN_SPACES_REGION=${DO_SPACES_REGION}     # Region (e.g., fra1)
  - DIGITALOCEAN_SPACES_BUCKET=${DO_SPACES_BUCKET}     # Bucket name
  - DIGITALOCEAN_SPACES_URL=${DO_SPACES_URL}           # Public URL
```

**Using .env file:**

Create a `.env` file:
```bash
DO_SPACES_KEY=your_access_key
DO_SPACES_SECRET=your_secret_key
DO_SPACES_ENDPOINT=https://fra1.digitaloceanspaces.com
DO_SPACES_REGION=fra1
DO_SPACES_BUCKET=your-bucket-name
DO_SPACES_URL=https://your-bucket.fra1.digitaloceanspaces.com
```

Reference in docker-compose.yml:
```yaml
services:
  dwd-fetcher:
    image: lfranko/imeteo-radar:latest
    env_file:
      - .env
```

## 🏭 Production Deployment

### Option 1: Docker Compose (Recommended)

1. **Clone repository** (for docker-compose.yml):
   ```bash
   git clone https://github.com/imeteo/imeteo-radar.git
   cd imeteo-radar
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. **Start production services**:
   ```bash
   docker-compose --profile production up -d
   ```

4. **Monitor logs**:
   ```bash
   docker-compose logs -f
   ```

### Option 2: Standalone Docker

```bash
# Create output directory
mkdir -p /data/radar-output

# Run DWD fetcher as daemon
docker run -d \
  --name dwd-fetcher \
  --restart unless-stopped \
  -v /data/radar-output/germany:/tmp/germany \
  -e TZ=Europe/Berlin \
  lfranko/imeteo-radar:latest \
  sh -c "while true; do imeteo-radar fetch --source dwd; sleep 300; done"

# Run SHMU fetcher as daemon
docker run -d \
  --name shmu-fetcher \
  --restart unless-stopped \
  -v /data/radar-output/slovakia:/tmp/slovakia \
  -e TZ=Europe/Bratislava \
  lfranko/imeteo-radar:latest \
  sh -c "while true; do imeteo-radar fetch --source shmu; sleep 300; done"
```

### Option 3: Kubernetes

Example deployment manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dwd-fetcher
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dwd-fetcher
  template:
    metadata:
      labels:
        app: dwd-fetcher
    spec:
      containers:
      - name: dwd-fetcher
        image: lfranko/imeteo-radar:latest
        command: ["sh", "-c"]
        args: ["while true; do imeteo-radar fetch --source dwd; sleep 300; done"]
        env:
        - name: TZ
          value: "Europe/Berlin"
        volumeMounts:
        - name: radar-output
          mountPath: /tmp/germany
      volumes:
      - name: radar-output
        persistentVolumeClaim:
          claimName: radar-output-pvc
```

## 📊 Data Output

### Output Structure

```
radar-output/
├── germany/              # DWD radar data
│   ├── 2024-09-25_1000.png
│   ├── 2024-09-25_1005.png
│   └── extent_index.json
└── slovakia/             # SHMU radar data
    ├── 2024-09-25_1000.png
    ├── 2024-09-25_1005.png
    └── extent_index.json
```

### File Naming Convention

- **PNG files**: `YYYY-MM-DD_HHMM.png` (e.g., `2024-09-25_1430.png`)
- **Extent files**: `extent_index.json` (geo-referencing metadata)

## 🔍 Troubleshooting

### Issue: "Permission denied" errors

**Problem**: Container can't write to mounted volumes.

**Solution**: Ensure volume directories have correct permissions:
```bash
mkdir -p ./radar-output
chmod -R 777 ./radar-output
```

Or run with user mapping:
```bash
docker run --rm \
  --user $(id -u):$(id -g) \
  -v $(pwd)/radar-output:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source dwd
```

### Issue: "No such file or directory"

**Problem**: Output directory doesn't exist.

**Solution**: Create it before running:
```bash
mkdir -p ./radar-output/germany ./radar-output/slovakia
```

### Issue: Image not pulling latest version

**Problem**: Docker cached old image.

**Solution**: Force pull latest:
```bash
docker pull lfranko/imeteo-radar:latest --no-cache
```

### Issue: Container exits immediately

**Problem**: No command specified or command finished.

**Solution**: Always specify a command:
```bash
docker run --rm lfranko/imeteo-radar:latest imeteo-radar --help
```

### Issue: Time zone is wrong

**Problem**: Container using UTC instead of local time.

**Solution**: Set TZ environment variable:
```bash
docker run --rm \
  -e TZ=Europe/Bratislava \
  -v $(pwd)/outputs:/tmp \
  lfranko/imeteo-radar:latest \
  imeteo-radar fetch --source shmu
```

## 🔄 Updating to Latest Version

```bash
# Pull latest image
docker pull lfranko/imeteo-radar:latest

# Restart running containers
docker-compose restart

# Or for standalone containers
docker restart dwd-fetcher shmu-fetcher
```

## 📋 Health Checks

Monitor your running containers:

```bash
# Check container status
docker ps

# View logs
docker logs -f dwd-fetcher

# Check last 100 lines
docker logs --tail 100 dwd-fetcher

# Follow logs with timestamps
docker logs -f --timestamps dwd-fetcher

# Execute command in running container
docker exec dwd-fetcher imeteo-radar --help
```

## 🔐 Security Best Practices

1. **Never commit credentials**: Use `.env` files and add to `.gitignore`
2. **Use secrets management**: In production, use Docker secrets or Kubernetes secrets
3. **Pin versions**: Use specific tags in production (e.g., `1.0.0` instead of `latest`)
4. **Regular updates**: Update to latest version regularly for security patches
5. **Minimal permissions**: Run with minimal required permissions

## 🤝 Team Collaboration

### Sharing with Team Members

1. **Share this guide** with your team
2. **Ensure team members have**:
   - Docker installed
   - Access to DockerHub (public image, no login required)
   - This documentation

3. **Team members can start immediately**:
   ```bash
   docker pull lfranko/imeteo-radar:latest
   docker run --rm lfranko/imeteo-radar:latest imeteo-radar --help
   ```

### No Build Required

Your team members **DO NOT** need to:
- Clone the repository
- Install Python dependencies
- Build the Docker image
- Configure development environment

They only need Docker and this guide!

## 📚 Additional Resources

- **DockerHub Repository**: https://hub.docker.com/r/lfranko/imeteo-radar
- **GitHub Repository**: https://github.com/imeteo/imeteo-radar
- **Issue Tracker**: https://github.com/imeteo/imeteo-radar/issues

## 💡 Tips

1. **Use named volumes** for better performance:
   ```bash
   docker volume create radar-data
   docker run --rm -v radar-data:/tmp lfranko/imeteo-radar:latest imeteo-radar fetch --source dwd
   ```

2. **Set resource limits** for production:
   ```yaml
   services:
     dwd-fetcher:
       image: lfranko/imeteo-radar:latest
       deploy:
         resources:
           limits:
             cpus: '1.0'
             memory: 2G
           reservations:
             memory: 512M
   ```

3. **Use health checks**:
   ```yaml
   services:
     dwd-fetcher:
       image: lfranko/imeteo-radar:latest
       healthcheck:
         test: ["CMD", "ls", "/tmp/germany"]
         interval: 5m
         timeout: 10s
         retries: 3
   ```

---

**Need help?** Open an issue on GitHub or contact the development team.
