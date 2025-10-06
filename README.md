# iMeteo Radar

Weather radar data processor for DWD (Germany) and SHMU (Slovakia). Downloads, processes, and uploads high-quality radar images to DigitalOcean Spaces.

---

## 📖 Table of Contents

- [Quick Start](#-quick-start)
- [Installation](#-installation)
  - [Prerequisites](#prerequisites)
  - [Basic Setup](#basic-setup)
  - [DigitalOcean Spaces Setup](#digitalocean-spaces-setup)
- [Usage](#-usage)
  - [Fetch Latest Data](#fetch-latest-data)
  - [Backload Historical Data](#backload-historical-data)
  - [Local Development (No Upload)](#local-development-no-upload)
- [Common Tasks](#-common-tasks)
  - [One-Time Backload (1 Hour)](#one-time-backload-1-hour)
  - [Testing Upload to Spaces](#testing-upload-to-spaces)
  - [Debugging Issues](#debugging-issues)
- [Docker](#-docker)
  - [Build Image](#build-image)
  - [Run Single Commands](#run-single-commands)
  - [Production Deployment](#production-deployment-docker-compose)
- [Kubernetes](#%EF%B8%8F-kubernetes)
- [Data Specifications](#-data-specifications)
- [Troubleshooting](#-troubleshooting)

---

## 🚀 Quick Start

```bash
# Install
pip install -e .

# Fetch latest (local-only, no upload)
imeteo-radar fetch --source dwd --disable-upload

# Fetch with upload to Spaces (requires env vars)
imeteo-radar fetch --source dwd

# Backload 1 hour
imeteo-radar fetch --source dwd --backload --hours 1
```

---

## 📦 Installation

### Prerequisites

1. **Python 3.9+**
2. **DigitalOcean Spaces** (optional, for cloud upload)
   - [Create a Space](https://cloud.digitalocean.com/spaces)
   - [Generate API keys](https://cloud.digitalocean.com/account/api/spaces)

### Basic Setup

```bash
# Clone and install
git clone https://github.com/your-org/radar-shmu.git
cd radar-shmu
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

### DigitalOcean Spaces Setup

**Only needed if you want to upload to cloud storage.**

**Step 1: Create a Space (Bucket)**
- Go to [DigitalOcean Spaces](https://cloud.digitalocean.com/spaces)
- Create new Space (e.g., `my-radar-data`)
- Choose region (e.g., `nyc3`, `fra1`)
- Note your Space URL: `https://my-radar-data.nyc3.digitaloceanspaces.com`

**Step 2: Generate API Keys**
- Go to [API Tokens](https://cloud.digitalocean.com/account/api/spaces)
- Generate Spaces access keys
- Save your **Access Key** and **Secret Key**

**Step 3: Configure Credentials**

Choose one method:

**Method A: Using `.env` file (Recommended)**
```bash
# Copy example file
cp .env.example .env

# Edit with your credentials
nano .env
```

Your `.env` file:
```bash
DIGITALOCEAN_SPACES_KEY=DO00XXXXXXXXXXXXX
DIGITALOCEAN_SPACES_SECRET=your-secret-key
DIGITALOCEAN_SPACES_ENDPOINT=https://nyc3.digitaloceanspaces.com
DIGITALOCEAN_SPACES_REGION=nyc3
DIGITALOCEAN_SPACES_BUCKET=my-radar-data
DIGITALOCEAN_SPACES_URL=https://my-radar-data.nyc3.digitaloceanspaces.com
```

Then load it:
```bash
set -a; source .env; set +a
```

**Method B: Export directly in shell**
```bash
export DIGITALOCEAN_SPACES_KEY="DO00XXXXXXXXXXXXX"
export DIGITALOCEAN_SPACES_SECRET="your-secret-key"
export DIGITALOCEAN_SPACES_ENDPOINT="https://nyc3.digitaloceanspaces.com"
export DIGITALOCEAN_SPACES_REGION="nyc3"
export DIGITALOCEAN_SPACES_BUCKET="my-radar-data"
export DIGITALOCEAN_SPACES_URL="https://my-radar-data.nyc3.digitaloceanspaces.com"

# Add to ~/.bashrc or ~/.zshrc for persistence
```

⚠️ **Never commit `.env` to git!**

---

## 📡 Usage

### Fetch Latest Data

```bash
# Germany (DWD)
imeteo-radar fetch --source dwd

# Slovakia (SHMU)
imeteo-radar fetch --source shmu
```

**Output:**
- Local: `/tmp/germany/<timestamp>.png` or `/tmp/slovakia/<timestamp>.png`
- Cloud: `https://your-bucket.nyc3.digitaloceanspaces.com/iradar/germany/<timestamp>.png`

### Backload Historical Data

```bash
# Last 6 hours
imeteo-radar fetch --source dwd --backload --hours 6

# Specific time range
imeteo-radar fetch --source dwd --backload \
  --from "2024-09-25 10:00" --to "2024-09-25 16:00"
```

### Local Development (No Upload)

```bash
# Skip Spaces upload completely
imeteo-radar fetch --source dwd --disable-upload
```

**No environment variables needed!**

---

## 💡 Common Tasks

### One-Time Backload (1 Hour)

**Scenario:** Download last hour of data for testing or backfilling.

```bash
# Local-only (no upload)
imeteo-radar fetch --source dwd --backload --hours 1 --disable-upload

# With upload to Spaces
imeteo-radar fetch --source dwd --backload --hours 1
```

**What this does:**
- Downloads ~12 images (5-minute intervals)
- Processes to PNG with transparency
- Saves locally to `/tmp/germany/`
- (Optional) Uploads to Spaces
- Cleans up files older than 6 hours

**Example output:**
```
📡 Fetching DWD dmax radar data...
⏰ Backload period: 2024-10-06 14:00 to 2024-10-06 15:00
📥 Downloading up to 12 timestamps...
✅ Downloaded 12 files
💾 Saved: /tmp/germany/1728222000.png
☁️  Uploaded to Spaces: https://bucket.nyc3.digitaloceanspaces.com/iradar/germany/1728222000.png
💾 Saved: /tmp/germany/1728222300.png
☁️  Uploaded to Spaces: https://bucket.nyc3.digitaloceanspaces.com/iradar/germany/1728222300.png
✅ Summary: Processed 12 files
🗑️  Cleaned up 5 old PNG files (older than 6h)
```

### Testing Upload to Spaces

**Use a separate test bucket to avoid affecting production data:**

```bash
# Create test bucket in DigitalOcean: my-radar-data-test

# Use test credentials
export DIGITALOCEAN_SPACES_BUCKET="my-radar-data-test"
export DIGITALOCEAN_SPACES_URL="https://my-radar-data-test.nyc3.digitaloceanspaces.com"

# Test upload
imeteo-radar fetch --source dwd

# Check your bucket in DO Console
# Files should appear at: /iradar/germany/<timestamp>.png
```

### Debugging Issues

**Check if credentials are loaded:**
```bash
python -c "import os; print('Key:', os.getenv('DIGITALOCEAN_SPACES_KEY'))"
```

**Test connection manually:**
```bash
# Using AWS CLI (compatible with Spaces)
aws s3 ls s3://my-radar-data --endpoint-url https://nyc3.digitaloceanspaces.com
```

**View detailed logs:**
```bash
imeteo-radar fetch --source dwd 2>&1 | tee debug.log
```

**Common errors:**

| Error | Solution |
|-------|----------|
| `Missing required environment variables` | Load `.env` file or export variables |
| `403 Forbidden` | Check API keys are correct |
| `404 Not Found` | Verify bucket name and region match |
| `Import "boto3" could not be resolved` | Run `pip install boto3` |

---

## 🐳 Docker

### Build Image

```bash
# Build
docker build -t imeteo-radar .

# Rebuild (force clean build)
docker build --no-cache -t imeteo-radar .
```

### Run Single Commands

**Local-only (no credentials needed):**
```bash
docker run --rm -v /tmp/germany:/tmp/germany \
  imeteo-radar imeteo-radar fetch --source dwd --disable-upload
```

**With Spaces upload:**
```bash
# Using .env file (recommended)
docker run --rm --env-file .env -v /tmp/germany:/tmp/germany \
  imeteo-radar imeteo-radar fetch --source dwd
```

**One-time 1-hour backload:**
```bash
# Without upload
docker run --rm -v /tmp/germany:/tmp/germany \
  imeteo-radar imeteo-radar fetch --source dwd --backload --hours 1 --disable-upload

# With upload
docker run --rm --env-file .env -v /tmp/germany:/tmp/germany \
  imeteo-radar imeteo-radar fetch --source dwd --backload --hours 1
```

### Production Deployment (docker-compose)

**Setup:**
```bash
# 1. Configure credentials
cp .env.example .env
nano .env  # Add your DO Spaces credentials

# 2. Start services (runs every 5 minutes)
docker-compose --profile production up -d

# 3. View logs
docker-compose logs -f dwd-fetcher
```

**Services:**
- `dwd-fetcher` - Fetches DWD data every 5 min
- `shmu-fetcher` - Fetches SHMU data every 5 min
- `backloader` - Daily backload (24h)
- `extent-generator` - Updates extent files daily

**Stop/Restart:**
```bash
docker-compose --profile production down
docker-compose --profile production up -d
```

---

## ☸️ Kubernetes

**Example CronJob (runs every 5 minutes):**

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
            image: your-registry/imeteo-radar:latest
            command: ["imeteo-radar", "fetch", "--source", "dwd"]

            # Load credentials from Secret
            envFrom:
            - secretRef:
                name: radar-digitalocean-spaces

            resources:
              requests:
                memory: "256Mi"
                cpu: "250m"
              limits:
                memory: "512Mi"
                cpu: "500m"
```

**Create Secret (one-time):**
```bash
kubectl create secret generic radar-digitalocean-spaces \
  --from-literal=DIGITALOCEAN_SPACES_KEY='your-key' \
  --from-literal=DIGITALOCEAN_SPACES_SECRET='your-secret' \
  --from-literal=DIGITALOCEAN_SPACES_ENDPOINT='https://nyc3.digitaloceanspaces.com' \
  --from-literal=DIGITALOCEAN_SPACES_REGION='nyc3' \
  --from-literal=DIGITALOCEAN_SPACES_BUCKET='your-bucket' \
  --from-literal=DIGITALOCEAN_SPACES_URL='https://your-bucket.nyc3.digitaloceanspaces.com'
```

**One-time Job (1-hour backload):**
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: radar-backload-1h
spec:
  template:
    spec:
      containers:
      - name: radar-backload
        image: your-registry/imeteo-radar:latest
        command: ["imeteo-radar", "fetch", "--source", "dwd", "--backload", "--hours", "1"]
        envFrom:
        - secretRef:
            name: radar-digitalocean-spaces
      restartPolicy: Never
```

```bash
# Deploy one-time job
kubectl apply -f backload-job.yaml

# Check status
kubectl get jobs
kubectl logs job/radar-backload-1h
```

---

## 📊 Data Specifications

| Source | Coverage | Resolution | Update Freq | Output |
|--------|----------|------------|-------------|--------|
| **DWD** | Germany | 4800×4400px | 5 min | `/iradar/germany/<timestamp>.png` |
| **SHMU** | Slovakia | 1560×2270px | 5 min | `/iradar/slovakia/<timestamp>.png` |

**File Format:**
- PNG with transparency
- SHMU colorscale (-35 to 85 dBZ)
- Filename: Unix timestamp (e.g., `1728221400.png`)

**Data Retention:**
- **Raw HDF5**: Deleted immediately (temp files)
- **Local PNG**: 6 hours
- **Cloud (Spaces)**: Permanent

**Coverage:**
- DWD: 45.7-56.2°N, 1.5-18.7°E
- SHMU: 46.0-50.7°N, 13.6-23.8°E

---

## 🔧 Troubleshooting

### Check if Upload was Successful

**Option 1: Use AWS CLI with explicit credentials**

AWS CLI needs credentials configured. Set them up:

```bash
# Load your .env file
set -a; source .env; set +a

# Configure AWS CLI for DigitalOcean Spaces (one-time setup)
aws configure set aws_access_key_id "$DIGITALOCEAN_SPACES_KEY"
aws configure set aws_secret_access_key "$DIGITALOCEAN_SPACES_SECRET"
aws configure set default.region "$DIGITALOCEAN_SPACES_REGION"

# Now list files
aws s3 ls s3://your-bucket-name/ \
  --endpoint-url https://nyc3.digitaloceanspaces.com \
  --recursive

# List Germany radar images
aws s3 ls s3://your-bucket-name/iradar/germany/ \
  --endpoint-url https://nyc3.digitaloceanspaces.com

# List Slovakia radar images
aws s3 ls s3://your-bucket-name/iradar/slovakia/ \
  --endpoint-url https://nyc3.digitaloceanspaces.com
```

**Option 2: Pass credentials directly (works without aws configure)**

```bash
# Load your .env file
set -a; source .env; set +a

# List all files with inline credentials
AWS_ACCESS_KEY_ID="$DIGITALOCEAN_SPACES_KEY" \
AWS_SECRET_ACCESS_KEY="$DIGITALOCEAN_SPACES_SECRET" \
aws s3 ls s3://$DIGITALOCEAN_SPACES_BUCKET/ \
  --endpoint-url $DIGITALOCEAN_SPACES_ENDPOINT \
  --recursive

# List Germany radar images
AWS_ACCESS_KEY_ID="$DIGITALOCEAN_SPACES_KEY" \
AWS_SECRET_ACCESS_KEY="$DIGITALOCEAN_SPACES_SECRET" \
aws s3 ls s3://$DIGITALOCEAN_SPACES_BUCKET/iradar/germany/ \
  --endpoint-url $DIGITALOCEAN_SPACES_ENDPOINT

# Check specific file
AWS_ACCESS_KEY_ID="$DIGITALOCEAN_SPACES_KEY" \
AWS_SECRET_ACCESS_KEY="$DIGITALOCEAN_SPACES_SECRET" \
aws s3 ls s3://$DIGITALOCEAN_SPACES_BUCKET/iradar/germany/1728221400.png \
  --endpoint-url $DIGITALOCEAN_SPACES_ENDPOINT
```

**Option 3: Use DigitalOcean Web Console**

1. Go to [DigitalOcean Spaces](https://cloud.digitalocean.com/spaces)
2. Click on your Space (e.g., `my-radar-data`)
3. Navigate to `/iradar/germany/` or `/iradar/slovakia/`
4. Check if PNG files are present

**Option 4: View file directly in browser**

If your Space is **public**, open URL directly:
```
https://your-bucket-name.nyc3.digitaloceanspaces.com/iradar/germany/1728221400.png
```

Replace `your-bucket-name`, `nyc3`, and timestamp with actual values.

### Upload Not Working

**1. Check credentials are loaded:**
```bash
echo "Key: $DIGITALOCEAN_SPACES_KEY"
echo "Bucket: $DIGITALOCEAN_SPACES_BUCKET"
echo "Endpoint: $DIGITALOCEAN_SPACES_ENDPOINT"
```

If empty, reload your `.env`:
```bash
set -a; source .env; set +a
```

**2. Verify bucket exists and is accessible:**
```bash
# List bucket contents
aws s3 ls s3://${DIGITALOCEAN_SPACES_BUCKET}/ \
  --endpoint-url ${DIGITALOCEAN_SPACES_ENDPOINT}
```

If this fails with `403 Forbidden`: Check your API keys are correct.

**3. Test manual upload:**
```bash
# Create test file
echo "test upload" > test.txt

# Upload to your bucket
aws s3 cp test.txt s3://${DIGITALOCEAN_SPACES_BUCKET}/test.txt \
  --endpoint-url ${DIGITALOCEAN_SPACES_ENDPOINT} \
  --acl public-read

# Verify upload
aws s3 ls s3://${DIGITALOCEAN_SPACES_BUCKET}/test.txt \
  --endpoint-url ${DIGITALOCEAN_SPACES_ENDPOINT}

# Clean up test file
aws s3 rm s3://${DIGITALOCEAN_SPACES_BUCKET}/test.txt \
  --endpoint-url ${DIGITALOCEAN_SPACES_ENDPOINT}
```

**4. Check application logs:**
```bash
imeteo-radar fetch --source dwd 2>&1 | tee debug.log
```

Look for:
- ✅ `☁️  DigitalOcean Spaces upload enabled` - Upload is working
- ❌ `⚠️  DigitalOcean Spaces not configured` - Credentials missing
- ❌ `⚠️  Failed to initialize Spaces uploader` - Connection/auth issue

### No Data Downloaded

- Check source is online: [DWD](https://opendata.dwd.de/weather/radar/composite/) | [SHMU](https://opendata.shmu.sk/)
- Verify time range is valid (not in the future)
- Check network connectivity
- For backload: Ensure data exists for requested time range

### Docker Not Reading .env

**Docker doesn't auto-load `.env` files!** You must explicitly pass them:

```bash
# Correct: Use --env-file flag
docker run --rm --env-file .env -v /tmp/germany:/tmp/germany \
  imeteo-radar imeteo-radar fetch --source dwd

# Wrong: Docker won't see .env
docker run --rm -v /tmp/germany:/tmp/germany \
  imeteo-radar imeteo-radar fetch --source dwd
```

**Or use docker-compose** (reads `.env` automatically):
```bash
docker-compose --profile production up -d
```

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `Missing required environment variables` | Credentials not loaded | Run `set -a; source .env; set +a` |
| `403 Forbidden` | Invalid API keys | Check keys in DO Console |
| `404 Not Found` | Bucket doesn't exist | Verify bucket name and region |
| `NoCredentialsError` | AWS CLI not configured | Use `--endpoint-url` with all AWS commands |
| `Import "boto3" could not be resolved` | Missing dependency | Run `pip install boto3` |

---

## 🔗 Resources

- **DWD OpenData**: https://opendata.dwd.de/weather/radar/composite/
- **SHMU OpenData**: https://opendata.shmu.sk/
- **DigitalOcean Spaces**: https://www.digitalocean.com/products/spaces

---

## 📝 License

MIT

## 🤝 Contributing

Contributions welcome! Submit a Pull Request.
