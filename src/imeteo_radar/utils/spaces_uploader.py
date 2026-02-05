#!/usr/bin/env python3
"""
DigitalOcean Spaces Uploader

Handles uploading radar PNG files to DigitalOcean Spaces (S3-compatible storage).

Credentials are loaded from environment variables (or .env file via python-dotenv):
- DIGITALOCEAN_SPACES_KEY
- DIGITALOCEAN_SPACES_SECRET
- DIGITALOCEAN_SPACES_ENDPOINT
- DIGITALOCEAN_SPACES_REGION
- DIGITALOCEAN_SPACES_BUCKET
- DIGITALOCEAN_SPACES_URL

If credentials are not available, upload is disabled and processing continues locally.
"""

import os
from pathlib import Path

from ..core.logging import get_logger

logger = get_logger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


def _get_folder_for_source(source: str) -> str:
    """Get Spaces folder name for a source using centralized registry.

    Args:
        source: Source identifier (e.g., 'dwd', 'shmu')

    Returns:
        Folder name for cloud storage (e.g., 'germany', 'slovakia')
    """
    from ..config.sources import get_folder_for_source

    return get_folder_for_source(source)


class SpacesUploader:
    """Upload files to DigitalOcean Spaces.

    All data is stored in a single bucket with path prefixes:
    - iradar/{source}/ - Radar output images
    - iradar-data/ - Metadata, cache, transforms
    """

    def __init__(self):
        """Initialize Spaces uploader with environment variables"""
        if not BOTO3_AVAILABLE:
            raise ImportError(
                "boto3 is required for DigitalOcean Spaces uploads. "
                "Install it with: pip install boto3"
            )

        # Read environment variables
        self.access_key = os.getenv("DIGITALOCEAN_SPACES_KEY")
        self.secret_key = os.getenv("DIGITALOCEAN_SPACES_SECRET")
        self.endpoint = os.getenv("DIGITALOCEAN_SPACES_ENDPOINT")
        self.region = os.getenv("DIGITALOCEAN_SPACES_REGION")
        self.bucket = os.getenv("DIGITALOCEAN_SPACES_BUCKET")
        self.spaces_url = os.getenv("DIGITALOCEAN_SPACES_URL")

        # Validate required environment variables
        missing_vars = []
        if not self.access_key:
            missing_vars.append("DIGITALOCEAN_SPACES_KEY")
        if not self.secret_key:
            missing_vars.append("DIGITALOCEAN_SPACES_SECRET")
        if not self.endpoint:
            missing_vars.append("DIGITALOCEAN_SPACES_ENDPOINT")
        if not self.region:
            missing_vars.append("DIGITALOCEAN_SPACES_REGION")
        if not self.bucket:
            missing_vars.append("DIGITALOCEAN_SPACES_BUCKET")
        if not self.spaces_url:
            missing_vars.append("DIGITALOCEAN_SPACES_URL")

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}\n"
                "Please set all DigitalOcean Spaces credentials in environment variables or .env file."
            )

        # Initialize boto3 client
        try:
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
            )

            # Test connection by checking if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket)

        except NoCredentialsError as e:
            raise ValueError("Invalid DigitalOcean Spaces credentials") from e
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                raise ValueError(f"Bucket not found") from e
            elif error_code == "403":
                raise ValueError(f"Access denied to bucket") from e
            else:
                raise ValueError(
                    f"Failed to connect to DigitalOcean Spaces: {e}"
                ) from e

    def _detect_content_type(self, path: Path) -> str:
        """Detect MIME content type from file extension.

        Args:
            path: File path

        Returns:
            MIME type string
        """
        suffix = Path(path).suffix.lower()
        return {
            ".png": "image/png",
            ".avif": "image/avif",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".webp": "image/webp",
            ".json": "application/json",
        }.get(suffix, "application/octet-stream")

    def upload_file(
        self,
        local_path: Path,
        source: str,
        filename: str,
        content_type: str | None = None,
    ) -> str | None:
        """
        Upload a file to DigitalOcean Spaces

        Args:
            local_path: Local file path to upload
            source: Source name ('dwd' for germany, 'shmu' for slovakia)
            filename: Filename to use in Spaces (e.g., '1234567890.png')
            content_type: MIME type (auto-detected from extension if None)

        Returns:
            str: Public URL of uploaded file, or None if upload failed
        """
        local_path = Path(local_path)

        if not local_path.exists():
            logger.error(f"Local file not found: {local_path}")
            return None

        # Auto-detect content type if not provided
        if content_type is None:
            content_type = self._detect_content_type(local_path)

        # Determine folder based on source using centralized registry
        folder = _get_folder_for_source(source)

        # Construct S3 key (path in Spaces)
        s3_key = f"iradar/{folder}/{filename}"

        try:
            # Upload file with public-read ACL
            self.s3_client.upload_file(
                str(local_path),
                self.bucket,
                s3_key,
                ExtraArgs={"ACL": "public-read", "ContentType": content_type},
            )

            # Construct public URL
            public_url = f"{self.spaces_url}/{s3_key}"

            logger.info(
                f"Uploaded to Spaces: {public_url}",
                extra={"source": source, "operation": "upload"},
            )
            return public_url

        except ClientError as e:
            logger.error(f"Failed to upload to Spaces: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during upload: {e}")
            return None

    def upload_metadata(
        self, local_path: Path, s3_key: str, content_type: str = "application/json"
    ) -> str | None:
        """
        Upload a metadata file (extent JSON, coverage mask PNG) to Spaces.

        Args:
            local_path: Local file path to upload
            s3_key: Full S3 key (e.g., 'iradar-data/extent/dwd/extent_index.json')
            content_type: MIME type (default: application/json)

        Returns:
            str: Public URL of uploaded file, or None if upload failed
        """
        local_path = Path(local_path)

        if not local_path.exists():
            logger.error(f"Local file not found: {local_path}")
            return None

        try:
            self.s3_client.upload_file(
                str(local_path),
                self.bucket,
                s3_key,
                ExtraArgs={"ACL": "public-read", "ContentType": content_type},
            )

            public_url = f"{self.spaces_url}/{s3_key}"

            logger.info(
                f"Uploaded metadata to Spaces: {s3_key}",
                extra={"operation": "upload"},
            )
            return public_url

        except ClientError as e:
            logger.error(f"Failed to upload metadata to Spaces: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during metadata upload: {e}")
            return None

    def download_metadata(self, s3_key: str, local_path: Path) -> bool:
        """
        Download a metadata file from Spaces to local path.

        Uses atomic download with temp file to prevent corruption.

        Args:
            s3_key: Full S3 key (e.g., 'iradar-data/extent/dwd/extent_index.json')
            local_path: Local file path to save to

        Returns:
            bool: True if download successful, False if not found or error
        """
        local_path = Path(local_path)
        temp_path = local_path.with_suffix(local_path.suffix + ".tmp")

        try:
            # Create parent directories
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Download to temp file first (atomic approach)
            self.s3_client.download_file(self.bucket, s3_key, str(temp_path))

            # Atomic rename on success
            temp_path.rename(local_path)

            logger.info(
                f"Downloaded metadata from Spaces: {s3_key}",
                extra={"operation": "download"},
            )
            return True

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                # Not found is expected for first run
                logger.debug(f"Metadata not found in Spaces: {s3_key}")
                return False
            logger.warning(f"Error downloading metadata from Spaces: {e}")
            return False
        except Exception as e:
            logger.warning(f"Unexpected error during metadata download: {e}")
            return False
        finally:
            # Clean up temp file on failure
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass

    def metadata_exists(self, s3_key: str) -> bool:
        """
        Check if a metadata file exists in Spaces.

        Args:
            s3_key: Full S3 key (e.g., 'iradar-data/extent/dwd/extent_index.json')

        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                return False
            logger.warning(f"Error checking metadata existence in Spaces: {e}")
            return False
        except Exception as e:
            logger.warning(f"Unexpected error checking metadata existence: {e}")
            return False

    def delete_file(self, source: str, filename: str) -> bool:
        """
        Delete a file from DigitalOcean Spaces

        Args:
            source: Source name ('dwd' for germany, 'shmu' for slovakia)
            filename: Filename to delete

        Returns:
            bool: True if deletion successful, False otherwise
        """
        # Determine folder based on source using centralized registry
        folder = _get_folder_for_source(source)

        # Construct S3 key
        s3_key = f"iradar/{folder}/{filename}"

        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=s3_key)
            logger.info(
                f"Deleted from Spaces: {s3_key}",
                extra={"operation": "delete"},
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete from Spaces: {e}")
            return False

    def file_exists(self, source: str, filename: str) -> bool:
        """
        Check if a file exists in DigitalOcean Spaces

        Args:
            source: Source name ('dwd' for germany, 'shmu' for slovakia, 'composite')
            filename: Filename to check (e.g., '1234567890.png')

        Returns:
            bool: True if file exists, False otherwise
        """
        # Determine folder based on source using centralized registry
        folder = _get_folder_for_source(source)

        # Construct S3 key
        s3_key = f"iradar/{folder}/{filename}"

        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                return False
            # Log unexpected errors but don't raise
            logger.warning(f"Error checking file existence in Spaces: {e}")
            return False
        except Exception as e:
            logger.warning(f"Unexpected error checking file existence: {e}")
            return False

    def list_files(self, source: str, prefix: str = "") -> list:
        """
        List files in DigitalOcean Spaces for a given source

        Args:
            source: Source name ('dwd' for germany, 'shmu' for slovakia)
            prefix: Optional prefix to filter files

        Returns:
            list: List of file keys in Spaces
        """
        # Determine folder based on source using centralized registry
        folder = _get_folder_for_source(source)

        # Construct S3 prefix
        s3_prefix = f"iradar/{folder}/{prefix}"

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=s3_prefix
            )

            if "Contents" not in response:
                return []

            return [obj["Key"] for obj in response["Contents"]]

        except ClientError as e:
            logger.error(f"Failed to list files from Spaces: {e}")
            return []


def is_spaces_configured() -> bool:
    """
    Check if DigitalOcean Spaces is configured via environment variables

    Returns:
        bool: True if all required env vars are set, False otherwise
    """
    required_vars = [
        "DIGITALOCEAN_SPACES_KEY",
        "DIGITALOCEAN_SPACES_SECRET",
        "DIGITALOCEAN_SPACES_ENDPOINT",
        "DIGITALOCEAN_SPACES_REGION",
        "DIGITALOCEAN_SPACES_BUCKET",
        "DIGITALOCEAN_SPACES_URL",
    ]

    return all(os.getenv(var) for var in required_vars)


# Cached uploader instance for reuse across calls
_cached_uploader: SpacesUploader | None = None


def get_uploader_if_configured() -> SpacesUploader | None:
    """
    Get a cached SpacesUploader instance if Spaces is configured.

    Returns the same instance on subsequent calls to avoid repeated
    initialization overhead (env var reads, boto3 client creation,
    bucket validation).

    Returns:
        SpacesUploader instance or None if not configured
    """
    global _cached_uploader

    if _cached_uploader is not None:
        return _cached_uploader

    if not is_spaces_configured():
        return None

    try:
        _cached_uploader = SpacesUploader()
        return _cached_uploader
    except Exception as e:
        logger.debug(f"Could not initialize SpacesUploader: {e}")
        return None
