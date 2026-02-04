#!/usr/bin/env python3
"""
DigitalOcean Spaces Uploader

Handles uploading radar PNG files to DigitalOcean Spaces (S3-compatible storage).

Credentials are loaded from environment variables:
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
    """Upload files to DigitalOcean Spaces"""

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
                "Please set all DigitalOcean Spaces credentials in environment variables."
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

        except NoCredentialsError:
            raise ValueError("Invalid DigitalOcean Spaces credentials")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                raise ValueError(f"Bucket '{self.bucket}' not found")
            elif error_code == "403":
                raise ValueError(f"Access denied to bucket '{self.bucket}'")
            else:
                raise ValueError(f"Failed to connect to DigitalOcean Spaces: {e}")

    def upload_file(self, local_path: Path, source: str, filename: str) -> str | None:
        """
        Upload a file to DigitalOcean Spaces

        Args:
            local_path: Local file path to upload
            source: Source name ('dwd' for germany, 'shmu' for slovakia)
            filename: Filename to use in Spaces (e.g., '1234567890.png')

        Returns:
            str: Public URL of uploaded file, or None if upload failed
        """
        local_path = Path(local_path)

        if not local_path.exists():
            logger.error(f"Local file not found: {local_path}")
            return None

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
                ExtraArgs={"ACL": "public-read", "ContentType": "image/png"},
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
