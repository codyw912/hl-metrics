"""Shared utilities for S3 operations."""

import sys
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import boto3
from botocore.exceptions import NoCredentialsError, ClientError


def check_aws_credentials() -> bool:
    """
    Check if AWS credentials are configured.

    Returns:
        True if credentials are valid, False otherwise
    """
    try:
        s3_client = boto3.client("s3")
        s3_client.list_buckets()
        return True
    except NoCredentialsError:
        print("❌ Error: AWS credentials not found!")
        print()
        print("Please configure your AWS credentials using one of these methods:")
        print("  1. Run: aws configure")
        print(
            "  2. Set environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        )
        print("  3. Use IAM role (if running on EC2)")
        print()
        return False
    except ClientError as e:
        print(f"❌ Error: AWS credentials invalid: {e}")
        return False


def list_s3_objects(
    bucket: str,
    prefix: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    verbose: bool = True,
) -> Tuple[List[dict], int]:
    """
    List objects in an S3 requester-pays bucket.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix to list
        start_date: Optional start date filter (inclusive)
        end_date: Optional end date filter (inclusive)
        verbose: Whether to print progress

    Returns:
        Tuple of (list of objects, number of LIST requests made)
    """
    s3_client = boto3.client("s3")

    objects = []
    continuation_token = None
    list_requests = 0

    if verbose:
        print(f"  Listing s3://{bucket}/{prefix}...", end=" ", flush=True)

    while True:
        list_requests += 1

        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "RequestPayer": "requester",
            "MaxKeys": 1000,
        }

        if continuation_token:
            params["ContinuationToken"] = continuation_token

        try:
            response = s3_client.list_objects_v2(**params)
        except Exception as e:
            print(f"\n  Error listing objects: {e}")
            sys.exit(1)

        if "Contents" in response:
            for obj in response["Contents"]:
                obj_data = {
                    "Key": obj["Key"],
                    "Size": obj["Size"],
                    "LastModified": obj["LastModified"],
                }

                # Apply date filtering if specified
                if start_date and obj["LastModified"] < start_date:
                    continue
                if end_date and obj["LastModified"] > end_date:
                    continue

                objects.append(obj_data)

        # Progress indicator for long listings
        if verbose and list_requests % 10 == 0:
            print(".", end="", flush=True)

        if not response.get("IsTruncated", False):
            break

        continuation_token = response.get("NextContinuationToken")

    if verbose:
        print(f" Found {len(objects):,} objects")

    return objects, list_requests


def format_size(bytes_size: float) -> str:
    """Format bytes into human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def calculate_download_cost(
    total_gb: float, num_files: int, num_list_requests: int = 0
) -> dict:
    """
    Calculate estimated AWS S3 costs for downloading data.

    Args:
        total_gb: Total data size in GB
        num_files: Number of files to download
        num_list_requests: Number of LIST requests (if already made)

    Returns:
        Dictionary with cost breakdown
    """
    # AWS S3 pricing (US regions, as of 2025)
    DATA_TRANSFER_COST_PER_GB = 0.09  # First 10 TB
    LIST_REQUEST_COST = 0.005 / 1000  # Per request
    GET_REQUEST_COST = 0.0004 / 1000  # Per request

    list_cost = num_list_requests * LIST_REQUEST_COST
    get_cost = num_files * GET_REQUEST_COST
    transfer_cost = total_gb * DATA_TRANSFER_COST_PER_GB
    total_cost = list_cost + get_cost + transfer_cost

    return {
        "list_cost": list_cost,
        "get_cost": get_cost,
        "transfer_cost": transfer_cost,
        "total_cost": total_cost,
        "total_gb": total_gb,
        "num_files": num_files,
    }


# Common S3 bucket and paths
HYPERLIQUID_BUCKET = "hl-mainnet-node-data"

HYPERLIQUID_PATHS = {
    "current": ("node_fills_by_block/", "Current format (batched by block)"),
    "legacy_fills": ("node_fills/", "Legacy API format"),
    "legacy_trades": ("node_trades/", "Legacy alternative format"),
}
