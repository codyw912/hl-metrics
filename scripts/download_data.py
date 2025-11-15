#!/usr/bin/env python3
"""
Download Hyperliquid trade fills data from S3.

This script downloads objects from the requester-pays S3 bucket with progress tracking.
Downloads from all three trade data locations:
- node_fills_by_block/ (current format)
- node_fills/ (legacy, API format)
- node_trades/ (legacy, alternative format)
"""

import boto3
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import List


def list_s3_objects(bucket: str, prefix: str) -> List[dict]:
    """List all objects in an S3 requester-pays bucket prefix."""
    s3_client = boto3.client("s3")

    objects = []
    continuation_token = None

    print(f"  Listing objects in s3://{bucket}/{prefix}...", end=" ", flush=True)

    while True:
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
                objects.append(
                    {
                        "Key": obj["Key"],
                        "Size": obj["Size"],
                        "LastModified": obj["LastModified"],
                    }
                )

        if not response.get("IsTruncated", False):
            break

        continuation_token = response.get("NextContinuationToken")

    print(f"Found {len(objects):,} objects")
    return objects


def format_size(bytes_size: float) -> str:
    """Format bytes into human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def download_file(s3_client, bucket: str, key: str, output_path: Path) -> bool:
    """Download a single file from S3."""
    try:
        # Create parent directories if they don't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Download with requester pays
        s3_client.download_file(
            bucket, key, str(output_path), ExtraArgs={"RequestPayer": "requester"}
        )
        return True
    except Exception as e:
        print(f"Error downloading {key}: {e}")
        return False


def main():
    # Configuration
    BUCKET = "hl-mainnet-node-data"
    OUTPUT_DIR = Path("./data/hyperliquid")

    # All three trade data paths
    PATHS = [
        ("node_trades/", "Legacy format (alternative)"),
        ("node_fills/", "Legacy format (API format)"),
        ("node_fills_by_block/", "Current format (batched by block)"),
    ]

    print("=" * 80)
    print("Hyperliquid Trade Fills Data Downloader")
    print("=" * 80)
    print()
    print(f"Output directory: {OUTPUT_DIR}")
    print()
    print("This will download data from all three trade data locations:")
    for prefix, desc in PATHS:
        print(f"  - {prefix} ({desc})")
    print()

    # List all objects from all paths
    print("Scanning S3 buckets...")
    print()

    all_objects = []
    for prefix, desc in PATHS:
        print(f"{desc}:")
        objects = list_s3_objects(BUCKET, prefix)
        all_objects.extend(objects)
        print()

    if not all_objects:
        print("No objects found in any bucket.")
        return

    # Calculate total size
    total_bytes = sum(obj["Size"] for obj in all_objects)
    print(
        f"Total data to download: {format_size(total_bytes)} ({total_bytes / (1024**3):.2f} GB)"
    )
    print(f"Number of files: {len(all_objects):,}")

    # Cost estimate
    transfer_cost = (total_bytes / (1024**3)) * 0.09
    request_cost = len(all_objects) * (0.0004 / 1000)
    total_cost = transfer_cost + request_cost
    print(f"Estimated cost: ${total_cost:.2f}")
    print()

    # Confirm before downloading
    response = input("This will incur AWS costs. Continue? (yes/no): ")
    if response.lower() not in ["yes", "y"]:
        print("Download cancelled.")
        return
    print()

    # Create S3 client
    s3_client = boto3.client("s3")

    # Download files
    downloaded = 0
    failed = 0
    skipped = 0
    downloaded_bytes = 0

    print(f"Starting download of {len(all_objects):,} files...")
    print()

    for i, obj in enumerate(all_objects, 1):
        key = obj["Key"]
        size = obj["Size"]

        # Create output path maintaining the S3 structure
        output_path = OUTPUT_DIR / key

        # Skip if already exists
        if output_path.exists() and output_path.stat().st_size == size:
            if i % 100 == 0:  # Only print every 100 skipped files to reduce noise
                print(f"[{i}/{len(all_objects)}] Skipping (already exists): {key}")
            skipped += 1
            downloaded_bytes += size
            continue

        print(f"[{i}/{len(all_objects)}] Downloading: {key} ({format_size(size)})")

        if download_file(s3_client, BUCKET, key, output_path):
            downloaded += 1
            downloaded_bytes += size
        else:
            failed += 1

        # Progress update every 50 files
        if i % 50 == 0:
            progress_pct = (i / len(all_objects)) * 100
            print(
                f"Progress: {progress_pct:.1f}% ({downloaded} downloaded, {skipped} skipped, {failed} failed)"
            )

    print()
    print("=" * 80)
    print("DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"Successfully downloaded: {downloaded:,} files")
    print(f"Skipped (already exists): {skipped:,} files")
    print(f"Failed: {failed:,} files")
    print(f"Total data size: {format_size(downloaded_bytes)}")
    print(f"Output directory: {OUTPUT_DIR}")
    print()


if __name__ == "__main__":
    main()
