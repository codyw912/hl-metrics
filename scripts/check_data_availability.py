#!/usr/bin/env python3
"""
Check availability of Hyperliquid trade data across all S3 paths.

This script checks all three trade data locations:
- node_fills_by_block/ (current format)
- node_fills/ (legacy, API format)
- node_trades/ (legacy, different format)
"""

import boto3
from datetime import datetime, timezone
from typing import List, Dict


def list_s3_objects_simple(bucket: str, prefix: str) -> List[dict]:
    """List all objects in an S3 prefix."""
    s3_client = boto3.client("s3")

    objects = []
    continuation_token = None

    print(f"  Scanning s3://{bucket}/{prefix}...", end=" ", flush=True)

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
            print(f"\n  Error: {e}")
            return objects

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


def analyze_objects(objects: List[dict], name: str) -> Dict:
    """Analyze a list of objects and return summary stats."""
    if not objects:
        return None

    earliest = min(objects, key=lambda x: x["LastModified"])
    latest = max(objects, key=lambda x: x["LastModified"])
    total_bytes = sum(obj["Size"] for obj in objects)

    time_span = latest["LastModified"] - earliest["LastModified"]

    return {
        "name": name,
        "count": len(objects),
        "earliest": earliest["LastModified"],
        "latest": latest["LastModified"],
        "days_span": time_span.days,
        "total_bytes": total_bytes,
        "total_gb": total_bytes / (1024**3),
    }


def main():
    BUCKET = "hl-mainnet-node-data"

    # Three different trade data paths
    PATHS = [
        ("node_fills_by_block/", "Current format (batched by block)"),
        ("node_fills/", "Legacy format (API format)"),
        ("node_trades/", "Legacy format (alternative)"),
    ]

    print("=" * 80)
    print("Hyperliquid Trade Data Availability Check")
    print("=" * 80)
    print()
    print("Checking all trade data paths in S3...")
    print()

    results = []

    for prefix, description in PATHS:
        print(f"\n{description}:")
        objects = list_s3_objects_simple(BUCKET, prefix)

        if objects:
            analysis = analyze_objects(objects, description)
            results.append(analysis)
        else:
            print("  No data found")

    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print()

    if not results:
        print("No trade data found in any location.")
        return

    # Sort by earliest date
    results.sort(key=lambda x: x["earliest"])

    # Overall statistics
    overall_earliest = min(r["earliest"] for r in results)
    overall_latest = max(r["latest"] for r in results)
    overall_span = (overall_latest - overall_earliest).days
    total_files = sum(r["count"] for r in results)
    total_gb = sum(r["total_gb"] for r in results)

    print("Overall Data Availability:")
    print(f"  Earliest data:  {overall_earliest.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Latest data:    {overall_latest.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Total span:     {overall_span} days (~{overall_span / 365:.1f} years)")
    print(f"  Total files:    {total_files:,}")
    print(f"  Total size:     {format_size(total_gb * 1024**3)} ({total_gb:.2f} GB)")
    print()

    print("Breakdown by dataset:")
    print()

    for result in results:
        print(f"  {result['name']}:")
        print(f"    Files:       {result['count']:,}")
        print(
            f"    Size:        {format_size(result['total_bytes'])} ({result['total_gb']:.2f} GB)"
        )
        print(
            f"    Date range:  {result['earliest'].strftime('%Y-%m-%d')} to {result['latest'].strftime('%Y-%m-%d')}"
        )
        print(
            f"    Span:        {result['days_span']} days (~{result['days_span'] / 365:.1f} years)"
        )
        print()

    # Cost estimate for all data
    DATA_TRANSFER_COST_PER_GB = 0.09  # First 10TB
    GET_REQUEST_COST = 0.0004 / 1000

    transfer_cost = total_gb * DATA_TRANSFER_COST_PER_GB
    request_cost = total_files * GET_REQUEST_COST
    total_cost = transfer_cost + request_cost

    print("Estimated cost to download ALL trade data:")
    print(f"  Data transfer:  ${transfer_cost:.2f}")
    print(f"  GET requests:   ${request_cost:.2f}")
    print(f"  TOTAL:          ${total_cost:.2f}")
    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
