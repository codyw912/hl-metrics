#!/usr/bin/env python3
"""
Estimate the cost of downloading Hyperliquid trade fills data from S3.

This script lists objects in the requester-pays S3 bucket and calculates:
- Total data size
- AWS S3 data transfer costs
- AWS S3 request costs
"""

import boto3
from datetime import datetime, timedelta, timezone
from typing import List, Tuple
import sys


# AWS S3 Pricing (as of 2025, US regions)
# Data transfer out to internet
DATA_TRANSFER_PRICING = [
    (10 * 1024, 0.09),  # First 10 TB: $0.09/GB
    (40 * 1024, 0.085),  # Next 40 TB: $0.085/GB
    (100 * 1024, 0.07),  # Next 100 TB: $0.07/GB
    (float("inf"), 0.05),  # Over 150 TB: $0.05/GB
]

# Request pricing
LIST_REQUEST_COST = 0.005 / 1000  # $0.005 per 1,000 LIST requests
GET_REQUEST_COST = 0.0004 / 1000  # $0.0004 per 1,000 GET requests


def calculate_transfer_cost(size_gb: float) -> float:
    """Calculate data transfer cost based on tiered pricing."""
    cost = 0.0
    remaining_gb = size_gb
    previous_tier = 0

    for tier_limit_gb, price_per_gb in DATA_TRANSFER_PRICING:
        tier_size_gb = tier_limit_gb - previous_tier

        if remaining_gb <= 0:
            break

        if remaining_gb >= tier_size_gb:
            cost += tier_size_gb * price_per_gb
            remaining_gb -= tier_size_gb
        else:
            cost += remaining_gb * price_per_gb
            remaining_gb = 0

        previous_tier = tier_limit_gb

    return cost


def list_s3_objects(
    bucket: str, prefix: str, start_date: datetime, end_date: datetime
) -> Tuple[List[dict], int]:
    """
    List objects in an S3 requester-pays bucket within a date range.

    Returns:
        Tuple of (list of objects, number of LIST requests made)
    """
    s3_client = boto3.client("s3")

    objects = []
    continuation_token = None
    list_requests = 0

    print(f"Listing objects in s3://{bucket}/{prefix}")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print("This may take a few minutes...\n")

    while True:
        list_requests += 1

        # Build the request parameters
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "RequestPayer": "requester",
            "MaxKeys": 1000,  # Max allowed per request
        }

        if continuation_token:
            params["ContinuationToken"] = continuation_token

        try:
            response = s3_client.list_objects_v2(**params)
        except Exception as e:
            print(f"Error listing objects: {e}")
            sys.exit(1)

        if "Contents" in response:
            for obj in response["Contents"]:
                # Filter by date if the key contains date information
                # The path structure might be: node_fills_by_block/[something]/[date]/...
                # We'll just collect all objects for now and let the user filter if needed
                objects.append(
                    {
                        "Key": obj["Key"],
                        "Size": obj["Size"],
                        "LastModified": obj["LastModified"],
                    }
                )

        # Progress indicator
        if list_requests % 10 == 0:
            print(
                f"Processed {list_requests} LIST requests, found {len(objects)} objects so far..."
            )

        # Check if there are more objects
        if not response.get("IsTruncated", False):
            break

        continuation_token = response.get("NextContinuationToken")

    print(
        f"\nCompleted listing: {list_requests} LIST requests, {len(objects)} total objects\n"
    )

    return objects, list_requests


def filter_objects_by_date(
    objects: List[dict], start_date: datetime, end_date: datetime
) -> List[dict]:
    """Filter objects by their LastModified date."""
    filtered = [obj for obj in objects if start_date <= obj["LastModified"] <= end_date]
    return filtered


def format_size(bytes_size: float) -> str:
    """Format bytes into human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def main():
    # Configuration
    BUCKET = "hl-mainnet-node-data"
    PREFIX = "node_fills_by_block/"

    print("=" * 70)
    print("Hyperliquid Trade Fills S3 Cost Estimator")
    print("=" * 70)
    print()

    # List all objects (no date filtering initially)
    print("Listing all available objects...")
    # Use a very wide date range to get everything
    far_past = datetime(2020, 1, 1, tzinfo=timezone.utc)
    far_future = datetime(2030, 1, 1, tzinfo=timezone.utc)

    objects, list_requests = list_s3_objects(BUCKET, PREFIX, far_past, far_future)

    if not objects:
        print("No objects found in the bucket.")
        return

    # Find earliest and latest dates
    earliest_obj = min(objects, key=lambda x: x["LastModified"])
    latest_obj = max(objects, key=lambda x: x["LastModified"])

    print("Data availability:")
    print(
        f"  Earliest data:  {earliest_obj['LastModified'].strftime('%Y-%m-%d %H:%M UTC')}"
    )
    print(
        f"  Latest data:    {latest_obj['LastModified'].strftime('%Y-%m-%d %H:%M UTC')}"
    )
    print(f"  Total files:    {len(objects):,}")
    print()

    # Calculate time span
    time_span = latest_obj["LastModified"] - earliest_obj["LastModified"]
    days_span = time_span.days
    print(f"Time span: {days_span} days (~{days_span / 365:.1f} years)")
    print()

    # Ask user what date range to calculate costs for
    print("Options:")
    print("  1. Calculate cost for ALL available data")
    print("  2. Calculate cost for last year")
    print("  3. Calculate cost for last 6 months")
    print("  4. Calculate cost for last 3 months")
    print("  5. Exit without calculating costs")
    print()

    choice = input("Select an option (1-5): ").strip()

    end_date = datetime.now(timezone.utc)

    if choice == "1":
        filtered_objects = objects
        start_date = earliest_obj["LastModified"]
    elif choice == "2":
        start_date = end_date - timedelta(days=365)
        filtered_objects = filter_objects_by_date(objects, start_date, end_date)
    elif choice == "3":
        start_date = end_date - timedelta(days=180)
        filtered_objects = filter_objects_by_date(objects, start_date, end_date)
    elif choice == "4":
        start_date = end_date - timedelta(days=90)
        filtered_objects = filter_objects_by_date(objects, start_date, end_date)
    elif choice == "5":
        print("Exiting...")
        return
    else:
        print("Invalid choice. Exiting...")
        return

    print(
        f"\nCalculating costs for {len(filtered_objects):,} files from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...\n"
    )

    # Calculate total size
    total_bytes = sum(obj["Size"] for obj in filtered_objects)
    total_gb = total_bytes / (1024**3)
    total_tb = total_gb / 1024

    # Calculate costs
    listing_cost = list_requests * LIST_REQUEST_COST
    transfer_cost = calculate_transfer_cost(total_gb)
    get_request_cost = len(filtered_objects) * GET_REQUEST_COST
    total_cost = listing_cost + transfer_cost + get_request_cost

    # Display results
    print("=" * 70)
    print("COST ESTIMATE SUMMARY")
    print("=" * 70)
    print()
    print("Data Summary:")
    print(f"  Number of files:        {len(filtered_objects):,}")
    print(f"  Total size:             {format_size(total_bytes)} ({total_gb:.2f} GB)")
    print(f"  Date range:             {start_date.date()} to {end_date.date()}")
    print()
    print("Cost Breakdown:")
    print(
        f"  LIST requests:          {list_requests:,} requests × ${LIST_REQUEST_COST:.6f} = ${listing_cost:.4f}"
    )
    print(
        f"  GET requests:           {len(filtered_objects):,} requests × ${GET_REQUEST_COST:.6f} = ${get_request_cost:.4f}"
    )
    print(
        f"  Data transfer out:      {total_gb:.2f} GB × ~$0.09/GB = ${transfer_cost:.2f}"
    )
    print()
    print(f"TOTAL ESTIMATED COST:   ${total_cost:.2f}")
    print()
    print("=" * 70)
    print()
    print("Notes:")
    print("  - LIST request costs have already been incurred by running this script")
    print("  - Data transfer costs will be incurred when you download the files")
    print("  - GET request costs will be incurred when you download the files")
    print("  - Pricing assumes US region data transfer to internet")
    print("  - Actual costs may vary slightly based on AWS region and billing")
    print()

    # Sample of files
    print("Sample of files found (first 10):")
    for obj in filtered_objects[:10]:
        print(
            f"  {obj['Key']} - {format_size(obj['Size'])} - {obj['LastModified'].date()}"
        )

    if len(filtered_objects) > 10:
        print(f"  ... and {len(filtered_objects) - 10} more files")
    print()


if __name__ == "__main__":
    main()
