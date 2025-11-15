#!/usr/bin/env python3
"""
Estimate the cost of downloading Hyperliquid trade fills data from S3.

This script provides interactive cost estimation with various options:
- All data vs. specific time ranges
- Current format only vs. all formats
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from s3_utils import (
    HYPERLIQUID_BUCKET,
    HYPERLIQUID_PATHS,
    check_aws_credentials,
    format_size,
    list_s3_objects,
    calculate_download_cost,
)


def filter_objects_by_date(
    objects: list[dict], start_date: datetime, end_date: datetime
) -> list[dict]:
    """Filter objects by their LastModified date."""
    return [obj for obj in objects if start_date <= obj["LastModified"] <= end_date]


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Estimate AWS costs for downloading Hyperliquid data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--paths",
        type=str,
        default="current",
        help="Which paths to check: current, legacy_fills, legacy_trades, or 'all' (default: current)",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Check AWS credentials first
    if not check_aws_credentials():
        return 1

    print("=" * 80)
    print("Hyperliquid Trade Data Cost Estimator")
    print("=" * 80)
    print()

    # Parse paths
    if args.paths == "all":
        selected_paths = list(HYPERLIQUID_PATHS.keys())
    else:
        selected_paths = [p.strip() for p in args.paths.split(",")]

        # Validate
        invalid = [p for p in selected_paths if p not in HYPERLIQUID_PATHS]
        if invalid:
            print(f"❌ Error: Invalid path(s): {', '.join(invalid)}")
            print(f"   Valid options: {', '.join(HYPERLIQUID_PATHS.keys())}, all")
            return 1

    print("Checking data sources:")
    for path_key in selected_paths:
        prefix, description = HYPERLIQUID_PATHS[path_key]
        print(f"  - {prefix} ({description})")
    print()

    # List all objects
    print("Listing available objects...")
    print()

    all_objects = []
    total_list_requests = 0

    for path_key in selected_paths:
        prefix, description = HYPERLIQUID_PATHS[path_key]
        objects, list_requests = list_s3_objects(
            HYPERLIQUID_BUCKET, prefix, verbose=True
        )
        all_objects.extend(objects)
        total_list_requests += list_requests

    if not all_objects:
        print("\n❌ No objects found")
        return 1

    print()

    # Find date range
    earliest_obj = min(all_objects, key=lambda x: x["LastModified"])
    latest_obj = max(all_objects, key=lambda x: x["LastModified"])

    print("Data availability:")
    print(
        f"  Earliest:   {earliest_obj['LastModified'].strftime('%Y-%m-%d %H:%M UTC')}"
    )
    print(f"  Latest:     {latest_obj['LastModified'].strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Total files: {len(all_objects):,}")
    print()

    # Time span
    time_span = latest_obj["LastModified"] - earliest_obj["LastModified"]
    days_span = time_span.days
    print(f"Time span: {days_span} days (~{days_span / 365:.1f} years)")
    print()

    # Ask for date range
    print("Select date range to estimate:")
    print("  1. All available data")
    print("  2. Last year")
    print("  3. Last 6 months")
    print("  4. Last 3 months")
    print("  5. Last month")
    print("  6. Exit")
    print()

    choice = input("Select option (1-6): ").strip()

    end_date = datetime.now(timezone.utc)

    if choice == "1":
        filtered_objects = all_objects
        start_date = earliest_obj["LastModified"]
        range_desc = "ALL available data"
    elif choice == "2":
        start_date = end_date - timedelta(days=365)
        filtered_objects = filter_objects_by_date(all_objects, start_date, end_date)
        range_desc = "Last year"
    elif choice == "3":
        start_date = end_date - timedelta(days=180)
        filtered_objects = filter_objects_by_date(all_objects, start_date, end_date)
        range_desc = "Last 6 months"
    elif choice == "4":
        start_date = end_date - timedelta(days=90)
        filtered_objects = filter_objects_by_date(all_objects, start_date, end_date)
        range_desc = "Last 3 months"
    elif choice == "5":
        start_date = end_date - timedelta(days=30)
        filtered_objects = filter_objects_by_date(all_objects, start_date, end_date)
        range_desc = "Last month"
    elif choice == "6":
        print("Exiting...")
        return 0
    else:
        print("❌ Invalid choice")
        return 1

    print()
    print(f"Calculating costs for: {range_desc}")
    print(
        f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )
    print()

    # Calculate stats
    total_bytes = sum(obj["Size"] for obj in filtered_objects)
    total_gb = total_bytes / (1024**3)

    costs = calculate_download_cost(
        total_gb, len(filtered_objects), total_list_requests
    )

    # Display results
    print("=" * 80)
    print("COST ESTIMATE")
    print("=" * 80)
    print()
    print("Data Summary:")
    print(f"  Files:              {len(filtered_objects):,}")
    print(f"  Total size:         {format_size(total_bytes)} ({total_gb:.2f} GB)")
    print(f"  Date range:         {start_date.date()} to {end_date.date()}")
    print()
    print("Cost Breakdown:")
    print(
        f"  LIST requests:      ${costs['list_cost']:.4f} (already incurred by running this script)"
    )
    print(f"  GET requests:       ${costs['get_cost']:.4f}")
    print(f"  Data transfer:      ${costs['transfer_cost']:.2f}")
    print()
    print(f"TOTAL ESTIMATED:    ${costs['total_cost']:.2f}")
    print()
    print("=" * 80)
    print()

    # Helpful commands
    print("💡 To download this data, run:")
    print()
    if choice == "1":
        if args.paths == "current":
            print("  uv run scripts/download_data.py --paths current")
        else:
            print(f"  uv run scripts/download_data.py --paths {args.paths}")
    elif choice in ["2", "3", "4", "5"]:
        days_map = {"2": 365, "3": 180, "4": 90, "5": 30}
        days = days_map[choice]
        if args.paths == "current":
            print(f"  uv run scripts/download_data.py --last-days {days}")
        else:
            print(
                f"  uv run scripts/download_data.py --last-days {days} --paths {args.paths}"
            )
    print()

    print("Sample files (first 10):")
    for obj in filtered_objects[:10]:
        print(f"  {obj['Key']} - {format_size(obj['Size'])}")

    if len(filtered_objects) > 10:
        print(f"  ... and {len(filtered_objects) - 10:,} more files")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
