#!/usr/bin/env python3
"""
Check availability of Hyperliquid trade data across all S3 paths.

This script checks all three trade data locations and provides overview statistics.
"""

import sys
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


def analyze_objects(objects: list[dict], name: str) -> dict | None:
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
    # Check AWS credentials first
    if not check_aws_credentials():
        return 1

    print("=" * 80)
    print("Hyperliquid Trade Data Availability Check")
    print("=" * 80)
    print()
    print("Checking all trade data paths in S3...")
    print()

    results = []
    total_list_requests = 0

    for path_key, (prefix, description) in HYPERLIQUID_PATHS.items():
        print(f"{description}:")
        objects, list_requests = list_s3_objects(
            HYPERLIQUID_BUCKET, prefix, verbose=True
        )
        total_list_requests += list_requests

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
        return 1

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

    # Cost estimate
    costs = calculate_download_cost(total_gb, total_files, total_list_requests)

    print("Estimated cost to download ALL trade data:")
    print(f"  LIST requests:  ${costs['list_cost']:.4f} (already incurred)")
    print(f"  GET requests:   ${costs['get_cost']:.4f}")
    print(f"  Data transfer:  ${costs['transfer_cost']:.2f}")
    print(f"  TOTAL:          ${costs['total_cost']:.2f}")
    print()
    print("💡 Tip: Download only what you need to save costs!")
    print("   - Use --last-days to download recent data only")
    print("   - Use --paths current to skip legacy formats")
    print(
        "   - Example: uv run scripts/download_data.py --last-days 90 --paths current"
    )
    print()
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
