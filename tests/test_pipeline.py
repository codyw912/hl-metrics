#!/usr/bin/env python3
"""
Test the normalization pipeline on a small sample of data.

This script processes just a single date to verify the pipeline works
before running it on the full dataset.
"""

from datetime import datetime, date
from pathlib import Path
from normalize_data import process_date, get_all_dates
from query_data import HyperliquidAnalytics


def main():
    """Test the pipeline on a single date."""
    print("=" * 80)
    print("Pipeline Test - Processing Single Date")
    print("=" * 80)
    print()

    # Get available dates
    all_dates = get_all_dates()

    if not all_dates:
        print("No data found. Make sure raw data is downloaded to ./data/hyperliquid/")
        return

    # Pick a date from the middle of the range for testing
    test_date = all_dates[len(all_dates) // 2]

    print(f"Available dates: {all_dates[0]} to {all_dates[-1]}")
    print(f"Testing with date: {test_date}")
    print()

    # Create output directory
    output_dir = Path("./data/processed_test/fills.parquet")
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    # Process the single date
    print("Processing...")
    result = process_date(test_date, output_dir)

    print()
    print("Result:")
    print(f"  Status: {result['status']}")
    print(f"  Dataset: {result['dataset']}")
    print(f"  Files processed: {result['files_processed']}")
    print(f"  Records written: {result['records_written']:,}")
    print()

    # Verify the output
    partition_dir = output_dir / f"date={test_date.strftime('%Y-%m-%d')}"
    output_file = partition_dir / "data.parquet"

    if output_file.exists():
        file_size = output_file.stat().st_size
        print(f"✓ Output file created: {output_file}")
        print(f"  File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
        print()

        # Try querying the data
        print("Testing query interface...")
        analytics = HyperliquidAnalytics(data_dir=str(output_dir))

        summary = analytics.get_data_summary()
        print("Data summary:")
        for key, value in summary.items():
            print(
                f"  {key}: {value:,}" if isinstance(value, int) else f"  {key}: {value}"
            )

        print()
        print("✓ Pipeline test successful!")
        print()
        print("Next steps:")
        print("  1. Run: uv run python normalize_data.py")
        print("     to process all dates")
        print("  2. Run: uv run python validate_data.py")
        print("     to validate the full dataset")
        print("  3. Run: uv run python query_data.py")
        print("     to see example analytics")
        print()

    else:
        print("✗ Output file not created")
        print()


if __name__ == "__main__":
    main()
