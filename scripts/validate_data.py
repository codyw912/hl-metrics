#!/usr/bin/env python3
"""
Validation script for normalized Hyperliquid trade data.

This script performs various checks to ensure data quality:
- Record count verification
- Overlap period deduplication validation
- Data completeness checks
- Summary statistics
"""

import duckdb
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import lz4.frame
import json


def count_records_in_lz4(file_path: Path) -> int:
    """Count records in an LZ4 compressed JSONL file."""
    count = 0
    with lz4.frame.open(str(file_path), "r") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def validate_record_counts():
    """
    Validate that record counts match between raw and processed data.

    Note: node_trades will have fewer records than fills because it groups
    two parties into one record. node_fills and node_fills_by_block should
    match fill counts.
    """
    print("=" * 80)
    print("Record Count Validation")
    print("=" * 80)
    print()

    # Count raw data records
    raw_counts = defaultdict(int)

    for dataset_name in ["node_trades", "node_fills", "node_fills_by_block"]:
        base_path = Path(f"data/hyperliquid/{dataset_name}/hourly")

        if not base_path.exists():
            continue

        print(f"Counting records in {dataset_name}...")

        for date_dir in sorted(base_path.iterdir()):
            if not date_dir.is_dir():
                continue

            for file_path in date_dir.glob("*.lz4"):
                count = count_records_in_lz4(file_path)
                raw_counts[dataset_name] += count

        print(f"  {dataset_name}: {raw_counts[dataset_name]:,} records")

    print()

    # Count processed data records
    processed_dir = Path("./data/processed/fills.parquet")

    if not processed_dir.exists():
        print("Processed data not found. Run normalize_data.py first.")
        return

    print("Counting records in processed data...")
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE VIEW fills AS
        SELECT * FROM read_parquet('{processed_dir}/**/*.parquet', hive_partitioning=true)
    """)

    # Total count
    total_processed = conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
    print(f"  Total processed: {total_processed:,} records")
    print()

    # Count by dataset source
    by_source = conn.execute("""
        SELECT dataset_source, COUNT(*) as count
        FROM fills
        GROUP BY dataset_source
        ORDER BY dataset_source
    """).fetchall()

    print("Processed records by source:")
    for source, count in by_source:
        print(f"  {source}: {count:,} records")

    print()

    # Validation notes
    print("Validation notes:")
    print("  - node_trades records should be ~half of processed node_trades fills")
    print("    (because each trade generates 2 fills, one per party)")
    print("  - node_fills and node_fills_by_block should match closely")
    print()


def validate_overlap_deduplication():
    """
    Validate that overlap periods are properly deduplicated.

    Overlap periods:
    - May 25, 2025: node_trades vs node_fills (should use node_fills)
    - June 21, 2025: node_trades vs node_fills (should use node_fills)
    - July 27, 2025: node_fills vs node_fills_by_block (should use node_fills_by_block)
    """
    print("=" * 80)
    print("Overlap Period Deduplication Validation")
    print("=" * 80)
    print()

    processed_dir = Path("./data/processed/fills.parquet")

    if not processed_dir.exists():
        print("Processed data not found. Run normalize_data.py first.")
        return

    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE VIEW fills AS
        SELECT * FROM read_parquet('{processed_dir}/**/*.parquet', hive_partitioning=true)
    """)

    overlap_dates = [
        ("2025-05-25", "node_fills", "node_trades"),
        ("2025-06-21", "node_fills", "node_trades"),
        ("2025-07-27", "node_fills_by_block", "node_fills"),
    ]

    for date_str, expected_source, alternative_source in overlap_dates:
        result = conn.execute(f"""
            SELECT
                dataset_source,
                COUNT(*) as count
            FROM fills
            WHERE date = '{date_str}'
            GROUP BY dataset_source
        """).fetchall()

        print(f"Date: {date_str}")
        print(f"  Expected source: {expected_source}")

        if not result:
            print("  ⚠ No data found for this date")
        else:
            for source, count in result:
                if source == expected_source:
                    print(f"  ✓ Using {source}: {count:,} records")
                else:
                    print(
                        f"  ✗ WARNING: Found {source}: {count:,} records (should be {expected_source})"
                    )

        print()


def validate_data_completeness():
    """
    Check for missing dates and data completeness.
    """
    print("=" * 80)
    print("Data Completeness Validation")
    print("=" * 80)
    print()

    processed_dir = Path("./data/processed/fills.parquet")

    if not processed_dir.exists():
        print("Processed data not found. Run normalize_data.py first.")
        return

    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE VIEW fills AS
        SELECT * FROM read_parquet('{processed_dir}/**/*.parquet', hive_partitioning=true)
    """)

    # Get date range
    date_range = conn.execute("""
        SELECT MIN(date), MAX(date), COUNT(DISTINCT date)
        FROM fills
    """).fetchone()

    min_date, max_date, unique_dates = date_range

    print(f"Date range: {min_date} to {max_date}")
    print(f"Unique dates: {unique_dates}")
    print()

    # Check for gaps
    expected_days = (
        datetime.strptime(str(max_date), "%Y-%m-%d")
        - datetime.strptime(str(min_date), "%Y-%m-%d")
    ).days + 1

    print(f"Expected days (inclusive): {expected_days}")
    print(f"Actual days with data: {unique_dates}")

    if expected_days == unique_dates:
        print("✓ No gaps in data")
    else:
        print(f"⚠ Missing {expected_days - unique_dates} days")

        # Find missing dates
        missing_dates = conn.execute(f"""
            WITH RECURSIVE dates AS (
                SELECT DATE '{min_date}' AS date
                UNION ALL
                SELECT date + INTERVAL 1 DAY
                FROM dates
                WHERE date < '{max_date}'
            )
            SELECT dates.date
            FROM dates
            LEFT JOIN (SELECT DISTINCT date FROM fills) f ON dates.date = f.date
            WHERE f.date IS NULL
            ORDER BY dates.date
        """).fetchall()

        print("\nMissing dates:")
        for (missing_date,) in missing_dates:
            print(f"  - {missing_date}")

    print()


def generate_summary_statistics():
    """
    Generate summary statistics for the processed data.
    """
    print("=" * 80)
    print("Summary Statistics")
    print("=" * 80)
    print()

    processed_dir = Path("./data/processed/fills.parquet")

    if not processed_dir.exists():
        print("Processed data not found. Run normalize_data.py first.")
        return

    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE VIEW fills AS
        SELECT * FROM read_parquet('{processed_dir}/**/*.parquet', hive_partitioning=true)
    """)

    # Overall stats
    stats = conn.execute("""
        SELECT
            COUNT(*) as total_fills,
            COUNT(DISTINCT user_address) as unique_users,
            COUNT(DISTINCT coin) as unique_coins,
            COUNT(DISTINCT date) as total_days,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM fills
    """).fetchone()

    print("Overall Statistics:")
    print(f"  Total fills: {stats[0]:,}")
    print(f"  Unique users: {stats[1]:,}")
    print(f"  Unique coins: {stats[2]:,}")
    print(f"  Total days: {stats[3]}")
    print(f"  Date range: {stats[4]} to {stats[5]}")
    print()

    # Stats by dataset source
    by_source = conn.execute("""
        SELECT
            dataset_source,
            COUNT(*) as fills,
            COUNT(DISTINCT user_address) as users,
            COUNT(DISTINCT coin) as coins,
            COUNT(DISTINCT date) as days,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM fills
        GROUP BY dataset_source
        ORDER BY dataset_source
    """).fetchall()

    print("Statistics by Dataset Source:")
    for row in by_source:
        source, fills, users, coins, days, min_date, max_date = row
        print(f"\n  {source}:")
        print(f"    Fills: {fills:,}")
        print(f"    Users: {users:,}")
        print(f"    Coins: {coins}")
        print(f"    Days: {days}")
        print(f"    Date range: {min_date} to {max_date}")

    print()

    # Top coins
    print("Top 10 Coins by Trade Count:")
    top_coins = conn.execute("""
        SELECT coin, COUNT(*) as trades
        FROM fills
        GROUP BY coin
        ORDER BY trades DESC
        LIMIT 10
    """).fetchall()

    for coin, trades in top_coins:
        print(f"  {coin}: {trades:,} trades")

    print()


def main():
    """Run all validation checks."""
    print()
    print("Hyperliquid Data Validation")
    print()

    validate_record_counts()
    validate_overlap_deduplication()
    validate_data_completeness()
    generate_summary_statistics()

    print("=" * 80)
    print("Validation Complete")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
