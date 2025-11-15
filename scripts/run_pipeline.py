#!/usr/bin/env python3
"""
End-to-end data pipeline for Hyperliquid analytics.

This script runs the complete pipeline:
1. Download data from S3
2. Normalize to parquet format
3. Build optimized DuckDB database

Useful for:
- Initial setup
- Regular updates (downloading new data)
- One-command workflow
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str) -> bool:
    """
    Run a command and handle errors.

    Returns:
        True if successful, False otherwise
    """
    print()
    print("=" * 80)
    print(f"📋 {description}")
    print("=" * 80)
    print()

    try:
        result = subprocess.run(cmd, check=True)
        print()
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print()
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False
    except KeyboardInterrupt:
        print()
        print("❌ Interrupted by user")
        return False


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the complete Hyperliquid data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline with default settings
  uv run scripts/run_pipeline.py
  
  # Download last 30 days and process
  uv run scripts/run_pipeline.py --last-days 30
  
  # Specific date range
  uv run scripts/run_pipeline.py --start-date 2025-11-01 --end-date 2025-11-15
  
  # Skip download (only normalize and build DB)
  uv run scripts/run_pipeline.py --skip-download
  
  # Skip DB build (only download and normalize)
  uv run scripts/run_pipeline.py --skip-db
  
  # Force rebuild DB even if it exists
  uv run scripts/run_pipeline.py --rebuild-db
        """,
    )

    # Download options
    download_group = parser.add_argument_group("Download options")
    download_group.add_argument(
        "--last-days",
        type=int,
        help="Download only last N days of data",
    )
    download_group.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD, inclusive)",
    )
    download_group.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD, inclusive)",
    )
    download_group.add_argument(
        "--paths",
        type=str,
        default="current",
        help="Which paths to download: current, all (default: current)",
    )
    download_group.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel download workers (default: 10)",
    )

    # Pipeline control
    pipeline_group = parser.add_argument_group("Pipeline control")
    pipeline_group.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (only normalize and build DB)",
    )
    pipeline_group.add_argument(
        "--skip-normalize",
        action="store_true",
        help="Skip normalization step (only download and build DB)",
    )
    pipeline_group.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip DuckDB build step (only download and normalize)",
    )
    pipeline_group.add_argument(
        "--rebuild-db",
        action="store_true",
        help="Force rebuild DuckDB even if it exists",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    print()
    print("=" * 80)
    print("HYPERLIQUID DATA PIPELINE")
    print("=" * 80)
    print()
    print("This will run the complete data pipeline:")

    steps = []
    if not args.skip_download:
        steps.append("1. Download data from S3")
    if not args.skip_normalize:
        steps.append("2. Normalize to parquet format")
    if not args.skip_db:
        steps.append("3. Build DuckDB database")

    for step in steps:
        print(f"  {step}")
    print()

    # Step 1: Download
    if not args.skip_download:
        download_cmd = ["uv", "run", "scripts/download_data.py", "--yes"]

        if args.last_days:
            download_cmd.extend(["--last-days", str(args.last_days)])
        if args.start_date:
            download_cmd.extend(["--start-date", args.start_date])
        if args.end_date:
            download_cmd.extend(["--end-date", args.end_date])
        if args.paths:
            download_cmd.extend(["--paths", args.paths])
        if args.workers:
            download_cmd.extend(["--workers", str(args.workers)])

        if not run_command(download_cmd, "Step 1: Download data"):
            print()
            print("❌ Pipeline failed at download step")
            return 1

    # Step 2: Normalize
    if not args.skip_normalize:
        normalize_cmd = ["uv", "run", "scripts/normalize_data.py"]

        if not run_command(normalize_cmd, "Step 2: Normalize data"):
            print()
            print("❌ Pipeline failed at normalization step")
            return 1

    # Step 3: Build DuckDB
    if not args.skip_db:
        # Check if DB exists and whether to rebuild
        db_path = Path("data/processed/fills.duckdb")

        if db_path.exists() and not args.rebuild_db:
            print()
            print("=" * 80)
            print("📋 Step 3: Build DuckDB database")
            print("=" * 80)
            print()
            print("  ⏭️  DuckDB database already exists at data/processed/fills.duckdb")
            print("     Use --rebuild-db to force rebuild")
            print()
            print("✓ Step 3: Build DuckDB database completed successfully")
        else:
            # Import and run query_data.py's main function
            build_cmd = ["uv", "run", "src/query_data.py"]

            if not run_command(build_cmd, "Step 3: Build DuckDB database"):
                print()
                print("❌ Pipeline failed at DuckDB build step")
                return 1

    # Success!
    print()
    print("=" * 80)
    print("✅ PIPELINE COMPLETE")
    print("=" * 80)
    print()
    print("Next steps:")
    print("  - Run analytics: uv run marimo edit notebooks/hl_research.py")
    print("  - Or use query interface: from src.query_data import HyperliquidAnalytics")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
