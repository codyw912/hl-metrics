#!/usr/bin/env python3
"""
Download Hyperliquid trade fills data from S3.

This script downloads objects from the requester-pays S3 bucket with:
- Date range filtering
- Path selection (current/legacy formats)
- Parallel downloads
- Progress tracking
- Resume capability
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from tqdm import tqdm

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


def download_file(
    s3_client, bucket: str, key: str, output_path: Path
) -> tuple[bool, int]:
    """
    Download a single file from S3.

    Returns:
        Tuple of (success, bytes_downloaded)
    """
    try:
        # Create parent directories
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Download with requester pays
        s3_client.download_file(
            bucket, key, str(output_path), ExtraArgs={"RequestPayer": "requester"}
        )

        return True, output_path.stat().st_size
    except Exception as e:
        return False, 0


def download_files_parallel(
    bucket: str,
    objects: list[dict],
    output_dir: Path,
    max_workers: int = 10,
) -> tuple[int, int, int]:
    """
    Download files in parallel with progress tracking.

    Returns:
        Tuple of (downloaded, skipped, failed)
    """
    s3_client = boto3.client("s3")

    downloaded = 0
    skipped = 0
    failed = 0

    # Calculate total size for progress bar
    total_size = sum(obj["Size"] for obj in objects)

    # Check which files already exist
    download_queue = []
    for obj in objects:
        output_path = output_dir / obj["Key"]

        # Skip if already exists with correct size
        if output_path.exists() and output_path.stat().st_size == obj["Size"]:
            skipped += 1
        else:
            download_queue.append(obj)

    if skipped > 0:
        print(f"  ⏭️  Skipping {skipped:,} files (already downloaded)")

    if not download_queue:
        print("  ✓ All files already downloaded!")
        return 0, skipped, 0

    print(
        f"  📥 Downloading {len(download_queue):,} files ({format_size(sum(o['Size'] for o in download_queue))})..."
    )

    # Progress bar
    with tqdm(
        total=sum(obj["Size"] for obj in download_queue),
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc="  Downloading",
    ) as pbar:
        # Download in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            future_to_obj = {
                executor.submit(
                    download_file,
                    s3_client,
                    bucket,
                    obj["Key"],
                    output_dir / obj["Key"],
                ): obj
                for obj in download_queue
            }

            # Process completed downloads
            for future in as_completed(future_to_obj):
                obj = future_to_obj[future]
                success, bytes_downloaded = future.result()

                if success:
                    downloaded += 1
                    pbar.update(bytes_downloaded)
                else:
                    failed += 1

    return downloaded, skipped, failed


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download Hyperliquid trade data from S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all available data
  uv run scripts/download_data.py
  
  # Download only current format
  uv run scripts/download_data.py --paths current
  
  # Download last 30 days
  uv run scripts/download_data.py --last-days 30
  
  # Download specific date range
  uv run scripts/download_data.py --start-date 2025-11-08 --end-date 2025-11-15
  
  # Dry run (see what would be downloaded)
  uv run scripts/download_data.py --dry-run
  
  # Faster downloads with more parallel workers
  uv run scripts/download_data.py --workers 20
        """,
    )

    parser.add_argument(
        "--paths",
        type=str,
        default="current",
        help="Which paths to download: current, legacy_fills, legacy_trades, or 'all' (default: current)",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD, inclusive)",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD, inclusive)",
    )

    parser.add_argument(
        "--last-days",
        type=int,
        help="Download only last N days of data",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel download workers (default: 10)",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./data/hyperliquid"),
        help="Output directory (default: ./data/hyperliquid)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without actually downloading",
    )

    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Check AWS credentials first
    if not check_aws_credentials():
        return 1

    print("=" * 80)
    print("Hyperliquid Trade Data Downloader")
    print("=" * 80)
    print()

    # Parse date range
    end_date = datetime.now(timezone.utc)
    start_date = None

    if args.last_days:
        start_date = end_date - timedelta(days=args.last_days)
        print(f"📅 Date range: Last {args.last_days} days")
        print(f"   {start_date.date()} to {end_date.date()}")
    elif args.start_date or args.end_date:
        if args.start_date:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
        print(
            f"📅 Date range: {start_date.date() if start_date else 'beginning'} to {end_date.date()}"
        )
    else:
        print("📅 Date range: All available data")

    print()

    # Parse paths
    if args.paths == "all":
        selected_paths = list(HYPERLIQUID_PATHS.keys())
    else:
        selected_paths = [p.strip() for p in args.paths.split(",")]

        # Validate paths
        invalid = [p for p in selected_paths if p not in HYPERLIQUID_PATHS]
        if invalid:
            print(f"❌ Error: Invalid path(s): {', '.join(invalid)}")
            print(f"   Valid options: {', '.join(HYPERLIQUID_PATHS.keys())}, all")
            return 1

    print("📂 Data sources:")
    for path_key in selected_paths:
        prefix, description = HYPERLIQUID_PATHS[path_key]
        print(f"   - {prefix} ({description})")
    print()

    # List objects from selected paths
    print("🔍 Scanning S3 bucket...")
    print()

    all_objects = []
    total_list_requests = 0

    for path_key in selected_paths:
        prefix, description = HYPERLIQUID_PATHS[path_key]
        objects, list_requests = list_s3_objects(
            HYPERLIQUID_BUCKET, prefix, start_date, end_date, verbose=True
        )
        all_objects.extend(objects)
        total_list_requests += list_requests

    if not all_objects:
        print()
        print("❌ No files found matching criteria")
        return 1

    print()

    # Calculate stats
    total_bytes = sum(obj["Size"] for obj in all_objects)
    total_gb = total_bytes / (1024**3)

    # Cost estimate
    costs = calculate_download_cost(total_gb, len(all_objects), total_list_requests)

    print("=" * 80)
    print("DOWNLOAD SUMMARY")
    print("=" * 80)
    print()
    print(f"Files to download:    {len(all_objects):,}")
    print(f"Total size:           {format_size(total_bytes)} ({total_gb:.2f} GB)")
    print()
    print("Estimated AWS costs:")
    print(f"  LIST requests:      ${costs['list_cost']:.4f} (already incurred)")
    print(f"  GET requests:       ${costs['get_cost']:.4f}")
    print(f"  Data transfer:      ${costs['transfer_cost']:.2f}")
    print(f"  TOTAL:              ${costs['total_cost']:.2f}")
    print()
    print(f"Output directory:     {args.output_dir}")
    print(f"Parallel workers:     {args.workers}")
    print()

    if args.dry_run:
        print("🔍 DRY RUN - No files will be downloaded")
        print()
        print("Sample files (first 10):")
        for obj in all_objects[:10]:
            print(f"  {obj['Key']} ({format_size(obj['Size'])})")
        if len(all_objects) > 10:
            print(f"  ... and {len(all_objects) - 10:,} more files")
        return 0

    # Confirm before downloading
    if not args.yes:
        response = input(
            "Continue with download? This will incur AWS costs. (yes/no): "
        )
        if response.lower() not in ["yes", "y"]:
            print("❌ Download cancelled")
            return 0

    print()
    print("=" * 80)
    print("DOWNLOADING")
    print("=" * 80)
    print()

    # Download files
    downloaded, skipped, failed = download_files_parallel(
        HYPERLIQUID_BUCKET, all_objects, args.output_dir, max_workers=args.workers
    )

    print()
    print("=" * 80)
    print("DOWNLOAD COMPLETE")
    print("=" * 80)
    print()
    print(f"✓ Downloaded:         {downloaded:,} files")
    print(f"⏭️  Skipped:            {skipped:,} files (already existed)")
    if failed > 0:
        print(f"❌ Failed:             {failed:,} files")
    print(f"📁 Output directory:   {args.output_dir}")
    print()

    if failed > 0:
        print("⚠️  Some files failed to download. Run the script again to retry.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
