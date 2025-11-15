#!/usr/bin/env python3
"""
Normalize Hyperliquid trade data from all three formats into a unified schema.

This script processes data chunk-by-chunk to handle datasets larger than RAM,
deduplicates overlap periods, and writes to date-partitioned Parquet files.
"""

import json
import lz4.frame
from pathlib import Path
from typing import List, Iterator, Optional
from datetime import datetime, date
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict

from schema import NormalizedFill, NORMALIZED_FILL_SCHEMA, DATASET_CONFIG


def read_lz4_jsonl(file_path: Path) -> Iterator[dict]:
    """Read and decompress LZ4 file, yielding JSON records line by line."""
    with lz4.frame.open(str(file_path), "r") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def iso_to_unix_ms(iso_str: str) -> int:
    """Convert ISO timestamp string to Unix milliseconds."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def convert_node_trades(record: dict) -> List[NormalizedFill]:
    """
    Convert node_trades record to normalized fills.

    node_trades has both parties in side_info array, so we expand
    into 2 separate fill records.
    """
    fills = []

    # Convert ISO timestamp to Unix ms
    time_ms = iso_to_unix_ms(record["time"])

    # Extract both parties from side_info
    for party in record.get("side_info", []):
        fill = NormalizedFill(
            # Core fields
            coin=record["coin"],
            px=record["px"],
            sz=record["sz"],
            side=record["side"],
            time=time_ms,
            hash=record["hash"],
            # User info
            user_address=party["user"],
            # Order ID from side_info
            oid=party.get("oid"),
            # Fields not available in node_trades (set to None)
            tid=None,
            start_position=party.get("start_pos"),
            direction=None,
            closed_pnl=None,
            crossed=None,
            fee=None,
            fee_token=None,
            cloid=party.get("cloid"),
            # Block metadata not available
            block_number=None,
            block_time=None,
            builder=None,
            builder_fee=None,
            # Metadata
            dataset_source="node_trades",
            local_time=None,
        )
        fills.append(fill)

    return fills


def convert_node_fills(record: list) -> NormalizedFill:
    """
    Convert node_fills record to normalized fill.

    node_fills format: [user_address, fill_data]
    """
    user_address = record[0]
    data = record[1]

    return NormalizedFill(
        # Core fields
        coin=data["coin"],
        px=data["px"],
        sz=data["sz"],
        side=data["side"],
        time=data["time"],
        hash=data["hash"],
        # User info
        user_address=user_address,
        # Order/trade IDs
        oid=data.get("oid"),
        tid=data.get("tid"),
        # Position/P&L
        start_position=data.get("startPosition"),
        direction=data.get("dir"),
        closed_pnl=data.get("closedPnl"),
        # Execution details
        crossed=data.get("crossed"),
        # Fees
        fee=data.get("fee"),
        fee_token=data.get("feeToken"),
        # Client order ID
        cloid=data.get("cloid"),
        # Block metadata not available in node_fills
        block_number=None,
        block_time=None,
        builder=None,
        builder_fee=None,
        # Metadata
        dataset_source="node_fills",
        local_time=None,
    )


def convert_node_fills_by_block(record: dict) -> List[NormalizedFill]:
    """
    Convert node_fills_by_block record to normalized fills.

    node_fills_by_block groups multiple fills by block, so we
    extract and flatten the events array.
    """
    fills = []

    block_number = record.get("block_number")
    block_time = record.get("block_time")
    local_time = record.get("local_time")

    # Extract all events from the block
    for event in record.get("events", []):
        user_address = event[0]
        data = event[1]

        fill = NormalizedFill(
            # Core fields
            coin=data["coin"],
            px=data["px"],
            sz=data["sz"],
            side=data["side"],
            time=data["time"],
            hash=data["hash"],
            # User info
            user_address=user_address,
            # Order/trade IDs
            oid=data.get("oid"),
            tid=data.get("tid"),
            # Position/P&L
            start_position=data.get("startPosition"),
            direction=data.get("dir"),
            closed_pnl=data.get("closedPnl"),
            # Execution details
            crossed=data.get("crossed"),
            # Fees
            fee=data.get("fee"),
            fee_token=data.get("feeToken"),
            # Client order ID
            cloid=data.get("cloid"),
            # Block metadata (unique to this format)
            block_number=block_number,
            block_time=block_time,
            builder=data.get("builder"),
            builder_fee=data.get("builderFee"),
            # Metadata
            dataset_source="node_fills_by_block",
            local_time=local_time,
        )
        fills.append(fill)

    return fills


def process_file(dataset_name: str, file_path: Path) -> Iterator[NormalizedFill]:
    """Process a single LZ4 file and yield normalized fills."""
    converter_map = {
        "node_trades": convert_node_trades,
        "node_fills": convert_node_fills,
        "node_fills_by_block": convert_node_fills_by_block,
    }

    converter = converter_map[dataset_name]

    for record in read_lz4_jsonl(file_path):
        result = converter(record)

        # Handle both single fill and list of fills
        if isinstance(result, list):
            yield from result
        else:
            yield result


def get_date_from_path(file_path: Path) -> date:
    """Extract date from file path: .../YYYYMMDD/HH.lz4"""
    date_str = file_path.parent.name
    return datetime.strptime(date_str, "%Y%m%d").date()


def get_files_for_date(dataset_name: str, target_date: date) -> List[Path]:
    """Get all hourly files for a specific date from a dataset."""
    config = DATASET_CONFIG[dataset_name]
    base_path = Path(config["path"])

    date_str = target_date.strftime("%Y%m%d")
    date_dir = base_path / date_str

    if not date_dir.exists():
        return []

    return sorted(date_dir.glob("*.lz4"))


def determine_dataset_for_date(target_date: date) -> Optional[str]:
    """
    Determine which dataset to use for a given date.

    For overlap periods, uses the highest priority dataset.
    Returns None if no dataset covers this date.
    """
    available_datasets = []

    for dataset_name, config in DATASET_CONFIG.items():
        start_date = datetime.strptime(config["date_range"][0], "%Y-%m-%d").date()
        end_date = datetime.strptime(config["date_range"][1], "%Y-%m-%d").date()

        if start_date <= target_date <= end_date:
            available_datasets.append((config["priority"], dataset_name))

    if not available_datasets:
        return None

    # Return dataset with lowest priority number (highest priority)
    available_datasets.sort()
    return available_datasets[0][1]


def process_date(target_date: date, output_dir: Path) -> dict:
    """
    Process all data for a specific date and write to Parquet.

    Returns statistics about the processing.
    """
    dataset_name = determine_dataset_for_date(target_date)

    if dataset_name is None:
        return {
            "date": target_date,
            "dataset": None,
            "files_processed": 0,
            "records_written": 0,
            "status": "no_data",
        }

    files = get_files_for_date(dataset_name, target_date)

    if not files:
        return {
            "date": target_date,
            "dataset": dataset_name,
            "files_processed": 0,
            "records_written": 0,
            "status": "no_files",
        }

    # Collect all fills for this date
    fills = []
    for file_path in files:
        for fill in process_file(dataset_name, file_path):
            fills.append(fill)

    # Convert to PyArrow table
    fill_dicts = [fill.__dict__ for fill in fills]
    table = pa.Table.from_pylist(fill_dicts, schema=NORMALIZED_FILL_SCHEMA)

    # Write to partitioned Parquet
    date_str = target_date.strftime("%Y-%m-%d")
    partition_dir = output_dir / f"date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    output_file = partition_dir / "data.parquet"
    pq.write_table(table, output_file, compression="snappy")

    return {
        "date": target_date,
        "dataset": dataset_name,
        "files_processed": len(files),
        "records_written": len(fills),
        "status": "success",
    }


def get_all_dates() -> List[date]:
    """Get all unique dates across all datasets."""
    all_dates = set()

    for dataset_name, config in DATASET_CONFIG.items():
        base_path = Path(config["path"])

        if not base_path.exists():
            continue

        for date_dir in base_path.iterdir():
            if date_dir.is_dir() and date_dir.name.isdigit():
                try:
                    date_obj = datetime.strptime(date_dir.name, "%Y%m%d").date()
                    all_dates.add(date_obj)
                except ValueError:
                    continue

    return sorted(all_dates)


def main():
    """Main normalization pipeline."""
    output_dir = Path("./data/processed/fills.parquet")

    print("=" * 80)
    print("Hyperliquid Data Normalization Pipeline")
    print("=" * 80)
    print()

    # Get all dates to process
    all_dates = get_all_dates()

    print(f"Found {len(all_dates)} unique dates to process")
    print(f"Date range: {all_dates[0]} to {all_dates[-1]}")
    print()

    # Check which dates already exist
    existing_dates = set()
    if output_dir.exists():
        for partition_dir in output_dir.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith("date="):
                date_str = partition_dir.name.split("=")[1]
                existing_dates.add(datetime.strptime(date_str, "%Y-%m-%d").date())

    if existing_dates:
        print(f"Found {len(existing_dates)} already processed dates")
        response = input("Skip already processed dates? (yes/no): ")
        skip_existing = response.lower() in ["yes", "y"]
    else:
        skip_existing = False

    print()

    # Process each date
    stats_by_dataset = defaultdict(lambda: {"dates": 0, "files": 0, "records": 0})
    total_records = 0

    for i, target_date in enumerate(all_dates, 1):
        if skip_existing and target_date in existing_dates:
            print(f"[{i}/{len(all_dates)}] Skipping {target_date} (already processed)")
            continue

        print(
            f"[{i}/{len(all_dates)}] Processing {target_date}...", end=" ", flush=True
        )

        result = process_date(target_date, output_dir)

        if result["status"] == "success":
            dataset = result["dataset"]
            stats_by_dataset[dataset]["dates"] += 1
            stats_by_dataset[dataset]["files"] += result["files_processed"]
            stats_by_dataset[dataset]["records"] += result["records_written"]
            total_records += result["records_written"]

            print(f"✓ {result['records_written']:,} records from {dataset}")
        else:
            print(f"⚠ {result['status']}")

        # Progress update every 10 dates
        if i % 10 == 0:
            print(
                f"  Progress: {i}/{len(all_dates)} dates ({total_records:,} records so far)"
            )

    print()
    print("=" * 80)
    print("NORMALIZATION COMPLETE")
    print("=" * 80)
    print()
    print("Summary by dataset:")
    for dataset, stats in sorted(stats_by_dataset.items()):
        print(f"  {dataset}:")
        print(f"    Dates:   {stats['dates']}")
        print(f"    Files:   {stats['files']}")
        print(f"    Records: {stats['records']:,}")
    print()
    print(f"Total records written: {total_records:,}")
    print(f"Output directory: {output_dir}")
    print()


if __name__ == "__main__":
    main()
