"""
Schema definitions for Hyperliquid trade data normalization.

This module defines the unified schema that all three data formats
(node_trades, node_fills, node_fills_by_block) will be converted to.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from datetime import datetime
import pyarrow as pa


@dataclass
class NormalizedFill:
    """
    Unified schema for all trade fills across all formats.

    This schema combines fields from:
    - node_trades (legacy alternative format)
    - node_fills (legacy API format)
    - node_fills_by_block (current format)
    """

    # Core fields (present in all formats)
    coin: str  # Trading pair (e.g., "BTC", "ETH/USDC")
    px: str  # Price as string (preserve precision)
    sz: str  # Size as string (preserve precision)
    side: str  # 'A' (Ask) or 'B' (Bid)
    time: int  # Unix timestamp in milliseconds
    hash: str  # Transaction hash

    # User information
    user_address: str  # User's address

    # Order and trade IDs (null for node_trades)
    oid: Optional[int] = None  # Order ID
    tid: Optional[int] = None  # Trade ID

    # Position and P&L info (null for node_trades)
    start_position: Optional[str] = None  # Starting position as string
    direction: Optional[str] = (
        None  # Trade direction (e.g., "Buy", "Sell", "Close Long")
    )
    closed_pnl: Optional[str] = None  # Closed P&L as string

    # Order execution details (null for node_trades)
    crossed: Optional[bool] = None  # Whether order crossed the spread

    # Fee information (null for node_trades)
    fee: Optional[str] = None  # Fee paid/received as string
    fee_token: Optional[str] = None  # Token used for fee payment

    # Client order ID (optional, sometimes null even in newer formats)
    cloid: Optional[str] = None  # Client order ID

    # Block-level metadata (null except for node_fills_by_block)
    block_number: Optional[int] = None  # Block number
    block_time: Optional[str] = None  # Block timestamp (ISO format)
    builder: Optional[str] = None  # Builder address
    builder_fee: Optional[str] = None  # Builder fee as string

    # Metadata fields for tracking data lineage
    dataset_source: str = (
        ""  # Source dataset: "node_trades", "node_fills", or "node_fills_by_block"
    )
    local_time: Optional[str] = None  # Local processing time (from node_fills_by_block)


# PyArrow schema for efficient Parquet storage
NORMALIZED_FILL_SCHEMA = pa.schema(
    [
        # Core fields
        ("coin", pa.string()),
        ("px", pa.string()),
        ("sz", pa.string()),
        ("side", pa.string()),
        ("time", pa.int64()),
        ("hash", pa.string()),
        # User info
        ("user_address", pa.string()),
        # Order/trade IDs
        ("oid", pa.int64()),
        ("tid", pa.int64()),
        # Position/P&L
        ("start_position", pa.string()),
        ("direction", pa.string()),
        ("closed_pnl", pa.string()),
        # Execution details
        ("crossed", pa.bool_()),
        # Fees
        ("fee", pa.string()),
        ("fee_token", pa.string()),
        # Client order ID
        ("cloid", pa.string()),
        # Block metadata
        ("block_number", pa.int64()),
        ("block_time", pa.string()),
        ("builder", pa.string()),
        ("builder_fee", pa.string()),
        # Metadata
        ("dataset_source", pa.string()),
        ("local_time", pa.string()),
    ]
)


def normalize_fill_to_dict(fill: NormalizedFill) -> dict:
    """Convert a NormalizedFill to a dictionary for Parquet writing."""
    return {
        "coin": fill.coin,
        "px": fill.px,
        "sz": fill.sz,
        "side": fill.side,
        "time": fill.time,
        "hash": fill.hash,
        "user_address": fill.user_address,
        "oid": fill.oid,
        "tid": fill.tid,
        "start_position": fill.start_position,
        "direction": fill.direction,
        "closed_pnl": fill.closed_pnl,
        "crossed": fill.crossed,
        "fee": fill.fee,
        "fee_token": fill.fee_token,
        "cloid": fill.cloid,
        "block_number": fill.block_number,
        "block_time": fill.block_time,
        "builder": fill.builder,
        "builder_fee": fill.builder_fee,
        "dataset_source": fill.dataset_source,
        "local_time": fill.local_time,
    }


# Dataset configuration
DATASET_CONFIG = {
    "node_trades": {
        "path": "data/hyperliquid/node_trades/hourly",
        "date_range": ("2025-03-22", "2025-06-21"),
        "priority": 3,  # Lowest priority (use if no other data available)
    },
    "node_fills": {
        "path": "data/hyperliquid/node_fills/hourly",
        "date_range": ("2025-05-25", "2025-07-27"),
        "priority": 2,  # Medium priority
    },
    "node_fills_by_block": {
        "path": "data/hyperliquid/node_fills_by_block/hourly",
        "date_range": ("2025-07-27", "2025-11-07"),
        "priority": 1,  # Highest priority (most complete format)
    },
}

# Overlap handling: for dates with multiple formats, use highest priority (lowest number)
# May 25: node_trades (3) vs node_fills (2) → use node_fills
# June 21: node_trades (3) vs node_fills (2) → use node_fills
# July 27: node_fills (2) vs node_fills_by_block (1) → use node_fills_by_block
