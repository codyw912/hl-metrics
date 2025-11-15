# Hyperliquid Metrics Analysis

Data normalization and analytics pipeline for Hyperliquid historical trade data with interactive visualizations and optimized performance.

## Quick Start

```bash
# Install dependencies
uv sync

# Download and process data
python scripts/download_data.py
python scripts/normalize_data.py

# Launch interactive analytics notebook
marimo edit notebooks/hl_research_optimized.py
```

## Overview

This project downloads, normalizes, and analyzes historical trade data from Hyperliquid's S3 buckets. It provides:

- **Optimized analytics** with persistent DuckDB (25-30s → <1s load time)
- **Interactive notebooks** using marimo with Altair visualizations
- **Unified data pipeline** handling three different data formats
- **User-focused metrics**: DAU, MAU, volume buckets, new user acquisition, asset analysis

## Project Structure

```
hl-metrics/
├── scripts/          # Data pipeline scripts
├── src/              # Analytics source code
├── notebooks/        # Interactive marimo notebooks
├── docs/             # Documentation
├── tests/            # Tests
└── data/             # Data directory (gitignored)
```

See [STRUCTURE.md](STRUCTURE.md) for detailed organization.

## Data Pipeline

### 1. Download Raw Data

```bash
python scripts/estimate_download_cost.py  # Check costs first
python scripts/download_data.py            # Download from S3
```

**Raw Data (~104 GB)** in `./data/hyperliquid/`:
- **node_trades/** (14 GB) - Legacy format (Mar 22 - Jun 21, 2025)
- **node_fills/** (30 GB) - Legacy API format (May 25 - Jul 27, 2025)
- **node_fills_by_block/** (61 GB) - Current format (Jul 27 - Nov 7, 2025)

### 2. Normalize Data

```bash
python scripts/normalize_data.py
python scripts/validate_data.py  # Optional: verify integrity
```

**Normalized Data (~40-60 GB)** in `./data/processed/fills.parquet/`:
- Date-partitioned Parquet files
- Unified schema across all formats
- Overlap periods deduplicated (prefers newer format)
- Efficient columnar storage with Snappy compression

### 3. Build Optimized Database (Recommended)

```bash
python src/query_data_optimized.py
```

Creates `data/processed/fills.duckdb` with pre-aggregated tables for instant queries.

## Analytics

### Interactive Notebook (Recommended)

```bash
marimo edit notebooks/hl_research_optimized.py
```

Features:
- Toggle data loading on/off
- DAU/MAU analysis with visualizations
- Volume distribution (all assets + filtered by coin)
- New user acquisition tracking
- Asset trading activity
- Summary reports

**Performance**: Instant load, <1s queries with optimized backend.

### Query Interface

```python
from src.query_data_optimized import HyperliquidAnalytics

analytics = HyperliquidAnalytics(
    data_dir='./data/processed/fills.parquet',
    db_path='./data/processed/fills.duckdb'
)

# Get metrics
dau = analytics.get_dau()
mau = analytics.get_mau()
buckets = analytics.get_volume_buckets()
```

See [docs/PERFORMANCE_OPTIMIZATION.md](docs/PERFORMANCE_OPTIMIZATION.md) for optimization details.

## Data Schema

Unified schema for all normalized data:

```python
{
    'date': pl.Date,              # Trading date
    'time': pl.Int64,             # Unix timestamp (ms)
    'user_address': pl.Utf8,      # Trader address
    'coin': pl.Utf8,              # Trading pair (e.g., "BTC", "ETH")
    'side': pl.Utf8,              # 'A' (ask/sell) or 'B' (bid/buy)
    'px': pl.Float64,             # Price
    'sz': pl.Float64,             # Size
    'hash': pl.Utf8,              # Transaction hash (unique ID)
    'closed_pnl': pl.Float64,     # Closed P&L (nullable)
    'fee': pl.Float64,            # Trading fee (nullable)
}
```

## Key Metrics

- **DAU (Daily Active Users)**: Unique traders per day
- **MAU (Monthly Active Users)**: Unique traders per month
- **Volume Buckets**: User distribution by trading volume thresholds
- **New User Acquisition**: First-time traders per day
- **Asset Statistics**: Volume, traders, trades per coin
- **Top Traders**: Highest volume users

## Development

```bash
# Run tests
uv run pytest tests/

# Check marimo notebooks
uv run marimo check notebooks/*.py

# Validate data integrity
python scripts/validate_data.py
```

## Performance

| Operation | Original | Optimized | Speedup |
|-----------|----------|-----------|---------|
| Initial load | 25-30s | <1s | 25-30x |
| DAU query | ~5s | ~50ms | 100x |
| Volume buckets | ~8s | ~200ms | 40x |
| New users | ~3s | ~20ms | 150x |

See [docs/PERFORMANCE_OPTIMIZATION.md](docs/PERFORMANCE_OPTIMIZATION.md) for details.

## Files Overview

### Scripts (`scripts/`)
- `download_data.py` - Download raw data from S3
- `normalize_data.py` - Convert to unified parquet format
- `validate_data.py` - Verify data integrity
- `check_data_availability.py` - Check what data exists
- `estimate_download_cost.py` - Estimate S3 costs

### Source (`src/`)
- `query_data_optimized.py` - Optimized analytics (recommended)
- `query_data.py` - Original analytics interface
- `schema.py` - Data schema definitions

### Notebooks (`notebooks/`)
- `hl_research_optimized.py` - Main analytics notebook (use this!)
- `hl_research_altair.py` - Original notebook (reference)

## Notes

- **Storage**: Ensure ~150-200 GB free space for raw + processed data
- **Memory**: 4-8 GB RAM recommended for analytics
- **Network**: S3 downloads are ~104 GB (egress charges apply)
- **Time**: Initial download ~30-60 min, normalization ~10-20 min

## License

MIT
