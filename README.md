# Hyperliquid Metrics Analysis

Data normalization and analytics pipeline for Hyperliquid historical trade data with interactive visualizations and optimized performance.

## Quick Start

```bash
# Install dependencies
uv sync

# Run the complete pipeline (download, normalize, build DB)
uv run scripts/run_pipeline.py --last-days 30

# Launch interactive analytics notebook
uv run marimo edit notebooks/hl_research.py
```

**Or run steps individually:**
```bash
uv run scripts/download_data.py --last-days 30 --yes
uv run scripts/normalize_data.py
uv run src/query_data.py
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
# Check costs first
uv run scripts/estimate_download_cost.py --paths current

# Download recent data only (recommended - saves time and money)
uv run scripts/download_data.py --last-days 30

# Or download specific date range
uv run scripts/download_data.py --start-date 2025-11-01 --end-date 2025-11-15

# Or download all available data
uv run scripts/download_data.py --paths current
```

**Download Options:**
- `--last-days N` - Download only last N days (recommended for updates)
- `--start-date / --end-date` - Specific date range
- `--paths current` - Current format only (default, ~65 GB)
- `--paths all` - All formats including legacy (~109 GB)
- `--dry-run` - Preview download without downloading
- `--workers N` - Parallel workers for faster downloads (default: 10)

**Available Data:**
- **node_fills_by_block/** (~65 GB) - Current format (Jul 27 - present)
- **node_fills/** (~30 GB) - Legacy API format (May 25 - Jul 27)
- **node_trades/** (~14 GB) - Legacy alternative format (Mar 22 - Jun 21)

### 2. Normalize Data

```bash
uv run scripts/normalize_data.py
uv run scripts/validate_data.py  # Optional: verify integrity
```

**Normalized Data (~40-60 GB)** in `./data/processed/fills.parquet/`:
- Date-partitioned Parquet files
- Unified schema across all formats
- Overlap periods deduplicated (prefers newer format)
- Efficient columnar storage with Snappy compression

### 3. Build Database (Recommended)

```bash
uv run src/query_data.py
```

Creates `data/processed/fills.duckdb` with pre-aggregated tables for instant queries.

## Analytics

### Interactive Notebook

```bash
uv run marimo edit notebooks/hl_research.py
```

Features:
- Toggle data loading on/off
- DAU/MAU analysis with visualizations
- Volume distribution (all assets + filtered by coin)
- New user acquisition tracking
- Asset trading activity
- Summary reports

**Performance**: Instant load, <1s queries.

### Query Interface

```python
from src.query_data import HyperliquidAnalytics

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

### Analytics
| Operation | Time | Notes |
|-----------|------|-------|
| Notebook load | <1s | With persistent DuckDB |
| DAU query | ~50ms | Pre-aggregated tables |
| Volume buckets | ~200ms | Optimized queries |
| New users | ~20ms | Indexed lookups |

### Data Pipeline
| Operation | Time | Notes |
|-----------|------|-------|
| Download (30 days) | ~2-5 min | Parallel downloads (10 workers) |
| Download (all data) | ~15-20 min | ~109 GB at 10-20 MB/s |
| Normalization | ~10-20 min | Processes all 3 formats |

See [docs/PERFORMANCE_OPTIMIZATION.md](docs/PERFORMANCE_OPTIMIZATION.md) for details.

## Files Overview

### Scripts (`scripts/`)
- `download_data.py` - Download raw data from S3
- `normalize_data.py` - Convert to unified parquet format
- `validate_data.py` - Verify data integrity
- `check_data_availability.py` - Check what data exists
- `estimate_download_cost.py` - Estimate S3 costs

### Source (`src/`)
- `query_data.py` - Analytics interface with DuckDB optimization
- `schema.py` - Data schema definitions

### Notebooks (`notebooks/`)
- `hl_research.py` - Main analytics notebook
- `hl_user_study.py` - User study template

## Notes

- **Storage**: Full pipeline requires ~150-170 GB:
  - Raw data: ~104 GB (can be deleted after normalization)
  - Processed parquet: ~40-60 GB (can be deleted after building DuckDB)
  - DuckDB database: ~2-5 GB (required for analytics)
  - *Tip: After setup, delete `data/hyperliquid/` and `data/processed/fills.parquet/` to keep only the DuckDB file*
- **Memory**: 4-8 GB RAM recommended for analytics
- **Network**: S3 downloads are ~104 GB (egress charges apply)
- **Time**: Initial download ~30-60 min, normalization ~10-20 min

## License

MIT
