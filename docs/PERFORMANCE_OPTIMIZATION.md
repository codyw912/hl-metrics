# Performance Guide

## Overview

The analytics system uses persistent DuckDB with pre-aggregated tables for fast, interactive queries on large datasets.

## Architecture

### Persistent DuckDB Database

Instead of scanning parquet files on every query, we build a persistent database once:

```python
analytics = HyperliquidAnalytics(
    data_dir='./data/processed/fills.parquet',
    db_path='./data/processed/fills.duckdb',
    rebuild=False  # Set to True only when parquet data changes
)
```

The database contains:
- **`fills`**: Main table with properly typed columns (no per-query CASTs)
- **`daily_user_volume`**: User volumes per day/coin (for DAU, volume buckets)
- **`user_first_trade`**: First trade dates (for new user acquisition)
- **`daily_metrics`**: Daily aggregates (for quick DAU queries without scanning fills)

### DuckDB Optimizations

```python
self.conn.execute("PRAGMA threads=8;")  # Parallel execution
self.conn.execute("PRAGMA enable_object_cache=true;")  # Cache metadata
self.conn.execute("PRAGMA memory_limit='4GB';")  # Memory allocation
```

### Lazy Loading in Marimo

The notebook doesn't load data until you toggle it on, preserving reactivity:

```python
load_data_toggle = mo.ui.switch(value=True, label="Load Data")
```

## Usage

### First Time Setup

Build the database from processed parquet files:

```bash
uv run src/query_data.py
```

Or in the marimo notebook, toggle "Rebuild DB" to True once. This creates `data/processed/fills.duckdb`.

### Subsequent Runs

The notebook loads instantly from the persistent database.

### When to Rebuild

Rebuild the database when:
- New parquet data is available (downloaded new data)
- Schema changes
- Database file is corrupted

Toggle "Rebuild DB" in the notebook or run:
```python
analytics = HyperliquidAnalytics(rebuild=True)
```

## Performance Metrics

### Analytics Queries
- **Notebook load**: <1s (with persistent DuckDB)
- **DAU query**: ~50ms (pre-aggregated daily_metrics table)
- **Volume buckets**: ~200ms (aggregates from daily_user_volume)
- **New users**: ~20ms (indexed lookups on user_first_trade)
- **Coin stats**: ~80ms (aggregates from daily_user_volume)

### Data Pipeline
- **Download (30 days)**: ~2-5 min with parallel downloads
- **Download (all data)**: ~15-20 min (~109 GB at 10-20 MB/s)
- **Normalization**: ~10-20 min (processes all 3 formats)
- **DuckDB build**: ~5-10 min (creates pre-aggregated tables)

## Advanced Optimizations

### Partition Pruning

Use date filters with comparison operators for efficient partition pruning:

```python
# Good - prunes partitions efficiently
analytics.get_dau(start_date='2025-10-01', end_date='2025-11-15')

# Less optimal - requires full scan
df.filter(lambda row: row['date'] in date_list)
```

### Approximate Distinct Counts (Optional)

For faster interactive queries when exact counts aren't critical:

```sql
-- Faster approximate count
SELECT approx_count_distinct(user_address) FROM fills

-- vs exact count
SELECT COUNT(DISTINCT user_address) FROM fills
```

### Custom Pre-Aggregations

Add domain-specific pre-aggregations for frequently used filters:

```sql
-- Example: Pre-aggregate BTC/ETH metrics if queried often
CREATE TABLE btc_eth_daily AS
SELECT date, user_address, SUM(sz*px) AS volume
FROM fills
WHERE coin IN ('BTC', 'ETH') AND side = 'A'
GROUP BY date, user_address;
```

## Monitoring

### Check Database Size

```bash
ls -lh data/processed/fills.duckdb
```

Typical size: 2-5 GB depending on data volume.

### Memory Usage

Monitor query memory usage:

```python
# Check DuckDB memory setting
analytics.conn.execute("PRAGMA memory_limit;").fetchall()

# Adjust if needed
analytics.conn.execute("PRAGMA memory_limit='2GB';")
```

## Troubleshooting

### Database is Stale

If parquet data has been updated but database hasn't:

```python
# In Python
analytics = HyperliquidAnalytics(rebuild=True)

# Or in marimo notebook
# Toggle "Rebuild DB" to True
```

### Out of Memory Errors

Reduce memory limit in `src/query_data.py`:

```python
self.conn.execute("PRAGMA memory_limit='2GB';")
```

Or reduce the number of parallel threads:

```python
self.conn.execute("PRAGMA threads=4;")
```

### Slow Queries After Rebuild

Update table statistics for query optimization:

```python
analytics.conn.execute("ANALYZE;")
```

### Clear Query Cache

If experiencing memory pressure:

```python
analytics.clear_cache()
```

## Tips

1. **Storage savings**: After building DuckDB, delete parquet files to save ~40-60 GB (see README)
2. **Download efficiency**: Use `--last-days 30` to download only recent data
3. **Query efficiency**: Use the coin selector in the notebook rather than re-running queries manually
4. **Rebuild frequency**: Only rebuild when new data is added, not on every notebook run
