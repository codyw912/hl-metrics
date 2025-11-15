# Project Structure

```
hl-metrics/
├── README.md                      # Main project documentation
├── pyproject.toml                 # Python project configuration
├── uv.lock                        # Dependency lock file
│
├── docs/                          # Documentation
│   ├── PERFORMANCE_OPTIMIZATION.md
│   └── VOLUME_CALCULATION_NOTES.md
│
├── scripts/                       # Data pipeline scripts
│   ├── download_data.py          # Download raw data from Hyperliquid
│   ├── normalize_data.py         # Convert raw data to normalized parquet
│   ├── validate_data.py          # Validate data integrity
│   ├── check_data_availability.py # Check what data exists
│   └── estimate_download_cost.py  # Estimate download costs
│
├── src/                          # Source code
│   ├── __init__.py
│   ├── query_data.py             # Original analytics interface
│   ├── query_data_optimized.py   # Optimized analytics with DuckDB
│   └── schema.py                 # Data schema definitions
│
├── notebooks/                    # Analysis notebooks
│   ├── hl_research_optimized.py  # Main marimo notebook (optimized)
│   ├── hl_research_altair.py     # Original marimo notebook
│   └── hl_user_study.py          # User study notebook
│
├── data/                         # Data directory (gitignored)
│   ├── hyperliquid/              # Raw downloaded data
│   └── processed/                # Processed parquet files
│
└── tests/                        # Tests
    └── test_pipeline.py
```

## Key Files

- **Primary Notebook**: `notebooks/hl_research_optimized.py` - Use this for analytics
- **Data Pipeline**: Run scripts in `scripts/` directory in order
- **Query Interface**: Import from `src/query_data_optimized.py`
