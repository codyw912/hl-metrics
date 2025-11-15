import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from query_data import HyperliquidAnalytics

    return (HyperliquidAnalytics,)


@app.cell
def _(HyperliquidAnalytics):
    data_dir = "./data/processed/fills.parquet"
    analytics = HyperliquidAnalytics(data_dir=data_dir)

    summary = analytics.get_data_summary()
    print("Dataset Summary:")
    print("=" * 60)
    for key, value in summary.items():
        if isinstance(value, (int, float)):
            print(f"{key:20s}: {value:,}")
        else:
            print(f"{key:20s}: {value}")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
