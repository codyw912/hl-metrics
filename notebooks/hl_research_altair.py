import marimo

__generated_with = "0.17.7"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Hyperliquid User Analytics

    Analysis of user activity, growth, and trading behavior on Hyperliquid exchange.

    ## Metrics Covered:
    - Daily Active Users (DAU)
    - Monthly Active Users (MAU)
    - User acquisition and retention
    - Volume distribution and user cohorts
    - Trading activity trends
    """)
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    from datetime import datetime, timedelta
    import sys

    sys.path.insert(0, "../src")
    from query_data import HyperliquidAnalytics

    # Configure altair for better performance with large datasets
    alt.data_transformers.enable("default")

    # Suppress warnings
    import warnings

    warnings.filterwarnings("ignore")
    return HyperliquidAnalytics, alt, datetime, mo, pl


@app.cell
def _(HyperliquidAnalytics):
    # Initialize analytics interface
    data_dir = "../data/processed/fills.parquet"
    analytics = HyperliquidAnalytics(data_dir=data_dir)
    summary = analytics.get_data_summary()
    print("Dataset Summary:")
    print("=" * 60)
    for key, value in summary.items():
        if isinstance(value, (int, float)):
            print(f"{key:20s}: {value:,}")
        else:
            print(f"{key:20s}: {value}")
    return analytics, summary


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Daily Active Users (DAU)

    Track the number of unique users trading each day.
    """)
    return


@app.cell
def _(alt, analytics, pl):
    dau_df = analytics.get_dau()

    # DAU line chart
    dau_chart = (
        alt.Chart(dau_df)
        .mark_line(color="#636EFA", size=2)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("dau:Q", title="Active Users"),
            tooltip=["date:T", alt.Tooltip("dau:Q", format=",")],
        )
        .properties(width=900, height=350, title="Daily Active Users")
    )

    # Volume line chart
    volume_chart = (
        alt.Chart(dau_df)
        .mark_line(color="#00CC96", size=2)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("total_volume:Q", title="Volume ($)"),
            tooltip=["date:T", alt.Tooltip("total_volume:Q", format="$,.0f")],
        )
        .properties(width=900, height=350, title="Daily Trading Volume")
    )

    # Combine charts
    _combined = (
        alt.vconcat(dau_chart, volume_chart)
        .resolve_scale(y="independent")
        .properties(title="Hyperliquid Daily Metrics")
    )

    _combined.display()

    # Statistics
    print("\nDAU Statistics:")
    dau_stats = dau_df.select(
        [
            pl.col("dau").mean().alias("Average DAU"),
            pl.col("dau").median().alias("Median DAU"),
            pl.col("dau").max().alias("Max DAU"),
            pl.col("dau").min().alias("Min DAU"),
        ]
    )
    for col in dau_stats.columns:
        val = dau_stats[col][0]
        print(f"  {col:20s}: {val:,.0f}")

    # Find max/min dates
    _max_row = dau_df.filter(pl.col("dau") == pl.col("dau").max())
    _min_row = dau_df.filter(pl.col("dau") == pl.col("dau").min())
    print(f"  Peak DAU date:      {_max_row['date'][0]} ({_max_row['dau'][0]:,})")
    print(f"  Lowest DAU date:    {_min_row['date'][0]} ({_min_row['dau'][0]:,})")

    return (dau_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Monthly Active Users (MAU)

    Track unique users per month and growth trends.
    """)
    return


@app.cell
def _(alt, analytics, pl):
    mau_df = analytics.get_mau()

    # Bar chart
    mau_chart = (
        alt.Chart(mau_df)
        .mark_bar(color="#636EFA")
        .encode(
            x=alt.X("month:N", title="Month", sort=None),
            y=alt.Y("mau:Q", title="Active Users"),
            tooltip=["month:N", alt.Tooltip("mau:Q", format=",")],
        )
        .properties(width=900, height=400, title="Monthly Active Users")
    )

    mau_chart.display()

    # Calculate and display month-over-month growth
    if len(mau_df) > 1:
        mau_with_growth = mau_df.with_columns(
            pl.col("mau").pct_change().mul(100).alias("mom_growth")
        )
        print("\nMonth-over-Month Growth:")
        for _row in mau_with_growth.iter_rows(named=True):
            if _row["mom_growth"] is not None:
                print(f"  {_row['month']}: {_row['mom_growth']:+.1f}%")
            else:
                print(f"  {_row['month']}: (baseline)")

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. User Volume Distribution (All Assets)

    Segment users into volume buckets to understand the distribution of trading activity.
    """)
    return


@app.cell
def _(alt, analytics, pl):
    buckets_df = analytics.get_volume_buckets(
        buckets=[100, 1000, 10000, 100000, 1000000, 10000000]
    )

    print(
        f"Total rows: {len(buckets_df)} ({buckets_df['date'].n_unique()} days × {buckets_df['volume_bucket'].n_unique()} buckets)"
    )
    print(f"Buckets found: {sorted(buckets_df['volume_bucket'].unique())}")
    print()

    # Define bucket order
    _bucket_order = [
        "< $100",
        "$100 - $1,000",
        "$1,000 - $10,000",
        "$10,000 - $100,000",
        "$100,000 - $1,000,000",
        "$1,000,000 - $10,000,000",
        ">= $10,000,000",
    ]
    _colors = [
        "#EF553B",
        "#FFA15A",
        "#FEB74C",
        "#00CC96",
        "#AB63FA",
        "#636EFA",
        "#FF6B9D",
    ]

    # Line chart with one line per bucket
    _chart = (
        alt.Chart(buckets_df)
        .mark_line(size=2)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("user_count:Q", title="Number of Users"),
            color=alt.Color(
                "volume_bucket:N",
                title="Volume Bucket",
                scale=alt.Scale(domain=_bucket_order, range=_colors),
                sort=_bucket_order,
            ),
            tooltip=[
                "date:T",
                "volume_bucket:N",
                alt.Tooltip("user_count:Q", format=","),
            ],
        )
        .properties(width=1000, height=500, title="User Volume Distribution Over Time")
        .interactive()
    )

    _chart.display()

    # Summary statistics
    print("\nDaily Average by Bucket:")
    _avg_by_bucket = (
        buckets_df.group_by("volume_bucket")
        .agg(pl.col("user_count").mean())
        .sort_by(
            pl.col("volume_bucket").map_elements(
                lambda x: _bucket_order.index(x) if x in _bucket_order else 99
            )
        )
    )
    for _row in _avg_by_bucket.iter_rows(named=True):
        print(f"  {_row['volume_bucket']:30s}: {_row['user_count']:>10,.0f} users")

    _total_avg = (
        buckets_df.group_by("date")
        .agg(pl.col("user_count").sum())
        .select(pl.col("user_count").mean())
    )
    print(f"\nTotal daily average: {_total_avg['user_count'][0]:,.0f} users")

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. New User Acquisition

    Track how many new users join the platform each day.
    """)
    return


@app.cell
def _(alt, analytics, pl):
    new_users_df = analytics.get_daily_new_users()

    # Add 7-day moving average
    new_users_with_ma = new_users_df.with_columns(
        pl.col("new_users").rolling_mean(window_size=7, center=True).alias("ma_7")
    )

    # New users area chart
    area_chart = (
        alt.Chart(new_users_with_ma)
        .mark_area(color="#EF553B", opacity=0.3)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("new_users:Q", title="New Users"),
            tooltip=["date:T", alt.Tooltip("new_users:Q", format=",")],
        )
    )

    # 7-day MA line
    ma_chart = (
        alt.Chart(new_users_with_ma)
        .mark_line(color="#000000", size=2, strokeDash=[5, 5])
        .encode(
            x="date:T",
            y=alt.Y("ma_7:Q", title="New Users"),
            tooltip=["date:T", alt.Tooltip("ma_7:Q", title="7-day MA", format=",")],
        )
    )

    _combined = (area_chart + ma_chart).properties(
        width=900, height=400, title="Daily New User Acquisition"
    )

    _combined.display()

    print("\nNew User Statistics:")
    total_new = new_users_df["new_users"].sum()
    avg_new = new_users_df["new_users"].mean()
    print(f"  Total new users: {total_new:,}")
    print(f"  Average per day: {avg_new:,.0f}")

    _max_row = new_users_df.filter(pl.col("new_users") == pl.col("new_users").max())
    print(f"  Peak day: {_max_row['date'][0]} ({_max_row['new_users'][0]:,} users)")

    return (new_users_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Top Traders Analysis

    Identify and analyze the most active traders by volume.
    """)
    return


@app.cell
def _(alt, analytics, pl, summary):
    top_users_df = analytics.get_top_users_by_volume(limit=100)

    # Add rank column
    top_users_ranked = top_users_df.with_columns(
        pl.int_range(1, len(top_users_df) + 1).alias("rank")
    )

    print("Top 20 Users by Trading Volume:")
    print("=" * 100)
    display_df = top_users_ranked.head(20).select(
        ["rank", "user_address", "total_volume"]
    )
    display_df = display_df.with_columns(
        pl.col("user_address")
        .str.slice(0, 10)
        .str.concat("...")
        .alias("user_address_short")
    )
    for _row in display_df.iter_rows(named=True):
        print(
            f"  {_row['rank']:3d}. {_row['user_address_short']:15s} ${_row['total_volume']:>15,.0f}"
        )

    # Log-scale bar chart
    _chart = (
        alt.Chart(top_users_ranked)
        .mark_bar(color="#AB63FA")
        .encode(
            x=alt.X("rank:Q", title="User Rank"),
            y=alt.Y(
                "total_volume:Q", title="Total Volume ($)", scale=alt.Scale(type="log")
            ),
            tooltip=["rank:Q", alt.Tooltip("total_volume:Q", format="$,.0f")],
        )
        .properties(
            width=900, height=400, title="Trading Volume Distribution - Top 100 Users"
        )
    )

    _chart.display()

    # Concentration metrics
    _total_volume = summary["total_volume"]
    _top_10_volume = top_users_ranked.head(10)["total_volume"].sum()
    _top_100_volume = top_users_ranked["total_volume"].sum()

    print("\nVolume Concentration:")
    print(
        f"  Top 10 users:  {_top_10_volume / _total_volume * 100:.1f}% of total volume"
    )
    print(
        f"  Top 100 users: {_top_100_volume / _total_volume * 100:.1f}% of total volume"
    )

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. Asset Trading Activity

    Analyze which assets are most actively traded.
    """)
    return


@app.cell
def _(alt, analytics):
    all_coins = analytics.get_coin_statistics()
    coins_df = all_coins.head(20)

    # Horizontal bar chart
    _chart = (
        alt.Chart(coins_df)
        .mark_bar(color="#00CC96")
        .encode(
            x=alt.X("total_volume:Q", title="Total Volume ($)", format="$,.0f"),
            y=alt.Y("coin:N", title="Asset", sort="-x"),
            tooltip=[
                "coin:N",
                alt.Tooltip("total_volume:Q", format="$,.0f"),
                alt.Tooltip("unique_traders:Q", title="Traders", format=","),
            ],
        )
        .properties(width=800, height=450, title="Top 20 Assets by Trading Volume")
    )

    _chart.display()

    # Statistics
    print(f"\nTotal unique assets traded: {len(all_coins)}")
    print("\nTop 10 Assets:")
    print(
        all_coins.head(10)
        .select(["coin", "total_volume", "unique_traders", "total_trades"])
        .to_string()
    )

    return (all_coins,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 7. Custom Analysis

    Run custom SQL queries for specific analyses.
    """)
    return


@app.cell
def _(analytics):
    # Example: User retention - users who traded in both first and last week
    query = "\nWITH first_week AS (\n    SELECT DISTINCT user_address\n    FROM fills\n    WHERE date <= (SELECT MIN(date) + INTERVAL 7 DAY FROM fills)\n),\nlast_week AS (\n    SELECT DISTINCT user_address\n    FROM fills\n    WHERE date >= (SELECT MAX(date) - INTERVAL 7 DAY FROM fills)\n)\nSELECT\n    (SELECT COUNT(*) FROM first_week) as first_week_users,\n    (SELECT COUNT(*) FROM last_week) as last_week_users,\n    (SELECT COUNT(*) FROM first_week INNER JOIN last_week USING(user_address)) as retained_users\n"
    retention = analytics.execute_custom_query(query)

    if len(retention) > 0:
        _row = retention.row(0, named=True)
        _retention_rate = (
            _row["retained_users"] / _row["first_week_users"] * 100
            if _row["first_week_users"] > 0
            else 0
        )
        print("User Retention Analysis:")
        print(f"  First week users: {_row['first_week_users']:,}")
        print(f"  Last week users:  {_row['last_week_users']:,}")
        print(f"  Retained users:   {_row['retained_users']:,}")
        print(f"  Retention rate:   {_retention_rate:.1f}%")

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 8. Export Summary Report
    """)
    return


@app.cell
def _(all_coins, datetime, dau_df, new_users_df, summary):
    _report = f"\nHYPERLIQUID ANALYTICS SUMMARY REPORT\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 60}\n\nOVERALL METRICS\n  Total Fills:       {summary['total_fills']:,}\n  Unique Users:      {summary['unique_users']:,}\n  Unique Assets:     {summary['unique_coins']:,}\n  Date Range:        {summary['earliest_date']} to {summary['latest_date']}\n  Total Days:        {summary['total_days']}\n  Total Volume:      ${summary['total_volume']:,.0f}\n\nDAILY METRICS\n  Average DAU:       {dau_df['dau'].mean():,.0f}\n  Peak DAU:          {dau_df['dau'].max():,}\n  Avg Daily Volume:  ${dau_df['total_volume'].mean():,.0f}\n  Peak Daily Volume: ${dau_df['total_volume'].max():,.0f}\n\nUSER ACQUISITION\n  Total New Users:   {new_users_df['new_users'].sum():,}\n  Avg New Users/Day: {new_users_df['new_users'].mean():,.0f}\n  Peak Acquisition:  {new_users_df['new_users'].max():,}\n\nTOP ASSETS (by volume)\n"

    for _i, _row in enumerate(all_coins.head(10).iter_rows(named=True), 1):
        _report += f"  {_i:2d}. {_row['coin']:15s} ${_row['total_volume']:>15,.0f}  ({_row['unique_traders']:,} traders)\n"

    print(_report)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 9. User Volume Distribution (BTC & ETH only)

    Segment users into volume buckets for major assets.
    """)
    return


@app.cell
def _(alt, analytics, pl):
    btc_eth_buckets_df = analytics.get_volume_buckets(
        buckets=[1000, 10000, 100000], coins=["BTC", "ETH"]
    )

    print(
        f"Total rows: {len(btc_eth_buckets_df)} ({btc_eth_buckets_df['date'].n_unique()} days × {btc_eth_buckets_df['volume_bucket'].n_unique()} buckets)"
    )
    print(f"Buckets found: {sorted(btc_eth_buckets_df['volume_bucket'].unique())}")
    print()

    # Stacked area chart
    _chart = (
        alt.Chart(btc_eth_buckets_df)
        .mark_area()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("user_count:Q", title="Number of Users", stack="zero"),
            color=alt.Color(
                "volume_bucket:N",
                title="Volume Bucket",
                scale=alt.Scale(
                    domain=["$1,000 - $10,000", "$10,000 - $100,000", ">= $100,000"],
                    range=["#FFA15A", "#00CC96", "#636EFA"],
                ),
                sort=["$1,000 - $10,000", "$10,000 - $100,000", ">= $100,000"],
            ),
            tooltip=[
                "date:T",
                "volume_bucket:N",
                alt.Tooltip("user_count:Q", format=","),
            ],
        )
        .properties(
            width=1000,
            height=500,
            title="User Volume Distribution: Stacked Area Chart (BTC & ETH only)",
        )
        .interactive()
    )

    _chart.display()

    # Summary statistics
    print("\nDaily Average by Bucket:")
    _bucket_order = ["$1,000 - $10,000", "$10,000 - $100,000", ">= $100,000"]
    _avg_by_bucket = (
        btc_eth_buckets_df.group_by("volume_bucket")
        .agg(pl.col("user_count").mean())
        .sort_by(
            pl.col("volume_bucket").map_elements(
                lambda x: _bucket_order.index(x) if x in _bucket_order else 99
            )
        )
    )
    for _row in _avg_by_bucket.iter_rows(named=True):
        print(f"  {_row['volume_bucket']:30s}: {_row['user_count']:>10,.0f} users")

    _total_avg = (
        btc_eth_buckets_df.group_by("date")
        .agg(pl.col("user_count").sum())
        .select(pl.col("user_count").mean())
    )
    print(f"\nTotal daily average: {_total_avg['user_count'][0]:,.0f} users")
    return


if __name__ == "__main__":
    app.run()
