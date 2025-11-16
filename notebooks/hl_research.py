import marimo

__generated_with = "0.17.7"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Hyperliquid User Analytics
    """)
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    from datetime import datetime, timedelta
    from functools import lru_cache

    # Configure altair
    alt.data_transformers.enable("default")

    # Suppress warnings
    import warnings

    warnings.filterwarnings("ignore")
    return alt, datetime, lru_cache, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Data Loading Controls

    Toggle to load/unload data. Set **Rebuild DB** to True only when parquet files have changed.
    """)
    return


@app.cell
def _(mo):
    # UI controls for data loading
    load_data_toggle = mo.ui.switch(value=True, label="Load Data")
    rebuild_toggle = mo.ui.switch(
        value=False, label="Rebuild DB (only if parquet changed)"
    )

    mo.hstack([load_data_toggle, rebuild_toggle], justify="start")
    return load_data_toggle, rebuild_toggle


@app.cell
def _(load_data_toggle, lru_cache, mo, rebuild_toggle):
    # Lazy initialization with caching
    import sys
    from pathlib import Path

    # Detect project root (works whether run from root or notebooks/)
    _cwd = Path.cwd()
    if _cwd.name == "notebooks":
        _project_root = _cwd.parent
    else:
        _project_root = _cwd

    # Add src to path
    sys.path.insert(0, str(_project_root / "src"))
    from query_data import HyperliquidAnalytics

    @lru_cache(maxsize=1)
    def get_analytics(data_dir: str, db_path: str, rebuild: bool):
        """Cached analytics initialization."""
        return HyperliquidAnalytics(data_dir=data_dir, db_path=db_path, rebuild=rebuild)

    # Initialize only if toggle is on
    if load_data_toggle.value:
        data_dir = str(_project_root / "data/processed/fills.parquet")
        db_path = str(_project_root / "data/processed/fills.duckdb")
        analytics = get_analytics(data_dir, db_path, rebuild_toggle.value)
        summary = analytics.get_data_summary()

        mo.md(f"""
        ### ✅ Data Loaded

        - **Total Fills**: {summary["total_fills"]:,}
        - **Unique Users**: {summary["unique_users"]:,}
        - **Date Range**: {summary["earliest_date"]} to {summary["latest_date"]}
        - **Total Volume**: ${summary["total_volume"]:,.0f}
        """)
    else:
        analytics = None
        summary = None
        mo.md("### ⚠️ Data not loaded. Enable 'Load Data' toggle above.")
    return analytics, summary


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Daily Active Users (DAU)
    """)
    return


@app.cell
def _(alt, analytics, mo, pl):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        dau_df = analytics.get_dau()

        # DAU line chart
        _dau_chart = (
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
        _volume_chart = (
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
            alt.vconcat(_dau_chart, _volume_chart)
            .resolve_scale(y="independent")
            .properties(title="Hyperliquid Daily Metrics")
        )

        _combined.display()

        # Statistics
        print("\nDAU Statistics:")
        _dau_stats = dau_df.select(
            [
                pl.col("dau").mean().alias("Average DAU"),
                pl.col("dau").median().alias("Median DAU"),
                pl.col("dau").max().alias("Max DAU"),
                pl.col("dau").min().alias("Min DAU"),
            ]
        )
        for _col in _dau_stats.columns:
            _val = _dau_stats[_col][0]
            print(f"  {_col:20s}: {_val:,.0f}")

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
    """)
    return


@app.cell
def _(alt, analytics, mo):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        mau_df = analytics.get_mau()

        # Bar chart
        _mau_chart = (
            alt.Chart(mau_df)
            .mark_bar(color="#636EFA")
            .encode(
                x=alt.X("month:N", title="Month", sort=None),
                y=alt.Y("mau:Q", title="Active Users"),
                tooltip=["month:N", alt.Tooltip("mau:Q", format=",")],
            )
            .properties(width=900, height=400, title="Monthly Active Users")
        )

        _mau_chart.display()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. User Volume Distribution (All Assets)
    """)
    return


@app.cell
def _(alt, analytics, mo, pl):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        # Get daily user volumes for cumulative buckets
        _daily_volumes = analytics.conn.execute("""
            SELECT
                date,
                user_address,
                SUM(daily_volume) AS daily_volume
            FROM daily_user_volume
            GROUP BY date, user_address
        """).pl()

        # Create cumulative buckets: >1k, >10k, >100k
        _buckets = [(1000, "> $1,000"), (10000, "> $10,000"), (100000, "> $100,000")]

        # Count users per bucket per day
        _bucket_data = []
        for _threshold, _label in _buckets:
            _bucket_counts = (
                _daily_volumes.filter(pl.col("daily_volume") >= _threshold)
                .group_by("date")
                .agg(pl.count().alias("user_count"))
                .with_columns(pl.lit(_label).alias("volume_bucket"))
            )
            _bucket_data.append(_bucket_counts)

        buckets_df = pl.concat(_bucket_data)

        print(
            f"Total rows: {len(buckets_df)} ({buckets_df['date'].n_unique()} days × {buckets_df['volume_bucket'].n_unique()} buckets)"
        )
        print()

        # Define bucket order and colors
        _bucket_order = ["> $1,000", "> $10,000", "> $100,000"]
        _colors = ["#636EFA", "#00CC96", "#AB63FA"]

        # Line chart
        _chart = (
            alt.Chart(buckets_df)
            .mark_line(size=3)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("user_count:Q", title="Number of Users"),
                color=alt.Color(
                    "volume_bucket:N",
                    title="Daily Volume",
                    scale=alt.Scale(domain=_bucket_order, range=_colors),
                    sort=_bucket_order,
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
                title="Cumulative User Volume Distribution Over Time",
            )
            .interactive()
        )

        _chart.display()

        # Summary statistics
        print("\nDaily Average by Bucket:")
        for _label in _bucket_order:
            _avg = buckets_df.filter(pl.col("volume_bucket") == _label)[
                "user_count"
            ].mean()
            print(f"  {_label:20s}: {_avg:>10,.0f} users")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. User Volume Distribution (By Asset)

    Filter by specific coins to see volume distribution for particular trading pairs.
    """)
    return


@app.cell
def _(analytics, mo):
    # Get list of top coins by volume
    if analytics is not None:
        _top_coins = analytics.get_coin_statistics().head(20)
        _coin_options = _top_coins["coin"].to_list()

        coin_selector = mo.ui.multiselect(
            options=_coin_options, label="Select coins to analyze:"
        )
    else:
        coin_selector = None
        mo.md("_Enable data loading to select coins_")

    coin_selector
    return (coin_selector,)


@app.cell
def _(alt, analytics, coin_selector, mo, pl):
    if analytics is None:
        mo.md("_Enable data loading above_")
    elif len(coin_selector.value) == 0:
        mo.md("_Select at least one coin above_")
    else:
        # Get daily user volumes filtered by selected coins
        _selected_coins = coin_selector.value
        _coins_sql = "'" + "', '".join(_selected_coins) + "'"

        _daily_volumes_filtered = analytics.conn.execute(f"""
            SELECT
                date,
                user_address,
                SUM(daily_volume) AS daily_volume
            FROM daily_user_volume
            WHERE coin IN ({_coins_sql})
            GROUP BY date, user_address
        """).pl()

        # Create cumulative buckets: >1k, >10k, >100k
        _buckets_filtered = [
            (1000, "> $1,000"),
            (10000, "> $10,000"),
            (100000, "> $100,000"),
        ]

        # Count users per bucket per day
        _bucket_data_filtered = []
        for _threshold, _label in _buckets_filtered:
            _bucket_counts_filtered = (
                _daily_volumes_filtered.filter(pl.col("daily_volume") >= _threshold)
                .group_by("date")
                .agg(pl.count().alias("user_count"))
                .with_columns(pl.lit(_label).alias("volume_bucket"))
            )
            _bucket_data_filtered.append(_bucket_counts_filtered)

        buckets_by_coin_df = pl.concat(_bucket_data_filtered)

        _coin_list_str = ", ".join(_selected_coins)
        print(f"Analyzing: {_coin_list_str}")
        print(
            f"Total rows: {len(buckets_by_coin_df)} ({buckets_by_coin_df['date'].n_unique()} days × {buckets_by_coin_df['volume_bucket'].n_unique()} buckets)"
        )
        print()

        # Define bucket order and colors
        _bucket_order_filtered = ["> $1,000", "> $10,000", "> $100,000"]
        _colors_filtered = ["#636EFA", "#00CC96", "#AB63FA"]

        # Line chart
        _chart_filtered = (
            alt.Chart(buckets_by_coin_df)
            .mark_line(size=3)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("user_count:Q", title="Number of Users"),
                color=alt.Color(
                    "volume_bucket:N",
                    title="Daily Volume",
                    scale=alt.Scale(
                        domain=_bucket_order_filtered, range=_colors_filtered
                    ),
                    sort=_bucket_order_filtered,
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
                title=f"Cumulative User Volume Distribution: {_coin_list_str}",
            )
            .interactive()
        )

        _chart_filtered.display()

        # Summary statistics
        print("\nDaily Average by Bucket:")
        for _label in _bucket_order_filtered:
            _avg_filtered = buckets_by_coin_df.filter(
                pl.col("volume_bucket") == _label
            )["user_count"].mean()
            print(f"  {_label:20s}: {_avg_filtered:>10,.0f} users")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. New User Acquisition
    """)
    return


@app.cell
def _(alt, analytics, mo, pl):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        new_users_df = analytics.get_daily_new_users()

        # Add 7-day moving average
        _new_users_with_ma = new_users_df.with_columns(
            pl.col("new_users").rolling_mean(window_size=7, center=True).alias("ma_7")
        )

        # Area chart
        _area_chart = (
            alt.Chart(_new_users_with_ma)
            .mark_area(color="#EF553B", opacity=0.3)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("new_users:Q", title="New Users"),
                tooltip=["date:T", alt.Tooltip("new_users:Q", format=",")],
            )
        )

        # 7-day MA line
        _ma_chart = (
            alt.Chart(_new_users_with_ma)
            .mark_line(color="#000000", size=2, strokeDash=[5, 5])
            .encode(
                x="date:T",
                y=alt.Y("ma_7:Q", title="New Users"),
                tooltip=["date:T", alt.Tooltip("ma_7:Q", title="7-day MA", format=",")],
            )
        )

        _combined = (_area_chart + _ma_chart).properties(
            width=900, height=400, title="Daily New User Acquisition"
        )

        _combined.display()

        print("\nNew User Statistics:")
        _total_new = new_users_df["new_users"].sum()
        _avg_new = new_users_df["new_users"].mean()
        print(f"  Total new users: {_total_new:,}")
        print(f"  Average per day: {_avg_new:,.0f}")

        _max_row = new_users_df.filter(pl.col("new_users") == pl.col("new_users").max())
        print(f"  Peak day: {_max_row['date'][0]} ({_max_row['new_users'][0]:,} users)")
    return (new_users_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 6. Asset Trading Activity
    """)
    return


@app.cell
def _(alt, analytics, mo):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        all_coins = analytics.get_coin_statistics()
        coins_df = all_coins.head(20)

        # Horizontal bar chart
        _chart = (
            alt.Chart(coins_df)
            .mark_bar(color="#00CC96")
            .encode(
                x=alt.X("total_volume:Q", title="Total Volume ($)"),
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
            all_coins.head(10).select(
                ["coin", "total_volume", "unique_traders", "total_trades"]
            )
        )
    return (all_coins,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 7. User Cohorts & Behavior
    
    Deep dive into how users move between volume tiers and cohort dynamics.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 7.1 User Mix by Volume Bucket
    
    Composition of active users across exclusive volume buckets over time.
    """)
    return


@app.cell
def _(alt, analytics, mo, pl):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        user_buckets_df = analytics.get_user_daily_buckets()

        # Calculate shares
        _mix_df = user_buckets_df.with_columns(
            pl.col("user_count")
            / pl.col("user_count").sum().over("date").alias("share")
        )

        _bucket_order_mix = [
            "< $1,000",
            "$1,000 - $10,000",
            "$10,000 - $100,000",
            ">= $100,000",
        ]
        _colors_mix = ["#B3CDE3", "#6497B1", "#005B96", "#03396C"]

        # Normalized stacked area chart
        _mix_chart = (
            alt.Chart(_mix_df)
            .mark_area()
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y(
                    "share:Q",
                    stack="normalize",
                    title="User Share",
                    axis=alt.Axis(format="%"),
                ),
                color=alt.Color(
                    "volume_bucket:N",
                    scale=alt.Scale(domain=_bucket_order_mix, range=_colors_mix),
                    sort=_bucket_order_mix,
                    title="Volume Bucket",
                ),
                tooltip=[
                    "date:T",
                    "volume_bucket:N",
                    alt.Tooltip("share:Q", format=".1%"),
                    alt.Tooltip("user_count:Q", format=","),
                ],
            )
            .properties(
                width=1000,
                height=400,
                title="Daily User Mix by Volume Bucket (Normalized)",
            )
        )

        _mix_chart.display()

        # Summary stats
        print("\nAverage User Mix:")
        for _bucket in _bucket_order_mix:
            _avg_share = (
                _mix_df.filter(pl.col("volume_bucket") == _bucket)["share"].mean() * 100
            )
            print(f"  {_bucket:25s}: {_avg_share:>6.1f}%")
    return (user_buckets_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 7.2 Bucket Transition Matrix
    
    How users move between buckets from one day to the next (day-over-day transitions).
    """)
    return


@app.cell
def _(alt, analytics, mo, pl):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        transitions_df = analytics.get_bucket_transitions()

        _bucket_order_trans = [
            "< $1,000",
            "$1,000 - $10,000",
            "$10,000 - $100,000",
            ">= $100,000",
        ]

        # Heatmap
        _heat = (
            alt.Chart(transitions_df)
            .mark_rect()
            .encode(
                x=alt.X(
                    "bucket_t1:N", title="Next Day Bucket", sort=_bucket_order_trans
                ),
                y=alt.Y(
                    "bucket_t:N", title="Current Day Bucket", sort=_bucket_order_trans
                ),
                color=alt.Color(
                    "pct:Q", scale=alt.Scale(scheme="blues"), title="Probability"
                ),
                tooltip=[
                    "bucket_t:N",
                    "bucket_t1:N",
                    alt.Tooltip("pct:Q", format=".1%"),
                    alt.Tooltip("cnt:Q", format=","),
                ],
            )
            .properties(
                width=500,
                height=500,
                title="Day-over-Day Bucket Transition Probabilities",
            )
        )

        # Text overlay
        _text = (
            alt.Chart(transitions_df)
            .mark_text(baseline="middle", fontSize=14)
            .encode(
                x=alt.X("bucket_t1:N", sort=_bucket_order_trans),
                y=alt.Y("bucket_t:N", sort=_bucket_order_trans),
                text=alt.Text("pct:Q", format=".0%"),
                color=alt.condition(
                    alt.datum.pct > 0.5, alt.value("white"), alt.value("black")
                ),
            )
        )

        (_heat + _text).display()

        # Calculate retention rates
        print("\nSame-Bucket Retention (day-over-day):")
        for _bucket in _bucket_order_trans:
            _same_pct = transitions_df.filter(
                (pl.col("bucket_t") == _bucket) & (pl.col("bucket_t1") == _bucket)
            )["pct"]
            if len(_same_pct) > 0:
                print(f"  {_bucket:25s}: {_same_pct[0] * 100:>6.1f}%")
    return (transitions_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 7.3 Upgrade/Downgrade Dynamics
    
    What fraction of users from each starting bucket upgrade or downgrade within 7 and 30 days?
    """)
    return


@app.cell
def _(alt, analytics, mo, pl):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        mobility_df = analytics.get_bucket_mobility()

        # Map rank to labels
        _rank_labels = {
            0: "< $1,000",
            1: "$1,000 - $10,000",
            2: "$10,000 - $100,000",
            3: ">= $100,000",
        }
        _mobility_labeled = mobility_df.with_columns(
            pl.col("start_rank").replace(_rank_labels).alias("bucket")
        )

        # Melt for visualization
        _mobility_melted = _mobility_labeled.melt(
            id_vars=["bucket", "horizon", "cohort_size"],
            value_vars=["upgrade_rate", "downgrade_rate"],
            variable_name="type",
            value_name="rate",
        )

        # Grouped bar chart
        _mobility_chart = (
            alt.Chart(_mobility_melted)
            .mark_bar()
            .encode(
                x=alt.X("bucket:N", title="Start Bucket"),
                y=alt.Y("rate:Q", title="Rate", axis=alt.Axis(format="%")),
                column=alt.Column("horizon:N", title="Horizon (days)"),
                color=alt.Color(
                    "type:N",
                    scale=alt.Scale(
                        domain=["upgrade_rate", "downgrade_rate"],
                        range=["#2CA02C", "#D62728"],
                    ),
                    legend=alt.Legend(
                        title="Movement",
                        labelExpr="datum.value == 'upgrade_rate' ? 'Upgrade' : 'Downgrade'",
                    ),
                ),
                tooltip=[
                    "bucket:N",
                    "horizon:N",
                    "type:N",
                    alt.Tooltip("rate:Q", format=".1%"),
                    alt.Tooltip("cohort_size:Q", format=","),
                ],
            )
            .properties(
                width=220, height=300, title="Upgrade/Downgrade Rates by Start Bucket"
            )
        )

        _mobility_chart.display()

        # Print summary
        print("\nMobility Rates by Start Bucket:")
        for _rank, _label in _rank_labels.items():
            _mob_data = mobility_df.filter(pl.col("start_rank") == _rank)
            if len(_mob_data) > 0:
                print(f"\n  {_label}:")
                for _row in _mob_data.iter_rows(named=True):
                    print(
                        f"    {_row['horizon']:2d} days - Upgrade: {_row['upgrade_rate'] * 100:5.1f}%  Downgrade: {_row['downgrade_rate'] * 100:5.1f}%  (cohort: {_row['cohort_size']:,})"
                    )
    return (mobility_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 8. Summary Report
    """)
    return


@app.cell
def _(all_coins, analytics, datetime, dau_df, mo, new_users_df, summary):
    if analytics is None:
        mo.md("_Enable data loading above_")
    else:
        _report = f"""
    HYPERLIQUID ANALYTICS SUMMARY REPORT
    Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    {"=" * 60}

    OVERALL METRICS
      Total Fills:       {summary["total_fills"]:,}
      Unique Users:      {summary["unique_users"]:,}
      Unique Assets:     {summary["unique_coins"]:,}
      Date Range:        {summary["earliest_date"]} to {summary["latest_date"]}
      Total Days:        {summary["total_days"]}
      Total Volume:      ${summary["total_volume"]:,.0f}

    DAILY METRICS
      Average DAU:       {dau_df["dau"].mean():,.0f}
      Peak DAU:          {dau_df["dau"].max():,}
      Avg Daily Volume:  ${dau_df["total_volume"].mean():,.0f}
      Peak Daily Volume: ${dau_df["total_volume"].max():,.0f}

    USER ACQUISITION
      Total New Users:   {new_users_df["new_users"].sum():,}
      Avg New Users/Day: {new_users_df["new_users"].mean():,.0f}
      Peak Acquisition:  {new_users_df["new_users"].max():,}

    TOP ASSETS (by volume)
    """

        for _i, _row in enumerate(all_coins.head(10).iter_rows(named=True), 1):
            _report += f"  {_i:2d}. {_row['coin']:15s} ${_row['total_volume']:>15,.0f}  ({_row['unique_traders']:,} traders)\n"

        print(_report)
    return


if __name__ == "__main__":
    app.run()
