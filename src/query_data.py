#!/usr/bin/env python3
"""
DuckDB query interface for analyzing normalized Hyperliquid trade data.

This module provides high-level functions for common user analytics queries:
- Daily Active Users (DAU)
- Monthly Active Users (MAU)
- Volume buckets and user cohorts
- Individual user statistics

DuckDB can efficiently query the Parquet files without loading everything into RAM.
"""

import duckdb
from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl


class HyperliquidAnalytics:
    """Analytics interface for Hyperliquid trade data."""

    def __init__(self, data_dir: str = "./data/processed/fills.parquet"):
        """
        Initialize analytics interface.

        Args:
            data_dir: Path to the partitioned Parquet directory
        """
        self.data_dir = Path(data_dir)
        self.conn = duckdb.connect(":memory:")

        # Register the Parquet files as a table
        # DuckDB will scan partitions efficiently
        if self.data_dir.exists():
            self.conn.execute(f"""
                CREATE VIEW fills AS
                SELECT * FROM read_parquet('{self.data_dir}/**/*.parquet', hive_partitioning=true)
            """)
        else:
            print(f"Warning: Data directory {self.data_dir} does not exist.")
            print("Run normalize_data.py first to create the processed dataset.")

    def get_dau(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        coins: str | list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Get Daily Active Users (DAU) for a date range.

        Args:
            start_date: Start date (YYYY-MM-DD), defaults to earliest available
            end_date: End date (YYYY-MM-DD), defaults to latest available
            coins: Optional coin/trading pair or list of coins to filter by

        Returns:
            Polars DataFrame with columns: date, dau, total_volume, total_trades
        """
        filters = []
        if start_date and end_date:
            filters.append(f"date >= '{start_date}' AND date <= '{end_date}'")
        elif start_date:
            filters.append(f"date >= '{start_date}'")
        elif end_date:
            filters.append(f"date <= '{end_date}'")

        if coins:
            if isinstance(coins, str):
                filters.append(f"coin = '{coins}'")
            else:
                coin_list = "', '".join(coins)
                filters.append(f"coin IN ('{coin_list}')")

        date_filter = "WHERE " + " AND ".join(filters) if filters else ""

        query = f"""
            WITH all_users AS (
                SELECT DISTINCT date, user_address
                FROM fills
                {date_filter}
            ),
            side_a_metrics AS (
                SELECT
                    date,
                    SUM(CAST(sz AS DOUBLE) * CAST(px AS DOUBLE)) as total_volume,
                    COUNT(DISTINCT hash) as total_trades
                FROM fills
                WHERE side = 'A'
                {"" if not date_filter else "AND " + date_filter.replace("WHERE ", "")}
                GROUP BY date
            )
            SELECT
                all_users.date,
                COUNT(DISTINCT all_users.user_address) as dau,
                COALESCE(side_a_metrics.total_volume, 0) as total_volume,
                COALESCE(side_a_metrics.total_trades, 0) as total_trades
            FROM all_users
            LEFT JOIN side_a_metrics ON all_users.date = side_a_metrics.date
            GROUP BY all_users.date, side_a_metrics.total_volume, side_a_metrics.total_trades
            ORDER BY all_users.date
        """

        return self.conn.execute(query).pl()

    def get_mau(
        self, month: str | None = None, coins: str | list[str] | None = None
    ) -> pl.DataFrame:
        """
        Get Monthly Active Users (MAU).

        Args:
            month: Month in format YYYY-MM, or None for all months
            coins: Optional coin/trading pair or list of coins to filter by

        Returns:
            Polars DataFrame with columns: month, mau, total_volume, total_trades
        """
        filters = []
        if month:
            filters.append(f"strftime(date, '%Y-%m') = '{month}'")

        if coins:
            if isinstance(coins, str):
                filters.append(f"coin = '{coins}'")
            else:
                coin_list = "', '".join(coins)
                filters.append(f"coin IN ('{coin_list}')")

        month_filter = "WHERE " + " AND ".join(filters) if filters else ""

        query = f"""
            WITH all_users AS (
                SELECT DISTINCT
                    strftime(date, '%Y-%m') as month,
                    user_address
                FROM fills
                {month_filter}
            ),
            side_a_metrics AS (
                SELECT
                    strftime(date, '%Y-%m') as month,
                    SUM(CAST(sz AS DOUBLE) * CAST(px AS DOUBLE)) as total_volume,
                    COUNT(DISTINCT hash) as total_trades
                FROM fills
                WHERE side = 'A'
                {"" if not month_filter else "AND " + month_filter.replace("WHERE ", "")}
                GROUP BY month
            )
            SELECT
                all_users.month,
                COUNT(DISTINCT all_users.user_address) as mau,
                COALESCE(side_a_metrics.total_volume, 0) as total_volume,
                COALESCE(side_a_metrics.total_trades, 0) as total_trades
            FROM all_users
            LEFT JOIN side_a_metrics ON all_users.month = side_a_metrics.month
            GROUP BY all_users.month, side_a_metrics.total_volume, side_a_metrics.total_trades
            ORDER BY all_users.month
        """

        return self.conn.execute(query).pl()

    def get_volume_buckets(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        buckets: list[float] | None = None,
        coins: str | list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Get daily user distribution across volume buckets.

        For each day, calculates how many users fall into each volume bucket
        based on their trading volume on that specific day.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            buckets: List of volume thresholds (e.g., [100, 1000, 10000, 100000])
            coins: Optional coin/trading pair or list of coins to filter by

        Returns:
            Polars DataFrame with columns: date, volume_bucket, user_count, bucket_volume
        """
        if buckets is None:
            buckets = [100, 1000, 10000, 100000, 1000000]

        filters = []
        if start_date and end_date:
            filters.append(f"date >= '{start_date}' AND date <= '{end_date}'")
        elif start_date:
            filters.append(f"date >= '{start_date}'")
        elif end_date:
            filters.append(f"date <= '{end_date}'")

        if coins:
            if isinstance(coins, str):
                filters.append(f"coin = '{coins}'")
            else:
                coin_list = "', '".join(coins)
                filters.append(f"coin IN ('{coin_list}')")

        date_filter = "WHERE " + " AND ".join(filters) if filters else ""

        # Build CASE statement for buckets
        bucket_cases = []
        for i, threshold in enumerate(buckets):
            if i == 0:
                bucket_cases.append(
                    f"WHEN daily_volume < {threshold} THEN '< ${threshold:,.0f}'"
                )
            else:
                prev_threshold = buckets[i - 1]
                bucket_cases.append(
                    f"WHEN daily_volume >= {prev_threshold} AND daily_volume < {threshold} "
                    f"THEN '${prev_threshold:,.0f} - ${threshold:,.0f}'"
                )

        bucket_cases.append(f"ELSE '>= ${buckets[-1]:,.0f}'")
        bucket_case_statement = " ".join(bucket_cases)

        query = f"""
            WITH daily_user_volumes AS (
                SELECT
                    date,
                    user_address,
                    SUM(CAST(sz AS DOUBLE) * CAST(px AS DOUBLE)) as daily_volume
                FROM fills
                {date_filter}
                GROUP BY date, user_address
            )
            SELECT
                date,
                CASE
                    {bucket_case_statement}
                END as volume_bucket,
                COUNT(*) as user_count,
                SUM(daily_volume) as bucket_volume
            FROM daily_user_volumes
            GROUP BY date, volume_bucket
            ORDER BY date, MIN(daily_volume)
        """

        return self.conn.execute(query).pl()

    def get_user_stats(self, user_address: str) -> dict:
        """
        Get comprehensive statistics for a specific user.

        Args:
            user_address: User's address

        Returns:
            Dictionary with user statistics
        """
        query = f"""
            SELECT
                COUNT(*) as total_trades,
                COUNT(DISTINCT date) as active_days,
                SUM(CAST(sz AS DOUBLE) * CAST(px AS DOUBLE)) as total_volume,
                COUNT(DISTINCT coin) as unique_coins,
                MIN(time) as first_trade_time,
                MAX(time) as last_trade_time,
                SUM(CASE WHEN closed_pnl IS NOT NULL THEN CAST(closed_pnl AS DOUBLE) ELSE 0 END) as total_pnl,
                SUM(CASE WHEN fee IS NOT NULL THEN CAST(fee AS DOUBLE) ELSE 0 END) as total_fees
            FROM fills
            WHERE user_address = '{user_address}'
        """

        result = self.conn.execute(query).fetchone()

        if result is None:
            return None

        return {
            "user_address": user_address,
            "total_trades": result[0],
            "active_days": result[1],
            "total_volume": result[2],
            "unique_coins": result[3],
            "first_trade": datetime.fromtimestamp(result[4] / 1000)
            if result[4]
            else None,
            "last_trade": datetime.fromtimestamp(result[5] / 1000)
            if result[5]
            else None,
            "total_pnl": result[6],
            "total_fees": result[7],
        }

    def get_top_users_by_volume(
        self,
        limit: int = 100,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pl.DataFrame:
        """
        Get top users by trading volume.

        Args:
            limit: Number of top users to return
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Polars DataFrame with top users and their stats
        """
        date_filter = ""
        if start_date and end_date:
            date_filter = f"WHERE date >= '{start_date}' AND date <= '{end_date}'"
        elif start_date:
            date_filter = f"WHERE date >= '{start_date}'"
        elif end_date:
            date_filter = f"WHERE date <= '{end_date}'"

        query = f"""
            SELECT
                user_address,
                COUNT(*) as total_trades,
                SUM(CAST(sz AS DOUBLE) * CAST(px AS DOUBLE)) as total_volume,
                COUNT(DISTINCT date) as active_days,
                COUNT(DISTINCT coin) as unique_coins,
                SUM(CASE WHEN closed_pnl IS NOT NULL THEN CAST(closed_pnl AS DOUBLE) ELSE 0 END) as total_pnl,
                SUM(CASE WHEN fee IS NOT NULL THEN CAST(fee AS DOUBLE) ELSE 0 END) as total_fees
            FROM fills
            {date_filter}
            GROUP BY user_address
            ORDER BY total_volume DESC
            LIMIT {limit}
        """

        return self.conn.execute(query).pl()

    def get_coin_statistics(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> pl.DataFrame:
        """
        Get statistics by coin/trading pair.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Polars DataFrame with coin statistics
        """
        date_filter = ""
        if start_date and end_date:
            date_filter = f"WHERE date >= '{start_date}' AND date <= '{end_date}'"
        elif start_date:
            date_filter = f"WHERE date >= '{start_date}'"
        elif end_date:
            date_filter = f"WHERE date <= '{end_date}'"

        query = f"""
            SELECT
                coin,
                COUNT(DISTINCT hash) as total_trades,
                COUNT(DISTINCT user_address) as unique_traders,
                SUM(CAST(sz AS DOUBLE) * CAST(px AS DOUBLE)) as total_volume,
                AVG(CAST(px AS DOUBLE)) as avg_price,
                MIN(CAST(px AS DOUBLE)) as min_price,
                MAX(CAST(px AS DOUBLE)) as max_price
            FROM fills
            WHERE side = 'A'
            {"" if not date_filter else "AND " + date_filter.replace("WHERE ", "")}
            GROUP BY coin
            ORDER BY total_volume DESC
        """

        return self.conn.execute(query).pl()

    def get_daily_new_users(self) -> pl.DataFrame:
        """
        Get count of new users per day (users who made their first trade that day).

        Returns:
            Polars DataFrame with daily new user counts
        """
        query = """
            WITH user_first_trade AS (
                SELECT
                    user_address,
                    MIN(date) as first_trade_date
                FROM fills
                GROUP BY user_address
            )
            SELECT
                first_trade_date as date,
                COUNT(*) as new_users
            FROM user_first_trade
            GROUP BY first_trade_date
            ORDER BY first_trade_date
        """

        return self.conn.execute(query).pl()

    def execute_custom_query(self, query: str) -> pl.DataFrame:
        """
        Execute a custom SQL query against the fills table.

        Args:
            query: SQL query string

        Returns:
            Polars DataFrame with query results
        """
        return self.conn.execute(query).pl()

    def get_data_summary(self) -> dict:
        """Get high-level summary of the entire dataset."""
        query = """
            SELECT
                COUNT(*) as total_fills,
                COUNT(DISTINCT user_address) as unique_users,
                COUNT(DISTINCT coin) as unique_coins,
                COUNT(DISTINCT date) as total_days,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                SUM(CAST(sz AS DOUBLE) * CAST(px AS DOUBLE)) as total_volume,
                COUNT(DISTINCT hash) as total_trades
            FROM fills
            WHERE side = 'A'
        """

        result = self.conn.execute(query).fetchone()

        return {
            "total_fills": result[0],
            "unique_users": result[1],
            "unique_coins": result[2],
            "total_days": result[3],
            "earliest_date": result[4],
            "latest_date": result[5],
            "total_volume": result[6],
            "total_trades": result[7],
        }


def main():
    """Example usage of the analytics interface."""
    analytics = HyperliquidAnalytics()

    print("=" * 80)
    print("Hyperliquid Analytics Query Interface")
    print("=" * 80)
    print()

    # Get data summary
    print("Dataset Summary:")
    summary = analytics.get_data_summary()
    for key, value in summary.items():
        print(f"  {key}: {value:,}" if isinstance(value, int) else f"  {key}: {value}")
    print()

    # Get DAU for last 30 days
    print("Daily Active Users (last 30 days):")
    dau = analytics.get_dau()
    if len(dau) > 0:
        recent_dau = dau.tail(30)
        print(recent_dau)
        print()

    # Get MAU
    print("Monthly Active Users:")
    mau = analytics.get_mau()
    print(mau)
    print()

    # Get volume buckets
    print("User Volume Distribution:")
    buckets = analytics.get_volume_buckets()
    print(buckets)
    print()


if __name__ == "__main__":
    main()
