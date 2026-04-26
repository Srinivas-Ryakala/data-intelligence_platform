"""
Trend report — queries DQ_SCORE_SUMMARY across multiple runs
and shows quality score trend per asset over time.
Can export to Excel using pandas.
"""

import sys
import os
import logging

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from db.connection import get_connection

logger = logging.getLogger(__name__)


def get_trend_data(last_n_runs: int = 30) -> pd.DataFrame:
    """
    Query DQ_SCORE_SUMMARY for the last N runs and return as a DataFrame.
    Shows quality score trend per asset per dimension over time.

    Args:
        last_n_runs: Number of most recent runs to include (default: 30).

    Returns:
        pd.DataFrame: Trend data with columns for run, asset, dimension, score.
    """
    try:
        conn = get_connection()

        query = f"""
            SELECT
                s.dq_run_id,
                r.run_name,
                r.started_at,
                r.run_status,
                a.asset_name,
                a.qualified_name,
                s.score_level,
                s.rule_dimension,
                s.score_value,
                s.total_rules,
                s.passed_rules,
                s.failed_rules,
                s.warned_rules,
                s.summary_status
            FROM DQ_SCORE_SUMMARY s
            JOIN DQ_RUN r ON s.dq_run_id = r.dq_run_id
            LEFT JOIN DATA_ASSET a ON s.asset_id = a.asset_id
            WHERE s.dq_run_id IN (
                SELECT TOP {last_n_runs} dq_run_id
                FROM DQ_RUN
                ORDER BY started_at DESC
            )
            ORDER BY r.started_at, a.asset_name, s.rule_dimension
        """

        df = pd.read_sql(query, conn)
        conn.close()

        logger.info(f"Fetched {len(df)} trend rows across {last_n_runs} runs.")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch trend data: {e}")
        return pd.DataFrame()


def print_trend_report(last_n_runs: int = 30) -> None:
    """
    Print a formatted trend report to the console.

    Args:
        last_n_runs: Number of most recent runs to include.

    Returns:
        None
    """
    df = get_trend_data(last_n_runs)

    if df.empty:
        print("No trend data available.")
        return

    print("=" * 80)
    print(f"DQ QUALITY TREND REPORT — Last {last_n_runs} Runs")
    print("=" * 80)

    # ── Summary by asset ──
    if "asset_name" in df.columns and "score_value" in df.columns:
        asset_avg = (
            df[df["score_level"] == "TABLE"]
            .groupby("asset_name")["score_value"]
            .agg(["mean", "min", "max", "count"])
            .round(4)
        )

        print("\nAverage Quality Score by Asset:")
        print("-" * 60)
        print(f"{'Asset':<30} {'Avg':>8} {'Min':>8} {'Max':>8} {'Runs':>6}")
        print("-" * 60)

        for asset, row in asset_avg.iterrows():
            print(
                f"{str(asset):<30} {row['mean']:>8.4f} {row['min']:>8.4f} "
                f"{row['max']:>8.4f} {int(row['count']):>6}"
            )

    # ── Summary by dimension ──
    if "rule_dimension" in df.columns and "score_value" in df.columns:
        dim_avg = (
            df[df["score_level"] == "PIPELINE"]
            .groupby("rule_dimension")["score_value"]
            .agg(["mean", "min", "max"])
            .round(4)
        )

        print("\nAverage Quality Score by Dimension (Pipeline-Level):")
        print("-" * 60)
        print(f"{'Dimension':<30} {'Avg':>8} {'Min':>8} {'Max':>8}")
        print("-" * 60)

        for dim, row in dim_avg.iterrows():
            print(
                f"{str(dim):<30} {row['mean']:>8.4f} {row['min']:>8.4f} "
                f"{row['max']:>8.4f}"
            )

    print("\n" + "=" * 80)


def export_to_excel(
    last_n_runs: int = 30,
    output_path: str = "dq_trend_report.xlsx"
) -> None:
    """
    Export trend data to an Excel file using pandas.

    Args:
        last_n_runs: Number of runs to include.
        output_path: File path for the Excel output.

    Returns:
        None
    """
    df = get_trend_data(last_n_runs)

    if df.empty:
        print("No data to export.")
        return

    try:
        df.to_excel(output_path, index=False, sheet_name="DQ Trend")
        logger.info(f"Trend report exported to {output_path}.")
        print(f"Trend report exported to: {output_path}")
    except Exception as e:
        logger.error(f"Failed to export trend report: {e}")
        print(f"Export failed: {e}")


if __name__ == "__main__":
    from configs.logging_config import setup_logging
    setup_logging()

    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    print_trend_report(runs)

    # Optional: export
    if "--export" in sys.argv:
        export_to_excel(runs)
