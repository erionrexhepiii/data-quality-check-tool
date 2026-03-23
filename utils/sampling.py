"""Intelligent data sampling for large tables."""

from __future__ import annotations

from typing import Optional

import pandas as pd


def smart_sample(
    df: pd.DataFrame,
    max_rows: int = 100_000,
    random_state: int = 42,
) -> tuple[pd.DataFrame, bool]:
    """Return a sample of the DataFrame if it exceeds max_rows.

    Returns:
        Tuple of (sampled_or_original_df, was_sampled: bool)
    """
    if len(df) <= max_rows:
        return df, False

    sampled = df.sample(n=max_rows, random_state=random_state)
    return sampled, True


def build_sample_query(
    table: str,
    schema: Optional[str],
    limit: int,
) -> str:
    """Build a SQL query that samples rows from a table.

    Uses TABLESAMPLE where possible, falls back to LIMIT.
    """
    qualified = f"{schema}.{table}" if schema else table
    return f"SELECT * FROM {qualified} LIMIT {limit}"
