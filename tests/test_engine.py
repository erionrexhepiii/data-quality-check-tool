"""Tests for the check engine."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest

from core.engine import CheckEngine
from core.models import CheckConfig, CheckSuite, CheckStatus


@pytest.fixture
def engine():
    return CheckEngine()


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", None, "Charlie", "", "Eve"],
        "email": ["a@b.com", "bad", "c@d.com", "e@f.com", "g@h.com"],
        "value": [10, 20, -5, 200, 30],
    })


class TestCheckEngine:
    def test_run_single_check(self, engine, sample_df):
        config = CheckConfig(
            check_type="null_check",
            table="test_table",
            columns=["name"],
        )
        results = engine.run_check(sample_df, config)
        assert len(results) > 0
        assert all(r.check_type == "null_check" for r in results)

    def test_run_suite(self, engine, sample_df):
        suite = CheckSuite(
            name="test_suite",
            connection_name="test_conn",
            checks=[
                CheckConfig(check_type="null_check", table="test"),
                CheckConfig(check_type="duplicate_check", table="test"),
            ],
        )
        summary = engine.run_suite(sample_df, suite)
        assert summary.total_checks > 0
        assert summary.started_at is not None
        assert summary.completed_at is not None
        # All results share the same run_id
        run_ids = {r.run_id for r in summary.results}
        assert len(run_ids) == 1

    def test_run_quick_check(self, engine, sample_df):
        summary = engine.run_quick_check(
            sample_df, "test_table",
            check_types=["null_check", "duplicate_check"],
        )
        assert summary.total_checks > 0
        assert 0 <= summary.quality_score <= 100

    def test_disabled_check_skipped(self, engine, sample_df):
        suite = CheckSuite(
            name="test_suite",
            connection_name="test",
            checks=[
                CheckConfig(check_type="null_check", table="test", enabled=False),
                CheckConfig(check_type="duplicate_check", table="test"),
            ],
        )
        summary = engine.run_suite(sample_df, suite)
        check_types = {r.check_type for r in summary.results}
        assert "null_check" not in check_types

    def test_invalid_check_type_in_suite(self, engine, sample_df):
        suite = CheckSuite(
            name="test_suite",
            connection_name="test",
            checks=[
                CheckConfig(check_type="nonexistent_check", table="test"),
            ],
        )
        summary = engine.run_suite(sample_df, suite)
        # Should have an error result, not crash
        assert summary.error_count == 1
