"""Tests for the result store — run persistence and retrieval."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import uuid
from datetime import datetime, timezone

import pytest

from core.models import CheckResult, CheckStatus, RunSummary, Severity
from storage.database import Database
from storage.result_store import ResultStore


@pytest.fixture
def store(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    db.initialize()
    return ResultStore(db)


def _make_summary(
    run_id: str = None,
    suite: str = "test_suite",
    conn: str = "test_conn",
    num_results: int = 3,
) -> RunSummary:
    """Helper to build a RunSummary with realistic results."""
    rid = run_id or uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc)

    results = []
    severities = [Severity.CRITICAL, Severity.WARNING, Severity.PASS]
    statuses = [CheckStatus.FAILED, CheckStatus.FAILED, CheckStatus.PASSED]

    for i in range(num_results):
        sev = severities[i % len(severities)]
        sta = statuses[i % len(statuses)]
        results.append(CheckResult(
            check_name="null_check",
            check_type="null_check",
            table="users",
            column=f"col_{i}",
            status=sta,
            severity=sev,
            message=f"Test result {i}",
            value=float(i * 5),
            threshold=5.0,
            affected_rows=i * 10,
            total_rows=100,
            details={"idx": i},
            run_id=rid,
            timestamp=now,
        ))

    return RunSummary(
        run_id=rid,
        suite_name=suite,
        connection_name=conn,
        results=results,
        started_at=now,
        completed_at=now,
    )


class TestResultStore:
    def test_save_and_get_run(self, store):
        summary = _make_summary(run_id="run001")
        store.save_run(summary)

        loaded = store.get_run("run001")
        assert loaded is not None
        assert loaded.run_id == "run001"
        assert loaded.suite_name == "test_suite"
        assert len(loaded.results) == 3

    def test_results_roundtrip(self, store):
        summary = _make_summary(run_id="run002")
        store.save_run(summary)

        loaded = store.get_run("run002")
        r = loaded.results[0]
        assert r.check_name == "null_check"
        assert r.table == "users"
        assert r.column is not None
        assert r.details.get("idx") is not None

    def test_list_runs(self, store):
        store.save_run(_make_summary(run_id="a1"))
        store.save_run(_make_summary(run_id="a2"))
        store.save_run(_make_summary(run_id="a3"))

        runs = store.list_runs(limit=10)
        assert len(runs) == 3

    def test_list_runs_filter_by_suite(self, store):
        store.save_run(_make_summary(run_id="b1", suite="suite_a"))
        store.save_run(_make_summary(run_id="b2", suite="suite_b"))

        runs = store.list_runs(suite_name="suite_a")
        assert len(runs) == 1
        assert runs[0]["suite_name"] == "suite_a"

    def test_list_runs_filter_by_connection(self, store):
        store.save_run(_make_summary(run_id="c1", conn="conn_x"))
        store.save_run(_make_summary(run_id="c2", conn="conn_y"))

        runs = store.list_runs(connection_name="conn_x")
        assert len(runs) == 1

    def test_get_failed_results(self, store):
        store.save_run(_make_summary(run_id="d1"))
        failed = store.get_failed_results("d1")
        assert all(r.status in (CheckStatus.FAILED, CheckStatus.ERROR) for r in failed)

    def test_get_failed_results_by_severity(self, store):
        store.save_run(_make_summary(run_id="d2"))
        critical = store.get_failed_results("d2", min_severity="critical")
        assert all(r.severity == Severity.CRITICAL for r in critical)

    def test_get_results_by_table(self, store):
        store.save_run(_make_summary(run_id="e1"))
        results = store.get_results_by_table("e1", "users")
        assert len(results) == 3
        assert all(r.table == "users" for r in results)

    def test_quality_trend(self, store):
        store.save_run(_make_summary(run_id="f1"))
        store.save_run(_make_summary(run_id="f2"))

        trend = store.get_quality_trend(limit=10)
        assert len(trend) == 2
        assert "quality_score" in trend[0]

    def test_compare_runs(self, store):
        store.save_run(_make_summary(run_id="g1"))
        store.save_run(_make_summary(run_id="g2"))

        comparison = store.compare_runs("g1", "g2")
        assert "score_delta" in comparison
        assert "comparisons" in comparison
        assert len(comparison["comparisons"]) > 0

    def test_compare_missing_run(self, store):
        store.save_run(_make_summary(run_id="h1"))
        comparison = store.compare_runs("h1", "nonexistent")
        assert "error" in comparison

    def test_delete_run(self, store):
        store.save_run(_make_summary(run_id="i1"))
        assert store.delete_run("i1") is True
        assert store.get_run("i1") is None

    def test_delete_nonexistent(self, store):
        assert store.delete_run("nonexistent") is False

    def test_count_runs(self, store):
        assert store.count_runs() == 0
        store.save_run(_make_summary(run_id="j1"))
        assert store.count_runs() == 1

    def test_get_nonexistent_run(self, store):
        assert store.get_run("nonexistent") is None
