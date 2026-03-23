"""CRUD operations for check run results and historical tracking."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from core.models import CheckResult, CheckStatus, RunSummary, Severity
from storage.database import Database

logger = structlog.get_logger(__name__)


class ResultStore:
    """Manages persistence and retrieval of check run results."""

    def __init__(self, db: Database) -> None:
        self._db = db

    # ── Save ─────────────────────────────────────────────────────────────

    def save_run(self, summary: RunSummary) -> None:
        """Persist a complete check run (summary + all individual results)."""
        # Save the summary row
        self._db.execute(
            """
            INSERT INTO run_summaries
                (run_id, suite_name, connection_name, started_at, completed_at,
                 total_checks, passed_count, failed_count, error_count,
                 critical_count, warning_count, quality_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                summary.run_id,
                summary.suite_name,
                summary.connection_name,
                summary.started_at.isoformat() if summary.started_at else None,
                summary.completed_at.isoformat() if summary.completed_at else None,
                summary.total_checks,
                summary.passed_count,
                summary.failed_count,
                summary.error_count,
                summary.critical_count,
                summary.warning_count,
                round(summary.quality_score, 2),
            ),
        )

        # Batch-insert all individual check results
        if summary.results:
            self._db.executemany(
                """
                INSERT INTO check_results
                    (run_id, check_name, check_type, table_name, column_name,
                     status, severity, message, value, threshold,
                     affected_rows, total_rows, details_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r.run_id,
                        r.check_name,
                        r.check_type,
                        r.table,
                        r.column,
                        r.status.value,
                        r.severity.value,
                        r.message,
                        r.value,
                        r.threshold,
                        r.affected_rows,
                        r.total_rows,
                        json.dumps(r.details) if r.details else None,
                        r.timestamp.isoformat(),
                    )
                    for r in summary.results
                ],
            )

        self._db.commit()
        logger.info("run_saved", run_id=summary.run_id, total=summary.total_checks)

    # ── Read — Run summaries ─────────────────────────────────────────────

    def get_run(self, run_id: str) -> Optional[RunSummary]:
        """Retrieve a run summary with all its check results."""
        row = self._db.fetchone(
            "SELECT * FROM run_summaries WHERE run_id = ?", (run_id,)
        )
        if row is None:
            return None

        results = self._get_results_for_run(run_id)
        return RunSummary(
            run_id=row["run_id"],
            suite_name=row["suite_name"],
            connection_name=row["connection_name"],
            results=results,
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
        )

    def list_runs(
        self,
        limit: int = 50,
        offset: int = 0,
        suite_name: Optional[str] = None,
        connection_name: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Return recent run summaries as dicts (without full results).

        Results are ordered by completed_at descending (most recent first).
        """
        conditions = []
        params: list[Any] = []

        if suite_name:
            conditions.append("suite_name = ?")
            params.append(suite_name)
        if connection_name:
            conditions.append("connection_name = ?")
            params.append(connection_name)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.extend([limit, offset])

        rows = self._db.fetchall(
            f"""
            SELECT * FROM run_summaries
            {where}
            ORDER BY completed_at DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params),
        )
        return [dict(r) for r in rows]

    def count_runs(self) -> int:
        """Return the total number of stored runs."""
        row = self._db.fetchone("SELECT COUNT(*) as cnt FROM run_summaries")
        return row["cnt"] if row else 0

    # ── Read — Individual results ────────────────────────────────────────

    def get_results_for_run(self, run_id: str) -> list[CheckResult]:
        """Public method: retrieve all check results for a given run."""
        return self._get_results_for_run(run_id)

    def get_failed_results(
        self,
        run_id: str,
        min_severity: Optional[str] = None,
    ) -> list[CheckResult]:
        """Retrieve only failed/error results, optionally filtered by severity."""
        conditions = ["run_id = ?", "status IN ('failed', 'error')"]
        params: list[Any] = [run_id]

        if min_severity:
            conditions.append("severity = ?")
            params.append(min_severity)

        rows = self._db.fetchall(
            f"""
            SELECT * FROM check_results
            WHERE {' AND '.join(conditions)}
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 1
                    WHEN 'warning'  THEN 2
                    WHEN 'info'     THEN 3
                    ELSE 4
                END,
                table_name, column_name
            """,
            tuple(params),
        )
        return [self._row_to_check_result(r) for r in rows]

    def get_results_by_table(self, run_id: str, table_name: str) -> list[CheckResult]:
        """Retrieve results for a specific table within a run."""
        rows = self._db.fetchall(
            """
            SELECT * FROM check_results
            WHERE run_id = ? AND table_name = ?
            ORDER BY severity, column_name
            """,
            (run_id, table_name),
        )
        return [self._row_to_check_result(r) for r in rows]

    # ── Historical / trend queries ───────────────────────────────────────

    def get_quality_trend(
        self,
        suite_name: Optional[str] = None,
        connection_name: Optional[str] = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        """Return quality score over time for trend charts.

        Returns list of dicts with: run_id, completed_at, quality_score,
        total_checks, passed_count, failed_count.
        """
        conditions = []
        params: list[Any] = []

        if suite_name:
            conditions.append("suite_name = ?")
            params.append(suite_name)
        if connection_name:
            conditions.append("connection_name = ?")
            params.append(connection_name)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        rows = self._db.fetchall(
            f"""
            SELECT run_id, suite_name, connection_name, completed_at,
                   quality_score, total_checks, passed_count, failed_count,
                   critical_count, warning_count
            FROM run_summaries
            {where}
            ORDER BY completed_at DESC
            LIMIT ?
            """,
            tuple(params),
        )
        return [dict(r) for r in rows]

    def compare_runs(self, run_id_a: str, run_id_b: str) -> dict[str, Any]:
        """Compare two runs side by side.

        Returns a dict with summary-level diffs and per-check comparisons.
        """
        run_a = self.get_run(run_id_a)
        run_b = self.get_run(run_id_b)

        if not run_a or not run_b:
            return {"error": "One or both runs not found."}

        # Build lookup: (check_type, table, column) -> result
        def _key(r: CheckResult) -> str:
            return f"{r.check_type}|{r.table}|{r.column or ''}"

        results_a = {_key(r): r for r in run_a.results}
        results_b = {_key(r): r for r in run_b.results}

        all_keys = sorted(set(results_a.keys()) | set(results_b.keys()))
        comparisons = []
        for k in all_keys:
            ra = results_a.get(k)
            rb = results_b.get(k)
            comparisons.append({
                "key": k,
                "run_a_status": ra.status.value if ra else "missing",
                "run_b_status": rb.status.value if rb else "missing",
                "run_a_severity": ra.severity.value if ra else None,
                "run_b_severity": rb.severity.value if rb else None,
                "run_a_value": ra.value if ra else None,
                "run_b_value": rb.value if rb else None,
                "changed": (
                    (ra.status if ra else None) != (rb.status if rb else None)
                ),
            })

        return {
            "run_a": {"run_id": run_id_a, "score": run_a.quality_score},
            "run_b": {"run_id": run_id_b, "score": run_b.quality_score},
            "score_delta": run_b.quality_score - run_a.quality_score,
            "comparisons": comparisons,
        }

    # ── Delete ───────────────────────────────────────────────────────────

    def delete_run(self, run_id: str) -> bool:
        """Delete a run and all its results (cascade)."""
        # Results are cascaded via FK, but let's be explicit
        self._db.execute("DELETE FROM check_results WHERE run_id = ?", (run_id,))
        cursor = self._db.execute(
            "DELETE FROM run_summaries WHERE run_id = ?", (run_id,)
        )
        self._db.commit()
        return cursor.rowcount > 0

    def delete_older_than(self, days: int) -> int:
        """Delete runs older than N days. Returns number of runs deleted."""
        cutoff = datetime.now(timezone.utc)
        # Compute cutoff by subtracting days in seconds
        from datetime import timedelta
        cutoff = (cutoff - timedelta(days=days)).isoformat()

        # Get run IDs to delete
        rows = self._db.fetchall(
            "SELECT run_id FROM run_summaries WHERE completed_at < ?",
            (cutoff,),
        )
        if not rows:
            return 0

        run_ids = [r["run_id"] for r in rows]
        placeholders = ",".join("?" for _ in run_ids)

        self._db.execute(
            f"DELETE FROM check_results WHERE run_id IN ({placeholders})",
            tuple(run_ids),
        )
        self._db.execute(
            f"DELETE FROM run_summaries WHERE run_id IN ({placeholders})",
            tuple(run_ids),
        )
        self._db.commit()
        logger.info("old_runs_deleted", count=len(run_ids), days=days)
        return len(run_ids)

    # ── Helpers ──────────────────────────────────────────────────────────

    def _get_results_for_run(self, run_id: str) -> list[CheckResult]:
        rows = self._db.fetchall(
            """
            SELECT * FROM check_results
            WHERE run_id = ?
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 1
                    WHEN 'warning'  THEN 2
                    WHEN 'info'     THEN 3
                    ELSE 4
                END,
                table_name, column_name
            """,
            (run_id,),
        )
        return [self._row_to_check_result(r) for r in rows]

    def _row_to_check_result(self, row: Any) -> CheckResult:
        details = json.loads(row["details_json"]) if row["details_json"] else {}
        return CheckResult(
            check_name=row["check_name"],
            check_type=row["check_type"],
            table=row["table_name"],
            column=row["column_name"],
            status=CheckStatus(row["status"]),
            severity=Severity(row["severity"]),
            message=row["message"],
            value=row["value"],
            threshold=row["threshold"],
            affected_rows=row["affected_rows"],
            total_rows=row["total_rows"],
            details=details,
            run_id=row["run_id"],
            timestamp=datetime.fromisoformat(row["timestamp"]),
        )
