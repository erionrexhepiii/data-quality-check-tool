"""Check engine — orchestrates running check suites against datasets."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import structlog

from core.models import CheckConfig, CheckResult, CheckSuite, RunSummary
from core.registry import get_registry

logger = structlog.get_logger(__name__)


class CheckEngine:
    """Orchestrator that runs configured checks against DataFrames."""

    def __init__(self) -> None:
        self.registry = get_registry()

    def run_check(
        self,
        df: pd.DataFrame,
        config: CheckConfig,
    ) -> list[CheckResult]:
        """Run a single check configuration against a DataFrame."""
        check = self.registry.get(config.check_type)

        try:
            results = check.run(
                df=df,
                table_name=config.table,
                columns=config.columns,
                params=config.params,
            )
            logger.info(
                "check_completed",
                check_type=config.check_type,
                table=config.table,
                result_count=len(results),
            )
            return results
        except Exception as e:
            logger.error(
                "check_failed",
                check_type=config.check_type,
                table=config.table,
                error=str(e),
            )
            raise

    def run_suite(
        self,
        df: pd.DataFrame,
        suite: CheckSuite,
    ) -> RunSummary:
        """Run all checks in a suite against a DataFrame.

        Returns a RunSummary with all results, even if individual checks error.
        """
        run_id = uuid.uuid4().hex[:12]
        summary = RunSummary(
            run_id=run_id,
            suite_name=suite.name,
            connection_name=suite.connection_name,
            started_at=datetime.now(timezone.utc),
        )

        for config in suite.checks:
            if not config.enabled:
                logger.debug("check_skipped", check_type=config.check_type)
                continue

            try:
                results = self.run_check(df, config)
                # Tag all results with the shared run_id
                for result in results:
                    result.run_id = run_id
                summary.results.extend(results)
            except Exception as e:
                logger.error(
                    "check_error_in_suite",
                    check_type=config.check_type,
                    error=str(e),
                )
                # Create an error result so the user knows what happened
                from core.models import CheckStatus, Severity
                summary.results.append(
                    CheckResult(
                        check_name=config.check_type,
                        check_type=config.check_type,
                        table=config.table,
                        column=None,
                        status=CheckStatus.ERROR,
                        severity=Severity.WARNING,
                        message=f"Check failed to execute: {e}",
                        run_id=run_id,
                    )
                )

        summary.completed_at = datetime.now(timezone.utc)
        logger.info(
            "suite_completed",
            suite=suite.name,
            run_id=run_id,
            total=summary.total_checks,
            passed=summary.passed_count,
            failed=summary.failed_count,
            errors=summary.error_count,
            score=summary.quality_score,
        )
        return summary

    def run_quick_check(
        self,
        df: pd.DataFrame,
        table_name: str,
        check_types: Optional[list[str]] = None,
        columns: Optional[list[str]] = None,
        params: Optional[dict] = None,
    ) -> RunSummary:
        """Convenience method: run selected checks without building a full suite.

        Args:
            df: DataFrame to check.
            table_name: Name for the dataset.
            check_types: List of check type names. None runs all registered checks.
            columns: Columns to check. None means all.
            params: Shared params for all checks.
        """
        if check_types is None:
            check_types = self.registry.list_names()

        configs = [
            CheckConfig(
                check_type=ct,
                table=table_name,
                columns=columns,
                params=params or {},
            )
            for ct in check_types
        ]

        suite = CheckSuite(
            name=f"quick_check_{table_name}",
            connection_name="adhoc",
            checks=configs,
        )
        return self.run_suite(df, suite)
