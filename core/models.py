"""Core data models for the DQC Tool.

All models are plain dataclasses with no Streamlit or UI dependencies.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class Severity(str, Enum):
    """Severity level for a check result."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"
    PASS = "pass"


class CheckStatus(str, Enum):
    """Execution status of a check."""
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class ConnectorType(str, Enum):
    """Supported connector types."""
    CSV = "csv"
    PARQUET = "parquet"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"
    DATABRICKS = "databricks"
    SNOWFLAKE = "snowflake"
    DUCKDB = "duckdb"


@dataclass
class ColumnInfo:
    """Metadata about a single column."""
    name: str
    dtype: str
    nullable: bool = True
    position: int = 0

    def __repr__(self) -> str:
        return f"ColumnInfo({self.name!r}, {self.dtype!r})"


@dataclass
class DatasetInfo:
    """Metadata about a dataset (table or file)."""
    name: str
    source: str  # connection name or file path
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: Optional[int] = None
    schema: Optional[str] = None

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


@dataclass
class CheckResult:
    """Result of a single data quality check execution."""
    check_name: str
    check_type: str  # e.g. "null_check", "duplicate_check"
    table: str
    column: Optional[str]  # None for table-level checks
    status: CheckStatus
    severity: Severity
    message: str
    value: Optional[float] = None  # measured value (e.g. null %)
    threshold: Optional[float] = None  # configured threshold
    details: dict[str, Any] = field(default_factory=dict)
    affected_rows: Optional[int] = None
    total_rows: Optional[int] = None
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def pass_rate(self) -> Optional[float]:
        if self.total_rows and self.affected_rows is not None:
            return 1.0 - (self.affected_rows / self.total_rows)
        return None


@dataclass
class CheckConfig:
    """Configuration for a single check to be executed."""
    check_type: str
    table: str
    columns: Optional[list[str]] = None  # None = all columns
    params: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class CheckSuite:
    """A named collection of check configurations."""
    name: str
    connection_name: str
    checks: list[CheckConfig] = field(default_factory=list)
    description: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    suite_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])


@dataclass
class RunSummary:
    """Summary of a complete check suite execution."""
    run_id: str
    suite_name: str
    connection_name: str
    results: list[CheckResult] = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def total_checks(self) -> int:
        return len(self.results)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.status == CheckStatus.PASSED)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status == CheckStatus.FAILED)

    @property
    def error_count(self) -> int:
        return sum(1 for r in self.results if r.status == CheckStatus.ERROR)

    @property
    def critical_count(self) -> int:
        return sum(1 for r in self.results if r.severity == Severity.CRITICAL)

    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.results if r.severity == Severity.WARNING)

    @property
    def quality_score(self) -> float:
        """Compute a 0-100 quality score based on severity counts."""
        score = 100.0 - (self.critical_count * 10 + self.warning_count * 3)
        return max(0.0, min(100.0, score))


@dataclass
class ConnectionConfig:
    """Stored configuration for a database connection."""
    name: str
    connector_type: ConnectorType
    params: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    connection_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
