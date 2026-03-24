"""SQLite database initialization and migration management."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Optional

import structlog

from core.exceptions import StorageError

logger = structlog.get_logger(__name__)

# Schema version — bump this when adding new migrations
CURRENT_SCHEMA_VERSION = 1

# ── DDL statements for the initial schema ────────────────────────────────────

_MIGRATIONS: dict[int, list[str]] = {
    1: [
        # ── Connections table ────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS connections (
            connection_id   TEXT PRIMARY KEY,
            name            TEXT NOT NULL UNIQUE,
            connector_type  TEXT NOT NULL,
            params_encrypted TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );
        """,
        # ── Check run summaries ──────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS run_summaries (
            run_id          TEXT PRIMARY KEY,
            suite_name      TEXT NOT NULL,
            connection_name TEXT NOT NULL,
            started_at      TEXT,
            completed_at    TEXT,
            total_checks    INTEGER NOT NULL DEFAULT 0,
            passed_count    INTEGER NOT NULL DEFAULT 0,
            failed_count    INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            critical_count  INTEGER NOT NULL DEFAULT 0,
            warning_count   INTEGER NOT NULL DEFAULT 0,
            quality_score   REAL NOT NULL DEFAULT 100.0
        );
        """,
        # ── Individual check results ─────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS check_results (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          TEXT NOT NULL,
            check_name      TEXT NOT NULL,
            check_type      TEXT NOT NULL,
            table_name      TEXT NOT NULL,
            column_name     TEXT,
            status          TEXT NOT NULL,
            severity        TEXT NOT NULL,
            message         TEXT NOT NULL,
            value           REAL,
            threshold       REAL,
            affected_rows   INTEGER,
            total_rows      INTEGER,
            details_json    TEXT,
            timestamp       TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES run_summaries(run_id)
                ON DELETE CASCADE
        );
        """,
        # ── Indexes for fast lookups ─────────────────────────────────────
        """
        CREATE INDEX IF NOT EXISTS idx_check_results_run_id
            ON check_results(run_id);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_check_results_table
            ON check_results(table_name);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_check_results_severity
            ON check_results(severity);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_run_summaries_suite
            ON run_summaries(suite_name);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_run_summaries_completed
            ON run_summaries(completed_at);
        """,
        # ── Schema version tracking ──────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        );
        """,
    ],
}


class Database:
    """Manages the SQLite database lifecycle — init, migrations, connections.

    Connections are stored per-thread using threading.local() so that each
    Streamlit page thread gets its own sqlite3.Connection.  This avoids the
    ``sqlite3.ProgrammingError`` that occurs when a connection created in one
    thread is used from another (the default SQLite behaviour).
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._ensure_directory()

    def _ensure_directory(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> str:
        return self._db_path

    # ── Connection management ────────────────────────────────────────────

    def get_connection(self) -> sqlite3.Connection:
        """Return a per-thread connection (created on first call per thread).

        Each thread gets its own ``sqlite3.Connection`` stored in
        ``threading.local()``.  ``check_same_thread=False`` is set as an
        extra safety net for environments (like Streamlit Cloud) where the
        calling thread may differ from the creating thread during cached
        resource reuse.
        """
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    def close(self) -> None:
        """Close the current thread's database connection."""
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # ── Schema initialization & migration ────────────────────────────────

    def initialize(self) -> None:
        """Create tables if needed and run any pending migrations."""
        conn = self.get_connection()
        current = self._get_schema_version(conn)

        if current >= CURRENT_SCHEMA_VERSION:
            logger.debug("schema_up_to_date", version=current)
            return

        for version in range(current + 1, CURRENT_SCHEMA_VERSION + 1):
            if version not in _MIGRATIONS:
                raise StorageError(f"Missing migration for version {version}")

            logger.info("applying_migration", version=version)
            for statement in _MIGRATIONS[version]:
                conn.execute(statement)

        # Record the new version
        if current == 0:
            conn.execute("INSERT INTO schema_version (version) VALUES (?)",
                         (CURRENT_SCHEMA_VERSION,))
        else:
            conn.execute("UPDATE schema_version SET version = ?",
                         (CURRENT_SCHEMA_VERSION,))

        conn.commit()
        logger.info("schema_initialized", version=CURRENT_SCHEMA_VERSION)

    def _get_schema_version(self, conn: sqlite3.Connection) -> int:
        """Return the current schema version, or 0 if the DB is fresh."""
        try:
            row = conn.execute("SELECT version FROM schema_version").fetchone()
            return row["version"] if row else 0
        except sqlite3.OperationalError:
            return 0

    # ── Convenience ──────────────────────────────────────────────────────

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute a single SQL statement."""
        return self.get_connection().execute(sql, params)

    def executemany(self, sql: str, params_list: list[tuple]) -> sqlite3.Cursor:
        """Execute a SQL statement with many parameter sets."""
        return self.get_connection().executemany(sql, params_list)

    def commit(self) -> None:
        self.get_connection().commit()

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        return self.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self.execute(sql, params).fetchall()


# ── Global database singleton ────────────────────────────────────────────────

_db: Optional[Database] = None


def _default_db_path() -> str:
    """Choose a writable database path.

    On Streamlit Community Cloud the app directory is read-only, so we
    fall back to /tmp which is always writable.  Locally we use a
    relative ``dqc_data/`` directory next to the project.
    """
    # /tmp is guaranteed writable on Linux-based cloud hosts
    tmp = Path("/tmp/dqc_data")
    local = Path("dqc_data")

    # Prefer local when writable (typical local dev), else /tmp
    try:
        local.mkdir(parents=True, exist_ok=True)
        return str(local / "dqc.db")
    except OSError:
        tmp.mkdir(parents=True, exist_ok=True)
        return str(tmp / "dqc.db")


def get_database(db_path: Optional[str] = None) -> Database:
    """Get or create the global Database instance."""
    global _db
    if db_path is None:
        db_path = _default_db_path()
    if _db is None or _db.path != db_path:
        _db = Database(db_path)
        _db.initialize()
    return _db
