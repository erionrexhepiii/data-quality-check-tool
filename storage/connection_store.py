"""CRUD operations for saved database connections with Fernet encryption."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from core.exceptions import StorageError, ValidationError
from core.models import ConnectionConfig, ConnectorType
from storage.database import Database
from utils.crypto import decrypt, encrypt, get_or_create_key

logger = structlog.get_logger(__name__)


class ConnectionStore:
    """Manages persistence of database connection configurations.

    Connection parameters (which may contain passwords/tokens) are encrypted
    at rest using Fernet symmetric encryption.
    """

    def __init__(self, db: Database, encryption_key_path: str = "data/.encryption_key") -> None:
        self._db = db
        self._key = get_or_create_key(encryption_key_path)

    # ── Create ───────────────────────────────────────────────────────────

    def save(self, config: ConnectionConfig) -> ConnectionConfig:
        """Save a new connection or update an existing one.

        If a connection with the same name exists, it is updated.
        """
        now = datetime.now(timezone.utc).isoformat()
        params_json = json.dumps(config.params)
        params_enc = encrypt(params_json, self._key)

        existing = self.get_by_name(config.name)
        if existing:
            self._db.execute(
                """
                UPDATE connections
                SET connector_type = ?, params_encrypted = ?, updated_at = ?
                WHERE name = ?
                """,
                (config.connector_type.value, params_enc, now, config.name),
            )
            config.connection_id = existing.connection_id
            logger.info("connection_updated", name=config.name)
        else:
            self._db.execute(
                """
                INSERT INTO connections
                    (connection_id, name, connector_type, params_encrypted, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (config.connection_id, config.name, config.connector_type.value,
                 params_enc, now, now),
            )
            logger.info("connection_saved", name=config.name)

        self._db.commit()
        return config

    # ── Read ─────────────────────────────────────────────────────────────

    def get_by_name(self, name: str) -> Optional[ConnectionConfig]:
        """Retrieve a connection by name, decrypting params."""
        row = self._db.fetchone(
            "SELECT * FROM connections WHERE name = ?", (name,)
        )
        if row is None:
            return None
        return self._row_to_config(row)

    def get_by_id(self, connection_id: str) -> Optional[ConnectionConfig]:
        """Retrieve a connection by ID."""
        row = self._db.fetchone(
            "SELECT * FROM connections WHERE connection_id = ?", (connection_id,)
        )
        if row is None:
            return None
        return self._row_to_config(row)

    def list_all(self) -> list[ConnectionConfig]:
        """Return all saved connections (params decrypted)."""
        rows = self._db.fetchall(
            "SELECT * FROM connections ORDER BY name"
        )
        return [self._row_to_config(r) for r in rows]

    def list_names(self) -> list[str]:
        """Return just the names of all saved connections."""
        rows = self._db.fetchall("SELECT name FROM connections ORDER BY name")
        return [r["name"] for r in rows]

    # ── Delete ───────────────────────────────────────────────────────────

    def delete(self, name: str) -> bool:
        """Delete a connection by name. Returns True if a row was deleted."""
        cursor = self._db.execute(
            "DELETE FROM connections WHERE name = ?", (name,)
        )
        self._db.commit()
        deleted = cursor.rowcount > 0
        if deleted:
            logger.info("connection_deleted", name=name)
        return deleted

    def delete_by_id(self, connection_id: str) -> bool:
        """Delete a connection by ID."""
        cursor = self._db.execute(
            "DELETE FROM connections WHERE connection_id = ?", (connection_id,)
        )
        self._db.commit()
        return cursor.rowcount > 0

    # ── Helpers ──────────────────────────────────────────────────────────

    def _row_to_config(self, row: Any) -> ConnectionConfig:
        """Convert a database row to a ConnectionConfig, decrypting params."""
        params_json = decrypt(row["params_encrypted"], self._key)
        params = json.loads(params_json)
        return ConnectionConfig(
            name=row["name"],
            connector_type=ConnectorType(row["connector_type"]),
            params=params,
            created_at=datetime.fromisoformat(row["created_at"]),
            connection_id=row["connection_id"],
        )

    def count(self) -> int:
        """Return the total number of saved connections."""
        row = self._db.fetchone("SELECT COUNT(*) as cnt FROM connections")
        return row["cnt"] if row else 0
