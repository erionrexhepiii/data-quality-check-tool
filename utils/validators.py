"""Input validation helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from core.exceptions import ValidationError


def validate_connection_name(name: str) -> str:
    """Validate and normalize a connection name."""
    name = name.strip()
    if not name:
        raise ValidationError("Connection name cannot be empty.")
    if len(name) > 100:
        raise ValidationError("Connection name must be 100 characters or fewer.")
    return name


def validate_file_path(path: str) -> Path:
    """Validate that a file path exists and is readable."""
    p = Path(path)
    if not p.exists():
        raise ValidationError(f"Path does not exist: {path}")
    return p


def validate_threshold(value: Any, name: str, min_val: float = 0, max_val: float = 100) -> float:
    """Validate a numeric threshold is within bounds."""
    try:
        val = float(value)
    except (TypeError, ValueError):
        raise ValidationError(f"{name} must be a number, got: {value}")
    if val < min_val or val > max_val:
        raise ValidationError(f"{name} must be between {min_val} and {max_val}, got: {val}")
    return val


def validate_columns_exist(columns: list[str], available: list[str]) -> list[str]:
    """Validate that requested columns exist in the dataset."""
    missing = [c for c in columns if c not in available]
    if missing:
        raise ValidationError(f"Columns not found in dataset: {', '.join(missing)}")
    return columns
