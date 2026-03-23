"""DQC Tool — Streamlit application entry point."""

import sys
from pathlib import Path

# Ensure the project root is on the Python path so imports like
# `from core.models import ...` work when Streamlit runs this file.
_project_root = str(Path(__file__).resolve().parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st
from utils.logging import setup_logging

# One-time logging setup
setup_logging()

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Data Quality Check Tool",
    page_icon="\U0001F50D",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Landing page ─────────────────────────────────────────────────────────────
st.title("\U0001F50D Data Quality Check Tool")
st.markdown(
    """
    Welcome to the **DQC Tool** — automatically detect, report, and monitor
    data quality issues in your datasets.

    ### Getting Started
    Use the sidebar to navigate between pages:

    | Page | Description |
    |------|-------------|
    | **Connections** | Add and manage database / file connections |
    | **Checks** | Configure and run data quality checks |
    | **Results** | View check results and drill into issues |

    ---
    *DQC Tool v0.1.0*
    """
)
