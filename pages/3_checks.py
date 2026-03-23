"""Checks page — configure and run data quality checks."""

import sys
from pathlib import Path

_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import pandas as pd
import streamlit as st

from connectors.factory import get_factory
from core.engine import CheckEngine
from core.models import CheckConfig, CheckSuite, RunSummary
from core.registry import get_registry
from storage.database import get_database
from storage.connection_store import ConnectionStore
from storage.result_store import ResultStore


# ── Initialize ───────────────────────────────────────────────────────────────

@st.cache_resource
def _get_stores():
    db = get_database()
    return ConnectionStore(db), ResultStore(db)


conn_store, result_store = _get_stores()
factory = get_factory()
registry = get_registry()
engine = CheckEngine()

# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="DQC — Checks", page_icon="\U0001F50D", layout="wide")
st.title("Run Data Quality Checks")

# ── Step 1: Select connection ────────────────────────────────────────────────

connections = conn_store.list_all()

if not connections:
    st.warning(
        "No connections configured. Go to the **Connections** page to add one first."
    )
    st.stop()

conn_names = [c.name for c in connections]
selected_conn_name = st.selectbox("Select Connection", conn_names)
selected_conn = next(c for c in connections if c.name == selected_conn_name)

# Connect and list tables
try:
    connector = factory.create(selected_conn.connector_type.value, selected_conn.params)
except Exception as e:
    st.error(f"Failed to connect: {e}")
    st.stop()

tables = connector.list_tables()
if not tables:
    st.warning("No tables found in this connection.")
    connector.disconnect()
    st.stop()

# ── Step 2: Select table ────────────────────────────────────────────────────

selected_table = st.selectbox("Select Table", tables)

# Show table preview
with st.expander("Table Preview", expanded=False):
    preview_df = connector.fetch_table(selected_table, limit=100)
    row_count = connector.get_row_count(selected_table)
    st.caption(f"Showing up to 100 of {row_count:,} rows")
    st.dataframe(preview_df, use_container_width=True, height=300)

# Get column info
columns_info = connector.get_columns(selected_table)
all_columns = [c.name for c in columns_info]

# ── Step 3: Configure checks ────────────────────────────────────────────────

st.subheader("Configure Checks")

available_checks = registry.list_checks()

col_checks, col_options = st.columns([1, 1])

with col_checks:
    st.markdown("**Select checks to run:**")
    selected_check_types: list[str] = []
    for check in available_checks:
        if st.checkbox(
            f"{check.name.replace('_', ' ').title()}",
            value=True,
            help=check.description,
            key=f"chk_{check.name}",
        ):
            selected_check_types.append(check.name)

with col_options:
    st.markdown("**Options:**")

    # Column selection
    column_mode = st.radio(
        "Columns to check",
        ["All columns", "Select specific columns"],
        horizontal=True,
    )

    selected_columns = None
    if column_mode == "Select specific columns":
        selected_columns = st.multiselect(
            "Choose columns",
            options=all_columns,
            default=all_columns,
        )
        if not selected_columns:
            st.warning("Select at least one column.")

    # Threshold overrides
    with st.expander("Custom Thresholds (optional)"):
        custom_warning = st.number_input(
            "Warning threshold (%)",
            min_value=0.0,
            max_value=100.0,
            value=5.0,
            step=1.0,
            help="Percentage above which a WARNING is triggered.",
        )
        custom_critical = st.number_input(
            "Critical threshold (%)",
            min_value=0.0,
            max_value=100.0,
            value=20.0,
            step=1.0,
            help="Percentage above which a CRITICAL is triggered.",
        )

    # Sampling
    with st.expander("Sampling (for large tables)"):
        max_rows = st.number_input(
            "Max rows to sample",
            min_value=1000,
            max_value=10_000_000,
            value=100_000,
            step=10_000,
            help="If the table has more rows than this, a random sample is used.",
        )

# ── Step 4: Run checks ──────────────────────────────────────────────────────

st.divider()

if not selected_check_types:
    st.info("Select at least one check to run.")
    st.stop()

if st.button("Run Checks", type="primary", use_container_width=True):
    with st.spinner("Fetching data..."):
        df = connector.fetch_table(selected_table, limit=int(max_rows))
        actual_rows = len(df)

    st.caption(f"Loaded {actual_rows:,} rows for checking.")

    # Build params
    params = {
        "warning_threshold": custom_warning,
        "critical_threshold": custom_critical,
    }

    # Build check configs
    configs = [
        CheckConfig(
            check_type=ct,
            table=selected_table,
            columns=selected_columns,
            params=params,
        )
        for ct in selected_check_types
    ]

    suite = CheckSuite(
        name=f"manual_{selected_table}",
        connection_name=selected_conn_name,
        checks=configs,
    )

    # Run with progress bar
    progress = st.progress(0, text="Running checks...")
    summary: RunSummary = engine.run_suite(df, suite)
    progress.progress(100, text="Complete!")

    # Persist results
    result_store.save_run(summary)

    # ── Display inline summary ───────────────────────────────────────
    st.subheader("Results Summary")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Quality Score", f"{summary.quality_score:.0f}/100")
    m2.metric("Total Checks", summary.total_checks)
    m3.metric("Passed", summary.passed_count)
    m4.metric("Failed", summary.failed_count, delta=-summary.failed_count if summary.failed_count else None)
    m5.metric("Critical", summary.critical_count, delta=-summary.critical_count if summary.critical_count else None)

    # Color-coded results table
    if summary.results:
        severity_colors = {
            "critical": "\U0001F534",
            "warning": "\U0001F7E0",
            "info": "\U0001F535",
            "pass": "\U0001F7E2",
        }

        rows = []
        for r in summary.results:
            icon = severity_colors.get(r.severity.value, "")
            rows.append({
                "Severity": f"{icon} {r.severity.value.upper()}",
                "Check": r.check_name.replace("_", " ").title(),
                "Table": r.table,
                "Column": r.column or "(table-level)",
                "Status": r.status.value.upper(),
                "Value": f"{r.value:.1f}%" if r.value is not None else "-",
                "Affected Rows": f"{r.affected_rows:,}" if r.affected_rows is not None else "-",
                "Message": r.message,
            })

        results_df = pd.DataFrame(rows)
        st.dataframe(results_df, use_container_width=True, height=400)

    # Link to full results page
    st.info(
        f"Run **{summary.run_id}** saved. "
        f"View full details on the **Results** page."
    )

# Cleanup connector
connector.disconnect()
