"""Results page — view check results, drill down, and export."""

import sys
from pathlib import Path

_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import pandas as pd
import streamlit as st

from reporting.csv_export import export_results_bytes, export_run_bytes, results_to_dataframe
from storage.database import get_database
from storage.result_store import ResultStore


# ── Initialize ───────────────────────────────────────────────────────────────

@st.cache_resource
def _get_result_store() -> ResultStore:
    db = get_database()
    return ResultStore(db)


result_store = _get_result_store()

# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="DQC — Results", page_icon="\U0001F50D", layout="wide")
st.title("Check Results")

# ── Run list ─────────────────────────────────────────────────────────────────

runs = result_store.list_runs(limit=100)

if not runs:
    st.info("No check runs yet. Go to the **Checks** page to run your first check.")
    st.stop()

# ── Sidebar filters ──────────────────────────────────────────────────────────

with st.sidebar:
    st.subheader("Filters")

    # Filter by connection
    all_connections = sorted({r["connection_name"] for r in runs})
    filter_conn = st.selectbox(
        "Connection", ["All"] + all_connections
    )

    # Filter by suite
    all_suites = sorted({r["suite_name"] for r in runs})
    filter_suite = st.selectbox(
        "Suite", ["All"] + all_suites
    )

# Apply filters
filtered_runs = runs
if filter_conn != "All":
    filtered_runs = [r for r in filtered_runs if r["connection_name"] == filter_conn]
if filter_suite != "All":
    filtered_runs = [r for r in filtered_runs if r["suite_name"] == filter_suite]

# ── Run list table ───────────────────────────────────────────────────────────

st.subheader(f"Recent Runs ({len(filtered_runs)})")

run_rows = []
for r in filtered_runs:
    score = r["quality_score"]
    if score >= 90:
        score_icon = "\U0001F7E2"
    elif score >= 70:
        score_icon = "\U0001F7E0"
    else:
        score_icon = "\U0001F534"

    run_rows.append({
        "Run ID": r["run_id"],
        "Suite": r["suite_name"],
        "Connection": r["connection_name"],
        "Score": f"{score_icon} {score:.0f}",
        "Total": r["total_checks"],
        "Passed": r["passed_count"],
        "Failed": r["failed_count"],
        "Critical": r["critical_count"],
        "Completed": r["completed_at"] or "-",
    })

run_list_df = pd.DataFrame(run_rows)
st.dataframe(run_list_df, use_container_width=True, height=250)

# ── Select a run to drill into ───────────────────────────────────────────────

run_ids = [r["run_id"] for r in filtered_runs]
selected_run_id = st.selectbox("Select a run to view details", run_ids)

if not selected_run_id:
    st.stop()

summary = result_store.get_run(selected_run_id)
if not summary:
    st.error("Run not found.")
    st.stop()

# ── Run detail ───────────────────────────────────────────────────────────────

st.divider()
st.subheader(f"Run: {summary.run_id}")

# Metrics row
m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Quality Score", f"{summary.quality_score:.0f}/100")
m2.metric("Total Checks", summary.total_checks)
m3.metric("Passed", summary.passed_count)
m4.metric("Failed", summary.failed_count)
m5.metric("Critical", summary.critical_count)
m6.metric("Warnings", summary.warning_count)

# ── Severity filter for results ──────────────────────────────────────────────

severity_filter = st.multiselect(
    "Filter by severity",
    options=["critical", "warning", "info", "pass"],
    default=["critical", "warning", "info", "pass"],
)

filtered_results = [
    r for r in summary.results
    if r.severity.value in severity_filter
]

# ── Results table ────────────────────────────────────────────────────────────

severity_colors = {
    "critical": "\U0001F534",
    "warning": "\U0001F7E0",
    "info": "\U0001F535",
    "pass": "\U0001F7E2",
}

if filtered_results:
    rows = []
    for r in filtered_results:
        icon = severity_colors.get(r.severity.value, "")
        rows.append({
            "Severity": f"{icon} {r.severity.value.upper()}",
            "Check": r.check_name.replace("_", " ").title(),
            "Table": r.table,
            "Column": r.column or "(table-level)",
            "Status": r.status.value.upper(),
            "Value": f"{r.value:.1f}%" if r.value is not None else "-",
            "Threshold": f"{r.threshold:.1f}%" if r.threshold is not None else "-",
            "Affected": f"{r.affected_rows:,}" if r.affected_rows is not None else "-",
            "Total Rows": f"{r.total_rows:,}" if r.total_rows is not None else "-",
            "Message": r.message,
        })

    results_df = pd.DataFrame(rows)
    st.dataframe(results_df, use_container_width=True, height=400)
else:
    st.info("No results match the selected severity filter.")

# ── Drill-down: expand individual result details ─────────────────────────────

st.subheader("Result Details")
st.caption("Expand a check result below to see full details.")

for r in filtered_results:
    icon = severity_colors.get(r.severity.value, "")
    label = (
        f"{icon} {r.check_name.replace('_', ' ').title()} — "
        f"{r.column or r.table} — {r.severity.value.upper()}"
    )
    with st.expander(label, expanded=False):
        dc1, dc2 = st.columns(2)
        with dc1:
            st.markdown(f"**Check:** {r.check_name}")
            st.markdown(f"**Table:** {r.table}")
            st.markdown(f"**Column:** {r.column or '(table-level)'}")
            st.markdown(f"**Status:** {r.status.value}")
            st.markdown(f"**Severity:** {r.severity.value}")
        with dc2:
            st.markdown(f"**Value:** {r.value}")
            st.markdown(f"**Threshold:** {r.threshold}")
            st.markdown(f"**Affected Rows:** {r.affected_rows}")
            st.markdown(f"**Total Rows:** {r.total_rows}")
            if r.pass_rate is not None:
                st.markdown(f"**Pass Rate:** {r.pass_rate:.2%}")
        st.markdown(f"**Message:** {r.message}")
        if r.details:
            st.json(r.details)

# ── Export ───────────────────────────────────────────────────────────────────

st.divider()
st.subheader("Export")

col_exp1, col_exp2 = st.columns(2)

with col_exp1:
    csv_data = export_results_bytes(filtered_results)
    st.download_button(
        "Download Results (CSV)",
        data=csv_data,
        file_name=f"dqc_results_{selected_run_id}.csv",
        mime="text/csv",
    )

with col_exp2:
    full_csv = export_run_bytes(summary)
    st.download_button(
        "Download Full Run Report (CSV)",
        data=full_csv,
        file_name=f"dqc_run_{selected_run_id}.csv",
        mime="text/csv",
    )

# ── Run comparison ───────────────────────────────────────────────────────────

if len(run_ids) >= 2:
    st.divider()
    st.subheader("Compare Runs")

    comp1, comp2 = st.columns(2)
    with comp1:
        compare_a = st.selectbox("Run A", run_ids, index=0, key="comp_a")
    with comp2:
        compare_b = st.selectbox("Run B", run_ids, index=min(1, len(run_ids) - 1), key="comp_b")

    if compare_a and compare_b and compare_a != compare_b:
        if st.button("Compare"):
            comparison = result_store.compare_runs(compare_a, compare_b)
            if "error" in comparison:
                st.error(comparison["error"])
            else:
                delta = comparison["score_delta"]
                delta_icon = "\U0001F4C8" if delta > 0 else "\U0001F4C9" if delta < 0 else "\u27A1\uFE0F"
                st.metric(
                    "Score Change",
                    f"{comparison['run_b']['score']:.0f}",
                    delta=f"{delta:+.1f}",
                )

                changed = [c for c in comparison["comparisons"] if c["changed"]]
                if changed:
                    st.markdown(f"**{len(changed)} check(s) changed:**")
                    change_rows = []
                    for c in changed:
                        change_rows.append({
                            "Check": c["key"],
                            "Run A Status": c["run_a_status"],
                            "Run B Status": c["run_b_status"],
                            "Run A Severity": c["run_a_severity"] or "-",
                            "Run B Severity": c["run_b_severity"] or "-",
                            "Run A Value": c["run_a_value"] if c["run_a_value"] is not None else "-",
                            "Run B Value": c["run_b_value"] if c["run_b_value"] is not None else "-",
                        })
                    st.dataframe(pd.DataFrame(change_rows), use_container_width=True)
                else:
                    st.success("No changes between the two runs.")

# ── Delete run ───────────────────────────────────────────────────────────────

st.divider()
with st.expander("Danger Zone"):
    if st.button("Delete this run", type="primary"):
        result_store.delete_run(selected_run_id)
        st.success(f"Run {selected_run_id} deleted.")
        st.rerun()
