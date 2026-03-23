"""Connections page — manage database and file connections."""

import sys
from pathlib import Path

_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import streamlit as st

from connectors.factory import get_factory
from core.exceptions import ConnectionError as DQCConnectionError
from core.models import ConnectionConfig, ConnectorType
from storage.database import get_database
from storage.connection_store import ConnectionStore


# ── Initialize stores ────────────────────────────────────────────────────────

@st.cache_resource
def _get_connection_store() -> ConnectionStore:
    db = get_database()
    return ConnectionStore(db)


def _get_store() -> ConnectionStore:
    return _get_connection_store()


# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="DQC — Connections", page_icon="\U0001F50D", layout="wide")
st.title("Connections")
st.markdown("Add, test, and manage your data source connections.")

factory = get_factory()
store = _get_store()

# ── Tabs: List / Add New ─────────────────────────────────────────────────────

tab_list, tab_add = st.tabs(["Saved Connections", "Add New Connection"])

# ── Tab 1: List saved connections ────────────────────────────────────────────

with tab_list:
    connections = store.list_all()

    if not connections:
        st.info("No saved connections yet. Use the **Add New Connection** tab to create one.")
    else:
        for conn in connections:
            with st.expander(f"{conn.name}  ({conn.connector_type.value})", expanded=False):
                col1, col2, col3 = st.columns([3, 1, 1])

                with col1:
                    st.markdown(f"**Type:** {conn.connector_type.value}")
                    # Show non-sensitive params
                    display_params = {
                        k: v for k, v in conn.params.items()
                        if k not in ("password", "token", "secret")
                    }
                    if display_params:
                        st.json(display_params)
                    st.caption(f"Created: {conn.created_at:%Y-%m-%d %H:%M}")

                with col2:
                    if st.button("Test", key=f"test_{conn.connection_id}"):
                        try:
                            connector = factory.create(
                                conn.connector_type.value, conn.params
                            )
                            if connector.test_connection():
                                st.success("Connection successful!")
                                tables = connector.list_tables()
                                st.write(f"Tables found: {len(tables)}")
                            else:
                                st.error("Connection test failed.")
                            connector.disconnect()
                        except Exception as e:
                            st.error(f"Error: {e}")

                with col3:
                    if st.button("Delete", key=f"del_{conn.connection_id}", type="primary"):
                        store.delete(conn.name)
                        st.success(f"Deleted '{conn.name}'.")
                        st.rerun()

# ── Tab 2: Add a new connection ──────────────────────────────────────────────

with tab_add:
    # Connector type picker
    available = factory.available_types()
    display_names = factory.get_display_names()
    type_options = {k: display_names.get(k, k) for k in available}

    selected_type = st.selectbox(
        "Connector Type",
        options=list(type_options.keys()),
        format_func=lambda k: type_options[k],
    )

    if selected_type:
        config_fields = factory.get_config_fields(selected_type)

        with st.form("add_connection_form", clear_on_submit=True):
            conn_name = st.text_input(
                "Connection Name",
                placeholder="e.g. my-csv-data",
                help="A unique friendly name for this connection.",
            )

            # Dynamically render config fields based on connector type
            field_values: dict[str, str] = {}
            for f in config_fields:
                default = f.get("default", "")
                if f.get("type") == "password":
                    val = st.text_input(
                        f["label"],
                        type="password",
                        help=f.get("help", ""),
                        key=f"field_{f['name']}",
                    )
                elif f.get("type") == "number":
                    val = st.number_input(
                        f["label"],
                        value=int(default) if default else 0,
                        help=f.get("help", ""),
                        key=f"field_{f['name']}",
                    )
                else:
                    val = st.text_input(
                        f["label"],
                        value=str(default) if default else "",
                        help=f.get("help", ""),
                        key=f"field_{f['name']}",
                    )
                field_values[f["name"]] = val

            # Allow file upload as alternative for CSV
            if selected_type in ("csv", "parquet"):
                uploaded = st.file_uploader(
                    "Or upload a file",
                    type=["csv", "tsv", "parquet"],
                    help="Upload a file instead of specifying a path.",
                )
            else:
                uploaded = None

            col_save, col_test = st.columns(2)
            save_clicked = col_save.form_submit_button("Save Connection")
            test_clicked = col_test.form_submit_button("Test & Save")

            if save_clicked or test_clicked:
                if not conn_name.strip():
                    st.error("Connection name is required.")
                elif store.get_by_name(conn_name.strip()):
                    st.error(f"A connection named '{conn_name.strip()}' already exists.")
                else:
                    # Handle file upload: save to data/ directory
                    params = {k: v for k, v in field_values.items() if v}

                    if uploaded is not None and not params.get("file_path"):
                        upload_dir = Path("data/uploads")
                        upload_dir.mkdir(parents=True, exist_ok=True)
                        dest = upload_dir / uploaded.name
                        dest.write_bytes(uploaded.getvalue())
                        params["file_path"] = str(dest)

                    # Validate required fields
                    missing = [
                        f["label"] for f in config_fields
                        if f.get("required") and not params.get(f["name"])
                    ]
                    if missing:
                        st.error(f"Missing required fields: {', '.join(missing)}")
                    else:
                        if test_clicked:
                            try:
                                connector = factory.create(selected_type, params)
                                if connector.test_connection():
                                    st.success("Connection test passed!")
                                else:
                                    st.error("Connection test failed.")
                                    st.stop()
                                connector.disconnect()
                            except Exception as e:
                                st.error(f"Connection test failed: {e}")
                                st.stop()

                        # Determine ConnectorType enum
                        try:
                            ct = ConnectorType(selected_type)
                        except ValueError:
                            ct = ConnectorType.CSV

                        config = ConnectionConfig(
                            name=conn_name.strip(),
                            connector_type=ct,
                            params=params,
                        )
                        store.save(config)
                        st.success(f"Connection '{conn_name.strip()}' saved!")
                        st.rerun()
