"""
NFCom XML Export — Databricks App (Streamlit)

Allows operators to:
  1. Select filter criteria (EMPRESA, UF, ANO, MES, DIA)
  2. Trigger the Lakeflow export job
  3. Monitor job progress in real-time
"""

import os
import time
from typing import List, Optional

import streamlit as st
from databricks.sdk import WorkspaceClient

# ── Configuration ────────────────────────────────────────────────────────────
CATALOG     = os.environ.get("CATALOG",  "catalog_1aphlh_uefz2w")
SCHEMA      = os.environ.get("SCHEMA",   "nfcom_poc")
TABLE       = os.environ.get("TABLE",    "nfcom_data")
JOB_NAME    = os.environ.get("JOB_NAME", "NFCom - Export XML Files")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/xml_exports"

# Databricks SDK auto-authenticates inside a Databricks App
w = WorkspaceClient()

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_warehouse_id() -> Optional[str]:
    """Return the first available SQL warehouse id."""
    try:
        warehouses = list(w.warehouses.list())
        running = [wh for wh in warehouses if str(wh.state) in ("RUNNING", "STARTING")]
        candidates = running or warehouses
        return candidates[0].id if candidates else None
    except Exception:
        return None


def get_distinct_values(column: str) -> List[str]:
    """Query the Delta table for distinct filter values."""
    wh_id = get_warehouse_id()
    if not wh_id:
        return []
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=(
                f"SELECT DISTINCT {column} "
                f"FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE}` "
                f"ORDER BY 1"
            ),
            wait_timeout="30s",
        )
        data = result.result.data_array or [] if result.result else []
        return [str(row[0]) for row in data]
    except Exception:
        return []


def find_job_id(name: str) -> Optional[int]:
    """Find a job by partial name match."""
    try:
        for job in w.jobs.list(name=name):
            settings_name = job.settings.name if job.settings else ""
            if name in (settings_name or ""):
                return job.job_id
    except Exception:
        pass
    return None


def trigger_export(
    job_id: int,
    empresa: str,
    uf: str,
    ano: str,
    mes: str,
    dia: str,
) -> int:
    """Submit a one-time job run with the given filter parameters."""
    run = w.jobs.run_now(
        job_id=job_id,
        job_parameters={
            "empresa": empresa,
            "uf":      uf,
            "ano":     ano,
            "mes":     mes,
            "dia":     dia,
        },
    )
    return run.run_id


def get_run_status(run_id: int) -> dict:
    """Return lifecycle state, result state, and run URL."""
    run = w.jobs.get_run(run_id=run_id)
    lc  = run.state.life_cycle_state if run.state else None
    rs  = run.state.result_state     if run.state else None
    return {
        "lifecycle": str(lc.value) if lc else "UNKNOWN",
        "result":    str(rs.value) if rs else "",
        "url":       run.run_page_url or "",
    }


# ── UI ────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="NFCom XML Export",
    page_icon="📄",
    layout="wide",
)

st.title("📄 NFCom XML Export")
st.caption(
    f"Exports NF-Com records from **{CATALOG}.{SCHEMA}.{TABLE}** "
    f"to individual XML files at `{VOLUME_PATH}`"
)
st.divider()

# ── Session state init ────────────────────────────────────────────────────────
if "run_id" not in st.session_state:
    st.session_state.run_id = None
if "error_msg" not in st.session_state:
    st.session_state.error_msg = None

# ── Filter Form ───────────────────────────────────────────────────────────────
st.subheader("Filter criteria")
st.info(
    "Leave any field blank to export **all** values for that dimension (wildcard). "
    "You may enter multiple comma-separated values.",
    icon="ℹ️",
)

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    empresa_input = st.text_input(
        "EMPRESA (CNPJ)",
        placeholder="e.g. 00394460007202",
        help="One or more CNPJs separated by commas. Empty = all companies.",
    )

with col2:
    uf_options = get_distinct_values("UF")
    uf_select  = st.multiselect(
        "UF",
        options=uf_options,
        help="Select zero or more states. Empty = all states.",
    )
    uf_input = ",".join(uf_select)

with col3:
    ano_options = get_distinct_values("ANO")
    ano_select  = st.multiselect(
        "ANO",
        options=ano_options,
        help="Select zero or more years. Empty = all years.",
    )
    ano_input = ",".join(str(a) for a in ano_select)

with col4:
    mes_options = [str(m) for m in range(1, 13)]
    mes_select  = st.multiselect(
        "MES",
        options=mes_options,
        format_func=lambda m: f"{int(m):02d}",
        help="Select zero or more months. Empty = all months.",
    )
    mes_input = ",".join(mes_select)

with col5:
    dia_options = [str(d) for d in range(1, 32)]
    dia_select  = st.multiselect(
        "DIA",
        options=dia_options,
        format_func=lambda d: f"{int(d):02d}",
        help="Select zero or more days. Empty = all days.",
    )
    dia_input = ",".join(dia_select)

# ── Active filter summary ─────────────────────────────────────────────────────
st.divider()
filter_parts = []
if empresa_input: filter_parts.append(f"EMPRESA = `{empresa_input}`")
if uf_input:      filter_parts.append(f"UF = `{uf_input}`")
if ano_input:     filter_parts.append(f"ANO = `{ano_input}`")
if mes_input:     filter_parts.append(f"MES = `{mes_input}`")
if dia_input:     filter_parts.append(f"DIA = `{dia_input}`")

if filter_parts:
    st.markdown("**Active filters:** " + " | ".join(filter_parts))
else:
    st.warning("No filters selected — **all** records will be exported.", icon="⚠️")

# ── Error display ─────────────────────────────────────────────────────────────
if st.session_state.error_msg:
    st.error(st.session_state.error_msg, icon="❌")
    if st.button("Dismiss"):
        st.session_state.error_msg = None
        st.rerun()

# ── Trigger Button ────────────────────────────────────────────────────────────
st.divider()

col_btn, col_reset = st.columns([2, 10])
with col_btn:
    start_clicked = st.button(
        "🚀 Start Export",
        type="primary",
        disabled=st.session_state.run_id is not None,
    )

with col_reset:
    if st.session_state.run_id is not None:
        if st.button("↩ New export"):
            st.session_state.run_id = None
            st.rerun()

if start_clicked:
    st.session_state.error_msg = None
    job_id = find_job_id(JOB_NAME)
    if job_id is None:
        st.session_state.error_msg = (
            f"Job **{JOB_NAME}** not found. "
            "Make sure the bundle has been deployed with `databricks bundle deploy`."
        )
        st.rerun()
    else:
        with st.spinner("Submitting job run…"):
            try:
                run_id = trigger_export(
                    job_id,
                    empresa=empresa_input,
                    uf=uf_input,
                    ano=ano_input,
                    mes=mes_input,
                    dia=dia_input,
                )
                st.session_state.run_id = run_id
            except Exception as exc:
                st.session_state.error_msg = f"Failed to submit job: {exc}"
        st.rerun()

# ── Job Status Monitor ────────────────────────────────────────────────────────
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

if st.session_state.run_id:
    st.divider()
    st.subheader("Job status")

    run_id = st.session_state.run_id
    info = get_run_status(run_id)
    lc   = info["lifecycle"]
    rs   = info["result"]
    url  = info["url"]

    col_lc, col_rs = st.columns(2)
    with col_lc:
        st.metric("Lifecycle state", lc)
    with col_rs:
        if rs:
            st.metric("Result", rs)

    if url:
        st.markdown(f"[Open run in Databricks]({url})")

    if lc in TERMINAL_STATES:
        if rs == "SUCCESS":
            st.balloons()
            st.success(
                f"Export completed! Files are at `{VOLUME_PATH}`.",
                icon="🎉",
            )
        else:
            st.error(
                f"Job ended with state: **{rs or lc}**. "
                f"Check the [run page]({url}) for details.",
                icon="❌",
            )
    else:
        st.info(f"Job is running… (auto-refreshing every 5 s)", icon="⏳")
        time.sleep(5)
        st.rerun()
