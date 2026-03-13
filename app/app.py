"""
NFCom XML Export — Databricks App (Streamlit)

Allows operators to:
  1. Select filter criteria (EMPRESA, UF, ANO, MES, DIA)
  2. Trigger the Lakeflow export job
  3. Monitor job progress in real-time
"""

import os
import time
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

# ── Configuration ────────────────────────────────────────────────────────────
CATALOG     = os.environ.get("CATALOG",  "catalog_1aphlh_uefz2w")
SCHEMA      = os.environ.get("SCHEMA",   "nfcom_poc")
TABLE       = os.environ.get("TABLE",    "nfcom_data")
JOB_NAME    = os.environ.get("JOB_NAME", "NFCom - Export XML Files")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/xml_exports"

# Databricks SDK auto-authenticates inside a Databricks App
w = WorkspaceClient()

# ── Helpers ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_distinct_values(column: str) -> list[str]:
    """Query the Delta table for distinct filter values (cached 5 min)."""
    try:
        rows = w.statement_execution.execute_statement(
            warehouse_id=_get_warehouse_id(),
            statement=f"SELECT DISTINCT {column} FROM {CATALOG}.{SCHEMA}.{TABLE} ORDER BY 1",
            wait_timeout="30s",
        )
        return [r[0] for r in (rows.result.data_array or [])]
    except Exception:
        return []


@st.cache_data(ttl=600)
def _get_warehouse_id() -> str:
    """Return the first available SQL warehouse."""
    warehouses = list(w.warehouses.list())
    running = [wh for wh in warehouses if str(wh.state) in ("RUNNING", "STARTING")]
    if running:
        return running[0].id
    if warehouses:
        return warehouses[0].id
    raise RuntimeError("No SQL warehouse found in this workspace.")


def find_job_id(name: str) -> int | None:
    """Find a job by exact name match (latest match wins)."""
    for job in w.jobs.list(name=name):
        if job.settings and job.settings.name and name in job.settings.name:
            return job.job_id
    return None


def trigger_export(job_id: int, empresa: str, uf: str, ano: str, mes: str, dia: str):
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
        "lifecycle":  str(lc.value) if lc else "UNKNOWN",
        "result":     str(rs.value) if rs else "",
        "url":        run.run_page_url or "",
        "start_time": run.start_time,
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
    uf_options = [""] + get_distinct_values("UF")
    uf_select  = st.multiselect(
        "UF",
        options=[u for u in uf_options if u],
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

# ── Summary of selected filter ────────────────────────────────────────────────
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

# ── Trigger Button ────────────────────────────────────────────────────────────
st.divider()

if "run_id" not in st.session_state:
    st.session_state.run_id = None

if st.button("🚀 Start Export", type="primary", use_container_width=False):
    job_id = find_job_id(JOB_NAME)
    if job_id is None:
        st.error(
            f"Job **{JOB_NAME}** not found. "
            "Make sure the bundle has been deployed with `databricks bundle deploy`.",
            icon="❌",
        )
    else:
        with st.spinner("Submitting job run…"):
            run_id = trigger_export(
                job_id,
                empresa=empresa_input,
                uf=uf_input,
                ano=ano_input,
                mes=mes_input,
                dia=dia_input,
            )
        st.session_state.run_id = run_id
        st.success(f"Job submitted — Run ID: **{run_id}**", icon="✅")

# ── Job Status Monitor ────────────────────────────────────────────────────────
if st.session_state.run_id:
    st.divider()
    st.subheader("Job status")

    run_id = st.session_state.run_id
    status_placeholder = st.empty()
    progress_placeholder = st.empty()

    TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    POLL_INTERVAL_S = 5

    while True:
        info = get_run_status(run_id)
        lc   = info["lifecycle"]
        rs   = info["result"]
        url  = info["url"]

        with status_placeholder.container():
            st.metric("Lifecycle state", lc)
            if rs:
                st.metric("Result", rs)
            if url:
                st.markdown(f"[Open run in Databricks]({url})")

        if lc in TERMINAL_STATES:
            if rs == "SUCCESS":
                st.balloons()
                st.success(
                    f"Export completed successfully! "
                    f"Files are at `{VOLUME_PATH}`.",
                    icon="🎉",
                )
            else:
                st.error(
                    f"Job ended with state: **{rs or lc}**. "
                    f"Check the [run page]({url}) for details.",
                    icon="❌",
                )
            break

        with progress_placeholder.container():
            st.info(f"Polling… (refreshing every {POLL_INTERVAL_S}s)", icon="⏳")

        time.sleep(POLL_INTERVAL_S)
        st.rerun()
