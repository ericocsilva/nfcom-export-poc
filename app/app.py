"""
NFCom XML Export — Databricks App (Streamlit)
"""

import os
import time
from typing import List, Optional

import streamlit as st

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG     = os.environ.get("CATALOG",  "catalog_1aphlh_uefz2w")
SCHEMA      = os.environ.get("SCHEMA",   "nfcom_poc")
TABLE       = os.environ.get("TABLE",    "nfcom_data")
JOB_NAME    = os.environ.get("JOB_NAME", "NFCom - Export XML Files")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/xml_exports"

# ── Databricks client (lazy, cached for the session) ─────────────────────────
@st.cache_resource
def get_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


# ── Helpers ───────────────────────────────────────────────────────────────────

def find_job_id(name: str) -> Optional[int]:
    try:
        w = get_client()
        for job in w.jobs.list(name=name):
            settings_name = (job.settings.name or "") if job.settings else ""
            if name in settings_name:
                return job.job_id
    except Exception as exc:
        st.error(f"Could not list jobs: {exc}", icon="❌")
    return None


def trigger_export(
    job_id: int,
    empresa: str,
    uf: str,
    ano: str,
    mes: str,
    dia: str,
) -> Optional[int]:
    try:
        w = get_client()
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
    except Exception as exc:
        st.error(f"Failed to submit job: {exc}", icon="❌")
        return None


def get_run_status(run_id: int) -> dict:
    try:
        w = get_client()
        run = w.jobs.get_run(run_id=run_id)
        lc  = run.state.life_cycle_state if run.state else None
        rs  = run.state.result_state     if run.state else None
        return {
            "lifecycle": str(lc.value) if lc else "UNKNOWN",
            "result":    str(rs.value) if rs else "",
            "url":       run.run_page_url or "",
        }
    except Exception as exc:
        return {"lifecycle": "ERROR", "result": str(exc), "url": ""}


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="NFCom XML Export", page_icon="📄", layout="wide")

# ── Session state ─────────────────────────────────────────────────────────────
if "run_id" not in st.session_state:
    st.session_state.run_id = None

# ── Header ────────────────────────────────────────────────────────────────────
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
    "Separate multiple values with commas.",
    icon="ℹ️",
)

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    empresa_input = st.text_input(
        "EMPRESA (CNPJ)",
        placeholder="e.g. 00394460007202",
        help="One or more CNPJs, comma-separated. Empty = all companies.",
    )
with col2:
    uf_input = st.text_input(
        "UF",
        placeholder="e.g. SP,RJ",
        help="One or more state codes, comma-separated. Empty = all states.",
    )
with col3:
    ano_input = st.text_input(
        "ANO",
        placeholder="e.g. 2025",
        help="One or more years, comma-separated. Empty = all years.",
    )
with col4:
    mes_input = st.text_input(
        "MES",
        placeholder="e.g. 1,2,3",
        help="One or more months (1–12), comma-separated. Empty = all months.",
    )
with col5:
    dia_input = st.text_input(
        "DIA",
        placeholder="e.g. 1,15",
        help="One or more days (1–31), comma-separated. Empty = all days.",
    )

# ── Active filter summary ─────────────────────────────────────────────────────
st.divider()
filter_parts = []
if empresa_input.strip(): filter_parts.append(f"EMPRESA = `{empresa_input.strip()}`")
if uf_input.strip():      filter_parts.append(f"UF = `{uf_input.strip()}`")
if ano_input.strip():     filter_parts.append(f"ANO = `{ano_input.strip()}`")
if mes_input.strip():     filter_parts.append(f"MES = `{mes_input.strip()}`")
if dia_input.strip():     filter_parts.append(f"DIA = `{dia_input.strip()}`")

if filter_parts:
    st.markdown("**Active filters:** " + " | ".join(filter_parts))
else:
    st.warning("No filters set — **all** records will be exported.", icon="⚠️")

# ── Trigger / Reset buttons ───────────────────────────────────────────────────
st.divider()
col_start, col_reset, _ = st.columns([2, 2, 8])

with col_start:
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
    job_id = find_job_id(JOB_NAME)
    if job_id is None:
        st.error(
            f"Job **{JOB_NAME}** not found. "
            "Run `databricks bundle deploy --profile=TKO` first.",
            icon="❌",
        )
    else:
        with st.spinner("Submitting job run…"):
            run_id = trigger_export(
                job_id,
                empresa=empresa_input.strip(),
                uf=uf_input.strip(),
                ano=ano_input.strip(),
                mes=mes_input.strip(),
                dia=dia_input.strip(),
            )
        if run_id:
            st.session_state.run_id = run_id
            st.success(f"Job submitted — Run ID: **{run_id}**", icon="✅")
            st.rerun()

# ── Job Status Monitor ────────────────────────────────────────────────────────
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

if st.session_state.run_id:
    st.divider()
    st.subheader("Job status")

    info = get_run_status(st.session_state.run_id)
    lc   = info["lifecycle"]
    rs   = info["result"]
    url  = info["url"]

    col_lc, col_rs = st.columns(2)
    col_lc.metric("Lifecycle state", lc)
    if rs:
        col_rs.metric("Result", rs)
    if url:
        st.markdown(f"[Open run in Databricks]({url})")

    if lc in TERMINAL:
        if rs == "SUCCESS":
            st.balloons()
            st.success(f"Export complete! Files are at `{VOLUME_PATH}`.", icon="🎉")
        else:
            st.error(
                f"Job ended with **{rs or lc}**. "
                + (f"See [run page]({url})." if url else ""),
                icon="❌",
            )
    else:
        st.info("Job is running… refreshing every 5 s", icon="⏳")
        time.sleep(5)
        st.rerun()
