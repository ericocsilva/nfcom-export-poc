"""
NFCom XML Export — Databricks App (Streamlit)
"""

import os
import time
from typing import List, Optional, Tuple

import streamlit as st

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG     = os.environ.get("CATALOG",  "catalog_1aphlh_uefz2w")
SCHEMA      = os.environ.get("SCHEMA",   "nfcom_poc")
TABLE       = os.environ.get("TABLE",    "nfcom_data")
JOB_NAME    = os.environ.get("JOB_NAME", "NFCom - Export XML Files")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/xml_exports"

# ── Static reference data ─────────────────────────────────────────────────────
ALL_UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]
CURRENT_YEAR = 2026
ALL_ANOS = [str(y) for y in range(2020, CURRENT_YEAR + 1)]

# ── Databricks client (lazy, cached for the session) ─────────────────────────
@st.cache_resource
def get_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


# ── Helpers ───────────────────────────────────────────────────────────────────

def list_all_jobs() -> Tuple[List[Tuple[int, str]], Optional[str]]:
    """Return ([(job_id, name), ...], error_msg)."""
    try:
        w = get_client()
        jobs = []
        for job in w.jobs.list():
            name = (job.settings.name or "") if job.settings else ""
            jobs.append((job.job_id, name))
        return jobs, None
    except Exception as exc:
        return [], str(exc)


def find_job_id(name: str) -> Tuple[Optional[int], List[Tuple[int, str]], Optional[str]]:
    """Return (job_id_or_None, all_jobs, error_msg)."""
    jobs, err = list_all_jobs()
    if err:
        return None, [], err
    for job_id, job_name in jobs:
        if name in job_name:
            return job_id, jobs, None
    return None, jobs, None


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

# ── Debug panel ───────────────────────────────────────────────────────────────
with st.expander("🔍 Debug / Diagnostics", expanded=False):
    st.markdown(f"**JOB_NAME env:** `{JOB_NAME}`")

    # Auth check
    try:
        w = get_client()
        me = w.current_user.me()
        st.success(f"SDK auth OK — user: `{me.user_name}`", icon="✅")
    except Exception as exc:
        st.error(f"SDK auth failed: {exc}", icon="❌")

    # Job listing
    jobs, err = list_all_jobs()
    if err:
        st.error(f"jobs.list() error: {err}", icon="❌")
    elif not jobs:
        st.warning("No jobs found in this workspace.", icon="⚠️")
    else:
        st.markdown(f"**Jobs visible to this app ({len(jobs)} total):**")
        match = [(jid, jname) for jid, jname in jobs if JOB_NAME in jname]
        if match:
            st.success(f"Match found: `{match[0][1]}` (id={match[0][0]})", icon="✅")
        else:
            st.error(f"No job name contains `{JOB_NAME}`", icon="❌")
        for jid, jname in jobs:
            marker = " ✅" if JOB_NAME in jname else ""
            st.code(f"{jid}  {jname}{marker}")

# ── Filter Form ───────────────────────────────────────────────────────────────
st.subheader("Filter criteria")
st.info(
    "Leave any field blank to export **all** values for that dimension (wildcard).",
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
    uf_select = st.multiselect("UF", options=ALL_UFS,
                               help="Select states. Empty = all.")
    uf_input = ",".join(uf_select)

with col3:
    ano_select = st.multiselect("ANO", options=ALL_ANOS,
                                help="Select years. Empty = all.")
    ano_input = ",".join(ano_select)

with col4:
    mes_select = st.multiselect("MES", options=[str(m) for m in range(1, 13)],
                                format_func=lambda m: f"{int(m):02d}",
                                help="Select months. Empty = all.")
    mes_input = ",".join(mes_select)

with col5:
    dia_select = st.multiselect("DIA", options=[str(d) for d in range(1, 32)],
                                format_func=lambda d: f"{int(d):02d}",
                                help="Select days. Empty = all.")
    dia_input = ",".join(dia_select)

# ── Active filter summary ─────────────────────────────────────────────────────
st.divider()
filter_parts = []
if empresa_input.strip(): filter_parts.append(f"EMPRESA = `{empresa_input.strip()}`")
if uf_input:              filter_parts.append(f"UF = `{uf_input}`")
if ano_input:             filter_parts.append(f"ANO = `{ano_input}`")
if mes_input:             filter_parts.append(f"MES = `{mes_input}`")
if dia_input:             filter_parts.append(f"DIA = `{dia_input}`")

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
    job_id, all_jobs, err = find_job_id(JOB_NAME)

    if err:
        st.error(f"Error listing jobs: {err}", icon="❌")
    elif job_id is None:
        names = "\n".join(f"  • {n}" for _, n in all_jobs) or "  (none)"
        st.error(
            f"Job containing **{JOB_NAME}** not found.\n\n"
            f"Jobs visible to this app:\n{names}",
            icon="❌",
        )
    else:
        with st.spinner("Submitting job run…"):
            run_id = trigger_export(
                job_id,
                empresa=empresa_input.strip(),
                uf=uf_input,
                ano=ano_input,
                mes=mes_input,
                dia=dia_input,
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
