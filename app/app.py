"""
NFCom XML Export — Databricks App (Streamlit)
"""

import logging
import os
import time
from typing import List, Optional, Tuple

import streamlit as st

# ── Logging ───────────────────────────────────────────────────────────────────
# Set LOG_LEVEL=DEBUG in app.yaml to enable verbose output in the Logs tab.
_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=_log_level, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG     = os.environ.get("CATALOG",  "catalog_1aphlh_uefz2w")
SCHEMA      = os.environ.get("SCHEMA",   "nfcom_poc")
TABLE       = os.environ.get("TABLE",    "nfcom_data")
JOB_NAME    = os.environ.get("JOB_NAME", "NFCom - Export XML Files")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/xml_exports"

logger.debug(f"App config — CATALOG={CATALOG} SCHEMA={SCHEMA} TABLE={TABLE} JOB_NAME={JOB_NAME}")

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
    w = WorkspaceClient()
    try:
        me = w.current_user.me()
        logger.info(f"SDK auth OK — user: {me.user_name}")
    except Exception as exc:
        logger.error(f"SDK auth failed: {exc}")
    return w


# ── Helpers ───────────────────────────────────────────────────────────────────

def list_all_jobs() -> Tuple[List[Tuple[int, str]], Optional[str]]:
    """Return ([(job_id, name), ...], error_msg)."""
    try:
        w = get_client()
        jobs = []
        for job in w.jobs.list():
            name = (job.settings.name or "") if job.settings else ""
            jobs.append((job.job_id, name))
        logger.debug(f"jobs.list() returned {len(jobs)} jobs: {[n for _, n in jobs]}")
        return jobs, None
    except Exception as exc:
        logger.error(f"jobs.list() failed: {exc}")
        return [], str(exc)


def find_job_id(name: str) -> Tuple[Optional[int], List[Tuple[int, str]], Optional[str]]:
    """Return (job_id_or_None, all_jobs, error_msg)."""
    jobs, err = list_all_jobs()
    if err:
        return None, [], err
    for job_id, job_name in jobs:
        if name in job_name:
            logger.debug(f"Job match: '{job_name}' (id={job_id})")
            return job_id, jobs, None
    logger.warning(f"No job contains '{name}'. Visible jobs: {[n for _, n in jobs]}")
    return None, jobs, None


def _get_warehouse_id() -> Optional[str]:
    """Pick the first available (preferably running) warehouse."""
    try:
        w = get_client()
        first = None
        for wh in w.warehouses.list():
            if first is None:
                first = wh.id
            state = (wh.state.value if wh.state and hasattr(wh.state, "value") else "")
            if state in ("RUNNING", "STARTING"):
                logger.debug(f"Using warehouse id={wh.id} name={wh.name} state={state}")
                return wh.id
        if first:
            logger.debug(f"No running warehouse; falling back to id={first}")
        else:
            logger.warning("No warehouses found in this workspace")
        return first
    except Exception as exc:
        logger.error(f"Warehouse listing failed: {exc}")
        return None


def count_matching_records(
    empresa: str, uf: str, ano: str, mes: str, dia: str
) -> Optional[int]:
    """Return COUNT(*) of records matching the filter, or None on error."""
    wh_id = _get_warehouse_id()
    if not wh_id:
        return None
    try:
        conditions = []
        if empresa.strip():
            safe = [e.strip() for e in empresa.split(",") if e.strip().isdigit()]
            if safe:
                conditions.append("(" + " OR ".join(f"EMPRESA = '{e}'" for e in safe) + ")")
        if uf:
            vals = [u for u in uf.split(",") if u]
            if vals:
                conditions.append("(" + " OR ".join(f"UF = '{u}'" for u in vals) + ")")
        if ano:
            vals = [a for a in ano.split(",") if a.isdigit()]
            if vals:
                conditions.append("(" + " OR ".join(f"ANO = {a}" for a in vals) + ")")
        if mes:
            vals = [m for m in mes.split(",") if m.isdigit()]
            if vals:
                conditions.append("(" + " OR ".join(f"MES = {m}" for m in vals) + ")")
        if dia:
            vals = [d for d in dia.split(",") if d.isdigit()]
            if vals:
                conditions.append("(" + " OR ".join(f"DIA = {d}" for d in vals) + ")")

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.`{TABLE}`{where}"
        logger.debug(f"Count query: {sql}")

        w = get_client()
        stmt = w.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=sql,
            wait_timeout="50s",
        )

        def _state_str(s) -> str:
            """Normalize StatementState to string regardless of SDK version."""
            return s.value if hasattr(s, "value") else str(s)

        # Poll if warehouse was starting up and the 50s timeout elapsed
        deadline = time.monotonic() + 120
        while (
            stmt.status
            and _state_str(stmt.status.state) in ("PENDING", "RUNNING")
            and time.monotonic() < deadline
        ):
            time.sleep(5)
            stmt = w.statement_execution.get_statement(stmt.statement_id)

        if (
            stmt.status
            and _state_str(stmt.status.state) == "SUCCEEDED"
            and stmt.result
            and stmt.result.data_array
        ):
            count = int(stmt.result.data_array[0][0])
            logger.info(f"Record count = {count:,}")
            return count

        # Log full error details for diagnosis
        if stmt.status and stmt.status.error:
            logger.warning(
                f"Count query failed — error_code={stmt.status.error.error_code} "
                f"message={stmt.status.error.message}"
            )
        else:
            logger.warning(
                f"Count query did not succeed: state="
                f"{_state_str(stmt.status.state) if stmt.status else 'unknown'}"
            )
        return None
    except Exception as exc:
        logger.warning(f"count_matching_records exception: {exc}")
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
        logger.info(
            f"Job submitted — job_id={job_id} run_id={run.run_id} "
            f"filters: empresa={empresa!r} uf={uf!r} ano={ano!r} mes={mes!r} dia={dia!r}"
        )
        return run.run_id
    except Exception as exc:
        logger.error(f"trigger_export failed: {exc}")
        st.error(f"Failed to submit job: {exc}", icon="❌")
        return None


def get_run_status(run_id: int) -> dict:
    try:
        w = get_client()
        run = w.jobs.get_run(run_id=run_id)
        lc  = run.state.life_cycle_state if run.state else None
        rs  = run.state.result_state     if run.state else None
        stats: dict = {}
        if getattr(run, "start_time", None):
            stats["start_time"] = run.start_time
        if getattr(run, "end_time", None):
            stats["end_time"] = run.end_time
        if getattr(run, "setup_duration", None) is not None:
            stats["setup_ms"] = run.setup_duration
        if getattr(run, "execution_duration", None) is not None:
            stats["execution_ms"] = run.execution_duration
        if getattr(run, "cleanup_duration", None) is not None:
            stats["cleanup_ms"] = run.cleanup_duration
        logger.debug(f"run_status run_id={run_id} lifecycle={lc} result={rs} stats={stats}")
        return {
            "lifecycle": str(lc.value) if lc else "UNKNOWN",
            "result":    str(rs.value) if rs else "",
            "url":       run.run_page_url or "",
            "stats":     stats,
        }
    except Exception as exc:
        logger.error(f"get_run_status failed: {exc}")
        return {"lifecycle": "ERROR", "result": str(exc), "url": "", "stats": {}}


def _fmt_ms(ms: int) -> str:
    if ms >= 60_000:
        return f"{ms // 60_000}m {(ms % 60_000) // 1_000}s"
    return f"{ms / 1_000:.1f}s"


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="NFCom XML Export", page_icon="📄", layout="wide")

# ── Session state ─────────────────────────────────────────────────────────────
for _key, _default in [
    ("run_id", None),
    ("file_count", None),
    ("submit_duration", None),
]:
    if _key not in st.session_state:
        st.session_state[_key] = _default

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📄 NFCom XML Export")
st.caption(
    f"Exports NF-Com records from **{CATALOG}.{SCHEMA}.{TABLE}** "
    f"to individual XML files at `{VOLUME_PATH}/<run_id>/`"
)
st.divider()

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
            for _k in ("run_id", "file_count", "submit_duration"):
                st.session_state[_k] = None
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
        with st.spinner("Counting matching records…"):
            count = count_matching_records(
                empresa_input.strip(), uf_input, ano_input, mes_input, dia_input
            )
            st.session_state.file_count = count

        t0 = time.monotonic()
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
            st.session_state.submit_duration = time.monotonic() - t0
            st.success(f"Job submitted — Run ID: **{run_id}**", icon="✅")
            st.rerun()

# ── Job Status Monitor ────────────────────────────────────────────────────────
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

if st.session_state.run_id:
    st.divider()
    st.subheader("Export status")

    info = get_run_status(st.session_state.run_id)
    lc    = info["lifecycle"]
    rs    = info["result"]
    url   = info["url"]
    stats = info.get("stats", {})

    run_volume_path = f"{VOLUME_PATH}/{st.session_state.run_id}"

    # ── Pre-run metrics ────────────────────────────────────────────────────────
    pre_cols = st.columns(3)
    pre_cols[0].metric("Files to export", f"{st.session_state.file_count:,}" if st.session_state.file_count is not None else "—")
    pre_cols[1].metric("Job submission time", f"{st.session_state.submit_duration:.1f}s" if st.session_state.submit_duration is not None else "—")
    pre_cols[2].metric("Lifecycle state", lc)

    if rs:
        st.metric("Result", rs)
    if url:
        st.markdown(f"[Open run in Databricks]({url})")

    if lc in TERMINAL:
        # ── Timing statistics from job run ─────────────────────────────────────
        setup_ms = stats.get("setup_ms", 0) or 0
        exec_ms  = stats.get("execution_ms", 0) or 0
        total_ms = (
            (stats["end_time"] - stats["start_time"])
            if stats.get("end_time") and stats.get("start_time")
            else 0
        )

        if any([setup_ms, exec_ms, total_ms]):
            st.divider()
            st.subheader("Timing statistics")
            t_cols = st.columns(3)
            t_cols[0].metric("Cluster setup", _fmt_ms(setup_ms) if setup_ms else "—")
            t_cols[1].metric("Execution time", _fmt_ms(exec_ms) if exec_ms else "—")
            t_cols[2].metric("Total job time", _fmt_ms(total_ms) if total_ms else "—")

        st.divider()
        if rs == "SUCCESS":
            st.balloons()
            st.success(
                f"Export complete! Files are at `{run_volume_path}`.",
                icon="🎉",
            )
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
