# Databricks notebook source
# MAGIC %md
# MAGIC # NFCom Export PoC — Step 2: High-Performance Parallel XML Export
# MAGIC
# MAGIC ## Algorithm overview
# MAGIC
# MAGIC | Stage | Description |
# MAGIC |-------|-------------|
# MAGIC | 1. Filter | Push-down predicates on the Liquid-Clustered Delta table — only matching rows are read |
# MAGIC | 2. Repartition | Adaptive repartition by `(EMPRESA, UF, ANO, MES)` — co-locates related records, controls parallelism |
# MAGIC | 3. mapPartitions | Each Spark task owns a slice of the data; tasks run concurrently across all executor cores |
# MAGIC | 4. Thread pool | Within each task a `ThreadPoolExecutor` issues concurrent `open()` calls to the Volume |
# MAGIC | 5. Accumulator | Failed-write count surfaced back to the driver for observability |
# MAGIC
# MAGIC ### Directory layout
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/xml_exports/
# MAGIC   <job_run_id>/            ← Databricks job run ID (isolates each export run)
# MAGIC     <EMPRESA_CNPJ>/        ← company CNPJ (dots/slashes sanitized)
# MAGIC       <UF>/                ← 2-letter state code
# MAGIC         <YYYY>/            ← year
# MAGIC           <MM>/            ← month zero-padded
# MAGIC             <DD>/          ← day  zero-padded
# MAGIC               <CHAVE_ACESSO>.xml
# MAGIC ```
# MAGIC The run-ID top-level folder isolates each export so concurrent or repeated
# MAGIC runs never overwrite each other. Every other level keeps leaf directory
# MAGIC size manageable for fast directory listings at scale.

# COMMAND ----------
# DBTITLE 1, Widget Parameters

dbutils.widgets.text("catalog",     "catalog_1aphlh_uefz2w",  "Catalog")
dbutils.widgets.text("schema",      "nfcom_poc",               "Schema")
dbutils.widgets.text("table",       "nfcom_data",              "Table")
dbutils.widgets.text("volume_path", "/Volumes/catalog_1aphlh_uefz2w/nfcom_poc/xml_exports", "Volume base path")
dbutils.widgets.text("empresa",     "", "EMPRESA filter (CSV or empty = all)")
dbutils.widgets.text("uf",          "", "UF filter (CSV or empty = all)")
dbutils.widgets.text("ano",         "", "ANO filter (CSV or empty = all)")
dbutils.widgets.text("mes",         "", "MES filter (CSV or empty = all)")
dbutils.widgets.text("dia",         "", "DIA filter (CSV or empty = all)")

# COMMAND ----------
# DBTITLE 1, Read Parameters

import json as _json

catalog     = dbutils.widgets.get("catalog").strip()
schema      = dbutils.widgets.get("schema").strip()
table       = dbutils.widgets.get("table").strip()
volume_path = dbutils.widgets.get("volume_path").strip()
p_empresa   = dbutils.widgets.get("empresa").strip()
p_uf        = dbutils.widgets.get("uf").strip()
p_ano       = dbutils.widgets.get("ano").strip()
p_mes       = dbutils.widgets.get("mes").strip()
p_dia       = dbutils.widgets.get("dia").strip()

# ── Resolve the job-level run ID (used as export folder name) ────────────────
# In Jobs API 2.1 currentRunId is the task-level (child) run ID.
# The Jobs REST API returns parent_run_id for the task run, but the Python SDK
# dataclass does not expose that field in all versions — so we call the API
# directly using the notebook's auth token.
try:
    import urllib.request as _ur
    _nb_ctx  = _json.loads(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    )
    _task_id = str((_nb_ctx.get("currentRunId") or {}).get("id") or "")
    if not _task_id:
        raise ValueError("currentRunId not found in context")
    _token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    _host    = spark.conf.get("spark.databricks.workspaceUrl")
    _req     = _ur.Request(
        f"https://{_host}/api/2.1/jobs/runs/get?run_id={_task_id}",
        headers={"Authorization": f"Bearer {_token}"},
    )
    with _ur.urlopen(_req, timeout=10) as _resp:
        _run_data  = _json.loads(_resp.read())
    _parent_id = _run_data.get("parent_run_id")
    _run_id    = str(_parent_id) if _parent_id else _task_id
    print(f"task_run_id={_task_id}  parent_run_id={_parent_id}  -> folder={_run_id}")
except Exception as _e:
    import time as _t
    _run_id = f"manual_{int(_t.time())}"
    print(f"Could not resolve run ID ({_e}), using fallback: {_run_id}")

# All files for this run go under a dedicated subfolder
volume_path = f"{volume_path}/{_run_id}"
print(f"Run ID     : {_run_id}")
print(f"Output dir : {volume_path}")

# COMMAND ----------
# DBTITLE 1, Build Filter Predicate

def csv_to_list(value: str):
    return [v.strip() for v in value.split(",") if v.strip()] if value else []

conditions = []

empresas = csv_to_list(p_empresa)
if empresas:
    placeholders = ", ".join(f"'{e}'" for e in empresas)
    conditions.append(f"EMPRESA IN ({placeholders})")

ufs = csv_to_list(p_uf)
if ufs:
    placeholders = ", ".join(f"'{u}'" for u in ufs)
    conditions.append(f"UF IN ({placeholders})")

anos = csv_to_list(p_ano)
if anos:
    placeholders = ", ".join(anos)
    conditions.append(f"ANO IN ({placeholders})")

meses = csv_to_list(p_mes)
if meses:
    placeholders = ", ".join(meses)
    conditions.append(f"MES IN ({placeholders})")

dias = csv_to_list(p_dia)
if dias:
    placeholders = ", ".join(dias)
    conditions.append(f"DIA IN ({placeholders})")

where_clause  = " AND ".join(conditions) if conditions else "1=1"
filter_desc   = where_clause if conditions else "(all records — no filter)"

print(f"Filter : {filter_desc}")

# COMMAND ----------
# DBTITLE 1, Load Filtered Data

spark.sql(f"USE CATALOG `{catalog}`")

df = spark.sql(f"""
    SELECT ID, EMPRESA, UF, ANO, MES, DIA, CHAVE_ACESSO, NFCOM
    FROM   `{catalog}`.`{schema}`.`{table}`
    WHERE  {where_clause}
""")

total_count = df.count()
print(f"Records matched : {total_count:,}")

if total_count == 0:
    dbutils.notebook.exit("0 records matched the supplied filter — nothing to export.")

# COMMAND ----------
# DBTITLE 1, Adaptive Repartitioning
# Target: ~50 000 records per partition for a good balance of
#   • enough parallelism (many tasks)
#   • low task-overhead ratio
# Floor: 8 partitions (small datasets still get concurrency)
# Ceil : 4 000 partitions (avoids scheduler overhead on huge runs)

import math

RECORDS_PER_PARTITION = 50_000
num_partitions = max(8, min(4_000, math.ceil(total_count / RECORDS_PER_PARTITION)))
print(f"Partitions : {num_partitions}")

from pyspark.sql.functions import col

df_part = (
    df
    .repartition(num_partitions, col("EMPRESA"), col("UF"), col("ANO"), col("MES"))
    .sortWithinPartitions("EMPRESA", "UF", "ANO", "MES", "DIA")
)

# COMMAND ----------
# DBTITLE 1, Accumulator for Error Tracking

error_acc = spark.sparkContext.accumulator(0)

# COMMAND ----------
# DBTITLE 1, Export Function (runs on executor)

# Capture volume_path in a local variable so it serialises cleanly to executors
_base_path = volume_path

def export_partition(rows):
    """
    Called once per Spark partition (on an executor).
    Uses a ThreadPoolExecutor to issue WRITE_THREADS concurrent file writes,
    multiplying throughput within each task beyond single-thread I/O limits.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    WRITE_THREADS = 16          # concurrent open() calls per executor task
    base          = _base_path
    rows_list     = list(rows)
    local_errors  = 0

    def write_one(row):
        try:
            # Sanitise CNPJ for use in a directory name
            empresa_dir = row.EMPRESA.replace("/", "_").replace(".", "").replace("-", "")

            dir_path = os.path.join(
                base,
                empresa_dir,
                row.UF,
                str(row.ANO),
                f"{row.MES:02d}",
                f"{row.DIA:02d}",
            )
            os.makedirs(dir_path, exist_ok=True)

            file_name = f"{row.CHAVE_ACESSO or row.ID}.xml"
            file_path = os.path.join(dir_path, file_name)

            with open(file_path, "w", encoding="utf-8") as fh:
                fh.write(row.NFCOM)
            return True
        except Exception:
            return False

    with ThreadPoolExecutor(max_workers=WRITE_THREADS) as pool:
        futures = {pool.submit(write_one, r): r for r in rows_list}
        for fut in as_completed(futures):
            if not fut.result():
                local_errors += 1

    # Accumulate errors back to driver (best-effort — serialisation-safe)
    if local_errors:
        error_acc.add(local_errors)

    yield local_errors   # mapPartitions expects an iterator

# COMMAND ----------
# DBTITLE 1, Execute Export

from pyspark.sql.types import LongType

errors_rdd    = df_part.rdd.mapPartitions(export_partition)
total_errors  = errors_rdd.sum()
total_written = total_count - total_errors

print(f"Export complete")
print(f"  Written : {total_written:,}")
print(f"  Errors  : {total_errors:,}")
print(f"  Path    : {volume_path}")

if total_errors > 0:
    raise Exception(
        f"Export finished with {total_errors:,} write error(s). "
        "Check executor logs for details."
    )

dbutils.notebook.exit(str(total_written))
