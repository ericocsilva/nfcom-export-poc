# NFCom XML Export PoC

High-performance parallel export of **NF-Com (Nota Fiscal Fatura de Comunicação)**
XML records from a Databricks Delta table to individual XML files stored in a
Databricks Unity Catalog Volume.

---

## Architecture overview

```
┌──────────────────────────────────────────────────────────────┐
│  Databricks Workspace (Azure)                                 │
│                                                               │
│  ┌─────────────────────┐     trigger      ┌───────────────┐  │
│  │  Databricks App      │ ─────────────→  │  Lakeflow Job │  │
│  │  (Streamlit)         │                 │  02_export_xml│  │
│  │                      │ ←─ job status ─ │               │  │
│  └─────────────────────┘                 └───────┬───────┘  │
│                                                   │          │
│  ┌─────────────────────────────────────┐          │ read     │
│  │  Delta Table (Liquid Clustering)    │ ◄────────┘          │
│  │  catalog.nfcom_poc.nfcom_data       │                     │
│  │  cluster by (EMPRESA, UF, ANO, MES) │          │ write    │
│  └─────────────────────────────────────┘          ▼          │
│                                                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  Unity Catalog Volume                                   │ │
│  │  /Volumes/catalog/nfcom_poc/xml_exports/                │ │
│  │    {EMPRESA}/{UF}/{YYYY}/{MM}/{DD}/{CHAVE_ACESSO}.xml   │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### Key design decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Clustering strategy | **Liquid Clustering** on `(EMPRESA, UF, ANO, MES)` | Adaptive, no manual ZORDER; handles skew better than static partitioning for variable-cardinality columns; ideal for point-filter queries |
| Export parallelism | `repartition(n, EMPRESA, UF, ANO, MES)` + `mapPartitions` | Each Spark task writes its slice concurrently across all executor cores |
| Intra-task parallelism | `ThreadPoolExecutor(16)` per task | Multiplies effective write throughput beyond single-thread I/O limits on ADLS Gen2 |
| Partition sizing | 50 000 records / partition (capped 8–4 000) | Balances task overhead vs. parallelism for 1 M – 1 B record runs |
| Directory hierarchy | `EMPRESA / UF / YYYY / MM / DD /` | Keeps per-directory file counts manageable; avoids cloud-storage hot-spots at scale |
| File naming | `{CHAVE_ACESSO}.xml` | Unique, deterministic, meaningful — the access key identifies the NF-Com document |

---

## Repository structure

```
ExportXML/
├── databricks.yml                    # Bundle definition (job + app)
├── notebooks/
│   ├── 01_create_table.py            # Create Delta table + load ~24 000 mock records
│   └── 02_export_xml.py              # Parallel XML export (Lakeflow job notebook)
├── app/
│   ├── app.py                        # Streamlit UI
│   ├── app.yaml                      # Databricks App entrypoint config
│   └── requirements.txt              # Python deps for the App
└── PL_NFCOM_1.00_NT2025.001 RTC_1.14/   # Official NF-Com XSD schemas
```

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Databricks CLI ≥ 0.220 | `brew install databricks` or `pip install databricks-cli` |
| Auth profile `TKO` | Already configured in `~/.databrickscfg` pointing to `adb-7405618499553895.15.azuredatabricks.net` |
| Unity Catalog permissions | `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME` on `catalog_1aphlh_uefz2w` |
| SQL Warehouse | At least one running warehouse (used by the Streamlit app for filter suggestions) |

---

## End-to-end setup

### 1 — Deploy the Databricks bundle

```bash
cd ExportXML

# Validate the bundle definition
databricks bundle validate --profile=TKO

# Deploy notebooks, job, and app to the workspace
databricks bundle deploy --profile=TKO
```

This creates:
- **Job**: `[dev <user>] NFCom - Export XML Files`
- **App**: `nfcom-export-app` (not yet running — starts in step 3)

---

### 2 — Initialize the Delta table with mock data

Run the setup notebook as a one-time job:

```bash
databricks jobs submit \
  --profile=TKO \
  --json '{
    "run_name": "NFCom - Setup Table",
    "tasks": [{
      "task_key": "create_table",
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "Standard_D4ds_v5",
        "num_workers": 2
      },
      "notebook_task": {
        "notebook_path": "/Workspace/Users/<you>/.bundle/nfcom-export-poc/dev/files/notebooks/01_create_table"
      }
    }]
  }'
```

> Replace `<you>` with your Databricks username (e.g. `erico.silva@databricks.com`).

The notebook:
1. Creates `catalog_1aphlh_uefz2w.nfcom_poc` schema
2. Creates the `nfcom_data` Delta table with Liquid Clustering
3. Creates the `/Volumes/catalog_1aphlh_uefz2w/nfcom_poc/xml_exports` Volume
4. Generates **~24 000 mock NF-Com records** (5 companies × 20 UFs × 2 years × 12 months × 5 days × 20 records/day)
5. Runs `OPTIMIZE` to trigger the first Liquid Clustering pass

To scale up mock data, edit the constants near the top of the `# Generate Mock Records` cell:

```python
RECS_PER_DAY = 20   # increase to generate more records
```

---

### 3 — Start the Databricks App

```bash
databricks bundle run nfcom_export_app --profile=TKO
```

Or navigate to **Workspace → Apps → nfcom-export-app** and click **Start**.

The app URL will be displayed in the CLI output and in the workspace UI.

---

### 4 — Export XML files via the App

1. Open the Databricks App URL in your browser.
2. Fill in filter fields (leave blank to match all values):
   - **EMPRESA** — one or more CNPJs (comma-separated)
   - **UF** — multi-select from dropdown
   - **ANO** — multi-select year
   - **MES** — multi-select month
   - **DIA** — multi-select day
3. Click **🚀 Start Export**.
4. The app submits a Lakeflow job run and polls its status every 5 seconds.
5. When the job completes, files appear at:

```
/Volumes/catalog_1aphlh_uefz2w/nfcom_poc/xml_exports/
  <EMPRESA_CNPJ>/
    <UF>/
      <YYYY>/
        <MM>/
          <DD>/
            <CHAVE_ACESSO>.xml
```

---

### 5 — (Optional) Trigger the export job directly via CLI

```bash
# Export all records for VIVO in SP for January 2025
databricks jobs run-now \
  --profile=TKO \
  --job-id <JOB_ID> \
  --job-parameters '{
    "empresa": "00394460007202",
    "uf":      "SP",
    "ano":     "2025",
    "mes":     "1",
    "dia":     ""
  }'
```

Find `<JOB_ID>` with:

```bash
databricks jobs list --profile=TKO | grep "NFCom"
```

---

## Performance notes

### For the PoC (~24 000 records)
- Single cluster (2–4 workers) is sufficient
- Export completes in seconds

### Scaling to 1 billion records
| Tunable | Recommended value | Where to change |
|---------|-------------------|-----------------|
| Cluster size | 32–64 `Standard_D8ds_v5` workers (256–512 vCores) | `databricks.yml` → `num_workers` |
| Records per partition | 50 000 (default) | `notebooks/02_export_xml.py` → `RECORDS_PER_PARTITION` |
| Write threads per task | 16 (default) | `notebooks/02_export_xml.py` → `WRITE_THREADS` |
| Max partitions | 4 000 (default) | `notebooks/02_export_xml.py` → `min(4_000, ...)` |

**Estimated throughput** (16 workers × 4 cores = 64 concurrent tasks × 16 threads = ~1 024 concurrent writes):
- At ~10 ms per ADLS Gen2 small-file write: ~100 000 files/second
- 1 billion files ÷ 100 000/s ≈ **~3 hours** (dominated by cloud storage I/O)

The primary bottleneck at 1 B records is cloud-storage request rate. To push further:
1. Enable **hierarchical namespace (HNS)** on the ADLS account
2. Increase **Storage account request-rate limits** via Azure Support
3. Consider writing **ZIP archives per day-bucket** if individual-file requirement is relaxed

---

## NF-Com schema reference

The XML payloads conform to **NF-Com 1.00 (NT 2025.001 RTC 1.14)**, available in the
`PL_NFCOM_1.00_NT2025.001 RTC_1.14/` directory.

Key schema files:
- `nfcom_v1.00.xsd` — root element `<NFCom>`
- `nfcomTiposBasico_v1.00.xsd` — type `TNFCom` (full document structure)
- `tiposGeralNFCom_v1.00.xsd` — shared primitive types

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| App shows "Job not found" | Bundle not deployed yet | Run `databricks bundle deploy --profile=TKO` |
| App shows "No SQL warehouse found" | No warehouse running | Start a warehouse in the workspace UI |
| Export job fails with `PERMISSION_DENIED` on Volume | Missing `WRITE FILES` privilege | Grant `GRANT WRITE FILES ON VOLUME ... TO <user>` |
| Export job fails with `RESOURCE_NOT_FOUND` on table | Step 2 not completed | Run the setup notebook first |
| Slow export on large dataset | Too few workers | Scale up `num_workers` in `databricks.yml` |
