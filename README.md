## Agentic Data Quality POC (Databricks Free Edition)

Minimal metadata-driven pipeline with SCD Type 2 and data quality checks, implemented in Python for Databricks Serverless.

### Architecture

- Source: MongoDB Atlas collection `ecommerce.customers`
- Bronze: `workspace.agentic_poc.bronze_customers`
- Silver (SCD2): `workspace.agentic_poc.silver_customers`
- Rules: `workspace.agentic_poc.dq_rules`
- Issues log: `workspace.agentic_poc.dq_issues_log`
- Workflow stages:
  - `SetupMetadata`
  - `IngestBronze`
  - `Scd2Silver`
  - `DqEngine`
- App: FastAPI service (`app/agent_app.py`) that reads latest DQ issue and generates remediation guidance with LLM.

### Repository Layout

- `databricks.yml`: Databricks Asset Bundle definition (job + app).
- `pipeline/poc_pipeline.py`: End-to-end PySpark pipeline (all workflow stages).
- `scripts/00_mongo_simulator.py`: Seed/incremental test data generator for MongoDB.
- `app/agent_app.py`: API for issue retrieval and analysis.
- `app/requirements.txt`: App dependencies.
- `.load-env.ps1`: Loads `.env` keys as `BUNDLE_VAR_*` process variables.

### Prerequisites

- Python 3.12+
- Databricks CLI (authenticated to your workspace)
- Databricks Free Edition workspace
- MongoDB Atlas URI
- Databricks SQL Warehouse (for the app query path)

### Environment

1. Copy template:
   - `Copy-Item .env.example .env`
2. Fill required values in `.env`.
3. Load variables in PowerShell:
   - `. .\.load-env.ps1`

### Deploy and Run

1. Validate bundle:
   - `databricks bundle validate -t dev`
2. Deploy:
   - `databricks bundle deploy -t dev`
3. Seed base data:
   - `python scripts/00_mongo_simulator.py --mode reset_and_seed --mongo-uri "$env:MONGODB_URI"`
4. Run pipeline:
   - `databricks bundle run -t dev poc_pipeline_job`
5. Start app:
   - `databricks bundle run -t dev dq_rescue_agent`

### Incremental Test Cycle

- Good records:
  - `python scripts/00_mongo_simulator.py --mode add_good --mongo-uri "$env:MONGODB_URI"`
- Bad records:
  - `python scripts/00_mongo_simulator.py --mode add_bad --mongo-uri "$env:MONGODB_URI"`
- Re-run workflow:
  - `databricks bundle run -t dev poc_pipeline_job`

### Notes for Free Edition

- Use catalog `workspace` (not `main`).
- Pipeline is Python-based (`spark_python_task`) to stay compatible with Free Edition serverless runtime.
