# Canadian Medical Device Risk Pipeline

Production-oriented ETL scaffold for aggregating Health Canada medical device data into PostgreSQL for risk scoring, adverse event tracking, recall monitoring, and regulatory analytics.

## Layout

- `canadian_med_device_pipeline.py`: main Python runner
- `sql/01_init_pipeline.sql`: PostgreSQL schemas, tables, indexes, and materialized view
- `.env` or `config/.env`: database and runtime configuration
- `config/.env.example`: database and runtime configuration template
- `data/raw/`: raw source payload snapshots
- `data/processed/`: processed exports ready for downstream analytics
- `dags/`: Airflow DAG placeholder area

## Quick Start

Run commands from the project root:

```powershell
cd D:\Grassstone\Projects\canadian-med-device-risk-pipeline
```

Install the PostgreSQL Python driver used by the database load steps:

```powershell
python -m pip install "psycopg[binary]>=3.2.1"
```

Create or update `.env` with your PostgreSQL connection values:

```text
DB_HOST=localhost
DB_PORT=5432
DB_NAME=canadian_med_device_risk
DB_USER=postgres
DB_PASSWORD=docker
DB_SSLMODE=prefer
HTTP_TIMEOUT_SECONDS=60
API_RATE_LIMIT_PER_MINUTE=100
SCRAPE_RATE_LIMIT_PER_MINUTE=30
```

Ensure `psql` is available on `PATH`, then initialize the database:

```powershell
python .\canadian_med_device_pipeline.py init-db
```

Inspect supported sources and the recommended run cadence:

```powershell
python .\canadian_med_device_pipeline.py sources
python .\canadian_med_device_pipeline.py plan-run
```

Validate the new database before loading data:

```powershell
python .\canadian_med_device_pipeline.py validate-no-duplicates
```

Run small test extracts and loads:

```powershell
python .\canadian_med_device_pipeline.py extract-mdall-licences --limit 25
python .\canadian_med_device_pipeline.py extract-mdall-companies --limit 25
python .\canadian_med_device_pipeline.py extract-recalls --pages 1
```

Re-check duplicate constraints after loading:

```powershell
python .\canadian_med_device_pipeline.py validate-no-duplicates
```

If the test loads pass, scale up gradually:

```powershell
python .\canadian_med_device_pipeline.py extract-mdall-licences --limit 500
python .\canadian_med_device_pipeline.py extract-mdall-companies --limit 500
python .\canadian_med_device_pipeline.py extract-recalls --pages 5
```

## Notes

- `init-db` uses the `psql` command-line client.
- Extract/load commands use the Python `psycopg` driver to write records into PostgreSQL.
- The runner loads configuration from the project root `.env` and `config/.env`.
- CIHI requires a formal data request and secure access environment. The runner treats CIHI as a managed ingest source instead of a public API.
- Canada Vigilance bulk collection is not directly exposed as a public list endpoint. The pipeline includes the endpoint catalogue and is structured so report-ID crawling or formal dataset ingestion can be added cleanly.
- This scaffold is designed to be extended into Airflow tasks, not locked into a single-machine workflow.
