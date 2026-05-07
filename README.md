<<<<<<< HEAD
# canadian-med-device-risk-pipeline
=======
# Canadian Medical Device Risk Pipeline

Production-oriented ETL scaffold for aggregating Health Canada medical device data into PostgreSQL for risk scoring, adverse event tracking, recall monitoring, and regulatory analytics.

## Layout

- `canadian_med_device_pipeline.py`: main Python runner
- `sql/01_init_pipeline.sql`: PostgreSQL schemas, tables, indexes, and materialized view
- `config/.env.example`: database and runtime configuration template
- `data/raw/`: raw source payload snapshots
- `data/processed/`: processed exports ready for downstream analytics
- `dags/`: Airflow DAG placeholder area

## Quick Start

1. Copy `config/.env.example` to `config/.env` and update values.
2. Ensure `psql` is available on `PATH`.
3. Initialize the database:

```powershell
python canadian_med_device_pipeline.py init-db
```

4. Inspect supported sources:

```powershell
python canadian_med_device_pipeline.py sources
```

5. Run a sample extraction:

```powershell
python canadian_med_device_pipeline.py extract-mdall-licences --limit 25
python canadian_med_device_pipeline.py extract-recalls --pages 2
```

## Notes

- CIHI requires a formal data request and secure access environment. The runner treats CIHI as a managed ingest source instead of a public API.
- Canada Vigilance bulk collection is not directly exposed as a public “list” endpoint. The pipeline includes the endpoint catalogue and is structured so report-ID crawling or formal dataset ingestion can be added cleanly.
- This scaffold is designed to be extended into Airflow tasks, not locked into a single-machine workflow.
>>>>>>> 6741827 (first commit)
