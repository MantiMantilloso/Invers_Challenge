# Invers Challenge - NYC TLC Data Pipeline

This project builds a simple ETL pipeline for NYC TLC Yellow Taxi data:
1. Download and clean raw parquet data.
2. Store curated data locally as parquet.
3. Load dimensional/fact tables into PostgreSQL (Docker).

## Project Files

- `clean_data.py`: download + clean + save curated parquet.
- `load_data.py`: load dimensions and fact table into PostgreSQL.
- `check_data_size.py`: report file sizes in MB under `data/`.
- `docker-compose.yml`: local PostgreSQL service.
- `.env.example`: template for local environment variables.
- `requirements.txt`: Python dependencies.

## Architecture Decisions and Justification

### 1) Use parquet as intermediate storage
Decision:
- Keep cleaned dataset as parquet in `data/clean/`.

Why:
- Parquet is columnar and compressed, so it is faster and smaller than plain CSV for analytics workflows.
- Pandas can read/write parquet efficiently when `pyarrow` (or `fastparquet`) is installed.

Trade-off:
- Requires an extra dependency (`pyarrow` or `fastparquet`).

### 2) Standardize columns to lowercase during cleaning
Decision:
- Convert all source column names to lowercase in `clean_data.py`.

Why:
- Avoids schema drift bugs such as `Airport_fee` vs `airport_fee`.
- Simplifies downstream selection/renaming in `load_data.py`.

### 3) Apply business-quality filters before loading
Decision:
- Remove clearly invalid trips (non-positive distance/fares, invalid durations, key nulls).

Why:
- Prevents low-quality records from contaminating analytics and fact tables.
- Enforces baseline data validity close to ingestion.

Trade-off:
- Some edge-case trips may be dropped, but this is preferable to loading invalid data.

### 4) Use PostgreSQL in Docker for local reproducibility
Decision:
- Run PostgreSQL 15 via `docker-compose.yml`.

Why:
- Team members can run the same DB setup without local Postgres installation differences.
- Easy reset and isolation of local development state.

### 5) Externalize credentials with `.env`
Decision:
- Reference DB credentials in `docker-compose.yml` through environment variables.
- Keep real values in `.env` (ignored by git), and share `.env.example`.

Why:
- Avoids committing secrets to source control.
- Makes onboarding easier with a clear config template.

### 6) Ignore heavy and generated files in git
Decision:
- Exclude `data/`, parquet/csv artifacts, db volume files, and env files in `.gitignore`.

Why:
- Prevents huge commits and keeps repository lightweight.
- Avoids accidental secret leakage (`.env`).

### 7) Use native PostgreSQL COPY for bulk fact loading
Decision:
- In `load_data.py`, load `fact_trips` using chunked COPY instead of large INSERT batches.

Why:
- COPY is significantly faster and more stable for millions of rows.
- Chunked progress logs (`Chunk cargado: X/Y`) improve observability and reduce uncertainty during long loads.

Trade-off:
- Slightly more implementation complexity than a single `to_sql` call.

### 8) Create indexes after data load
Decision:
- Build indexes after loading all fact rows.

Why:
- Creating indexes during insert is slower than bulk load first, index second.
- Improves query performance for common filters and joins.

## Dependency Decisions (`requirements.txt`)

Current dependencies:
- `pandas`: dataframe operations and ETL logic.
- `pyarrow`: parquet read/write engine (primary).
- `fastparquet`: alternative parquet engine for compatibility.
- `sqlalchemy`: DB abstraction/connection management.
- `psycopg2-binary`: PostgreSQL driver used by SQLAlchemy.

Why both `pyarrow` and `fastparquet`:
- `pyarrow` is the preferred engine in practice.
- `fastparquet` is retained as fallback compatibility option.

## How To Run

### 1) Install dependencies
```bash
pip install -r requirements.txt
```

### 2) Configure environment
```bash
copy .env.example .env
```
Then edit `.env` with your local values.

### 3) Start PostgreSQL
```bash
docker compose up -d
```

### 4) Clean source data
```bash
python clean_data.py
```

### 5) Load into PostgreSQL
```bash
python load_data.py
```

## Known Performance Notes

- First full load of ~2.8M records can still take time depending on CPU/RAM and Docker disk performance on Windows.
- If load appears slow, watch chunk logs to confirm progress.
- Index creation happens after bulk load and can add extra minutes, but improves query speed afterward.

## Future Improvements

- Add incremental load mode (`append`) to avoid full table rebuilds on each run.
- Add explicit primary/foreign key constraints for stronger warehouse semantics.
- Add simple data quality reports (dropped row counts by rule).
- Add Makefile or task runner commands for one-command pipeline execution.
