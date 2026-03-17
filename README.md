# Invers Challenge - NYC TLC Data Pipeline

This project builds a dynamic ETL pipeline for NYC TLC Yellow Taxi data across any given year:
1. Download, validate, and clean raw parquet data by month.
2. Load each month incrementally into PostgreSQL (Docker) to avoid memory overload.
3. Enrich data with taxi zone metadata.
4. Visualize results in an interactive Streamlit dashboard with month filtering.

## Project Files

- `run_pipeline.py`: Main orchestrator; downloads and processes all 12 months for a given year with retry logic.
- `clean_data.py`: Validates and cleans monthly data; enforces business rules and month-boundary constraints.
- `load_data.py`: Loads cleaned parquet into PostgreSQL with support for replace/append modes.
- `load_taxi_zones.py`: Downloads and loads the TLC taxi zone lookup as a dimension table.
- `dashboard.py`: Streamlit dashboard; queries the DB and visualizes trends with a sidebar month filter.
- `check_data_size.py`: Utility to report file sizes in `data/` directory.
- `docker-compose.yml`: Local PostgreSQL 15 service.
- `.env.example`: Template for environment variables (DB credentials, host, port).
- `requirements.txt`: Python dependencies (pandas, pyarrow, sqlalchemy, psycopg2, streamlit, plotly, python-dotenv).

## Architecture Decisions and Justification

### 1) Dynamic year-based pipeline with month-by-month streaming
Decision:
- Process 12 months independently in a single orchestrated run.
- Clean one month, immediately load to PostgreSQL, then move to the next month.
- No concatenation of all year data in memory before loading.

Why:
- Scales to large years without memory pressure.
- Preserves progress: if month 6 fails, months 1–5 are already in the database.
- Gracefully handles months that don't exist yet or are unavailable.

Trade-off:
- Slightly more I/O than batch loading all at once, but negligible for typical hardware.

### 2) Enforce month-boundary validation during cleaning
Decision:
- Every trip's pickup time must fall within `(first_day_of_month, first_day_of_next_month)`.
- Extract expected month from source URL or accept as parameter.

Why:
- Prevents "stale" trips from previous/future months contaminating monthly buckets.
- Catches data quality issues early, before load.

### 3) Retry logic with configurable backoff for transient network failures
Decision:
- Retry each month's download up to `--retries` times with `--retry-wait` seconds between attempts.
- Continue to next month on persistent failure rather than crashing.

Why:
- CloudFront and CDNs occasionally rate-limit or reset connections.
- Allows operator to tune for network conditions (e.g., `--retries 5 --retry-wait 10`).
- Logs each attempt for debugging.

### 4) Incremental append-mode loading into PostgreSQL
Decision:
- First month: `replace` mode (create/wipe tables, load dimensions, defer indexes).
- Subsequent months: `append` mode (skip dimensions, skip indexes).
- Create all indexes once at the end after all months are loaded.

Why:
- Avoids recreating dimensions (static) for every month.
- Index creation post-load is faster than creating during inserts.
- Allows safe resumption if interrupted.

### 5) Separate taxi zone enrichment script
Decision:
- `load_taxi_zones.py` is a standalone loader for the TLC zone lookup.
- Dashboard left-joins `dim_taxi_zone` to display zone names instead of numeric IDs.

Why:
- Decouples zone data from trip fact tables.
- Dimensions are immutable and downloaded once per pipeline run.
- Dashboard can gracefully degrade if zone table is missing.

### 6) Interactive Streamlit dashboard with month filtering
Decision:
- Dashboard dynamically queries available months from the database.
- Sidebar multiselect allows filtering by month(s) in real time.
- All KPIs and charts respond to the filter without reloading data.

Why:
- Enables comparative analysis (Jan vs Feb, or Jan+Feb combined).
- Avoids rerunning the pipeline to visualize different periods.
- Responsive and user-friendly.

### 7) Use PostgreSQL in Docker for local reproducibility
Decision:
- Run PostgreSQL 15 via `docker-compose.yml`.

Why:
- Team members can run the same DB setup without local Postgres installation differences.
- Easy reset: `docker compose down -v` wipes the volume.

### 8) Separate pipeline and dashboard execution
Decision:
- `run_pipeline.py` is the data layer; `dashboard.py` is the presentation layer.
- User runs both independently.

Why:
- Faster iteration: reload dashboard without re-running the 1-2 hour pipeline.
- Pipeline runs in CI/batch; dashboard runs in dev or on a server.

### 9) Use parquet as intermediate storage
Decision:
- Keep cleaned dataset as parquet in `data/clean/` before loading to PostgreSQL.

Why:
- Parquet is columnar and compressed, faster and smaller than CSV for analytics.
- Pandas can read/write parquet efficiently with `pyarrow` or `fastparquet`.

### 10) Standardize columns to lowercase during cleaning
Decision:
- Convert all source column names to lowercase in `clean_data.py`.

Why:
- Avoids schema drift bugs (e.g., `Airport_fee` vs `airport_fee`).
- Simplifies downstream selection/renaming.

### 11) Apply business-quality filters before loading
Decision:
- Remove clearly invalid trips (non-positive distance/fares, invalid durations, key nulls).

Why:
- Prevents low-quality records from contaminating analytics and fact tables.
- Enforces baseline data validity close to ingestion.

### 12) Use native PostgreSQL COPY for bulk fact loading
Decision:
- In `load_data.py`, load `fact_trips` using chunked COPY instead of large INSERT batches.

Why:
- COPY is significantly faster and more stable for millions of rows.
- Chunked progress logs (`Chunk cargado: X/Y`) improve observability.

Trade-off:
- Slightly more implementation complexity than a single `to_sql` call.

## Dependency Decisions

Current dependencies:
- `pandas`: Dataframe operations, ETL logic.
- `pyarrow`: Parquet read/write (primary engine).
- `fastparquet`: Fallback parquet compatibility.
- `sqlalchemy`: DB abstraction and connection pooling.
- `psycopg2-binary`: PostgreSQL driver.
- `streamlit`: Interactive dashboard framework.
- `plotly`: Chart library (used by Streamlit).
- `python-dotenv`: Load `.env` files for credentials.

Why:
- `pyarrow` is the industry standard for parquet; `fastparquet` is a safety net.
- `sqlalchemy` + `psycopg2` provide robust connection handling.
- `streamlit` offers rapid UI development with Pythonic syntax.
- `plotly` integrates seamlessly with Streamlit for interactive charts.

## Prerequisites

Before running any component, ensure you have:

1. **Python 3.10+** installed (tested with Python 3.12.4).
2. **Docker and Docker Compose** installed for running PostgreSQL.
3. A **`.env` file** in the project root with database credentials:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=tlc_taxi_db
   DB_USER=postgres
   DB_PASSWORD=your_secure_password
   ```
4. **Virtual environment activated** with dependencies installed:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

## How To Run

### Option A: Full Setup (Pipeline + Dashboard)

Run the complete ETL pipeline and then launch the dashboard:

```bash
# 1. Start PostgreSQL in Docker
docker-compose up -d

# 2. Run the pipeline for a specific year (e.g., 2025)
python run_pipeline.py --year 2025

# 3. In a separate terminal, run the dashboard
streamlit run dashboard.py
```

The dashboard will open at `http://localhost:8501`.

### Option B: Pipeline Only

Load data without running the dashboard:

```bash
# 1. Start PostgreSQL in Docker
docker-compose up -d

# 2. Run the pipeline with custom retry settings
python run_pipeline.py --year 2025 --retries 5 --retry-wait 10
```

**CLI Arguments:**
- `--year` (int, default `2025`): Year to load (e.g., `2024`, `2023`).
- `--retries` (int, default `3`): Number of download retry attempts per month.
- `--retry-wait` (int, default `5`): Seconds to wait between retry attempts.

**Output:**
- Cleaned parquet files saved to `data/clean/`.
- Data loaded into PostgreSQL tables (`fact_trips`, `dim_payment_type`, `dim_ratecode`, `dim_taxi_zone`).
- Console logs showing progress and any months that failed to load.

### Option C: Dashboard Only (Data Pre-Loaded)

If the database is already populated from a prior pipeline run:

```bash
streamlit run dashboard.py
```

The dashboard queries the PostgreSQL database and displays all loaded months.

### Manual Database Cleanup

If you need to reset the database:

**Option 1: Drop Tables Only (Keep PostgreSQL running)**
```bash
python -c "
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
db_url = f\"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}\"
engine = create_engine(db_url)
with engine.connect() as conn:
    conn.execute(text('DROP TABLE IF EXISTS fact_trips CASCADE'))
    conn.execute(text('DROP TABLE IF EXISTS dim_payment_type CASCADE'))
    conn.execute(text('DROP TABLE IF EXISTS dim_ratecode CASCADE'))
    conn.execute(text('DROP TABLE IF EXISTS dim_taxi_zone CASCADE'))
    conn.commit()
print('Database tables dropped.')
"
```

**Option 2: Full Reset (Drop Volume)**
```bash
docker-compose down -v
```

This removes all PostgreSQL data and requires running `docker-compose up -d` again to recreate the container and volume.

**Option 3: Selective Deletion** (e.g., delete 2025 data only)
```bash
python -c "
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
db_url = f\"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}\"
engine = create_engine(db_url)
with engine.connect() as conn:
    conn.execute(text(\"DELETE FROM fact_trips WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2025\"))
    conn.commit()
print('2025 data deleted from fact_trips.')
"
```

## Dashboard Features

The interactive dashboard displays five KPIs filtered by selected month(s):

1. **Daily Trip Volume** (Line Chart)
   - Shows trip count per day across selected months.
   - Useful for identifying peak travel periods.

2. **Top Pickup Zones by Revenue** (Bar Chart)
   - Displays zones with highest total fare revenue.
   - Color-coded by borough (Manhattan, Bronx, Queens, Brooklyn, Staten Island, etc.).
   - Hover to see zone name, location ID, and total revenue.

3. **Average Trip Duration** (Metric)
   - Mean time spent in vehicle across all filtered trips.

4. **Incident Rate** (Metric)
   - Percentage of trips with payment type "Dispute" or "Unknown".

5. **Average Cost per Mile by Hour** (Bar Chart)
   - Shows $ per mile for each hour of day (0–23).
   - Useful for understanding if peak hours have higher per-mile costs.

**Month Filter:**
- Use the sidebar multiselect to choose which months to display.
- Default: all available months.
- Charts update in real-time without any data reload.

## Known Performance Notes

- **First Pipeline Run:** 1–2 hours for a full year (12 months) depending on internet speed and PC specifications.
- **Retry Overhead:** Each retry adds `--retry-wait` seconds per failed month.
- **Dashboard Load Time:** First load queries available months (~1 second); subsequent filter selections are instant.
- **CloudFront Rate-Limiting:** If you experience frequent `WinError 10054` or connection resets, increase `--retry-wait` to 15–30 seconds.

## Troubleshooting

### Issue: `WinError 10054` connection reset during download

**Cause:** CloudFront resets connections when rate-limited (bot protection, rapid successive requests).

**Solutions:**
1. Wait 15–30 minutes before rerunning the pipeline.
2. Increase wait time between retries:
   ```bash
   python run_pipeline.py --year 2025 --retry-wait 30 --retries 5
   ```
3. Check the `run_pipeline.py` logs to see which month failed. You can then manually download that month via a browser or with `curl`, then re-run the pipeline using append mode.

### Issue: Dashboard shows "Missing or invalid selections" error

**Cause:** The `dim_taxi_zone` table was not loaded (either skipped or `load_taxi_zones.py` failed).

**Solution:** The dashboard gracefully falls back to showing location IDs. If you want zone names, re-run the pipeline to populate `dim_taxi_zone`:
```bash
python run_pipeline.py --year 2025
```
The load_taxi_zones.py function runs in Phase 4 of the pipeline.

### Issue: PostgreSQL connection refused

**Cause:** Docker container is not running or credentials are incorrect.

**Solutions:**
1. Check container status:
   ```bash
   docker-compose ps
   ```
2. Ensure PostgreSQL is running:
   ```bash
   docker-compose up -d
   ```
3. Verify `.env` credentials match `docker-compose.yml` values.

### Issue: Out of memory (OOM) during load

**Cause:** Unlikely with streaming model; possible if custom code loads all months at once in memory.

**Solution:** Streaming model processes one month at a time. If you increase chunk size in `load_data.py`, reduce it back to 250,000 rows.

### Issue: Dashboard shows data from old pipeline run

**Cause:** PostgreSQL tables still contain old data; new pipeline run didn't clear them.

**Solution:** 
- Option 1: Restart with clean slate:
  ```bash
  docker-compose down -v
  docker-compose up -d
  python run_pipeline.py --year 2025
  ```
- Option 2: Drop tables manually (see Manual Database Cleanup above).

## Future Improvements

- Add `--reset-db` CLI flag to `run_pipeline.py` for one-command table reset.
- Add local file caching to avoid re-fetching on retry (useful for slow internet).
- Export Prometheus metrics for monitoring pipeline health.
- Build data quality reports showing dropped row counts by validation rule.
- Add unit tests for cleaning and validation logic.

