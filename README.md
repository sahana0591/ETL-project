# Data Engineering ETL — Sample Project

This repository implements a small but realistic **batch ETL** pipeline:
- **Extract**: read CSV files from `data/`
- **Transform**: clean and type-cast, compute derived fields
- **Load**: write into a Postgres table using an idempotent staging approach

This is deliberately simple so you can run it locally, iterate, and extend to Airflow / Spark / Kafka.

## What's included
- `data/sales.csv` — sample input
- `src/etl.py` — main ETL script (extract, transform, load)
- `src/db.py` — database helper (SQLAlchemy engine)
- `src/schema.sql` — SQL to create target table
- `docker-compose.yml` — runs Postgres locally
- `tests/test_etl.py` — unit tests for transform logic
- GitHub Actions workflow for CI

## Quick start (macOS / Linux)
1. Start Postgres:
```bash
docker-compose up -d
```

2. Create the table (once):
```bash
# waits for Postgres to be ready; default creds are in docker-compose.yml
export PGPASSWORD=pgpass
psql -h localhost -U pguser -d salesdb -f src/schema.sql
```

3. Run the ETL:
```bash
export DATABASE_URL=postgresql://pguser:pgpass@localhost:5432/salesdb
python3 src/etl.py
```

4. Run tests:
```bash
pip install -r requirements.txt
pytest -q
```

## To push to GitHub
```bash
git init
git add .
git commit -m "Initial ETL project"
# create repo on GitHub and push (or use `gh repo create`)
git remote add origin https://github.com/<you>/<repo>.git
git branch -M main
git push -u origin main
```

## Next steps / extensions for interview talks
- Add an Airflow DAG to schedule the ETL and show retries/observability
- Replace Pandas with PySpark for big data
- Add tests for DB load (integration tests using a dockerized Postgres)
- Add data quality checks with Great Expectations
- Add CI setup for linting, type-checking, and deploying to a cloud data warehouse
