#!/usr/bin/env bash
set -e
export DATABASE_URL=${DATABASE_URL:-postgresql://pguser:pgpass@localhost:5432/salesdb}
python3 src/etl.py
