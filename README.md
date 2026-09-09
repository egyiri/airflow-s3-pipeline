# Airflow S3 ETL Pipeline

A practice data engineering project using Apache Airflow (Dockerized) to extract, transform, and load package delivery data through Amazon S3.

## Overview

This pipeline reads a raw CSV file from an S3 bucket, cleans and transforms it using pandas and DuckDB (SQL), and writes the transformed result back to a different S3 location, all orchestrated as an Airflow DAG.

## Pipeline Steps

1. **Extract** — Reads a CSV file from a source S3 bucket using Airflow's `S3Hook`.
2. **Transform** — Loads the CSV into a pandas DataFrame, cleans malformed/misaligned rows, and runs a SQL transformation using DuckDB to:
   - Calculate package volume from dimensions
   - Categorize packages by size (Small / Medium / Oversized)
   - Clean up missing scan statuses
3. **Load** — Writes the transformed CSV back to a destination S3 bucket.

## Tech Stack

- **Apache Airflow 3.3.1** (via Docker Compose)
- **AWS S3** (`apache-airflow-providers-amazon`)
- **pandas** for data cleaning
- **DuckDB** for SQL-based transformation
- **Docker** for local orchestration (Airflow webserver, scheduler, triggerer, dag-processor, worker, Postgres, Redis)

## Project Structure


## Setup

1. Clone this repo
2. Copy `.env.example` to `.env` and set `AIRFLOW_UID` (run `id -u` on Linux/WSL)
3. Build and start the containers:
```bash
   docker compose build
   docker compose up airflow-init
   docker compose up -d
```
4. Access the Airflow UI at `http://localhost:8080`
5. Add an `aws_default` connection in **Admin → Connections** with your AWS credentials
6. Trigger the `simple_dag` DAG

## Notes

- The source dataset contains some malformed rows (misaligned columns); the transform step detects and filters these out automatically.
- Scheduled to run daily at 9 AM (`0 9 * * *`).

## Data Source

Sample logistics/package delivery dataset (ALMRRC-style), used for practice purposes.
