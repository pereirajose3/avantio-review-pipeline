# avantio-review-pipeline

# avantio-review-pipeline

A fully automated ETL pipeline that collects guest reviews from the **Avantio API**, stores them in **MongoDB**, and progressively transforms them through a layered **MySQL** architecture — raw → clean → relational — with built-in validation, alerting, and scheduling support.

---

## Table of Contents

- [Overview](#overview)
- [Pipeline Architecture](#pipeline-architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Reference Data](#reference-data)
- [How to Run](#how-to-run)
- [Scheduling with Cron](#scheduling-with-cron)
- [Pipeline Steps](#pipeline-steps)
- [Logs and Diagnostics](#logs-and-diagnostics)
- [Security Notes](#security-notes)

---

## Overview

This project automates the full lifecycle of guest review data for a property management operation:

1. Fetches paginated reviews from the **Avantio PMS API**
2. Stores raw reviews in **MongoDB** (upsert by review ID — safe to re-run)
3. Migrates the data to **MySQL** in a raw layer
4. Loads supplementary CSV reference files (property mappings, identifiers)
5. Cleans and standardises the data into a clean MySQL layer
6. Builds a fully normalised relational schema ready for reporting or BI tools
7. Validates data consistency across all three database layers

The pipeline is orchestrated by a single script (`run_pipeline.py`) that handles step sequencing, soft/hard failure modes, email alerting, and lock-file protection against concurrent runs.

---

## Pipeline Architecture

```
Avantio PMS API
      │
      ▼
  MongoDB (raw reviews)
      │
      ▼
  MySQL — flh_raw        ← also receives CSV reference data
      │
      ▼
  MySQL — flh_clean
      │
      ▼
  MySQL — flh_relational  ← ready for reporting / BI
      │
      ▼
  Validation (cross-layer consistency checks)
```

---

## Project Structure

```
avantio-review-pipeline/
│
├── run_pipeline.py                      # Master orchestrator
├── run_pipeline.sh                      # Shell wrapper for cron
├── config.env                           # ⚠ NOT committed — see Configuration
│
├── getReviews.py                        # Step 1 — Avantio API → MongoDB
├── MongoMysql.py                        # Step 2 — MongoDB → MySQL raw
├── load_reference_data_to_Mysql_raw.py  # Step 3 — CSV files → MySQL raw
├── mysql_data_cleaning.py               # Step 4 — MySQL raw → MySQL clean
├── build_relational_schema.py           # Step 5 — MySQL clean → MySQL relational
├── validate_pipeline.py                 # Step 6 — Cross-layer validation
│
├── reference_data/                      # CSV reference files (not committed)
│   ├── IdAvantio.csv
│   └── propriedades.csv
│
└── diagnostics/                         # Auto-created at runtime
    ├── pipeline_central.log             # Unified log across all runs
    ├── pipeline_runner/                 # Per-run logs
    ├── getReviews/
    ├── MongoMysql/
    ├── load_reference_data_to_mysql_raw/
    ├── mysql_data_cleaning/
    ├── build_relational_schema/
    └── validate_pipeline/
```

---

## Prerequisites

- **Python** 3.10 or higher
- **MongoDB** instance (local or remote)
- **MySQL** instance (local or remote)
- An active **Avantio API key**
- Internet access to reach `https://api.avantio.pro`

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/your-username/avantio-review-pipeline.git
cd avantio-review-pipeline

# 2. (Recommended) Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate        # macOS / Linux
# venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

### requirements.txt

```
requests
pymongo
mysql-connector-python
sqlalchemy
pandas
python-dotenv
```

---

## Configuration

Create a file called `config.env` in the project root. This file is **never committed to version control**.

Use the template below and fill in your own values:

```env
# ── Avantio API ───────────────────────────────────────────
AVANTIO_KEY=your_avantio_api_key_here
BASE_API_URL=https://api.avantio.pro/pms/v2/reviews

# ── MongoDB ───────────────────────────────────────────────
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USER=your_mongo_user
MONGO_PASSWORD=your_mongo_password
MONGO_AUTH_SOURCE=admin
MONGO_TIMEOUT=5000
MONGO_DB_NAME=flh
MONGO_COLLECTION_REVIEWS=reviews

# ── MySQL ─────────────────────────────────────────────────
MYSQL_HOST=127.0.0.1
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_RAW_DATABASE=flh_raw
MYSQL_CLEAN_DATABASE=flh_clean
MYSQL_REL_DATABASE=flh_relational

# ── Email Alerts ──────────────────────────────────────────
ALERT_EMAIL_FROM=your_email@gmail.com
ALERT_EMAIL_TO=recipient@example.com
ALERT_EMAIL_PASSWORD=your_app_password
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587

# ── API Behaviour (optional — defaults shown) ─────────────
API_SLEEP=1
REQUEST_TIMEOUT=60
MAX_RETRIES=3
RETRY_BACKOFF_BASE_SECONDS=5
```

> **Gmail users:** use an [App Password](https://support.google.com/accounts/answer/185833), not your regular account password.

---

## Reference Data

Before running the pipeline, place the following CSV files inside a `reference_data/` folder in the project root:

| File | Description |
|------|-------------|
| `IdAvantio.csv` | Mapping between internal property IDs and Avantio IDs |
| `propriedades.csv` | Property master data (names, types, locations, etc.) |

Both files must be UTF-8 encoded with a comma separator. The loader will automatically clean column names to lowercase snake_case.

---

## How to Run

### Run manually

```bash
python3 run_pipeline.py
```

### Run via shell script

```bash
bash run_pipeline.sh
```

The pipeline will:
- Create a lock file (`pipeline.lock`) to prevent concurrent runs
- Execute each step in sequence
- Send an email alert on any failure
- Release the lock file on exit (even if a crash occurs)

---

## Scheduling with Cron

To run the pipeline automatically every day at 3:00 AM:

```bash
crontab -e
```

Add the following line:

```
0 3 * * * /bin/bash /full/path/to/run_pipeline.sh >> /full/path/to/diagnostics/cron.log 2>&1
```

Make sure `run_pipeline.sh` points to the correct Python interpreter and project directory.

---

## Pipeline Steps

| Step | Script | Databases | Fail Mode |
|------|--------|-----------|-----------|
| 1 | `getReviews.py` | Avantio API → MongoDB | **Soft** — pipeline continues |
| 2 | `MongoMysql.py` | MongoDB → MySQL raw | **Hard** — pipeline halts |
| 3 | `load_reference_data_to_Mysql_raw.py` | CSV → MySQL raw | **Hard** — pipeline halts |
| 4 | `mysql_data_cleaning.py` | MySQL raw → MySQL clean | **Hard** — pipeline halts |
| 5 | `build_relational_schema.py` | MySQL clean → MySQL relational | **Hard** — pipeline halts |
| 6 | `validate_pipeline.py` | Cross-layer consistency checks | **Soft** — pipeline continues |

**Soft fail:** an alert email is sent but the remaining steps still run.  
**Hard fail:** an alert email is sent and the pipeline stops immediately.

### What the validation step checks

- Row counts are consistent across all three MySQL layers
- No layer is suspiciously empty (minimum row thresholds)
- Primary keys are unique and not null
- Review scores are within the valid range (0–10)
- Date logic is valid (departure ≥ arrival, no future created_at dates)
- No nulls in required business columns
- No orphaned review aspects (aspects without a matching review)
- Avantio IDs and review IDs are consistent across layers

---

## Logs and Diagnostics

All logs are written to the `diagnostics/` folder, which is created automatically on first run.

| Log file | Contents |
|----------|----------|
| `diagnostics/pipeline_central.log` | Unified log of every pipeline run |
| `diagnostics/pipeline_runner/run_YYYYMMDD_HHMMSS.log` | Per-run detailed log |
| `diagnostics/getReviews/avantio_pipeline.log` | API fetch log |
| `diagnostics/getReviews/avantio_failed_pages.log` | Pages that failed after all retries |
| `diagnostics/getReviews/avantio_bad_payloads.log` | Malformed API responses |
| `diagnostics/mysql_data_cleaning/data_cleaning.log` | Cleaning step log |
| `diagnostics/build_relational_schema/build_relational_schema.log` | Schema build log |
| `diagnostics/validate_pipeline/validate_pipeline.log` | Validation results |

---

## Security Notes

- **Never commit `config.env`** — it contains all credentials. It is already listed in `.gitignore`.
- All passwords are scrubbed from log output before writing (`***` is substituted).
- The pipeline uses parameterised queries and validated database/table names to prevent SQL injection.
- Reference data CSVs may contain sensitive business data — review before committing.
