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
8. Builds a **star schema** optimised for BI tools and dashboards (e.g. Power BI), using a blue/green swap to keep the live database available with zero downtime during rebuilds

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
  MySQL — flh_raw           ← also receives CSV reference data
      │
      ▼
  MySQL — flh_clean
      │
      ▼
  MySQL — flh_relational    ← normalised relational schema
      │
      ▼
  Validation (cross-layer consistency checks)
      │
      ▼
  MySQL — flh_star          ← star schema for Power BI / dashboards
        (built in flh_star_shadow, then swapped live with zero downtime)
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
├── build_star_schema.py                 # Step 7 — MySQL relational → star schema
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
    ├── validate_pipeline/
    └── build_star_schema/
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
numpy
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
MYSQL_STAR_DATABASE=flh_star
MYSQL_STAR_SHADOW_DATABASE=flh_star_shadow

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
| 7 | `build_star_schema.py` | MySQL relational → star schema | **Hard** — pipeline halts |

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

### Relational model (flh_relational)

`build_relational_schema.py` reads from `flh_clean` and builds a normalised relational schema in `flh_relational`, with proper data types, primary keys, indexes, and foreign key constraints.

**Tables**

| Table | Description |
|-------|-------------|
| `properties` | One row per property — includes team, neighbourhood, floor, postcode, GPS coordinates (split into `latitude` and `longitude`), occupancy, amenities (elevator, washing machine, dishwasher, air conditioning, self check-in), and active/inactive status |
| `property_id_mapping` | Maps each internal `flh_property_id` to its corresponding `avantio_property_id` — used to join reviews (which carry the Avantio ID) to properties (which carry the internal ID) |
| `reviews` | One row per guest review — includes booking ID, sales channel, arrival and departure dates, overall score, language, guest name, positive/negative comments, and response text and date |
| `review_aspects` | One row per scored aspect per review — each review can have multiple aspect scores (e.g. Location, Cleanliness, Value for Money, Service, Accommodation) linked back to `reviews` via a foreign key |

**Key design decisions**

- GPS coordinates stored as a combined string in the source are split into separate `latitude` and `longitude` float columns.
- Four near-empty operational columns are dropped from `reviews` during this step (`hash_evaluationaspects`, `is_eligible_for_response`, `external_response_status`, `likes_count`).
- The foreign key between `property_id_mapping` and `properties` is intentionally not enforced — the mapping file contains historical property IDs that no longer exist in the current properties table. Joins use `LEFT JOIN` to handle this gracefully.
- Similarly, the foreign key between `reviews` and `property_id_mapping` is not enforced — a small number of reviews reference Avantio property IDs not present in the mapping (properties added in Avantio after the mapping file was last exported).
- `review_aspects` enforces a cascading foreign key to `reviews` — deleting a review automatically removes its aspect scores.

**Blue/green swap (zero-downtime rebuild)**

The relational schema is always built into a staging database (`flh_relational_new`) first. Once all four tables are built successfully, they are atomically swapped into the live database (`flh_relational`) using a single MySQL `RENAME TABLE` statement. The previous live data is briefly moved to `flh_relational_old` and then dropped. If the build fails at any point, `flh_relational` remains completely unchanged.

---

### Star schema (flh_star)

`build_star_schema.py` reads from `flh_relational` and builds a star schema in `flh_star`, optimised for Power BI and other BI tools.

**Dimension tables**

| Table | Description |
|-------|-------------|
| `dim_date` | Calendar dimension with year, quarter, month, week, day, and weekend flag — covering all dates present in the review data |
| `dim_property` | Property master data merged with Avantio ID mapping, including amenity labels and active/inactive status |
| `dim_sales_channel` | Booking channel dimension (Airbnb, Booking.com, HomeAway/VRBO, Direct) with a human-readable label |
| `dim_aspect` | Review aspect dimension (Location, Cleanliness, Value for Money, Service, Accommodation, etc.) |

**Fact tables**

| Table | Description |
|-------|-------------|
| `fact_reviews` | One row per review — links to all four dimensions via surrogate keys, plus measures: overall score, stay length, response flag, comment flags |
| `fact_review_aspects` | One row per aspect score — links to `dim_aspect`, `dim_property`, `dim_sales_channel`, and `dim_date` |

**Blue/green swap (zero-downtime rebuild)**

The star schema is always built into a shadow database (`flh_star_shadow`) first. Once the build succeeds, all tables are atomically swapped into the live database (`flh_star`) using MySQL `RENAME TABLE`. Power BI reads from `flh_star` throughout and is never exposed to a partially built state. If the build fails, `flh_star` remains unchanged.

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
| `diagnostics/build_star_schema/build_star_schema.log` | Star schema build log |

---

## Security Notes

- **Never commit `config.env`** — it contains all credentials. It is already listed in `.gitignore`.
- All passwords are scrubbed from log output before writing (`***` is substituted).
- The pipeline uses parameterised queries and validated database/table names to prevent SQL injection.
- Reference data CSVs may contain sensitive business data — review before committing.
