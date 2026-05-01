import argparse
import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError


# =============================================================================
# Paths / constants
# =============================================================================

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

REFERENCE_DATA_DIR = os.path.join(BASE_DIR, "reference_data")

DIAGNOSTICS_DIR = os.path.join(BASE_DIR, "diagnostics")
PIPELINE_DIAGNOSTICS_DIR = os.path.join(
    DIAGNOSTICS_DIR,
    "load_reference_data_to_mysql_raw"
)
os.makedirs(PIPELINE_DIAGNOSTICS_DIR, exist_ok=True)

LOG_FILE_PATH = os.path.join(
    PIPELINE_DIAGNOSTICS_DIR,
    "db_loader.log"
)

# Files to load: (filename, separator, encoding, expected_columns)
CSV_FILES: List[Tuple[str, str, str, Optional[List[str]]]] = [
    ("IdAvantio.csv", ",", "utf-8-sig", None),
    ("propriedades.csv", ",", "utf-8-sig", None),
]

_DB_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
_COL_CLEAN_RE = re.compile(r"[^A-Za-z0-9_]+")
_PASSWORD_RE = re.compile(r":([^/@]{1,256})@")

_MAX_RETRIES = 3
_RETRY_BACKOFF_S = 2.0
_CHUNKSIZE = 500


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("db_loader")


# =============================================================================
# Environment / validation
# =============================================================================

def load_config(env_path: str = "config.env") -> dict:
    """Load and validate environment variables."""
    load_dotenv(env_path)

    password = os.getenv("MYSQL_PASSWORD", "").strip()
    if not password:
        raise EnvironmentError(
            "MYSQL_PASSWORD is missing or empty in the environment / config.env"
        )

    return {
        "host": (os.getenv("MYSQL_HOST", "127.0.0.1")).strip(),
        "user": (os.getenv("MYSQL_USER", "root")).strip(),
        "password": password,
        "database": (os.getenv("MYSQL_RAW_DATABASE", "flh_raw")).strip(),
    }


# =============================================================================
# Security helpers
# =============================================================================

def _scrub_password(text_: str) -> str:
    """Replace the password portion of a connection URL with '***'."""
    return _PASSWORD_RE.sub(":***@", text_)


# =============================================================================
# Database helpers
# =============================================================================

def _validate_db_name(db_name: str) -> None:
    if not _DB_NAME_RE.match(db_name):
        raise ValueError(
            f"Invalid database name '{db_name}'. "
            "Only letters, digits, and underscores are allowed."
        )


def create_database_if_not_exists(cfg: dict) -> None:
    """Create the target database if it does not already exist."""
    db_name = cfg["database"]
    _validate_db_name(db_name)

    server_cfg = {
        "host": cfg["host"],
        "user": cfg["user"],
        "password": cfg["password"],
    }

    logger.info("Ensuring database '%s' exists on %s ...", db_name, cfg["host"])

    conn = mysql.connector.connect(**server_cfg)
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        conn.commit()
        cursor.close()
        logger.info("Database '%s' is ready.", db_name)
    finally:
        conn.close()


def build_engine(cfg: dict) -> Engine:
    """Return a SQLAlchemy engine with connection-pool health checks."""
    encoded_password = quote_plus(cfg["password"])
    url = (
        f"mysql+mysqlconnector://{cfg['user']}:{encoded_password}"
        f"@{cfg['host']}/{cfg['database']}?charset=utf8mb4"
    )

    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=2,
        max_overflow=0,
        echo=False,
    )

    logger.info("SQLAlchemy engine created for '%s'.", cfg["database"])
    return engine


@contextmanager
def get_engine(cfg: dict):
    """Context manager that builds an engine and disposes it on exit."""
    engine = build_engine(cfg)
    try:
        yield engine
    finally:
        engine.dispose()
        logger.debug("SQLAlchemy engine disposed.")


def ping_engine(engine: Engine) -> None:
    """Raise if the database is unreachable."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


# =============================================================================
# CSV helpers
# =============================================================================

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise column names to lowercase snake_case with unique suffixes."""
    cleaned_cols = []
    for col in df.columns:
        clean_col = _COL_CLEAN_RE.sub("_", str(col).strip().lower()).strip("_")
        if not clean_col:
            clean_col = "unnamed_column"
        cleaned_cols.append(clean_col)

    seen: Dict[str, int] = {}
    unique_cols: List[str] = []

    for col in cleaned_cols:
        if col not in seen:
            seen[col] = 1
            unique_cols.append(col)
        else:
            new_col = f"{col}_{seen[col]}"
            seen[col] += 1
            unique_cols.append(new_col)

    df.columns = unique_cols
    return df


def table_name_from_file(file_name: str) -> str:
    table_name = os.path.splitext(file_name)[0].strip().lower()
    table_name = _COL_CLEAN_RE.sub("_", table_name).strip("_")
    return table_name


def _validate_schema(
    df: pd.DataFrame,
    expected_columns: Optional[List[str]],
    file_name: str,
) -> None:
    if not expected_columns:
        return

    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"Schema mismatch in '{file_name}': "
            f"expected columns not found after cleaning: {sorted(missing)}. "
            f"Actual columns: {sorted(df.columns.tolist())}"
        )


def load_csv_to_raw(
    file_name: str,
    engine: Engine,
    db_name: str,
    sep: str = ",",
    encoding: str = "utf-8-sig",
    expected_columns: Optional[List[str]] = None,
    retries: int = _MAX_RETRIES,
) -> int:
    file_path = os.path.join(REFERENCE_DATA_DIR, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    table_name = table_name_from_file(file_name)
    logger.info("Loading '%s' -> table '%s.%s' ...", file_name, db_name, table_name)

    df = pd.read_csv(
        file_path,
        sep=sep,
        encoding=encoding,
        low_memory=False,
        dtype=str,
    )
    df = clean_column_names(df)
    _validate_schema(df, expected_columns, file_name)

    if df.empty:
        logger.warning("'%s' is empty - skipping.", file_name)
        return 0

    row_count = len(df)
    logger.info("  Read %d rows, %d columns from CSV.", row_count, len(df.columns))

    last_exc: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as probe:
                table_exists = engine.dialect.has_table(probe, table_name)

            if table_exists:
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE `{table_name}`"))
                if_exists_mode = "append"
            else:
                if_exists_mode = "replace"

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists_mode,
                index=False,
                method="multi",
                chunksize=_CHUNKSIZE,
            )

            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                db_count = result.scalar()

            if db_count != row_count:
                raise RuntimeError(
                    f"Row count mismatch for '{table_name}': "
                    f"CSV={row_count}, DB={db_count}"
                )

            logger.info(
                "  Table '%s' loaded successfully (%d rows).",
                table_name,
                db_count,
            )
            return row_count

        except (OperationalError, SQLAlchemyError) as exc:
            safe_msg = _scrub_password(str(exc))
            last_exc = exc

            if attempt < retries:
                wait = _RETRY_BACKOFF_S * attempt
                logger.warning(
                    "  Attempt %d/%d failed (%s). Retrying in %.1fs ...",
                    attempt, retries, safe_msg, wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "  All %d attempts failed for '%s': %s",
                    retries, file_name, safe_msg,
                )

        except RuntimeError as exc:
            last_exc = exc

            if attempt < retries:
                wait = _RETRY_BACKOFF_S * attempt
                logger.warning(
                    "  Attempt %d/%d failed (%s). Retrying in %.1fs ...",
                    attempt, retries, exc, wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "  All %d attempts failed for '%s': %s",
                    retries, file_name, exc,
                )

    if last_exc is not None:
        raise last_exc

    raise RuntimeError(f"Unexpected failure loading file: {file_name}")


# =============================================================================
# Main
# =============================================================================

def main(env_path: str = "config.env") -> None:
    logger.info("=== db_loader starting ===")
    logger.info("Base directory          : %s", BASE_DIR)
    logger.info("Reference data directory: %s", REFERENCE_DATA_DIR)
    logger.info("Diagnostics directory   : %s", PIPELINE_DIAGNOSTICS_DIR)
    logger.info("Log file path           : %s", LOG_FILE_PATH)

    if not os.path.isdir(REFERENCE_DATA_DIR):
        logger.error("Reference data directory not found: %s", REFERENCE_DATA_DIR)
        raise SystemExit(1)

    logger.info("Files present: %s", os.listdir(REFERENCE_DATA_DIR))

    try:
        cfg = load_config(env_path)
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        raise SystemExit(1) from exc

    try:
        create_database_if_not_exists(cfg)
    except Exception as exc:
        logger.error("Failed to create database: %s", exc)
        raise SystemExit(1) from exc

    total_rows = 0
    failed_files: List[str] = []

    with get_engine(cfg) as engine:
        try:
            ping_engine(engine)
        except Exception as exc:
            logger.error("Cannot reach database: %s", _scrub_password(str(exc)))
            raise SystemExit(1) from exc

        for file_name, sep, encoding, expected_columns in CSV_FILES:
            try:
                rows = load_csv_to_raw(
                    file_name=file_name,
                    engine=engine,
                    db_name=cfg["database"],
                    sep=sep,
                    encoding=encoding,
                    expected_columns=expected_columns,
                )
                total_rows += rows

            except FileNotFoundError as exc:
                logger.error("Missing file - %s", exc)
                failed_files.append(file_name)

            except Exception as exc:
                logger.error(
                    "Failed to load '%s': %s",
                    file_name,
                    _scrub_password(str(exc)),
                    exc_info=True,
                )
                failed_files.append(file_name)

    logger.info("=== Load complete ===")
    logger.info("  Total rows written : %d", total_rows)
    logger.info("  Files succeeded    : %d", len(CSV_FILES) - len(failed_files))
    logger.info("  Files failed       : %d", len(failed_files))

    if failed_files:
        logger.error("Failed files: %s", failed_files)
        raise SystemExit(1)

    logger.info("Raw reference data load finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load CSV reference data into MySQL.")
    parser.add_argument(
        "--env",
        default="config.env",
        help="Path to the .env config file (default: config.env)",
    )

    # Important: Jupyter passes extra internal arguments like --f=...
    # parse_known_args() ignores them, so the same file works in both:
    # - python db_loader.py
    # - Jupyter / IPython
    args, unknown = parser.parse_known_args()

    if unknown:
        logger.debug("Ignoring unknown arguments: %s", unknown)

    main(env_path=args.env)