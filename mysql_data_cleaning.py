import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from urllib.parse import quote_plus

import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError


# =============================================================================
# Paths / logging
# =============================================================================

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

DIAGNOSTICS_DIR = os.path.join(BASE_DIR, "diagnostics", "mysql_data_cleaning")
os.makedirs(DIAGNOSTICS_DIR, exist_ok=True)

LOG_FILE = os.path.join(DIAGNOSTICS_DIR, "data_cleaning.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("data_cleaner")

_PASSWORD_RE = re.compile(r":([^/@]{1,256})@")
_MIN_ROWS: dict[str, int] = {
    "reviews":            1,
    "review_aspects":     1,
    "property_id_mapping": 1,
    "properties":         1,
}


# =============================================================================
# Config
# =============================================================================

def load_config(env_path: str = "config.env") -> dict:
    load_dotenv(env_path)

    password = (os.getenv("MYSQL_PASSWORD") or "").strip()
    if not password:
        raise EnvironmentError("MYSQL_PASSWORD is missing or empty in config.env")

    return {
        "host":           (os.getenv("MYSQL_HOST")         or "127.0.0.1").strip(),
        "user":           (os.getenv("MYSQL_USER")          or "root").strip(),
        "password":       password,
        "raw_database":   (os.getenv("MYSQL_RAW_DATABASE")   or "flh_raw").strip(),
        "clean_database": (os.getenv("MYSQL_CLEAN_DATABASE") or "flh_clean").strip(),
    }


def _scrub(url: str) -> str:
    return _PASSWORD_RE.sub(":***@", url)


# =============================================================================
# Database helpers
# =============================================================================

def create_database_if_not_exists(cfg: dict, db_name: str) -> None:
    """Create db_name on the server if it doesn't already exist."""
    conn = mysql.connector.connect(
        host=cfg["host"], user=cfg["user"], password=cfg["password"]
    )
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        conn.commit()
        cursor.close()
        logger.info("Database '%s' is ready.", db_name)
    finally:
        conn.close()


def build_engine(cfg: dict, db_name: str):
    encoded_pw = quote_plus(cfg["password"])
    url = (
        f"mysql+mysqlconnector://{cfg['user']}:{encoded_pw}"
        f"@{cfg['host']}/{db_name}?charset=utf8mb4"
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=2,
        max_overflow=0,
    )


@contextmanager
def managed_engine(cfg: dict, db_name: str):
    """Context manager: build engine, yield it, dispose on exit."""
    engine = build_engine(cfg, db_name)
    try:
        yield engine
    finally:
        engine.dispose()
        logger.debug("Engine for '%s' disposed.", db_name)


# =============================================================================
# Atomic table swap (FIX — critical: no visibility gap for dashboards)
# =============================================================================

def load_table(df: pd.DataFrame, engine, table_name: str, clean_db: str,
               min_rows: int = 1) -> None:
    """
    Write df to clean DB using an atomic staging-table swap so that dashboards
    always see a complete table — never a partially written or missing one.

    Flow:
      1. Guard: refuse to write if df has fewer rows than min_rows.
      2. Write to  <table>_staging  (replace if exists).
      3. Verify row count in staging matches len(df).
      4. Within a single transaction: DROP <table>, RENAME staging → <table>.
    """
    t0 = time.time()
    staging = f"{table_name}_staging"

    # ── Guard: refuse empty / suspiciously small loads ───────────────────────
    if len(df) < min_rows:
        raise ValueError(
            f"[load_table] Refusing to load '{table_name}': "
            f"only {len(df)} rows (minimum required: {min_rows}). "
            "This may indicate an upstream data issue."
        )

    # ── Step 1: write to staging ──────────────────────────────────────────────
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{staging}`"))

    df.to_sql(staging, con=engine, if_exists="replace", index=False,
              method="multi", chunksize=500)

    # ── Step 2: verify staging row count ─────────────────────────────────────
    with engine.connect() as conn:
        db_count = conn.execute(text(f"SELECT COUNT(*) FROM `{staging}`")).scalar()

    if db_count != len(df):
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS `{staging}`"))
        raise RuntimeError(
            f"[load_table] Row count mismatch for '{table_name}': "
            f"DataFrame={len(df)}, staging DB={db_count}. Staging table dropped."
        )

    # ── Step 3: atomic swap ───────────────────────────────────────────────────
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
        conn.execute(text(f"RENAME TABLE `{staging}` TO `{table_name}`"))

    elapsed = time.time() - t0
    logger.info(
        "  Table `%s` loaded → `%s` (%d rows, %.1fs)",
        table_name, clean_db, len(df), elapsed,
    )


# =============================================================================
# Helpers
# =============================================================================

def extract_type(text_value):
    if pd.isna(text_value):
        return None
    match = re.search(r'"type"\s*:\s*"([^"]+)"', str(text_value))
    return match.group(1) if match else None


def rename_columns_if_needed(df: pd.DataFrame, rename_map: dict) -> pd.DataFrame:
    valid_map = {
        old: new
        for old, new in rename_map.items()
        if old in df.columns and old != new
    }
    if valid_map:
        logger.info("  Renaming columns: %s", valid_map)
    return df.rename(columns=valid_map)


# =============================================================================
# Postcode / floor cleaning  (unchanged logic, print → logger)
# =============================================================================

def split_portuguese_postcode(df, cod_col="wc_codpostal", zona_col="wc_zonapostal"):
    if cod_col not in df.columns:
        return df
    if zona_col not in df.columns:
        df[zona_col] = None

    cod_series  = df[cod_col].astype(str).str.strip()
    zona_series = df[zona_col]
    extracted   = cod_series.str.extract(r'^\s*(\d{4})\s*-\s*(\d{3})\s*$')

    mask_full = extracted[0].notna() & extracted[1].notna()
    df.loc[mask_full, cod_col] = extracted.loc[mask_full, 0]

    zona_empty = (
        zona_series.isna()
        | (zona_series.astype(str).str.strip() == "")
        | (zona_series.astype(str).str.lower().str.strip() == "nan")
    )
    df.loc[mask_full & zona_empty, zona_col] = extracted.loc[mask_full & zona_empty, 1]

    df[cod_col]  = df[cod_col].astype(str).str.extract(r'(\d{4})', expand=False)
    df[zona_col] = df[zona_col].astype(str).str.extract(r'(\d{3})', expand=False)

    invalid = (df[cod_col] == "0000") & (df[zona_col] == "000")
    df.loc[invalid, cod_col]  = None
    df.loc[invalid, zona_col] = None
    return df


def fix_postcode_zeros(df):
    affected = (df["postcode_prefix"] == 0).sum()
    df.loc[df["postcode_prefix"] == 0, "postcode_prefix"] = None
    df.loc[df["postcode_suffix"] == 0, "postcode_suffix"] = None
    if affected:
        logger.info("  [fix_postcode_zeros] %d rows nulled (postcode = 0)", affected)
    return df


def fix_inactive_date_conflict(df):
    mask     = (df["is_inactive"] == 0) & df["inactive_date"].notna()
    affected = mask.sum()
    df.loc[mask, "inactive_date"] = None
    if affected:
        logger.info(
            "  [fix_inactive_date_conflict] %d rows cleared "
            "(active properties with stale inactive_date)", affected
        )
    return df


def standardise_floor(val):
    if pd.isna(val):
        return val
    v = str(val).strip()
    if v == ".":
        return None
    v = v.replace("°", "º")
    v = re.sub(r"\s+º", "º", v)
    v = v.replace("ª", "º")
    if v in ("0", "0º", "Piso 0"):
        return "R/C"
    if re.match(r"^\d+$", v):
        return v + "º"
    v = re.sub(r"^r/c", "R/C", v, flags=re.IGNORECASE)
    v = re.sub(r"^RC(?=[^-]|$)", "R/C", v, flags=re.IGNORECASE)
    m = re.match(r"^piso\s+\d+\s*[-–]\s*(.+)$", v, re.IGNORECASE)
    if m:
        v = m.group(1).strip()
    return v


def fix_floor_encoding(df):
    before = df["floor"].copy()
    df["floor_clean"] = df["floor"].apply(standardise_floor)

    changed = (before != df["floor_clean"]) & ~(before.isna() & df["floor_clean"].isna())

    if changed.sum():
        preview = df.loc[changed, ["flh_property_id"]].copy()
        preview["floor_before"] = before[changed]
        preview["floor_after"]  = df.loc[changed, "floor_clean"]
        preview = preview.sort_values("flh_property_id")

        logger.info("\n  [fix_floor_encoding] %d values standardised", changed.sum())
        logger.info("\n  %-15s %-25s %s", "property_id", "floor_before", "floor_after")
        logger.info("  %s %s %s", "-" * 15, "-" * 25, "-" * 20)
        for _, row in preview.iterrows():
            b = str(row["floor_before"]) if pd.notna(row["floor_before"]) else "NULL"
            a = str(row["floor_after"])  if pd.notna(row["floor_after"])  else "NULL"
            logger.info("  %-15s %-25s %s", int(row["flh_property_id"]), b, a)

    df["floor"] = df["floor_clean"]
    df = df.drop(columns=["floor_clean"])
    return df


def fix_avantio_id_dtype(df):
    if "avantio_property_id" in df.columns:
        df["avantio_property_id"] = df["avantio_property_id"].astype("Int64")
        logger.info("  [fix_avantio_id_dtype] avantio_property_id cast to Int64")
    return df


# =============================================================================
# Main
# =============================================================================

def main(env_path: str = "config.env") -> None:
    logger.info("=== data_cleaner starting ===")

    cfg = load_config(env_path)
    clean_db = cfg["clean_database"]
    raw_db   = cfg["raw_database"]

    create_database_if_not_exists(cfg, clean_db)

    # ── Raw connection: use context manager so it always closes ───────────────
    # (FIX — critical: raw connection was never closed on connection failure)
    raw_conn = mysql.connector.connect(
        host=cfg["host"],
        user=cfg["user"],
        password=cfg["password"],
        database=raw_db,
    )

    try:
        # ── Clean engine: disposed in finally via context manager ─────────────
        # (FIX — warning: engine was created at module level and never disposed)
        with managed_engine(cfg, clean_db) as engine_clean:

            # ── 1. reviews ────────────────────────────────────────────────────
            logger.info("\n[1/4] reviews")
            t0 = time.time()
            df_reviews = pd.read_sql("SELECT * FROM reviews", raw_conn)
            logger.info("  Read %d rows from raw.reviews in %.1fs", len(df_reviews), time.time() - t0)

            df_reviews = df_reviews.drop(columns=["evaluationaspects"], errors="ignore")
            df_reviews = rename_columns_if_needed(df_reviews, {
                "id":                    "review_id",
                "_id":                   "mongo_id",
                "accommodationid":       "avantio_property_id",
                "arrivaldate":           "arrival_date",
                "bookingid":             "booking_id",
                "createdat":             "created_at",
                "customername":          "customer_name",
                "departuredate":         "departure_date",
                "externalid":            "external_id",
                "globalvaloration":      "overall_score",
                "neutralcomment":        "neutral_comment",
                "saleschannelcode":      "sales_channel_code",
                "response":              "response_text",
                "responsedate":          "response_date",
                "validated":             "is_validated",
                "negativecomment":       "negative_comment",
                "positivecomment":       "positive_comment",
                "title":                 "review_title",
                "iseligibleforresponse": "is_eligible_for_response",
                "externalresponsestatus":"external_response_status",
                "likes":                 "likes_count",
            })
            df_reviews = df_reviews.drop(columns=["neutral_comment"], errors="ignore")
            load_table(df_reviews, engine_clean, "reviews", clean_db,
                       min_rows=_MIN_ROWS["reviews"])

            # ── 2. review_aspects ─────────────────────────────────────────────
            logger.info("\n[2/4] review_aspects")
            t0 = time.time()
            df_eval = pd.read_sql("SELECT * FROM reviews_evaluationaspects", raw_conn)
            logger.info("  Read %d rows from raw.reviews_evaluationaspects in %.1fs",
                        len(df_eval), time.time() - t0)

            if "evaluationaspect" in df_eval.columns:
                df_eval["evaluation_aspect"] = df_eval["evaluationaspect"].apply(extract_type)
            elif "evaluation_aspect" in df_eval.columns:
                df_eval["evaluation_aspect"] = df_eval["evaluation_aspect"].apply(extract_type)

            cols_to_keep = []
            for col in ["id", "aspect_id"]:
                if col in df_eval.columns:
                    cols_to_keep.append(col)
                    break
            for col in ["parent_id", "review_id"]:
                if col in df_eval.columns:
                    cols_to_keep.append(col)
                    break
            if "created_at" in df_eval.columns:
                cols_to_keep.append("created_at")
            elif "createdAt" in df_eval.columns:
                cols_to_keep.append("createdAt")
            if "evaluation_aspect" in df_eval.columns:
                cols_to_keep.append("evaluation_aspect")
            if "valoration" in df_eval.columns:
                cols_to_keep.append("valoration")
            elif "score" in df_eval.columns:
                cols_to_keep.append("score")

            df_eval_clean = df_eval[cols_to_keep].copy()
            df_eval_clean = rename_columns_if_needed(df_eval_clean, {
                "id":                "aspect_id",
                "parent_id":         "review_id",
                "createdAt":         "created_at",
                "evaluation_aspect": "aspect",
                "valoration":        "score",
            })
            load_table(df_eval_clean, engine_clean, "review_aspects", clean_db,
                       min_rows=_MIN_ROWS["review_aspects"])

            # ── 3. property_id_mapping ────────────────────────────────────────
            logger.info("\n[3/4] property_id_mapping")
            t0 = time.time()
            df_idavantio = pd.read_sql("SELECT * FROM idavantio", raw_conn)
            logger.info("  Read %d rows from raw.idavantio in %.1fs",
                        len(df_idavantio), time.time() - t0)

            df_idavantio = rename_columns_if_needed(df_idavantio, {
                "idflh":     "flh_property_id",
                "idavantio": "avantio_property_id",
            })
            df_idavantio = fix_avantio_id_dtype(df_idavantio)
            load_table(df_idavantio, engine_clean, "property_id_mapping", clean_db,
                       min_rows=_MIN_ROWS["property_id_mapping"])

            # ── 4. properties ─────────────────────────────────────────────────
            logger.info("\n[4/4] properties")
            t0 = time.time()
            df_props = pd.read_sql("SELECT * FROM propriedades", raw_conn)
            logger.info("  Read %d rows from raw.propriedades in %.1fs",
                        len(df_props), time.time() - t0)

            df_props = split_portuguese_postcode(
                df_props, cod_col="wc_codpostal", zona_col="wc_zonapostal"
            )
            df_props = rename_columns_if_needed(df_props, {
                "id":            "flh_property_id",
                "equipa":        "team",
                "bairro":        "neighbourhood",
                "wc_codpostal":  "postcode_prefix",
                "wc_zonapostal": "postcode_suffix",
                "coordsgps":     "gps_coordinates",
                "inativo":       "is_inactive",
                "data_inicio":   "start_date",
                "data_inativo":  "inactive_date",
                "andar":         "floor",
                "elevador":      "has_elevator",
                "m_x_ocupa_o":   "max_occupancy",
                "numwcduche":    "num_bathrooms_with_shower",
                "numwcbasico":   "num_basic_bathrooms",
                "lugar_garagem": "garage_info",
                "rcm_maqroupa":  "has_washing_machine",
                "rcm_maqloica":  "has_dishwasher",
                "rcm_arcond":    "has_air_conditioning",
                "autocheckin":   "has_self_checkin",
            })

            df_props = fix_postcode_zeros(df_props)
            df_props = fix_floor_encoding(df_props)
            # fix_inactive_date_conflict — on hold, pending manual verification of the 22 properties
            # df_props = fix_inactive_date_conflict(df_props)

            load_table(df_props, engine_clean, "properties", clean_db,
                       min_rows=_MIN_ROWS["properties"])

        logger.info("\n✓ All tables loaded successfully")

    finally:
        # Always close the raw connection — even if engine setup or any step fails
        raw_conn.close()
        logger.debug("Raw MySQL connection closed.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clean MySQL raw → clean DB.")
    parser.add_argument("--env", default="config.env",
                        help="Path to config env file (default: config.env)")
    args, unknown = parser.parse_known_args()
    if unknown:
        logger.debug("Ignoring unknown arguments: %s", unknown)

    try:
        main(env_path=args.env)
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Unhandled error: %s", exc, exc_info=True)
        sys.exit(1)
