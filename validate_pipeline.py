"""
validate_pipeline.py
====================
Validates data consistency across all 3 pipeline databases:
  flh_raw → flh_clean → flh_relational

Checks performed:
  1.  Row counts          — no data lost between layers
  2.  Minimum rows        — no layer is suspiciously empty
  3.  Key integrity       — PKs are unique and not null
  4.  Score ranges        — overall_score and aspect scores within 0-10
  5.  Date logic          — departure >= arrival, no future created_at
  6.  Critical nulls      — no nulls in non-nullable business columns
  7.  Orphaned aspects    — no review_aspects without a matching review
  8.  Cross-db consistency — same avantio_ids, same review_ids

Run directly:
  python3 validate_pipeline.py

Or add as step 6/6 in run_pipeline.py (soft fail recommended —
validation failure should alert but not block the pipeline).
"""

import logging
import os
import sys
from datetime import date
from urllib.parse import quote_plus

import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# =============================================================================
# Paths & logging
# =============================================================================

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

DIAGNOSTICS_DIR = os.path.join(BASE_DIR, "diagnostics", "validate_pipeline")
os.makedirs(DIAGNOSTICS_DIR, exist_ok=True)
LOG_FILE = os.path.join(DIAGNOSTICS_DIR, "validate_pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("validate_pipeline")


# =============================================================================
# Config
# =============================================================================

def load_config() -> dict:
    load_dotenv(os.path.join(BASE_DIR, "config.env"))
    password = os.getenv("MYSQL_PASSWORD", "").strip()
    if not password:
        raise EnvironmentError("MYSQL_PASSWORD is missing in config.env")
    return {
        "host":       os.getenv("MYSQL_HOST", "127.0.0.1").strip(),
        "user":       os.getenv("MYSQL_USER", "root").strip(),
        "password":   password,
        "raw":        os.getenv("MYSQL_RAW_DATABASE",   "flh_raw").strip(),
        "clean":      os.getenv("MYSQL_CLEAN_DATABASE", "flh_clean").strip(),
        "relational": os.getenv("MYSQL_REL_DATABASE",   "flh_relational").strip(),
    }


def build_engine(cfg: dict, db_name: str):
    encoded_pw = quote_plus(cfg["password"])
    url = (
        f"mysql+mysqlconnector://{cfg['user']}:{encoded_pw}"
        f"@{cfg['host']}/{db_name}?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True, echo=False)


def scalar(engine, sql: str):
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar()


# =============================================================================
# Validation framework
# =============================================================================

class ValidationResult:
    def __init__(self):
        self.passed   = []
        self.warnings = []
        self.failures = []

    def ok(self, check: str, detail: str = ""):
        msg = f"  PASS  {check}" + (f" — {detail}" if detail else "")
        logger.info(msg)
        self.passed.append(check)

    def warn(self, check: str, detail: str = ""):
        msg = f"  WARN  {check}" + (f" — {detail}" if detail else "")
        logger.warning(msg)
        self.warnings.append(check)

    def fail(self, check: str, detail: str = ""):
        msg = f"  FAIL  {check}" + (f" — {detail}" if detail else "")
        logger.error(msg)
        self.failures.append(check)

    def check(
        self,
        name: str,
        condition: bool,
        ok_detail: str,
        fail_detail: str,
        warn: bool = False,
    ):
        if condition:
            self.ok(name, ok_detail)
        elif warn:
            self.warn(name, fail_detail)
        else:
            self.fail(name, fail_detail)


# =============================================================================
# 1. Row counts
# =============================================================================

def check_row_counts(engines: dict, r: ValidationResult):
    logger.info("\n── Row counts ──────────────────────────────────────────")

    raw_reviews   = scalar(engines["raw"],   "SELECT COUNT(*) FROM reviews")
    clean_reviews = scalar(engines["clean"], "SELECT COUNT(*) FROM reviews")
    rel_reviews   = scalar(engines["rel"],   "SELECT COUNT(*) FROM reviews")

    r.check(
        "reviews: raw → clean",
        raw_reviews == clean_reviews,
        f"{clean_reviews:,} rows match",
        f"raw={raw_reviews:,} vs clean={clean_reviews:,} — delta={abs(raw_reviews-clean_reviews):,}",
    )
    r.check(
        "reviews: clean → relational",
        clean_reviews == rel_reviews,
        f"{rel_reviews:,} rows match",
        f"clean={clean_reviews:,} vs relational={rel_reviews:,} — delta={abs(clean_reviews-rel_reviews):,}",
    )

    raw_aspects   = scalar(engines["raw"],   "SELECT COUNT(*) FROM reviews_evaluationaspects")
    clean_aspects = scalar(engines["clean"], "SELECT COUNT(*) FROM review_aspects")
    rel_aspects   = scalar(engines["rel"],   "SELECT COUNT(*) FROM review_aspects")

    r.check(
        "aspects: raw → clean",
        raw_aspects == clean_aspects,
        f"{clean_aspects:,} rows match",
        f"raw={raw_aspects:,} vs clean={clean_aspects:,}",
    )
    r.check(
        "aspects: clean → relational",
        clean_aspects == rel_aspects,
        f"{rel_aspects:,} rows match",
        f"clean={clean_aspects:,} vs relational={rel_aspects:,}",
    )

    raw_props   = scalar(engines["raw"],   "SELECT COUNT(*) FROM propriedades")
    clean_props = scalar(engines["clean"], "SELECT COUNT(*) FROM properties")
    rel_props   = scalar(engines["rel"],   "SELECT COUNT(*) FROM properties")

    r.check(
        "properties: raw → clean",
        raw_props == clean_props,
        f"{clean_props:,} rows match",
        f"raw={raw_props:,} vs clean={clean_props:,}",
    )
    r.check(
        "properties: clean → relational",
        clean_props == rel_props,
        f"{rel_props:,} rows match",
        f"clean={clean_props:,} vs relational={rel_props:,}",
    )


# =============================================================================
# 2. Minimum rows  — guards against a silent wipe of an entire layer
# =============================================================================

def check_minimum_rows(engines: dict, r: ValidationResult):
    logger.info("\n── Minimum rows ────────────────────────────────────────")

    # Thresholds: set conservatively below your known row counts.
    # If the real counts grow significantly, raise these accordingly.
    checks = [
        ("raw",   "reviews",              100),
        ("clean", "reviews",              100),
        ("rel",   "reviews",              100),
        ("raw",   "reviews_evaluationaspects", 100),
        ("clean", "review_aspects",       100),
        ("raw",   "propriedades",          10),
        ("clean", "properties",            10),
    ]

    for db_key, table, min_rows in checks:
        count = scalar(engines[db_key], f"SELECT COUNT(*) FROM `{table}`")
        r.check(
            f"{db_key}.{table} has at least {min_rows:,} rows",
            count >= min_rows,
            f"{count:,} rows",
            f"only {count:,} rows — expected at least {min_rows:,} (possible silent wipe)",
        )


# =============================================================================
# 3. Key integrity
# =============================================================================

def check_key_integrity(engines: dict, r: ValidationResult):
    logger.info("\n── Key integrity ───────────────────────────────────────")

    for db_key, table, pk in [
        ("clean", "reviews",             "review_id"),
        ("clean", "review_aspects",      "aspect_id"),
        ("clean", "properties",          "flh_property_id"),
        ("rel",   "reviews",             "review_id"),
        ("rel",   "review_aspects",      "aspect_id"),
        ("rel",   "properties",          "flh_property_id"),
    ]:
        dupes = scalar(
            engines[db_key],
            f"SELECT COUNT(*) - COUNT(DISTINCT `{pk}`) FROM `{table}`",
        )
        r.check(
            f"No duplicate {pk} in {db_key}.{table}",
            dupes == 0,
            "unique",
            f"{dupes:,} duplicate values found",
        )

    for db_key, table, pk in [
        ("clean", "reviews",      "review_id"),
        ("rel",   "reviews",      "review_id"),
    ]:
        nulls = scalar(
            engines[db_key],
            f"SELECT COUNT(*) FROM `{table}` WHERE `{pk}` IS NULL",
        )
        r.check(
            f"No null {pk} in {db_key}.{table}",
            nulls == 0,
            "no nulls",
            f"{nulls:,} null values found",
        )


# =============================================================================
# 4. Score ranges
# =============================================================================

def check_score_ranges(engines: dict, r: ValidationResult):
    logger.info("\n── Score ranges ────────────────────────────────────────")

    for db_key, table, col in [
        ("clean", "reviews",             "overall_score"),
        ("rel",   "reviews",             "overall_score"),
        ("clean", "review_aspects",      "score"),
        ("rel",   "review_aspects",      "score"),
    ]:
        out_of_range = scalar(
            engines[db_key],
            f"SELECT COUNT(*) FROM `{table}` "
            f"WHERE `{col}` IS NOT NULL AND (`{col}` < 0 OR `{col}` > 10)",
        )
        r.check(
            f"{col} in range 0-10 in {db_key}.{table}",
            out_of_range == 0,
            "all in range",
            f"{out_of_range:,} values outside 0-10",
        )


# =============================================================================
# 5. Score distribution sanity
#    Catches feed issues where scores are technically valid but implausible
#    e.g. everything suddenly 10.0, or average drops to 2.0 overnight
# =============================================================================

def check_score_distribution(engines: dict, r: ValidationResult):
    logger.info("\n── Score distribution ──────────────────────────────────")

    avg_score = scalar(
        engines["rel"],
        "SELECT ROUND(AVG(overall_score), 2) FROM reviews "
        "WHERE overall_score IS NOT NULL",
    )

    if avg_score is None:
        r.fail(
            "Average overall_score is plausible",
            "no scores found — cannot compute average",
        )
        return

    avg_score = float(avg_score)
    r.check(
        "Average overall_score is plausible (between 5.0 and 10.0)",
        5.0 <= avg_score <= 10.0,
        f"avg={avg_score}",
        f"avg={avg_score} — outside expected range, possible data feed issue",
        warn=True,  # warn not fail — could be legitimate if portfolio changes
    )

    avg_aspect = scalar(
        engines["rel"],
        "SELECT ROUND(AVG(score), 2) FROM review_aspects "
        "WHERE score IS NOT NULL",
    )

    if avg_aspect is not None:
        avg_aspect = float(avg_aspect)
        r.check(
            "Average aspect score is plausible (between 5.0 and 10.0)",
            5.0 <= avg_aspect <= 10.0,
            f"avg={avg_aspect}",
            f"avg={avg_aspect} — outside expected range, possible data feed issue",
            warn=True,
        )


# =============================================================================
# 6. Date logic
# =============================================================================

def check_date_logic(engines: dict, r: ValidationResult):
    logger.info("\n── Date logic ──────────────────────────────────────────")

    # departure >= arrival
    for db_key, table in [
        ("clean", "reviews"),
        ("rel",   "reviews"),
    ]:
        bad_dates = scalar(
            engines[db_key],
            f"""SELECT COUNT(*) FROM `{table}`
                WHERE arrival_date IS NOT NULL
                  AND departure_date IS NOT NULL
                  AND departure_date < arrival_date""",
        )
        r.check(
            f"departure >= arrival in {db_key}.{table}",
            bad_dates == 0,
            "all valid",
            f"{bad_dates:,} rows where departure < arrival",
            warn=True,  # source data issue, not a pipeline issue
        )

    # No future created_at — reviews cannot be created in the future
    for db_key, table in [
        ("clean", "reviews"),
        ("rel",   "reviews"),
    ]:
        future_reviews = scalar(
            engines[db_key],
            f"SELECT COUNT(*) FROM `{table}` WHERE created_at > NOW()",
        )
        r.check(
            f"No future created_at in {db_key}.{table}",
            future_reviews == 0,
            "none found",
            f"{future_reviews:,} reviews with a future created_at timestamp",
            warn=True,  # likely a timezone or source data issue
        )



def check_critical_nulls(engines: dict, r: ValidationResult):
    logger.info("\n── Critical nulls ──────────────────────────────────────")

    checks = [
        ("clean", "reviews",             "avantio_property_id"),
        ("clean", "reviews",             "created_at"),
        ("clean", "reviews",             "sales_channel_code"),
        ("rel",   "reviews",             "avantio_property_id"),
        ("rel",   "reviews",             "sales_channel_code"),
        ("clean", "review_aspects",      "aspect_id"),
        ("clean", "review_aspects",      "review_id"),
        ("clean", "review_aspects",      "aspect"),
        ("rel",   "review_aspects",      "aspect_id"),
    ]

    for db_key, table, col in checks:
        nulls = scalar(
            engines[db_key],
            f"SELECT COUNT(*) FROM `{table}` WHERE `{col}` IS NULL",
        )
        r.check(
            f"No nulls in {db_key}.{table}.{col}",
            nulls == 0,
            "no nulls",
            f"{nulls:,} nulls found",
            warn=(col in ["avantio_property_id"]),  # known issue — warn not fail
        )


# =============================================================================
# 9. Orphaned aspects
#    Checks that every review_id in review_aspects exists in reviews.
#    The FK in flh_relational enforces this at DB level, but flh_clean has
#    no FK so orphans can slip through silently.
# =============================================================================

def check_orphaned_aspects(engines: dict, r: ValidationResult):
    logger.info("\n── Orphaned aspects ────────────────────────────────────")

    for db_key in ["clean", "rel"]:
        orphans = scalar(
            engines[db_key],
            """SELECT COUNT(*) FROM review_aspects ra
               LEFT JOIN reviews r ON ra.review_id = r.review_id
               WHERE r.review_id IS NULL""",
        )
        r.check(
            f"No orphaned aspects in {db_key}.review_aspects",
            orphans == 0,
            "none found",
            f"{orphans:,} aspects with no matching review in {db_key}.reviews",
        )


# =============================================================================
# 10. Cross-database consistency
# =============================================================================

def check_cross_db_consistency(engines: dict, r: ValidationResult):
    logger.info("\n── Cross-database consistency ───────────────────────────")

    clean_ids = set(pd.read_sql("SELECT review_id FROM reviews", engines["clean"])["review_id"])
    rel_ids   = set(pd.read_sql("SELECT review_id FROM reviews", engines["rel"])["review_id"])

    only_clean = clean_ids - rel_ids
    only_rel   = rel_ids - clean_ids
    r.check(
        "Same review_ids in clean and relational",
        len(only_clean) == 0 and len(only_rel) == 0,
        f"{len(clean_ids):,} IDs match exactly",
        f"only in clean: {len(only_clean):,} | only in relational: {len(only_rel):,}",
    )

    # overall_score consistency: clean vs relational
    df_clean = pd.read_sql(
        "SELECT review_id, overall_score FROM reviews WHERE overall_score IS NOT NULL",
        engines["clean"],
    )
    df_rel = pd.read_sql(
        "SELECT review_id, overall_score FROM reviews WHERE overall_score IS NOT NULL",
        engines["rel"],
    )
    merged = df_clean.merge(df_rel, on="review_id", suffixes=("_clean", "_rel"))
    score_mismatch = (merged["overall_score_clean"] != merged["overall_score_rel"]).sum()
    r.check(
        "overall_score consistent: clean vs relational",
        score_mismatch == 0,
        f"{len(merged):,} scores match",
        f"{score_mismatch:,} scores differ between clean and relational",
    )


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    logger.info("=" * 60)
    logger.info("PIPELINE VALIDATION START")
    logger.info("=" * 60)

    try:
        cfg = load_config()
    except EnvironmentError as exc:
        logger.error("Config error: %s", exc)
        raise SystemExit(1) from exc

    engines = {
        "raw":   build_engine(cfg, cfg["raw"]),
        "clean": build_engine(cfg, cfg["clean"]),
        "rel":   build_engine(cfg, cfg["relational"]),
    }

    r = ValidationResult()

    try:
        check_row_counts(engines, r)
        check_minimum_rows(engines, r)
        check_key_integrity(engines, r)
        check_score_ranges(engines, r)
        check_score_distribution(engines, r)
        check_date_logic(engines, r)
        check_critical_nulls(engines, r)
        check_orphaned_aspects(engines, r)
        check_cross_db_consistency(engines, r)
    finally:
        for e in engines.values():
            e.dispose()

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    logger.info("  PASSED   : %d", len(r.passed))
    logger.info("  WARNINGS : %d", len(r.warnings))
    logger.info("  FAILURES : %d", len(r.failures))

    if r.warnings:
        logger.warning("  Warnings : %s", r.warnings)
    if r.failures:
        logger.error("  Failures : %s", r.failures)
        logger.error("Data consistency issues detected — check log for details.")
        raise SystemExit(1)

    logger.info("All validation checks passed.")


if __name__ == "__main__":
    main()
