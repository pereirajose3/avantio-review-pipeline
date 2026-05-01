
import logging
import os
import sys
from contextlib import contextmanager
from urllib.parse import quote_plus

import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# =============================================================================
# Paths
# =============================================================================

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

DIAGNOSTICS_DIR = os.path.join(BASE_DIR, "diagnostics", "build_relational_schema")
os.makedirs(DIAGNOSTICS_DIR, exist_ok=True)
LOG_FILE = os.path.join(DIAGNOSTICS_DIR, "build_relational_schema.log")


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("build_relational")


# =============================================================================
# Config
# =============================================================================

def load_config() -> dict:
    load_dotenv(os.path.join(BASE_DIR, "config.env"))
    password = os.getenv("MYSQL_PASSWORD", "").strip()
    if not password:
        raise EnvironmentError("MYSQL_PASSWORD is missing in config.env")
    return {
        "host":           os.getenv("MYSQL_HOST", "127.0.0.1").strip(),
        "user":           os.getenv("MYSQL_USER", "root").strip(),
        "password":       password,
        "clean_database":   os.getenv("MYSQL_CLEAN_DATABASE", "flh_clean").strip(),
        "rel_database":     os.getenv("MYSQL_REL_DATABASE", "flh_relational").strip(),
        "rel_new_database": os.getenv("MYSQL_REL_DATABASE", "flh_relational").strip() + "_new",
        "rel_old_database": os.getenv("MYSQL_REL_DATABASE", "flh_relational").strip() + "_old",
    }


# =============================================================================
# Database helpers
# =============================================================================

def create_database_if_not_exists(cfg: dict, db_name: str) -> None:
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
        echo=False,
        connect_args={"connection_timeout": 30},
    )


@contextmanager
def get_engines(cfg: dict):
    e_clean   = build_engine(cfg, cfg["clean_database"])
    e_rel_new = build_engine(cfg, cfg["rel_new_database"])
    try:
        yield e_clean, e_rel_new
    finally:
        e_clean.dispose()
        e_rel_new.dispose()


def execute_ddl(engine, statements: list) -> None:
    """Execute DDL statements one at a time to avoid multi-statement issues."""
    for stmt in statements:
        with engine.begin() as conn:
            conn.execute(text(stmt))


# =============================================================================
# Blue/green swap
# =============================================================================

RELATIONAL_TABLES = [
    "review_aspects",       # must come first — has FK to reviews
    "reviews",
    "property_id_mapping",
    "properties",
]


def blue_green_swap(cfg: dict) -> None:
    """
    Atomically swap flh_relational_new → flh_relational using RENAME TABLE.

    Steps:
      1. Ensure flh_relational_old exists (creates it empty if not)
      2. RENAME TABLE in a single atomic statement:
           flh_relational.T      → flh_relational_old.T   (old live → old)
           flh_relational_new.T  → flh_relational.T        (new → live)
      3. Drop flh_relational_old

    During step 2 the live database is never in a partial state —
    MySQL's RENAME TABLE is atomic across multiple tables in one statement.
    Any query hitting flh_relational sees either the old or the new data,
    never a half-built schema.

    If flh_relational does not exist yet (first ever run) we skip the
    old→old part and just rename new→live directly.
    """
    db_rel     = cfg["rel_database"]
    db_rel_new = cfg["rel_new_database"]
    db_rel_old = cfg["rel_old_database"]

    raw = mysql.connector.connect(
        host=cfg["host"], user=cfg["user"], password=cfg["password"]
    )
    try:
        cur = raw.cursor()

        # 1. Ensure _old database exists
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_rel_old}`")
        raw.commit()

        # 2. Check whether the live database already has tables
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{db_rel}' AND table_name IN ({','.join(['%s']*len(RELATIONAL_TABLES))})",
            RELATIONAL_TABLES,
        )
        live_table_count = cur.fetchone()[0]
        first_run = (live_table_count == 0)

        if first_run:
            logger.info("First run detected — no existing tables in '%s'. Renaming new → live directly.", db_rel)
            # Ensure live db exists
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_rel}`")
            raw.commit()

            rename_parts = ", ".join(
                f"`{db_rel_new}`.`{t}` TO `{db_rel}`.`{t}`"
                for t in reversed(RELATIONAL_TABLES)  # reverse: no-FK tables first
            )
        else:
            logger.info("Swapping '%s' → '%s' and '%s' → '%s' atomically ...",
                        db_rel_new, db_rel, db_rel, db_rel_old)

            rename_parts = ", ".join(
                [
                    # old live → _old
                    f"`{db_rel}`.`{t}` TO `{db_rel_old}`.`{t}`"
                    for t in RELATIONAL_TABLES
                ] + [
                    # _new → live
                    f"`{db_rel_new}`.`{t}` TO `{db_rel}`.`{t}`"
                    for t in RELATIONAL_TABLES
                ]
            )

        cur.execute(f"SET FOREIGN_KEY_CHECKS = 0")
        cur.execute(f"RENAME TABLE {rename_parts}")
        cur.execute(f"SET FOREIGN_KEY_CHECKS = 1")
        raw.commit()
        logger.info("Atomic rename complete. '%s' is now live.", db_rel)

        # 3. Drop _old database
        if not first_run:
            cur.execute(f"DROP DATABASE IF EXISTS `{db_rel_old}`")
            raw.commit()
            logger.info("Dropped old database '%s'.", db_rel_old)

        # 4. Drop _new database (now empty)
        cur.execute(f"DROP DATABASE IF EXISTS `{db_rel_new}`")
        raw.commit()
        logger.info("Dropped staging database '%s'.", db_rel_new)

        cur.close()

    finally:
        raw.close()


# =============================================================================
# Type helpers
# =============================================================================

def to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def to_nullable_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def to_nullable_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("float64")


def split_gps(series: pd.Series):
    extracted = series.str.extract(r'^\s*([-\d.]+)\s*,\s*([-\d.]+)\s*$')
    lat = pd.to_numeric(extracted[0], errors="coerce")
    lon = pd.to_numeric(extracted[1], errors="coerce")
    return lat, lon


def convert_text_to_varchar(engine, table: str, col_sizes: dict) -> None:
    """
    Convert TEXT columns to VARCHAR after pandas to_sql load.
    pandas loads all strings as TEXT in MySQL.
    TEXT cannot be used in foreign keys or standard indexes.
    col_sizes: {column_name: max_length}
    """
    stmts = [
        f"ALTER TABLE `{table}` MODIFY `{col}` VARCHAR({size})"
        for col, size in col_sizes.items()
    ]
    execute_ddl(engine, stmts)


# =============================================================================
# Table builders
# =============================================================================

def build_properties(engine_clean, engine_rel) -> int:
    logger.info("[1/4] Building: properties")

    df = pd.read_sql("SELECT * FROM properties", engine_clean)

    # Split GPS into lat/lon
    df["latitude"], df["longitude"] = split_gps(df["gps_coordinates"])
    df = df.drop(columns=["gps_coordinates"])

    # Fix types
    df["flh_property_id"]           = to_nullable_int(df["flh_property_id"])
    df["postcode_prefix"]           = to_nullable_int(df["postcode_prefix"])
    df["postcode_suffix"]           = to_nullable_int(df["postcode_suffix"])
    df["is_inactive"]               = to_nullable_int(df["is_inactive"])
    df["has_elevator"]              = to_nullable_int(df["has_elevator"])
    df["max_occupancy"]             = to_nullable_int(df["max_occupancy"])
    df["num_bathrooms_with_shower"] = to_nullable_int(df["num_bathrooms_with_shower"])
    df["num_basic_bathrooms"]       = to_nullable_int(df["num_basic_bathrooms"])
    df["has_washing_machine"]       = to_nullable_int(df["has_washing_machine"])
    df["has_dishwasher"]            = to_nullable_int(df["has_dishwasher"])
    df["has_air_conditioning"]      = to_nullable_int(df["has_air_conditioning"])
    df["has_self_checkin"]          = to_nullable_int(df["has_self_checkin"])
    df["start_date"]                = to_datetime(df["start_date"])
    df["inactive_date"]             = to_datetime(df["inactive_date"])

    df.to_sql("properties", engine_rel, if_exists="replace", index=False)

    # Convert TEXT -> VARCHAR before applying indexes and constraints
    convert_text_to_varchar(engine_rel, "properties", {
        "team":          100,
        "neighbourhood": 200,
        "floor":          20,
        "garage_info":   200,
    })

    execute_ddl(engine_rel, [
        "ALTER TABLE properties MODIFY flh_property_id INT NOT NULL",
        "ALTER TABLE properties ADD PRIMARY KEY (flh_property_id)",
        "ALTER TABLE properties MODIFY is_inactive TINYINT NOT NULL DEFAULT 0",
        "ALTER TABLE properties MODIFY has_washing_machine TINYINT NOT NULL DEFAULT 0",
        "ALTER TABLE properties MODIFY has_dishwasher TINYINT NOT NULL DEFAULT 0",
        "ALTER TABLE properties MODIFY has_air_conditioning TINYINT NOT NULL DEFAULT 0",
        "ALTER TABLE properties MODIFY has_self_checkin TINYINT NOT NULL DEFAULT 0",
        "CREATE INDEX idx_properties_team ON properties (team)",
        "CREATE INDEX idx_properties_is_inactive ON properties (is_inactive)",
        "CREATE INDEX idx_properties_postcode ON properties (postcode_prefix)",
    ])

    logger.info("  properties: %d rows loaded.", len(df))
    return len(df)


def build_property_id_mapping(engine_clean, engine_rel) -> int:
    logger.info("[2/4] Building: property_id_mapping")

    df = pd.read_sql("SELECT * FROM property_id_mapping", engine_clean)

    df["flh_property_id"]     = to_nullable_int(df["flh_property_id"])
    df["avantio_property_id"] = to_nullable_int(df["avantio_property_id"])

    df.to_sql("property_id_mapping", engine_rel, if_exists="replace", index=False)

    execute_ddl(engine_rel, [
        "ALTER TABLE property_id_mapping MODIFY flh_property_id INT NOT NULL",
        "ALTER TABLE property_id_mapping ADD PRIMARY KEY (flh_property_id)",
        "ALTER TABLE property_id_mapping ADD UNIQUE KEY uq_mapping_avantio_id (avantio_property_id)",
        "CREATE INDEX idx_mapping_avantio_id ON property_id_mapping (avantio_property_id)",
        # NOTE: FK to properties intentionally omitted.
        # property_id_mapping contains 891 historical flh_property_ids that no
        # longer exist in properties (properties only has active/current records).
        # Enforcing the FK would reject those rows. The join is done in queries
        # using LEFT JOIN, which handles the mismatch gracefully.
    ])

    logger.info("  property_id_mapping: %d rows loaded.", len(df))
    return len(df)


def build_reviews(engine_clean, engine_rel) -> int:
    logger.info("[3/4] Building: reviews")

    df = pd.read_sql("SELECT * FROM reviews", engine_clean)

    # Fix types
    df["avantio_property_id"] = to_nullable_int(df["avantio_property_id"])
    df["booking_id"]          = to_nullable_int(df["booking_id"])
    df["overall_score"]       = to_nullable_float(df["overall_score"])
    df["is_validated"]        = to_nullable_int(df["is_validated"])
    df["arrival_date"]        = to_date(df["arrival_date"])
    df["departure_date"]      = to_date(df["departure_date"])
    df["created_at"]          = to_datetime(df["created_at"])
    df["response_date"]       = to_date(df["response_date"])

    # Drop near-empty operational columns
    df = df.drop(columns=[
        "hash_evaluationaspects",   # internal hash, not needed for analysis
        "is_eligible_for_response", # 97% null
        "external_response_status", # 98% null
        "likes_count",              # 94% null
    ], errors="ignore")

    df.to_sql("reviews", engine_rel, if_exists="replace", index=False)

    # Convert TEXT -> VARCHAR for FK and index columns
    convert_text_to_varchar(engine_rel, "reviews", {
        "review_id":          255,
        "mongo_id":           255,
        "sales_channel_code":  50,
        "language":            10,
        "external_id":        100,
        "customer_name":      255,
    })

    execute_ddl(engine_rel, [
        "ALTER TABLE reviews MODIFY review_id VARCHAR(255) NOT NULL",
        "ALTER TABLE reviews ADD PRIMARY KEY (review_id)",
        "CREATE INDEX idx_reviews_avantio_id ON reviews (avantio_property_id)",
        "CREATE INDEX idx_reviews_arrival_date ON reviews (arrival_date)",
        "CREATE INDEX idx_reviews_created_at ON reviews (created_at)",
        "CREATE INDEX idx_reviews_sales_channel ON reviews (sales_channel_code)",
        "CREATE INDEX idx_reviews_overall_score ON reviews (overall_score)",

        "ALTER TABLE reviews MODIFY is_validated INT",
        "ALTER TABLE properties MODIFY postcode_prefix INT",
        "ALTER TABLE properties MODIFY postcode_suffix INT",
        "ALTER TABLE properties MODIFY has_elevator INT",
        "ALTER TABLE properties MODIFY max_occupancy INT",
        "ALTER TABLE properties MODIFY num_bathrooms_with_shower INT",  # <-- add this
        "ALTER TABLE properties MODIFY num_basic_bathrooms INT",        # <-- add this
        # NOTE: FK to property_id_mapping intentionally omitted.
        # 3 reviews reference avantio_ids (706897, 708801, 704990) that are not
        # present in property_id_mapping — likely properties added in Avantio
        # after the mapping file was last exported by the DB manager.
        # Use LEFT JOIN in queries to handle gracefully.
    ])

    logger.info("  reviews: %d rows loaded.", len(df))
    return len(df)


def build_review_aspects(engine_clean, engine_rel) -> int:
    logger.info("[4/4] Building: review_aspects")

    df = pd.read_sql("SELECT * FROM review_aspects", engine_clean)

    df["aspect_id"]  = to_nullable_int(df["aspect_id"])
    df["score"]      = to_nullable_float(df["score"])
    df["created_at"] = to_datetime(df["created_at"])

    df.to_sql("review_aspects", engine_rel, if_exists="replace", index=False)

    # Convert TEXT -> VARCHAR for FK and index columns
    convert_text_to_varchar(engine_rel, "review_aspects", {
        "review_id": 255,
        "aspect":     50,
    })

    execute_ddl(engine_rel, [
        "ALTER TABLE review_aspects MODIFY aspect_id INT NOT NULL",
        "ALTER TABLE review_aspects ADD PRIMARY KEY (aspect_id)",
        "CREATE INDEX idx_aspects_review_id ON review_aspects (review_id)",
        "CREATE INDEX idx_aspects_aspect ON review_aspects (aspect)",
        (
            "ALTER TABLE review_aspects "
            "ADD CONSTRAINT fk_aspects_review "
            "FOREIGN KEY (review_id) REFERENCES reviews (review_id) "
            "ON DELETE CASCADE ON UPDATE CASCADE"
        ),
    ])

    logger.info("  review_aspects: %d rows loaded.", len(df))
    return len(df)


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    logger.info("=== build_relational_schema starting ===")

    try:
        cfg = load_config()
    except EnvironmentError as exc:
        logger.error("Config error: %s", exc)
        raise SystemExit(1) from exc

    # Build into the staging database (_new), not the live one
    create_database_if_not_exists(cfg, cfg["rel_new_database"])

    with get_engines(cfg) as (engine_clean, engine_rel_new):

        try:
            with engine_clean.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as exc:
            logger.error("Cannot reach flh_clean: %s", exc)
            raise SystemExit(1) from exc

        total_rows = 0
        failed = []

        steps = [
            ("properties",          build_properties),
            ("property_id_mapping", build_property_id_mapping),
            ("reviews",             build_reviews),
            ("review_aspects",      build_review_aspects),
        ]

        # Drop all tables in _new (clean slate) with FK checks OFF
        logger.info("Dropping all tables in staging db '%s' with FK checks disabled...", cfg["rel_new_database"])
        _raw = mysql.connector.connect(
            host=cfg["host"], user=cfg["user"], password=cfg["password"],
            database=cfg["rel_new_database"]
        )
        try:
            _cur = _raw.cursor()
            _cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            for tbl in RELATIONAL_TABLES:
                _cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
                logger.info("  Dropped (if existed): %s", tbl)
            _cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            _raw.commit()
            _cur.close()
        finally:
            _raw.close()
        logger.info("Staging db clean. Starting fresh load into '%s'.", cfg["rel_new_database"])

        for name, fn in steps:
            try:
                rows = fn(engine_clean, engine_rel_new)
                total_rows += rows
            except Exception as exc:
                logger.error("Failed to build '%s': %s", name, exc, exc_info=True)
                failed.append(name)
                break  # stop on first failure

    if failed:
        logger.error("Build failed — aborting swap. Live database '%s' is untouched.", cfg["rel_database"])
        logger.info("  Tables failed: %s", failed)
        raise SystemExit(1)

    # All tables built successfully — atomically swap _new → live
    logger.info("All tables built successfully. Performing blue/green swap...")
    try:
        blue_green_swap(cfg)
    except Exception as exc:
        logger.error("Blue/green swap failed: %s", exc, exc_info=True)
        raise SystemExit(1) from exc

    logger.info("=== build_relational_schema complete ===")
    logger.info("  Total rows written : %d", total_rows)
    logger.info("  Tables failed      : %s", failed or "none")
    logger.info("flh_relational built successfully.")


if __name__ == "__main__":
    main()