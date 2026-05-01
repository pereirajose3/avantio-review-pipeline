import os
import re
import json
import time
import signal
import logging
import hashlib
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from bson import ObjectId

import mysql.connector
from mysql.connector import Error as MySQLError


# ============================================================
# Load environment
# ============================================================
load_dotenv("config.env")


# ============================================================
# Global shutdown flag
# ============================================================
SHUTDOWN_REQUESTED = False


def _handle_shutdown(signum, frame):
    global SHUTDOWN_REQUESTED
    SHUTDOWN_REQUESTED = True


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ============================================================
# Config
# ============================================================
@dataclass
class Config:
    # Mongo
    mongo_host: str
    mongo_port: int
    mongo_auth_source: str
    mongo_timeout_ms: int
    mongo_user: str
    mongo_password: str
    mongo_db: str
    mongo_collection_raw: str

    # MySQL
    mysql_host: str
    mysql_user: str
    mysql_password: str
    mysql_database: str

    # Migration
    batch_size: int
    batch_sleep_seconds: float
    schema_sample_limit: int

    # Diagnostics
    base_dir: str
    diagnostics_dir: str
    pipeline_diagnostics_dir: str
    log_file: str
    checkpoint_file: str
    failed_docs_file: str 

    @property
    def mongo_collection(self) -> str:
        return sanitize_identifier(self.mongo_collection_raw, max_len=64)

    @property
    def mongo_uri(self) -> str:
        if self.mongo_user and self.mongo_password:
            user = quote_plus(self.mongo_user)
            password = quote_plus(self.mongo_password)
            auth_source = quote_plus(self.mongo_auth_source)
            return (
                f"mongodb://{user}:{password}"
                f"@{self.mongo_host}:{self.mongo_port}/?authSource={auth_source}"
            )
        return f"mongodb://{self.mongo_host}:{self.mongo_port}/"

    @classmethod
    def from_env(cls) -> "Config":
        base_dir = os.getcwd()
        diagnostics_dir = os.path.join(base_dir, "diagnostics")
        pipeline_dir = os.path.join(diagnostics_dir, "MongoMysql")
        os.makedirs(pipeline_dir, exist_ok=True)

        config = cls(
            mongo_host=(os.getenv("MONGO_HOST") or "localhost").strip(),
            mongo_port=int(os.getenv("MONGO_PORT", "27017")),
            mongo_auth_source=(os.getenv("MONGO_AUTH_SOURCE") or "admin").strip(),
            mongo_timeout_ms=int(os.getenv("MONGO_TIMEOUT", "5000")),
            mongo_user=(os.getenv("MONGO_USER") or "").strip(),
            mongo_password=(os.getenv("MONGO_PASSWORD") or "").strip(),
            mongo_db=(os.getenv("MONGO_DB_NAME") or "flh").strip(),
            mongo_collection_raw=(os.getenv("MONGO_COLLECTION_REVIEWS") or "reviews").strip(),
            mysql_host=(os.getenv("MYSQL_HOST") or "127.0.0.1").strip(),
            mysql_user=(os.getenv("MYSQL_USER") or "root").strip(),
            mysql_password=(os.getenv("MYSQL_PASSWORD") or "").strip(),
            mysql_database=(os.getenv("MYSQL_RAW_DATABASE") or "flh_raw").strip(),
            batch_size=int(os.getenv("MIGRATION_BATCH_SIZE", "100")),
            batch_sleep_seconds=float(os.getenv("MIGRATION_BATCH_SLEEP_MS", "50")) / 1000.0,
            schema_sample_limit=int(os.getenv("SCHEMA_SAMPLE_LIMIT", "0")),
            base_dir=base_dir,
            diagnostics_dir=diagnostics_dir,
            pipeline_diagnostics_dir=pipeline_dir,
            log_file=os.path.join(pipeline_dir, "mongo_to_mysql_migration.log"),
            checkpoint_file=os.path.join(pipeline_dir, "migration_checkpoint.txt"),
            failed_docs_file=os.path.join(pipeline_dir, "migration_failed_docs.txt"),
        )

        config.validate()
        return config

    def validate(self) -> None:
        if not self.mongo_host:
            raise ValueError("MONGO_HOST cannot be empty")
        if self.mongo_port <= 0:
            raise ValueError("MONGO_PORT must be > 0")
        if self.mongo_timeout_ms <= 0:
            raise ValueError("MONGO_TIMEOUT must be > 0")
        if not self.mongo_db:
            raise ValueError("MONGO_DB_NAME cannot be empty")
        if not self.mongo_collection:
            raise ValueError("Mongo collection name is invalid after sanitization")
        if not self.mysql_host:
            raise ValueError("MYSQL_HOST cannot be empty")
        if not self.mysql_user:
            raise ValueError("MYSQL_USER cannot be empty")
        if not self.mysql_database:
            raise ValueError("MYSQL_DATABASE cannot be empty")
        if self.batch_size <= 0:
            raise ValueError("MIGRATION_BATCH_SIZE must be > 0")
        if self.batch_sleep_seconds < 0:
            raise ValueError("MIGRATION_BATCH_SLEEP_MS cannot be negative")
        if self.schema_sample_limit < 0:
            raise ValueError("SCHEMA_SAMPLE_LIMIT cannot be negative")


# ============================================================
# Logging
# ============================================================
def setup_logging(log_file: str) -> None:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)


# ============================================================
# Utilities
# ============================================================
def sanitize_identifier(value: str, max_len: int = 64) -> str:
    """
    Make a safe SQL identifier:
    - lowercase
    - only a-z, A-Z, 0-9, _
    - never empty
    - truncates with stable hash suffix
    """
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", str(value or "")).strip("_").lower()
    if not clean:
        clean = "col"

    if len(clean) > max_len:
        suffix = hashlib.md5(clean.encode("utf-8")).hexdigest()[:8]
        clean = clean[: max_len - 9] + "_" + suffix

    return clean


def is_list_of_dicts(value: Any) -> bool:
    return isinstance(value, list) and len(value) > 0 and any(isinstance(item, dict) for item in value)


def normalize_for_hash(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {
            str(k): normalize_for_hash(v)
            for k, v in sorted(value.items(), key=lambda x: str(x[0]))
        }
    if isinstance(value, list):
        return [normalize_for_hash(v) for v in value]
    return value


def make_hash(value: Any) -> str:
    normalized = normalize_for_hash(value)
    payload = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def deduplicate_list_of_dicts(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique = []
    for item in items:
        if not isinstance(item, dict):
            continue
        h = make_hash(item)
        if h not in seen:
            seen.add(h)
            unique.append(item)
    return unique


def convert_value_for_mysql(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def infer_mysql_type(value: Any) -> str:
    if value is None:
        return "LONGTEXT"
    if isinstance(value, bool):
        return "TINYINT(1)"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, Decimal):
        return "DECIMAL(30,10)"
    if isinstance(value, datetime):
        return "DATETIME"
    if isinstance(value, date):
        return "DATE"
    return "LONGTEXT"


def merge_mysql_types(existing: str, new_type: str) -> str:
    """
    Conservative widening rules.
    """
    if existing == new_type:
        return existing

    priority = {
        "TINYINT(1)": 1,
        "BIGINT": 2,
        "DOUBLE": 3,
        "DECIMAL(30,10)": 4,
        "DATE": 5,
        "DATETIME": 6,
        "LONGTEXT": 99,
    }
    return existing if priority.get(existing, 99) >= priority.get(new_type, 99) else new_type


def quote_identifier(name: str) -> str:
    return f"`{name}`"


# ============================================================
# Checkpoint
# ============================================================
class CheckpointManager:
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file

    def load(self) -> Optional[str]:
        if not os.path.exists(self.checkpoint_file):
            return None
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                value = f.read().strip()
                return value or None
        except Exception as e:
            logging.warning(f"Could not read checkpoint file: {e}")
            return None

    def save(self, mongo_id: ObjectId) -> None:
        tmp_file = self.checkpoint_file + ".tmp"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                f.write(str(mongo_id))
            os.replace(tmp_file, self.checkpoint_file)
        except Exception as e:
            logging.warning(f"Could not write checkpoint file: {e}")

    def clear(self) -> None:
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
        except Exception as e:
            logging.warning(f"Could not remove checkpoint file: {e}")


# ============================================================
# Dead-letter logger
# ============================================================
class FailedDocumentLogger:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def log(self, mongo_id: Any, error: Exception) -> None:
        try:
            with open(self.file_path, "a", encoding="utf-8") as f:
                f.write(
                    f"{datetime.now().isoformat()} | _id={mongo_id} | "
                    f"error={repr(error)}\n"
                )
        except Exception as e:
            logging.warning(f"Could not write failed document log: {e}")


# ============================================================
# Schema discovery
# ============================================================
@dataclass
class DiscoveredSchema:
    main_schema: Dict[str, str]
    nested_original_by_sanitized: Dict[str, str]
    child_schemas: Dict[str, Dict[str, str]]


def discover_schema(collection: Collection, sample_limit: int = 0) -> DiscoveredSchema:
    """
    Single-pass schema discovery.
    """
    logging.info("Starting schema discovery...")

    main_schema: Dict[str, str] = {
        "id": "VARCHAR(255)",
        "_id": "VARCHAR(24)",
    }
    nested_original_by_sanitized: Dict[str, str] = {}
    child_schemas: Dict[str, Dict[str, str]] = {}

    cursor = collection.find({}, no_cursor_timeout=True, batch_size=500)

    scanned = 0
    try:
        for doc in cursor:
            scanned += 1
            if sample_limit and scanned > sample_limit:
                break

            for field, value in doc.items():
                if field in ("id", "_id"):
                    continue

                clean_field = sanitize_identifier(field)

                if is_list_of_dicts(value):
                    nested_original_by_sanitized[clean_field] = field

                    hash_col = sanitize_identifier(f"__hash_{clean_field}")
                    existing_type = main_schema.get(hash_col)
                    hash_type = "CHAR(32)"
                    main_schema[hash_col] = merge_mysql_types(existing_type, hash_type) if existing_type else hash_type

                    if clean_field not in child_schemas:
                        child_schemas[clean_field] = {}

                    for item in value:
                        if not isinstance(item, dict):
                            continue
                        for child_key, child_val in item.items():
                            child_col = sanitize_identifier(child_key)
                            child_type = infer_mysql_type(child_val)

                            existing_child_type = child_schemas[clean_field].get(child_col)
                            if existing_child_type:
                                child_schemas[clean_field][child_col] = merge_mysql_types(existing_child_type, child_type)
                            else:
                                child_schemas[clean_field][child_col] = child_type
                    continue

                col_type = infer_mysql_type(value)
                existing_type = main_schema.get(clean_field)
                if existing_type:
                    main_schema[clean_field] = merge_mysql_types(existing_type, col_type)
                else:
                    main_schema[clean_field] = col_type

    finally:
        cursor.close()

    logging.info(
        f"Schema discovery complete. Docs scanned: {scanned}. "
        f"Main columns: {len(main_schema)}. "
        f"Nested fields: {len(child_schemas)}."
    )

    return DiscoveredSchema(
        main_schema=main_schema,
        nested_original_by_sanitized=nested_original_by_sanitized,
        child_schemas=child_schemas,
    )


# ============================================================
# MySQL helpers
# ============================================================
def create_database_if_not_exists(cfg: Config) -> None:
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(
            host=cfg.mysql_host,
            user=cfg.mysql_user,
            password=cfg.mysql_password,
            autocommit=True,
        )
        cursor = conn.cursor()
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {quote_identifier(cfg.mysql_database)} "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        logging.info(f"Database {cfg.mysql_database!r} is ready.")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def column_exists(cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        LIMIT 1
        """,
        (table_name, column_name),
    )
    return cursor.fetchone() is not None


def index_exists(cursor, table_name: str, index_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND INDEX_NAME = %s
        LIMIT 1
        """,
        (table_name, index_name),
    )
    return cursor.fetchone() is not None


def create_main_table(cursor, table_name: str) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(table_name)} (
            id VARCHAR(255) NOT NULL,
            _id VARCHAR(24) NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def apply_schema_to_table(cursor, table_name: str, schema: Dict[str, str]) -> None:
    for col_name, col_type in schema.items():
        if not column_exists(cursor, table_name, col_name):
            cursor.execute(
                f"ALTER TABLE {quote_identifier(table_name)} "
                f"ADD COLUMN {quote_identifier(col_name)} {col_type}"
            )
            logging.info(f"Added column {col_name!r} ({col_type}) to {table_name!r}")


def create_child_table(cursor, parent_table: str, child_suffix: str, child_schema: Dict[str, str]) -> str:
    table_name = sanitize_identifier(f"{parent_table}_{child_suffix}", max_len=64)

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(table_name)} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            parent_id VARCHAR(255) NOT NULL,
            row_hash CHAR(32) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_parent_id (parent_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

    if not index_exists(cursor, table_name, "uniq_parent_rowhash"):
        cursor.execute(
            f"""
            ALTER TABLE {quote_identifier(table_name)}
            ADD UNIQUE KEY uniq_parent_rowhash (parent_id, row_hash)
            """
        )

    apply_schema_to_table(cursor, table_name, child_schema)
    logging.info(f"Child table ready: {table_name!r}")
    return table_name


def add_missing_column_without_commit(cursor, table_name: str, col_name: str, col_type: str) -> bool:
    if column_exists(cursor, table_name, col_name):
        return True
    try:
        cursor.execute(
            f"ALTER TABLE {quote_identifier(table_name)} "
            f"ADD COLUMN {quote_identifier(col_name)} {col_type}"
        )
        logging.warning(
            f"Schema drift handled: added column {col_name!r} ({col_type}) "
            f"to {table_name!r}"
        )
        return True
    except Exception as e:
        logging.error(f"Failed to add drifted column {col_name!r}: {e}")
        return False


# ============================================================
# Child row logic
# ============================================================
def get_existing_hash(cursor, table_name: str, parent_id: str, hash_column: str) -> Optional[str]:
    try:
        cursor.execute(
            f"""
            SELECT {quote_identifier(hash_column)}
            FROM {quote_identifier(table_name)}
            WHERE id = %s
            LIMIT 1
            """,
            (parent_id,),
        )
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def insert_child_rows(cursor, table_name: str, parent_id: str, items: List[Dict[str, Any]]) -> None:
    unique_items = deduplicate_list_of_dicts(items)

    for item in unique_items:
        row_hash = make_hash(item)

        columns = ["parent_id", "row_hash"]
        values = [parent_id, row_hash]
        placeholders = ["%s", "%s"]

        for k, v in item.items():
            if not isinstance(k, str):
                k = str(k)
            clean_k = sanitize_identifier(k)
            columns.append(clean_k)
            values.append(convert_value_for_mysql(v))
            placeholders.append("%s")

        update_cols = [c for c in columns if c not in ("parent_id", "row_hash")]
        if update_cols:
            update_clause = ", ".join(
                f"{quote_identifier(c)} = VALUES({quote_identifier(c)})"
                for c in update_cols
            )
        else:
            update_clause = "row_hash = VALUES(row_hash)"

        query = (
            f"INSERT INTO {quote_identifier(table_name)} "
            f"({', '.join(quote_identifier(c) for c in columns)}) "
            f"VALUES ({', '.join(placeholders)}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )
        cursor.execute(query, values)


def process_nested_field(
    cursor,
    parent_table: str,
    child_tables: Dict[str, str],
    parent_id: str,
    clean_field_name: str,
    field_value: List[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (main_table_value, nested_hash).
    main_table_value is None when child table is used.
    """
    table_name = child_tables.get(clean_field_name)
    if not table_name:
        return json.dumps(field_value, ensure_ascii=False), None

    unique_items = deduplicate_list_of_dicts(field_value)
    new_hash = make_hash(unique_items)
    hash_col = sanitize_identifier(f"__hash_{clean_field_name}")
    old_hash = get_existing_hash(cursor, parent_table, parent_id, hash_col)

    if old_hash == new_hash:
        return None, new_hash

    savepoint_name = f"sp_{hashlib.md5((parent_id + clean_field_name).encode()).hexdigest()[:12]}"
    cursor.execute(f"SAVEPOINT {savepoint_name}")
    try:
        cursor.execute(
            f"DELETE FROM {quote_identifier(table_name)} WHERE parent_id = %s",
            (parent_id,),
        )
        insert_child_rows(cursor, table_name, parent_id, unique_items)
        cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        return None, new_hash
    except Exception:
        cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        raise


# ============================================================
# Migration service
# ============================================================
class MongoToMySQLMigrator:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.checkpoint = CheckpointManager(cfg.checkpoint_file)
        self.failed_logger = FailedDocumentLogger(cfg.failed_docs_file)
        self.mongo_client: Optional[MongoClient] = None
        self.mysql_conn = None
        self.cursor = None

    def connect(self) -> Collection:
        create_database_if_not_exists(self.cfg)

        self.mongo_client = MongoClient(
            self.cfg.mongo_uri,
            serverSelectionTimeoutMS=self.cfg.mongo_timeout_ms,
        )

        self.mysql_conn = mysql.connector.connect(
            host=self.cfg.mysql_host,
            user=self.cfg.mysql_user,
            password=self.cfg.mysql_password,
            database=self.cfg.mysql_database,
            autocommit=False,
        )
        self.cursor = self.mysql_conn.cursor()

        mongo_db = self.mongo_client[self.cfg.mongo_db]
        return mongo_db[self.cfg.mongo_collection_raw]

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.mysql_conn and self.mysql_conn.is_connected():
            self.mysql_conn.close()
        if self.mongo_client:
            self.mongo_client.close()
        logging.info("Connections closed.")

    def prepare_schema(self, collection: Collection) -> Tuple[DiscoveredSchema, Dict[str, str]]:
        create_main_table(self.cursor, self.cfg.mongo_collection)
        self.mysql_conn.commit()

        discovered = discover_schema(collection, sample_limit=self.cfg.schema_sample_limit)
        apply_schema_to_table(self.cursor, self.cfg.mongo_collection, discovered.main_schema)

        child_tables: Dict[str, str] = {}
        for clean_field, child_schema in discovered.child_schemas.items():
            if child_schema:
                child_tables[clean_field] = create_child_table(
                    self.cursor,
                    self.cfg.mongo_collection,
                    clean_field,
                    child_schema,
                )

        self.mysql_conn.commit()
        return discovered, child_tables

    def build_query_filter(self) -> Dict[str, Any]:
        checkpoint_id = self.checkpoint.load()
        if checkpoint_id:
            logging.info(f"Resuming from checkpoint _id > {checkpoint_id}")
            return {"_id": {"$gt": ObjectId(checkpoint_id)}}
        return {}

    def handle_schema_drift(self, table_name: str, columns: List[str], values: List[Any]) -> None:
        for col, val in zip(columns, values):
            col_type = infer_mysql_type(val)
            add_missing_column_without_commit(self.cursor, table_name, col, col_type)

    def migrate(self) -> None:
        collection = self.connect()
        discovered, child_tables = self.prepare_schema(collection)

        processed = 0
        skipped = 0
        failed = 0
        committed_since_last_batch = 0
        last_committed_id: Optional[ObjectId] = None
        overall_success = False

        query_filter = self.build_query_filter()
        mongo_cursor = collection.find(
            query_filter,
            no_cursor_timeout=True,
            batch_size=self.cfg.batch_size,
        ).sort("_id", 1)

        try:
            for doc in mongo_cursor:
                if SHUTDOWN_REQUESTED:
                    logging.warning("Shutdown requested. Finishing current safe point and stopping.")
                    break

                mongo_id = doc.get("_id", "unknown")
                savepoint_name = f"doc_{str(mongo_id).replace('-', '_')}"

                try:
                    self.cursor.execute(f"SAVEPOINT {savepoint_name}")

                    api_id = doc.get("id")
                    if api_id is None:
                        skipped += 1
                        logging.warning(f"Document without 'id'. Mongo _id={mongo_id}. Skipping.")
                        self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        continue

                    parent_id = str(api_id)
                    row: Dict[str, Any] = {
                        "id": parent_id,
                        "_id": str(doc["_id"]),
                    }
                    hash_updates: Dict[str, str] = {}

                    for field, value in doc.items():
                        if field in ("id", "_id"):
                            continue

                        clean_field = sanitize_identifier(field)

                        if is_list_of_dicts(value):
                            main_val, nested_hash = process_nested_field(
                                cursor=self.cursor,
                                parent_table=self.cfg.mongo_collection,
                                child_tables=child_tables,
                                parent_id=parent_id,
                                clean_field_name=clean_field,
                                field_value=[x for x in value if isinstance(x, dict)],
                            )
                            if main_val is not None:
                                row[clean_field] = main_val
                            if nested_hash is not None:
                                hash_updates[sanitize_identifier(f"__hash_{clean_field}")] = nested_hash
                        else:
                            row[clean_field] = convert_value_for_mysql(value)

                    row.update(hash_updates)

                    columns = list(row.keys())
                    values = list(row.values())
                    update_cols = [c for c in columns if c != "id"]

                    insert_query = (
                        f"INSERT INTO {quote_identifier(self.cfg.mongo_collection)} "
                        f"({', '.join(quote_identifier(c) for c in columns)}) "
                        f"VALUES ({', '.join(['%s'] * len(values))}) "
                        f"ON DUPLICATE KEY UPDATE "
                        f"{', '.join(f'{quote_identifier(c)} = VALUES({quote_identifier(c)})' for c in update_cols)}"
                    )

                    try:
                        self.cursor.execute(insert_query, values)
                    except MySQLError as e:
                        if e.errno == 1054:
                            logging.warning(
                                f"Schema drift detected on Mongo _id={mongo_id}: {e}. "
                                "Adding missing columns and retrying."
                            )
                            self.handle_schema_drift(self.cfg.mongo_collection, columns, values)
                            self.cursor.execute(insert_query, values)
                        else:
                            raise

                    self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")

                    processed += 1
                    committed_since_last_batch += 1
                    last_committed_id = doc["_id"]

                    if committed_since_last_batch >= self.cfg.batch_size:
                        self.mysql_conn.commit()
                        self.checkpoint.save(last_committed_id)
                        logging.info(f"Progress: {processed} processed, {skipped} skipped, {failed} failed.")
                        committed_since_last_batch = 0

                        if self.cfg.batch_sleep_seconds > 0:
                            time.sleep(self.cfg.batch_sleep_seconds)

                except Exception as doc_error:
                    failed += 1
                    try:
                        self.cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        self.cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    except Exception:
                        self.mysql_conn.rollback()
                    logging.error(f"Failed on document _id={mongo_id}: {doc_error}")
                    self.failed_logger.log(mongo_id, doc_error)
                    continue

            if committed_since_last_batch > 0:
                self.mysql_conn.commit()
                if last_committed_id is not None:
                    self.checkpoint.save(last_committed_id)

            overall_success = not SHUTDOWN_REQUESTED

        finally:
            mongo_cursor.close()

        if overall_success:
            self.checkpoint.clear()

        logging.info("Migration complete.")
        logging.info(f"Processed: {processed}")
        logging.info(f"Skipped:   {skipped}")
        logging.info(f"Failed:    {failed}")

        print("\nMigration complete.")
        print(f"Processed: {processed}")
        print(f"Skipped:   {skipped}")
        print(f"Failed:    {failed}")

        if SHUTDOWN_REQUESTED:
            print("Stopped safely due to shutdown request. Checkpoint preserved.")

    def run(self) -> None:
        try:
            self.migrate()
        except MySQLError as e:
            logging.error(f"MySQL error: {e}")
            print(f"MySQL error: {e}")
            raise
        except PyMongoError as e:
            logging.error(f"MongoDB error: {e}")
            print(f"MongoDB error: {e}")
            raise
        except Exception as e:
            logging.error(f"General error: {e}")
            print(f"General error: {e}")
            raise
        finally:
            self.close()


# ============================================================
# Entry point
# ============================================================
def main() -> None:
    cfg = Config.from_env()
    setup_logging(cfg.log_file)

    print("Starting migration...")
    start = time.time()

    migrator = MongoToMySQLMigrator(cfg)
    migrator.run()

    elapsed = time.time() - start
    print(f"Total time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()