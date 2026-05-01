import os
import json
import time
import signal
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


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
    # API
    avantio_key: str
    base_api_url: str
    api_sleep_seconds: float
    request_timeout: int
    max_retries: int
    retry_backoff_base_seconds: int

    # Mongo
    mongo_host: str
    mongo_port: int
    mongo_auth_source: str
    mongo_timeout_ms: int
    mongo_user: str
    mongo_password: str
    mongo_db_name: str
    mongo_collection_reviews: str

    # Diagnostics
    base_dir: str
    diagnostics_dir: str
    pipeline_diagnostics_dir: str
    log_file: str
    checkpoint_file: str
    failed_pages_file: str
    bad_payload_file: str

    @classmethod
    def from_env(cls) -> "Config":
        base_dir = os.getcwd()
        diagnostics_dir = os.path.join(base_dir, "diagnostics")
        pipeline_dir = os.path.join(diagnostics_dir, "getReviews")
        os.makedirs(pipeline_dir, exist_ok=True)

        cfg = cls(
            avantio_key=(os.getenv("AVANTIO_KEY") or "").strip(),
            base_api_url=(os.getenv("BASE_API_URL") or "https://api.avantio.pro/pms/v2/reviews").strip(),
            api_sleep_seconds=float(os.getenv("API_SLEEP", "1")),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT", "60")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_backoff_base_seconds=int(os.getenv("RETRY_BACKOFF_BASE_SECONDS", "5")),
            mongo_host=(os.getenv("MONGO_HOST") or "localhost").strip(),
            mongo_port=int(os.getenv("MONGO_PORT", "27017")),
            mongo_auth_source=(os.getenv("MONGO_AUTH_SOURCE") or "admin").strip(),
            mongo_timeout_ms=int(os.getenv("MONGO_TIMEOUT", "5000")),
            mongo_user=(os.getenv("MONGO_USER") or "").strip(),
            mongo_password=(os.getenv("MONGO_PASSWORD") or "").strip(),
            mongo_db_name=(os.getenv("MONGO_DB_NAME") or "flh").strip(),
            mongo_collection_reviews=(os.getenv("MONGO_COLLECTION_REVIEWS") or "reviews").strip(),
            base_dir=base_dir,
            diagnostics_dir=diagnostics_dir,
            pipeline_diagnostics_dir=pipeline_dir,
            log_file=os.path.join(pipeline_dir, "avantio_pipeline.log"),
            checkpoint_file=os.path.join(pipeline_dir, "avantio_checkpoint.json"),
            failed_pages_file=os.path.join(pipeline_dir, "avantio_failed_pages.log"),
            bad_payload_file=os.path.join(pipeline_dir, "avantio_bad_payloads.log"),
        )

        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.avantio_key:
            raise ValueError("AVANTIO_KEY cannot be empty")
        if not self.base_api_url:
            raise ValueError("BASE_API_URL cannot be empty")
        if self.api_sleep_seconds < 0:
            raise ValueError("API_SLEEP cannot be negative")
        if self.request_timeout <= 0:
            raise ValueError("REQUEST_TIMEOUT must be > 0")
        if self.max_retries <= 0:
            raise ValueError("MAX_RETRIES must be > 0")
        if self.retry_backoff_base_seconds <= 0:
            raise ValueError("RETRY_BACKOFF_BASE_SECONDS must be > 0")
        if not self.mongo_host:
            raise ValueError("MONGO_HOST cannot be empty")
        if self.mongo_port <= 0:
            raise ValueError("MONGO_PORT must be > 0")
        if self.mongo_timeout_ms <= 0:
            raise ValueError("MONGO_TIMEOUT must be > 0")
        if not self.mongo_db_name:
            raise ValueError("MONGO_DB_NAME cannot be empty")
        if not self.mongo_collection_reviews:
            raise ValueError("MONGO_COLLECTION_REVIEWS cannot be empty")


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
# Checkpoint manager
# ============================================================
class CheckpointManager:
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file

    def load(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self.checkpoint_file):
            return None
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else None
        except Exception as e:
            logging.warning(f"Could not read checkpoint file: {e}")
            return None

    def save(self, payload: Dict[str, Any]) -> None:
        tmp_file = self.checkpoint_file + ".tmp"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
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
# Dead-letter / diagnostics
# ============================================================
class FailedPageLogger:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def log(self, page_number: int, url: str, error: Exception, response_text: Optional[str] = None) -> None:
        try:
            with open(self.file_path, "a", encoding="utf-8") as f:
                f.write(
                    f"{datetime.now().isoformat()} | page={page_number} | url={url} | "
                    f"error={repr(error)}\n"
                )
                if response_text:
                    f.write(f"response={response_text[:4000]}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            logging.warning(f"Could not write failed page log: {e}")


class BadPayloadLogger:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def log(self, page_number: int, url: str, payload: Any) -> None:
        try:
            with open(self.file_path, "a", encoding="utf-8") as f:
                f.write(
                    f"{datetime.now().isoformat()} | page={page_number} | url={url}\n"
                )
                f.write(json.dumps(payload, ensure_ascii=False, default=str)[:10000])
                f.write("\n" + "-" * 80 + "\n")
        except Exception as e:
            logging.warning(f"Could not write bad payload log: {e}")


# ============================================================
# Mongo helper
# ============================================================
def create_mongo_client(cfg: Config) -> MongoClient:
    if cfg.mongo_user:
        return MongoClient(
            host=cfg.mongo_host,
            port=cfg.mongo_port,
            username=cfg.mongo_user,
            password=cfg.mongo_password,
            authSource=cfg.mongo_auth_source,
            serverSelectionTimeoutMS=cfg.mongo_timeout_ms,
        )

    return MongoClient(
        host=cfg.mongo_host,
        port=cfg.mongo_port,
        serverSelectionTimeoutMS=cfg.mongo_timeout_ms,
    )


# ============================================================
# Extraction service
# ============================================================
class AvantioToMongoExtractor:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.checkpoint = CheckpointManager(cfg.checkpoint_file)
        self.failed_logger = FailedPageLogger(cfg.failed_pages_file)
        self.bad_payload_logger = BadPayloadLogger(cfg.bad_payload_file)

        self.mongo_client: Optional[MongoClient] = None
        self.collection: Optional[Collection] = None
        self.session: Optional[requests.Session] = None

        self.inserted_count = 0
        self.updated_count = 0
        self.matched_count = 0
        self.processed_pages = 0
        self.last_successful_url: Optional[str] = None
        self.last_successful_page: Optional[int] = None

    def connect(self) -> None:
        self.mongo_client = create_mongo_client(self.cfg)
        db = self.mongo_client[self.cfg.mongo_db_name]
        self.collection = db[self.cfg.mongo_collection_reviews]
        self.collection.create_index("id", unique=True)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Avantio-Auth": self.cfg.avantio_key,
                "Content-Type": "application/json",
            }
        )

        logging.info("MongoDB connection established successfully.")

    def close(self) -> None:
        if self.session:
            self.session.close()
        if self.mongo_client:
            self.mongo_client.close()
        logging.info("Connections closed.")

    def get_start_state(self) -> Dict[str, Any]:
        # ── Strategy A: always start from the beginning ──
        # Discard any checkpoint left over from a previous run (including failed ones).
        # The checkpoint file may still be written during this run for mid-run
        # debugging purposes, but it will never be read on the next startup.
        self.checkpoint.clear()
        logging.info("Starting fresh from page 1 (any previous checkpoint discarded)")
        return {
            "current_api_url": self.cfg.base_api_url,
            "page_number": 1,
        }

    def fetch_page(self, url: str, page_number: int) -> Dict[str, Any]:
        last_error: Optional[Exception] = None

        for attempt in range(1, self.cfg.max_retries + 1):
            if SHUTDOWN_REQUESTED:
                raise RuntimeError("Shutdown requested before request execution")

            try:
                logging.info(
                    f"Requesting page {page_number} | attempt {attempt}/{self.cfg.max_retries} | url={url}"
                )
                response = self.session.get(url, timeout=self.cfg.request_timeout)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                last_error = e
                response_text = None
                status_code = None

                if getattr(e, "response", None) is not None:
                    status_code = e.response.status_code
                    response_text = e.response.text

                logging.warning(
                    f"Request failed on page {page_number}, attempt {attempt}/{self.cfg.max_retries}. "
                    f"status={status_code} error={e}"
                )

                if attempt < self.cfg.max_retries:
                    sleep_seconds = self.cfg.retry_backoff_base_seconds * attempt
                    logging.info(f"Waiting {sleep_seconds}s before retry...")
                    time.sleep(sleep_seconds)
                else:
                    self.failed_logger.log(page_number, url, e, response_text)

        raise RuntimeError(f"Failed to fetch page {page_number} after retries: {last_error}")

    def validate_payload(self, payload: Dict[str, Any], page_number: int, url: str) -> None:
        if "data" not in payload or not isinstance(payload["data"], list):
            self.bad_payload_logger.log(page_number, url, payload)
            raise ValueError("Invalid payload: 'data' key missing or not a list")

    def upsert_reviews(self, reviews: list, page_number: int) -> None:
        if not reviews:
            logging.info(f"No reviews found on page {page_number}")
            return

        operations = []
        invalid_docs = 0

        for review in reviews:
            if not isinstance(review, dict):
                invalid_docs += 1
                logging.warning(f"Skipped non-dict review on page {page_number}: {review}")
                continue

            if "id" not in review:
                invalid_docs += 1
                logging.warning(f"Skipped review without 'id' on page {page_number}: {review}")
                continue

            operations.append(
                UpdateOne(
                    {"id": review["id"]},
                    {"$set": review},
                    upsert=True,
                )
            )

        if invalid_docs:
            logging.warning(f"Page {page_number}: skipped {invalid_docs} invalid review(s)")

        if not operations:
            logging.info(f"Page {page_number}: no valid operations to write")
            return

        result = self.collection.bulk_write(operations, ordered=False)
        self.inserted_count += result.upserted_count
        self.updated_count += result.modified_count
        self.matched_count += result.matched_count

        logging.info(
            f"Mongo bulk_write complete on page {page_number} | "
            f"upserted={result.upserted_count}, modified={result.modified_count}, matched={result.matched_count}"
        )

    def save_progress(self, next_url: Optional[str], page_number: int, current_url: str) -> None:
        self.last_successful_url = current_url
        self.last_successful_page = page_number

        self.checkpoint.save(
            {
                "last_successful_url": current_url,
                "last_successful_page": page_number,
                "next_url": next_url,
                "page_number": page_number + 1 if next_url else page_number,
                "saved_at": datetime.now().isoformat(),
            }
        )

    def run(self) -> None:
        self.connect()
        overall_success = False

        try:
            state = self.get_start_state()
            current_api_url = state["current_api_url"]
            page_number = state["page_number"]

            logging.info("Starting Avantio API extraction...")

            while current_api_url:
                if SHUTDOWN_REQUESTED:
                    logging.warning("Shutdown requested. Stopping safely with checkpoint preserved.")
                    break

                payload = self.fetch_page(current_api_url, page_number)
                self.validate_payload(payload, page_number, current_api_url)

                reviews = payload["data"]
                logging.info(f"Page {page_number}: received {len(reviews)} review(s)")
                self.upsert_reviews(reviews, page_number)

                next_url = None
                links = payload.get("_links")
                if isinstance(links, dict):
                    next_url = links.get("next")

                self.save_progress(next_url=next_url, page_number=page_number, current_url=current_api_url)
                self.processed_pages += 1

                if next_url:
                    logging.info(f"Moving to next page: {page_number + 1}")
                    current_api_url = next_url
                    page_number += 1
                    if self.cfg.api_sleep_seconds > 0:
                        time.sleep(self.cfg.api_sleep_seconds)
                else:
                    logging.info("No next page found. Extraction finished.")
                    current_api_url = None
                    overall_success = True

            if overall_success:
                self.checkpoint.clear()

            total_documents = self.collection.count_documents({}) if self.collection else 0

            logging.info("--- Extraction complete ---")
            logging.info(f"Processed pages: {self.processed_pages}")
            logging.info(f"Last successful page: {self.last_successful_page}")
            logging.info(f"Inserted: {self.inserted_count}")
            logging.info(f"Updated: {self.updated_count}")
            logging.info(f"Matched: {self.matched_count}")
            logging.info(f"Total documents in collection: {total_documents}")

            print("\n--- Extraction complete ---")
            print(f"Processed pages: {self.processed_pages}")
            print(f"Last successful page: {self.last_successful_page}")
            print(f"Inserted: {self.inserted_count}")
            print(f"Updated: {self.updated_count}")
            print(f"Matched: {self.matched_count}")
            print(f"Total documents in collection: {total_documents}")

            if SHUTDOWN_REQUESTED:
                print("Stopped safely due to shutdown request. Checkpoint preserved.")

        finally:
            self.close()


# ============================================================
# Entry point
# ============================================================
def main() -> None:
    cfg = Config.from_env()
    setup_logging(cfg.log_file)

    print("Starting Avantio to Mongo extraction...")
    start = time.time()

    extractor = AvantioToMongoExtractor(cfg)

    try:
        extractor.run()
    except PyMongoError as e:
        logging.error(f"MongoDB error: {e}")
        print(f"MongoDB error: {e}")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error: {e}")
        print(f"HTTP error: {e}")
        raise
    except Exception as e:
        logging.error(f"General error: {e}")
        print(f"General error: {e}")
        raise
    finally:
        elapsed = time.time() - start
        print(f"Total time: {elapsed:.2f}s")


if __name__ == "__main__":
    main()