"""
run_pipeline.py
===============
Orchestrates the full data pipeline in sequence:

  1. getReviews.py                       — Avantio API → MongoDB           [SOFT FAIL]
  2. MongoMysql.py                       — MongoDB → MySQL raw
  3. load_reference_data_to_Mysql_raw.py — CSV reference data → MySQL raw
  4. mysql_data_cleaning.py              — MySQL raw → MySQL clean
  5. build_relational_schema.py          — MySQL clean → MySQL relational
  6. validate_pipeline.py                — Cross-database consistency checks [SOFT FAIL]

Step 1 is marked as "soft fail": if it fails, an alert email is sent
but the remaining steps still run (they operate on existing MongoDB data).

Steps 2–5 are "hard fail": if any of them fail, the pipeline stops
and an alert email is sent.

Step 6 is marked as "soft fail": validation issues should alert but not
block the pipeline result from being available.

A lock file (pipeline.lock) is created at startup and removed on finish.
If a second instance starts while the first is still running, it exits
immediately — preventing two runs from writing to the same databases
simultaneously.

Run directly:
  python3 run_pipeline.py

Or via cron (see run_pipeline.sh).
"""

import logging
import os
import smtplib
import subprocess
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

# =============================================================================
# Paths
# =============================================================================

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

SCRIPTS_DIR = BASE_DIR

DIAGNOSTICS_DIR = os.path.join(BASE_DIR, "diagnostics", "pipeline_runner")
os.makedirs(DIAGNOSTICS_DIR, exist_ok=True)

RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(DIAGNOSTICS_DIR, f"run_{RUN_TIMESTAMP}.log")

# Central log — single file that accumulates every run across all scripts
CENTRAL_LOG_FILE = os.path.join(BASE_DIR, "diagnostics", "pipeline_central.log")

# Lock file — prevents two pipeline instances running simultaneously
LOCK_FILE = os.path.join(BASE_DIR, "pipeline.lock")

# =============================================================================
# Lock file helpers
# =============================================================================

def acquire_lock() -> bool:
    """
    Try to create the lock file. Returns True if successful (we got the lock),
    False if another instance is already running.
    Uses O_CREAT | O_EXCL which is atomic — no race condition possible.
    """
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False


def release_lock() -> None:
    """Remove the lock file when the pipeline finishes."""
    try:
        os.remove(LOCK_FILE)
    except FileNotFoundError:
        pass


# =============================================================================
# Logging
# =============================================================================

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"

# Per-run log handler (one file per run)
_run_handler     = logging.FileHandler(LOG_FILE, encoding="utf-8")
_run_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))

# Central log handler (appends every run into one file)
_central_handler = logging.FileHandler(CENTRAL_LOG_FILE, mode="a", encoding="utf-8")
_central_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))

# Console handler
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))

logging.root.setLevel(logging.INFO)
logging.root.addHandler(_run_handler)
logging.root.addHandler(_central_handler)
logging.root.addHandler(_console_handler)

logger = logging.getLogger("pipeline_runner")

# =============================================================================
# Email config
# =============================================================================

load_dotenv(os.path.join(BASE_DIR, "config.env"))

EMAIL_FROM     = os.getenv("ALERT_EMAIL_FROM", "").strip()
EMAIL_TO       = os.getenv("ALERT_EMAIL_TO", "").strip()
EMAIL_PASSWORD = os.getenv("ALERT_EMAIL_PASSWORD", "").strip()
SMTP_HOST      = os.getenv("ALERT_SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT      = int(os.getenv("ALERT_SMTP_PORT", "587"))

# =============================================================================
# Pipeline steps
# =============================================================================

# Each entry: (label, script_filename, extra_args, soft_fail)
#
# soft_fail=True  → if this step fails: send alert email, but continue
# soft_fail=False → if this step fails: send alert email, and stop pipeline

PIPELINE_STEPS = [
    (
        "1/6 — Fetch reviews (Avantio API → MongoDB)",
        "getReviews.py",
        [],
        True,   # soft fail: API might be down, but MongoDB already has data
    ),
    (
        "2/6 — Migrate reviews (MongoDB → MySQL raw)",
        "MongoMysql.py",
        [],
        False,  # hard fail
    ),
    (
        "3/6 — Load reference data (CSV → MySQL raw)",
        "load_reference_data_to_Mysql_raw.py",
        [],
        False,  # hard fail
    ),
    (
        "4/6 — Clean data (MySQL raw → MySQL clean)",
        "mysql_data_cleaning.py",
        [],
        False,  # hard fail
    ),
    (
        "5/6 — Build relational schema (MySQL clean → MySQL relational)",
        "build_relational_schema.py",
        [],
        False,  # hard fail
    ),
    (
        "6/6 — Validate pipeline (cross-database consistency checks)",
        "validate_pipeline.py",
        [],
        True,   # soft fail: validation issues should alert but not block
    ),
]

# =============================================================================
# Email helper
# =============================================================================

def send_alert_email(subject: str, body: str) -> None:
    """Send a plain-text alert email. Skips silently if not configured."""
    if not all([EMAIL_FROM, EMAIL_TO, EMAIL_PASSWORD]):
        logger.warning(
            "Email alert skipped — ALERT_EMAIL_FROM / ALERT_EMAIL_TO / "
            "ALERT_EMAIL_PASSWORD not set in config.env"
        )
        return

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_FROM
        msg["To"]      = EMAIL_TO
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())

        logger.info("Alert email sent to %s", EMAIL_TO)

    except Exception as exc:
        logger.error("Failed to send alert email: %s", exc)


# =============================================================================
# Step runner
# =============================================================================

def _write_to_central_log(lines: str, label: str) -> None:
    """Write captured subprocess output into the central log file."""
    if not lines.strip():
        return
    with open(CENTRAL_LOG_FILE, "a", encoding="utf-8") as f:
        for line in lines.splitlines():
            f.write(f"  [{label}] {line}\n")


def run_step(label: str, script: str, extra_args: list) -> bool:
    """Run a pipeline step as a subprocess. Returns True on success.
    
    stdout/stderr from the child script is streamed to the console in real time
    AND captured so it can be written into the central log file.
    """
    import threading

    script_path = os.path.join(SCRIPTS_DIR, script)

    if not os.path.exists(script_path):
        logger.error("Script not found: %s", script_path)
        return False

    cmd = [sys.executable, script_path] + extra_args

    logger.info("─" * 60)
    logger.info("Starting : %s", label)
    logger.info("─" * 60)

    start = datetime.now()

    # Run subprocess with pipes so we can capture output for the central log
    # while still streaming it live to the console
    process = subprocess.Popen(
        cmd,
        cwd=SCRIPTS_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # merge stderr into stdout
        text=True,
        bufsize=1,  # line-buffered
    )

    captured_lines = []

    def stream_output():
        for line in process.stdout:
            print(line, end="", flush=True)       # live console output
            captured_lines.append(line)

    # Stream output in a thread so the main thread can wait for the process
    t = threading.Thread(target=stream_output, daemon=True)
    t.start()
    process.wait()
    t.join()

    elapsed = (datetime.now() - start).total_seconds()

    # Write captured output into the central log
    _write_to_central_log("".join(captured_lines), label)

    if process.returncode == 0:
        logger.info("✓ Completed in %.1fs: %s", elapsed, label)
        return True
    else:
        logger.error(
            "✗ FAILED (exit code %d) after %.1fs: %s",
            process.returncode, elapsed, label,
        )
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    run_start = datetime.now()

    # ── Lock file check ───────────────────────────────────────────────────────
    if not acquire_lock():
        print(
            f"[{run_start.strftime('%Y-%m-%d %H:%M:%S')}] "
            "Pipeline already running — lock file exists. Exiting."
        )
        sys.exit(0)

    try:
        # ── Central log run header ────────────────────────────────────────────
        with open(CENTRAL_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n")
            f.write("=" * 70 + "\n")
            f.write(f"PIPELINE RUN START : {run_start.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Per-run log        : {LOG_FILE}\n")
            f.write("=" * 70 + "\n")

        logger.info("=" * 60)
        logger.info("PIPELINE START : %s", run_start.strftime("%Y-%m-%d %H:%M:%S"))
        logger.info("Log file       : %s", LOG_FILE)
        logger.info("Central log    : %s", CENTRAL_LOG_FILE)
        logger.info("Lock file      : %s", LOCK_FILE)
        logger.info("=" * 60)

        failed_steps  = []
        skipped_steps = []
        halt = False

        for label, script, extra_args, soft_fail in PIPELINE_STEPS:

            if halt:
                skipped_steps.append(label)
                logger.warning("SKIPPED (previous hard-fail): %s", label)
                continue

            success = run_step(label, script, extra_args)

            if not success:
                failed_steps.append(label)

                alert_subject = f"[Pipeline Alert] Step failed: {label}"
                alert_body = (
                    f"Pipeline run : {run_start.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"Log file     : {LOG_FILE}\n\n"
                    f"Failed step  : {label}\n"
                    f"Script       : {script}\n\n"
                    + (
                        "This is a soft-fail step — the remaining pipeline steps "
                        "continued running. Please check the log for the error.\n"
                        if soft_fail else
                        "This is a hard-fail step — the pipeline was halted. "
                        "Please check the log for the error.\n"
                    )
                )

                send_alert_email(alert_subject, alert_body)

                if soft_fail:
                    logger.warning(
                        "Soft fail — alert sent, continuing with remaining steps: %s", label
                    )
                else:
                    logger.error(
                        "Hard fail — alert sent, halting pipeline: %s", label
                    )
                    halt = True

        # ── Summary ──────────────────────────────────────────────────────────
        elapsed_total = (datetime.now() - run_start).total_seconds()

        logger.info("=" * 60)
        logger.info("PIPELINE FINISHED in %.1fs", elapsed_total)
        logger.info("  Failed  : %d — %s", len(failed_steps),  failed_steps  or "none")
        logger.info("  Skipped : %d — %s", len(skipped_steps), skipped_steps or "none")
        logger.info("=" * 60)

        # ── Central log run footer ────────────────────────────────────────────
        status = "FAILED" if failed_steps else "SUCCESS"
        with open(CENTRAL_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write(f"PIPELINE RUN END   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
                    f"| {elapsed_total:.1f}s | {status}\n")
            if failed_steps:
                f.write(f"  Failed  : {failed_steps}\n")
            if skipped_steps:
                f.write(f"  Skipped : {skipped_steps}\n")
            f.write("=" * 70 + "\n")

    finally:
        # Always release the lock — even if an unexpected error occurs
        release_lock()
        logger.info("Lock file released.")

    sys.exit(1 if failed_steps else 0)


if __name__ == "__main__":
    main()