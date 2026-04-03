"""
TimeSeriesDB Adaptor (Data Logger)
====================================
Responsibilities:
  - MQTT Subscriber: receives ALL sensor readings and stores them in SQLite
    (swap for InfluxDB / TimescaleDB in production).
  - REST Provider: exposes historical queries for the Clinician Portal,
    Patient-Facing App, and Data Processor.
  - Handles the async-to-sync bridge: MQTT writes are non-blocking;
    REST reads use standard DB queries with no race conditions.
"""

import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
import requests
from flask import Flask, jsonify, request

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TimeSeriesDB] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuration ───────────────────────────────────────────────────────────
HEALTH_CATALOG_URL = "http://health_catalog:5000"
DB_PATH = Path("/data/timeseries.db")
REST_PORT = 5001
METRICS = [
    "heart_rate",
    "body_temperature",
    "blood_pressure_systolic",
    "blood_pressure_diastolic",
]

# Thread-local SQLite connections (SQLite connections must not be shared across threads)
_local = threading.local()
_db_lock = threading.Lock()  # Only needed for DDL / batch writes
app = Flask(__name__)

mqtt_config: dict = {}
mqtt_client: mqtt.Client | None = None


# ══════════════════════════════════════════════════════════════════════════════
# Database layer
# ══════════════════════════════════════════════════════════════════════════════

def _get_conn() -> sqlite3.Connection:
    """Return a thread-local SQLite connection, creating it if needed."""
    if not hasattr(_local, "conn") or _local.conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")   # WAL mode: readers don't block writers
        conn.execute("PRAGMA synchronous=NORMAL;") # Balance durability vs. speed
        _local.conn = conn
    return _local.conn


@contextmanager
def get_db():
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db():
    """Create tables if they don't exist."""
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id      TEXT    NOT NULL,
                ts              REAL    NOT NULL,   -- Unix epoch (float)
                ts_iso          TEXT    NOT NULL,   -- ISO 8601 for readability
                heart_rate                  REAL,
                body_temperature            REAL,
                blood_pressure_systolic     REAL,
                blood_pressure_diastolic    REAL,
                raw_payload     TEXT            -- full JSON for auditability
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_patient_ts
            ON sensor_readings (patient_id, ts)
        """)
    log.info("Database initialised at %s", DB_PATH)


# ══════════════════════════════════════════════════════════════════════════════
# Write path (MQTT → DB)
# ══════════════════════════════════════════════════════════════════════════════

def store_reading(patient_id: str, payload: dict):
    """
    Persist a single sensor reading.  Called from the MQTT callback thread.
    WAL mode ensures concurrent REST reads are never blocked.
    """
    ts = payload.get("timestamp", time.time())
    ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    row = {
        "patient_id":                patient_id,
        "ts":                        ts,
        "ts_iso":                    ts_iso,
        "heart_rate":                payload.get("heart_rate"),
        "body_temperature":          payload.get("body_temperature"),
        "blood_pressure_systolic":   payload.get("blood_pressure_systolic"),
        "blood_pressure_diastolic":  payload.get("blood_pressure_diastolic"),
        "raw_payload":               json.dumps(payload),
    }

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO sensor_readings
                (patient_id, ts, ts_iso,
                 heart_rate, body_temperature,
                 blood_pressure_systolic, blood_pressure_diastolic,
                 raw_payload)
            VALUES
                (:patient_id, :ts, :ts_iso,
                 :heart_rate, :body_temperature,
                 :blood_pressure_systolic, :blood_pressure_diastolic,
                 :raw_payload)
            """,
            row,
        )
    log.debug("Stored reading for patient %s at %s", patient_id, ts_iso)


# ══════════════════════════════════════════════════════════════════════════════
# Read path (REST → DB)
# ══════════════════════════════════════════════════════════════════════════════

def query_history(
    patient_id: str,
    since: float,
    until: float | None = None,
    limit: int = 10_000,
) -> list[dict]:
    until = until or time.time()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT ts_iso, heart_rate, body_temperature,
                   blood_pressure_systolic, blood_pressure_diastolic
            FROM   sensor_readings
            WHERE  patient_id = ?
              AND  ts >= ?
              AND  ts <= ?
            ORDER  BY ts ASC
            LIMIT  ?
            """,
            (patient_id, since, until, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def query_latest(patient_id: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT ts_iso, heart_rate, body_temperature,
                   blood_pressure_systolic, blood_pressure_diastolic
            FROM   sensor_readings
            WHERE  patient_id = ?
            ORDER  BY ts DESC
            LIMIT  1
            """,
            (patient_id,),
        ).fetchone()
    return dict(row) if row else None


def list_patients() -> list[str]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT patient_id FROM sensor_readings ORDER BY patient_id"
        ).fetchall()
    return [r["patient_id"] for r in rows]


def count_readings(patient_id: str) -> int:
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM sensor_readings WHERE patient_id = ?",
            (patient_id,),
        ).fetchone()
    return row["n"] if row else 0


# ══════════════════════════════════════════════════════════════════════════════
# MQTT Callbacks
# ══════════════════════════════════════════════════════════════════════════════

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        topic = mqtt_config.get("subscribe_topic_sensors", "iothealth/+/sensors")
        client.subscribe(topic, qos=1)
        log.info("MQTT connected. Subscribed to %s", topic)
    else:
        log.error("MQTT connection refused, code=%d", rc)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        patient_id = payload.get("patient_id") or _patient_from_topic(msg.topic)
        if not patient_id:
            log.warning("Cannot determine patient_id from topic %s", msg.topic)
            return
        store_reading(patient_id, payload)
    except json.JSONDecodeError as exc:
        log.error("Bad JSON on %s: %s", msg.topic, exc)
    except Exception as exc:
        log.exception("Unexpected error storing reading: %s", exc)


def _patient_from_topic(topic: str) -> str | None:
    parts = topic.split("/")
    return parts[1] if len(parts) >= 2 else None


def setup_mqtt(broker_host: str, broker_port: int) -> mqtt.Client:
    client = mqtt.Client(client_id="timeseries_adaptor")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_host, broker_port, keepalive=60)
    client.loop_start()
    return client


# ══════════════════════════════════════════════════════════════════════════════
# REST API
# ══════════════════════════════════════════════════════════════════════════════

def _parse_since(since_str: str | None, default_days: int = 1) -> float:
    """Parse an ISO-8601 string or fallback to now minus default_days."""
    if since_str:
        try:
            dt = datetime.fromisoformat(since_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            pass
    return (datetime.now(timezone.utc) - timedelta(days=default_days)).timestamp()


@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "timeseries_adaptor"})


@app.get("/history/<patient_id>")
def get_history(patient_id: str):
    """
    Historical readings.  Consumed by Data Processor and Clinician Portal.

    Query params:
      since  – ISO-8601 datetime (default: 24h ago)
      until  – ISO-8601 datetime (default: now)
      period – "daily" | "weekly"  (overrides since/until with presets)
      limit  – max rows (default 10 000)
    """
    period = request.args.get("period")
    if period == "weekly":
        since_ts = _parse_since(None, default_days=7)
    elif period == "daily":
        since_ts = _parse_since(None, default_days=1)
    else:
        since_ts = _parse_since(request.args.get("since"))

    until_ts_raw = request.args.get("until")
    until_ts = (
        datetime.fromisoformat(until_ts_raw).timestamp() if until_ts_raw else None
    )
    limit = min(int(request.args.get("limit", 10_000)), 100_000)

    readings = query_history(patient_id, since_ts, until_ts, limit)
    return jsonify({
        "patient_id": patient_id,
        "count":      len(readings),
        "readings":   readings,
    })


@app.get("/patients/<patient_id>/latest")
def get_latest(patient_id: str):
    """Most-recent reading for a patient (for quick status checks)."""
    row = query_latest(patient_id)
    if not row:
        return jsonify({"error": "No readings found"}), 404
    return jsonify({"patient_id": patient_id, "latest": row})


@app.get("/patients")
def get_patients():
    return jsonify({"patients": list_patients()})


@app.get("/patients/<patient_id>/count")
def get_count(patient_id: str):
    return jsonify({"patient_id": patient_id, "count": count_readings(patient_id)})


# ── Admin / maintenance ──────────────────────────────────────────────────────

@app.delete("/patients/<patient_id>/history")
def delete_history(patient_id: str):
    """GDPR / admin: purge all readings for a patient."""
    with get_db() as conn:
        conn.execute("DELETE FROM sensor_readings WHERE patient_id = ?", (patient_id,))
    log.warning("All readings deleted for patient %s", patient_id)
    return jsonify({"deleted": True, "patient_id": patient_id})


# ══════════════════════════════════════════════════════════════════════════════
# Health Catalog bootstrap
# ══════════════════════════════════════════════════════════════════════════════

def fetch_catalog_config() -> dict:
    for attempt in range(10):
        try:
            resp = requests.get(
                f"{HEALTH_CATALOG_URL}/config/timeseries_adaptor", timeout=5
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("Catalog not ready (attempt %d/10): %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("Cannot reach Health Catalog after 10 attempts")


# ══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global mqtt_config, mqtt_client

    log.info("TimeSeriesDB Adaptor starting …")
    init_db()

    cfg = fetch_catalog_config()
    mqtt_config = cfg.get("mqtt", {})
    broker_host = mqtt_config.get("broker_host", "message_broker")
    broker_port = int(mqtt_config.get("broker_port", 1883))

    mqtt_client = setup_mqtt(broker_host, broker_port)

    log.info("REST API listening on port %d", REST_PORT)
    app.run(host="0.0.0.0", port=REST_PORT)


if __name__ == "__main__":
    main()
