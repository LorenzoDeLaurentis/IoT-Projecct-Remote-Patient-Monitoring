"""
Data Processor Microservice
============================
Responsibilities:
  - MQTT Subscriber: consumes real-time sensor readings from the Message Broker
  - Real-Time Rolling Window Analysis (short-term, last 5-15 min in memory)
  - Long-Term Trend Extraction (on-demand REST or hourly periodic)
  - MQTT Publisher: publishes rolling statistics back to the broker
  - REST Provider: exposes historical trend summaries on demand
"""

import json
import time
import threading
import logging
import statistics
import math
from collections import deque
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import requests
from flask import Flask, jsonify, request

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DataProcessor] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────
HEALTH_CATALOG_URL = "http://health_catalog:5000"
TIMESERIES_ADAPTOR_URL = "http://timeseries_adaptor:5001"
REST_PORT = 5002

# Rolling window duration (seconds). Keep last 15 minutes by default.
WINDOW_SECONDS = 15 * 60

# Metrics we track per patient
METRICS = ["heart_rate", "body_temperature", "blood_pressure_systolic", "blood_pressure_diastolic"]

# ─── In-memory rolling buffer ────────────────────────────────────────────────
# Structure: buffers[patient_id][metric] = deque of (timestamp, value)
_buffers: dict[str, dict[str, deque]] = {}
_buffers_lock = threading.Lock()

# Baseline means loaded from catalog (used for z-score)
# Structure: baselines[patient_id][metric] = {"mean": float, "std": float}
_baselines: dict[str, dict[str, dict]] = {}

# Published trend cache (for REST responses)
_trend_cache: dict[str, dict] = {}
_trend_cache_lock = threading.Lock()

app = Flask(__name__)

# ─── MQTT configuration (fetched from Health Catalog at startup) ─────────────
mqtt_config: dict = {}
mqtt_client: mqtt.Client | None = None


# ══════════════════════════════════════════════════════════════════════════════
# Health Catalog bootstrap
# ══════════════════════════════════════════════════════════════════════════════

def fetch_catalog_config() -> dict:
    """Retrieve broker and topic config from the Health Catalog."""
    for attempt in range(10):
        try:
            resp = requests.get(f"{HEALTH_CATALOG_URL}/config/data_processor", timeout=5)
            resp.raise_for_status()
            cfg = resp.json()
            log.info("Catalog config loaded: %s", cfg)
            return cfg
        except Exception as exc:
            log.warning("Catalog not ready (attempt %d/10): %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("Cannot reach Health Catalog after 10 attempts")


def fetch_patient_baselines(patient_id: str) -> dict:
    """
    Fetch per-patient baseline statistics from the catalog.
    Falls back to broad population defaults if not configured.
    """
    defaults = {
        "heart_rate":                  {"mean": 72.0,  "std": 10.0},
        "body_temperature":            {"mean": 36.6,  "std": 0.4},
        "blood_pressure_systolic":     {"mean": 120.0, "std": 12.0},
        "blood_pressure_diastolic":    {"mean": 80.0,  "std": 8.0},
    }
    try:
        resp = requests.get(
            f"{HEALTH_CATALOG_URL}/patients/{patient_id}/thresholds", timeout=5
        )
        if resp.ok:
            catalog_data = resp.json()
            # Merge catalog into defaults (catalog wins)
            for metric, vals in catalog_data.get("baselines", {}).items():
                defaults[metric] = vals
    except Exception as exc:
        log.warning("Could not fetch baselines for %s: %s", patient_id, exc)
    return defaults


# ══════════════════════════════════════════════════════════════════════════════
# Rolling Window Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _ensure_patient_buffer(patient_id: str):
    if patient_id not in _buffers:
        _buffers[patient_id] = {m: deque() for m in METRICS}
    if patient_id not in _baselines:
        _baselines[patient_id] = fetch_patient_baselines(patient_id)


def _prune_old_entries(dq: deque, cutoff_ts: float):
    """Remove entries older than cutoff_ts from the left of the deque."""
    while dq and dq[0][0] < cutoff_ts:
        dq.popleft()


def _rolling_stats(values: list[float]) -> dict:
    """Compute rolling statistics for a list of numeric values."""
    if not values:
        return {}
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n if n > 1 else 0.0
    std = math.sqrt(variance)
    rate_of_change = (values[-1] - values[0]) / max(n - 1, 1) if n > 1 else 0.0
    return {
        "count":          n,
        "mean":           round(mean, 4),
        "std":            round(std, 4),
        "min":            round(min(values), 4),
        "max":            round(max(values), 4),
        "latest":         round(values[-1], 4),
        "rate_of_change": round(rate_of_change, 6),
    }


def _z_score(value: float, baseline: dict) -> float:
    """Normalised z-score of value against a baseline mean/std."""
    std = baseline.get("std", 1.0) or 1.0
    return round((value - baseline.get("mean", value)) / std, 4)


# ══════════════════════════════════════════════════════════════════════════════
# Core Processing: Real-Time Rolling Analysis
# ══════════════════════════════════════════════════════════════════════════════

def process_sensor_reading(patient_id: str, payload: dict):
    """
    Ingest a new sensor reading into the in-memory rolling buffer,
    compute statistics, and publish them back over MQTT.
    """
    ts = payload.get("timestamp", time.time())
    cutoff = ts - WINDOW_SECONDS

    with _buffers_lock:
        _ensure_patient_buffer(patient_id)
        patient_buf = _buffers[patient_id]
        patient_baselines = _baselines[patient_id]

        stats_snapshot = {}
        anomalies = []

        for metric in METRICS:
            raw_value = payload.get(metric)
            if raw_value is None:
                continue

            dq = patient_buf[metric]
            dq.append((ts, float(raw_value)))
            _prune_old_entries(dq, cutoff)

            values = [v for _, v in dq]
            stats = _rolling_stats(values)

            z = _z_score(float(raw_value), patient_baselines.get(metric, {}))
            stats["z_score"] = z

            stats_snapshot[metric] = stats

            # Anomaly detection: |z| > 2 triggers a flag
            if abs(z) > 2.0:
                anomalies.append({
                    "metric":  metric,
                    "value":   raw_value,
                    "z_score": z,
                    "severity": "high" if abs(z) > 3.0 else "medium",
                })

    result = {
        "patient_id": patient_id,
        "window_seconds": WINDOW_SECONDS,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "stats": stats_snapshot,
        "anomalies": anomalies,
    }

    # Publish rolling stats back to broker
    _publish_stats(patient_id, result)

    if anomalies:
        log.warning("Anomaly detected for patient %s: %s", patient_id, anomalies)

    return result


def _publish_stats(patient_id: str, result: dict):
    if mqtt_client is None:
        return
    topic = mqtt_config.get("publish_topic_stats", f"iothealth/{patient_id}/stats")
    mqtt_client.publish(topic, json.dumps(result), qos=1)
    log.debug("Published rolling stats to %s", topic)


# ══════════════════════════════════════════════════════════════════════════════
# Core Processing: Long-Term Trend Extraction
# ══════════════════════════════════════════════════════════════════════════════

def extract_long_term_trends(patient_id: str, period: str = "daily") -> dict:
    """
    Pull historical data from the TimeSeriesDB Adaptor and compute trends.
    period: "daily" | "weekly"
    """
    now = datetime.now(timezone.utc)
    if period == "weekly":
        since = (now - timedelta(days=7)).isoformat()
    else:
        since = (now - timedelta(days=1)).isoformat()

    try:
        resp = requests.get(
            f"{TIMESERIES_ADAPTOR_URL}/history/{patient_id}",
            params={"since": since, "period": period},
            timeout=10,
        )
        resp.raise_for_status()
        history: list[dict] = resp.json().get("readings", [])
    except Exception as exc:
        log.error("Could not fetch history for %s: %s", patient_id, exc)
        return {"error": str(exc)}

    if not history:
        return {"patient_id": patient_id, "period": period, "trends": {}}

    # Group readings by metric
    metric_series: dict[str, list[float]] = {m: [] for m in METRICS}
    for entry in history:
        for metric in METRICS:
            if metric in entry:
                metric_series[metric].append(float(entry[metric]))

    trends = {}
    for metric, values in metric_series.items():
        if not values:
            continue
        n = len(values)
        mean = sum(values) / n
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / max(n - 1, 1))
        # Linear regression for trend direction
        slope = _linear_slope(values)
        trends[metric] = {
            "count":     n,
            "mean":      round(mean, 4),
            "std":       round(std, 4),
            "min":       round(min(values), 4),
            "max":       round(max(values), 4),
            "slope":     round(slope, 6),   # positive = rising trend
            "trend":     "rising" if slope > 0.01 else ("falling" if slope < -0.01 else "stable"),
        }

    result = {
        "patient_id": patient_id,
        "period":     period,
        "since":      since,
        "computed_at": now.isoformat(),
        "trends":     trends,
    }

    with _trend_cache_lock:
        _trend_cache[patient_id] = result

    log.info("Long-term trends computed for patient %s (%s)", patient_id, period)
    return result


def _linear_slope(values: list[float]) -> float:
    """Least-squares slope of a 1-D time series."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den != 0 else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Hourly Background Trend Job
# ══════════════════════════════════════════════════════════════════════════════

def _hourly_trend_job():
    """Runs in a background thread; extracts trends every hour for active patients."""
    while True:
        time.sleep(3600)
        with _buffers_lock:
            active_patients = list(_buffers.keys())
        for pid in active_patients:
            try:
                extract_long_term_trends(pid, period="daily")
            except Exception as exc:
                log.error("Hourly trend job failed for %s: %s", pid, exc)


# ══════════════════════════════════════════════════════════════════════════════
# MQTT Callbacks
# ══════════════════════════════════════════════════════════════════════════════

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        topic = mqtt_config.get("subscribe_topic_sensors", "iothealth/+/sensors")
        client.subscribe(topic, qos=1)
        log.info("MQTT connected. Subscribed to %s", topic)
    else:
        log.error("MQTT connection failed with code %d", rc)


def on_message(client, userdata, msg):
    """Handle incoming sensor MQTT messages."""
    try:
        payload = json.loads(msg.payload.decode())
        patient_id = payload.get("patient_id") or _extract_patient_from_topic(msg.topic)
        if not patient_id:
            log.warning("No patient_id in message on topic %s", msg.topic)
            return
        process_sensor_reading(patient_id, payload)
    except json.JSONDecodeError as exc:
        log.error("Invalid JSON on topic %s: %s", msg.topic, exc)
    except Exception as exc:
        log.exception("Unexpected error processing message: %s", exc)


def _extract_patient_from_topic(topic: str) -> str | None:
    """Extract patient ID from topic pattern iothealth/<patient_id>/sensors."""
    parts = topic.split("/")
    return parts[1] if len(parts) >= 2 else None


def setup_mqtt(broker_host: str, broker_port: int) -> mqtt.Client:
    client = mqtt.Client(client_id="data_processor")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_host, broker_port, keepalive=60)
    client.loop_start()
    return client


# ══════════════════════════════════════════════════════════════════════════════
# REST API (Flask)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "data_processor"})


@app.get("/patients/<patient_id>/rolling-stats")
def get_rolling_stats(patient_id: str):
    """
    Returns the latest in-memory rolling statistics for a patient.
    This is the key "synchronization" endpoint: callers get the most recent
    rolling analysis computed from the live MQTT stream.
    """
    with _buffers_lock:
        if patient_id not in _buffers:
            return jsonify({"error": "No data for patient"}), 404
        patient_buf = _buffers[patient_id]
        patient_baselines = _baselines.get(patient_id, {})
        ts = time.time()
        cutoff = ts - WINDOW_SECONDS
        snapshot = {}
        for metric, dq in patient_buf.items():
            _prune_old_entries(dq, cutoff)
            values = [v for _, v in dq]
            if not values:
                continue
            stats = _rolling_stats(values)
            baseline = patient_baselines.get(metric, {})
            stats["z_score"] = _z_score(values[-1], baseline)
            snapshot[metric] = stats

    return jsonify({
        "patient_id":     patient_id,
        "window_seconds": WINDOW_SECONDS,
        "computed_at":    datetime.now(timezone.utc).isoformat(),
        "stats":          snapshot,
    })


@app.get("/patients/<patient_id>/trends")
def get_trends(patient_id: str):
    """
    Returns long-term trends. Serves from cache if fresh (<1h),
    otherwise fetches live from TimeSeriesDB Adaptor.
    This is the "on-demand REST historical" endpoint described in the proposal.
    """
    period = request.args.get("period", "daily")

    # Check cache freshness
    with _trend_cache_lock:
        cached = _trend_cache.get(patient_id)

    if cached:
        try:
            computed_at = datetime.fromisoformat(cached["computed_at"])
            age = (datetime.now(timezone.utc) - computed_at).total_seconds()
            if age < 3600 and cached.get("period") == period:
                log.info("Returning cached trends for %s", patient_id)
                return jsonify(cached)
        except Exception:
            pass  # Fall through to fresh computation

    # On-demand fresh computation (blocking but fast enough for REST)
    result = extract_long_term_trends(patient_id, period=period)
    return jsonify(result)


@app.get("/patients/<patient_id>/unified-stats")
def get_unified_stats(patient_id: str):
    """
    **The hardest task** – Combines real-time rolling stats with long-term trends
    in a single response, with seamless synchronization.

    Strategy:
      1. Read the current in-memory rolling buffer (real-time, from MQTT stream).
      2. In parallel, kick off (or return cached) long-term trend computation.
      3. Merge both into a unified response with a 'data_source' annotation per field.
    """
    period = request.args.get("period", "daily")

    # ── Step 1: Real-time rolling stats (always fresh from buffer) ───────────
    with _buffers_lock:
        has_realtime = patient_id in _buffers
        if has_realtime:
            patient_buf = _buffers[patient_id]
            patient_baselines = _baselines.get(patient_id, {})
            ts = time.time()
            cutoff = ts - WINDOW_SECONDS
            realtime_snapshot = {}
            for metric, dq in patient_buf.items():
                _prune_old_entries(dq, cutoff)
                values = [v for _, v in dq]
                if not values:
                    continue
                stats = _rolling_stats(values)
                baseline = patient_baselines.get(metric, {})
                stats["z_score"] = _z_score(values[-1], baseline)
                realtime_snapshot[metric] = stats
        else:
            realtime_snapshot = {}

    # ── Step 2: Long-term trends (cached or freshly fetched) ─────────────────
    with _trend_cache_lock:
        cached = _trend_cache.get(patient_id)

    if cached and cached.get("period") == period:
        try:
            computed_at = datetime.fromisoformat(cached["computed_at"])
            age = (datetime.now(timezone.utc) - computed_at).total_seconds()
            if age < 3600:
                trend_result = cached
            else:
                trend_result = extract_long_term_trends(patient_id, period)
        except Exception:
            trend_result = extract_long_term_trends(patient_id, period)
    else:
        trend_result = extract_long_term_trends(patient_id, period)

    # ── Step 3: Merge ─────────────────────────────────────────────────────────
    unified = {}
    all_metrics = set(realtime_snapshot.keys()) | set(
        trend_result.get("trends", {}).keys()
    )
    for metric in all_metrics:
        rt = realtime_snapshot.get(metric)
        lt = trend_result.get("trends", {}).get(metric)
        unified[metric] = {
            "realtime":  rt,   # may be None if no live data yet
            "longterm":  lt,   # may be None if no history yet
            # Convenience: prefer real-time mean if available, else fall back
            "current_mean": (rt or {}).get("mean") or (lt or {}).get("mean"),
            "trend_direction": (lt or {}).get("trend", "unknown"),
            "latest_z_score":  (rt or {}).get("z_score"),
        }

    return jsonify({
        "patient_id":  patient_id,
        "period":      period,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "unified":     unified,
        "realtime_window_seconds": WINDOW_SECONDS,
        "longterm_since": trend_result.get("since"),
    })


# ══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global mqtt_config, mqtt_client

    log.info("Data Processor starting up …")
    cfg = fetch_catalog_config()
    mqtt_config = cfg.get("mqtt", {})
    broker_host = mqtt_config.get("broker_host", "message_broker")
    broker_port = int(mqtt_config.get("broker_port", 1883))

    mqtt_client = setup_mqtt(broker_host, broker_port)

    # Start hourly background job
    t = threading.Thread(target=_hourly_trend_job, daemon=True)
    t.start()
    log.info("Hourly trend job started.")

    log.info("REST API listening on port %d", REST_PORT)
    app.run(host="0.0.0.0", port=REST_PORT)


if __name__ == "__main__":
    main()