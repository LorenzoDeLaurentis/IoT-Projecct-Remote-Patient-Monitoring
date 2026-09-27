"""
ThingSpeak Forwarder Microservice
===================================
Responsibilities:
  - MQTT Subscriber: receives real-time sensor readings from the Message Broker
  - Forwards data to ThingSpeak channels via REST API (POST)
  - Enforces the 15-second minimum write interval per channel (free-tier limit)
  - Queues readings during the cooldown and sends the most-recent value when
    the interval expires (no data is silently dropped — latest wins)
  - REST Provider: exposes /status for monitoring and /flush for manual trigger

ThingSpeak channel field mapping (per patient channel):
  field1 → heart_rate
  field2 → body_temperature
  field3 → blood_pressure_systolic
  field4 → blood_pressure_diastolic
  field5 → rolling_mean_hr        (from Data Processor stats, if present)
  field6 → z_score_hr             (from Data Processor stats, if present)

Each patient maps to its own ThingSpeak channel (channel_id + write_api_key),
stored in the Health Catalog under /patients/<id>/thingspeak.
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import requests
from flask import Flask, jsonify

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ThingSpeakFwd] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
HEALTH_CATALOG_URL   = "http://health_catalog:5000"
THINGSPEAK_API_URL   = "https://api.thingspeak.com/update.json"
REST_PORT            = 5004
WRITE_INTERVAL_SEC   = 15       # ThingSpeak free-tier minimum interval
QUEUE_POLL_INTERVAL  = 1        # How often the sender thread checks the queue (s)

# Field mapping: metric name → ThingSpeak field number
FIELD_MAP = {
    "heart_rate":                 "field1",
    "body_temperature":           "field2",
    "blood_pressure_systolic":    "field3",
    "blood_pressure_diastolic":   "field4",
    "rolling_mean_hr":            "field5",
    "z_score_hr":                 "field6",
}

# ─── Shared state ────────────────────────────────────────────────────────────
# Per-patient channel config: {patient_id: {"channel_id": str, "write_api_key": str}}
_channel_cfg: dict[str, dict] = {}
_channel_cfg_lock = threading.Lock()

# Per-patient pending payload (latest reading waiting to be sent)
# {patient_id: {"payload": dict, "queued_at": float}}
_queue: dict[str, dict] = {}
_queue_lock = threading.Lock()

# Per-patient last-sent timestamp
_last_sent: dict[str, float] = {}
_last_sent_lock = threading.Lock()

# Counters for /status endpoint
_stats = {"sent": 0, "dropped": 0, "errors": 0, "queued": 0}
_stats_lock = threading.Lock()

app = Flask(__name__)
mqtt_config: dict = {}
mqtt_client: mqtt.Client | None = None


# ══════════════════════════════════════════════════════════════════════════════
# Health Catalog bootstrap
# ══════════════════════════════════════════════════════════════════════════════

def fetch_catalog_config() -> dict:
    for attempt in range(10):
        try:
            resp = requests.get(
                f"{HEALTH_CATALOG_URL}/config/thingspeak_forwarder", timeout=5
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("Catalog not ready (attempt %d/10): %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("Cannot reach Health Catalog after 10 attempts")


def fetch_patient_thingspeak_cfg(patient_id: str) -> dict | None:
    """
    Fetch the ThingSpeak channel config for a patient from the Health Catalog.
    Expected response: {"channel_id": "...", "write_api_key": "..."}
    Returns None if the patient has no ThingSpeak channel configured.
    """
    with _channel_cfg_lock:
        if patient_id in _channel_cfg:
            return _channel_cfg[patient_id]

    try:
        resp = requests.get(
            f"{HEALTH_CATALOG_URL}/patients/{patient_id}/thingspeak", timeout=5
        )
        if resp.status_code == 404:
            log.info("Patient %s has no ThingSpeak channel configured.", patient_id)
            return None
        resp.raise_for_status()
        cfg = resp.json()
        with _channel_cfg_lock:
            _channel_cfg[patient_id] = cfg
        log.info("ThingSpeak config loaded for patient %s: channel %s", patient_id, cfg.get("channel_id"))
        return cfg
    except Exception as exc:
        log.error("Failed to fetch ThingSpeak config for %s: %s", patient_id, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# ThingSpeak write logic
# ══════════════════════════════════════════════════════════════════════════════

def _build_thingspeak_payload(write_api_key: str, sensor_data: dict) -> dict:
    """
    Map our internal sensor payload to ThingSpeak field names.
    Also extracts rolling stats if the Data Processor has attached them.
    """
    body = {"api_key": write_api_key}

    # Direct sensor metrics
    for metric, field in FIELD_MAP.items():
        # Check both top-level and nested under "stats"
        value = sensor_data.get(metric)
        if value is None and "stats" in sensor_data:
            # Data Processor publishes nested: stats.heart_rate.mean, etc.
            stats = sensor_data["stats"]
            if metric == "rolling_mean_hr":
                value = (stats.get("heart_rate") or {}).get("mean")
            elif metric == "z_score_hr":
                value = (stats.get("heart_rate") or {}).get("z_score")

        if value is not None:
            body[field] = round(float(value), 4)

    # Optional: include patient status as ThingSpeak "status" field
    if sensor_data.get("anomalies"):
        severities = [a.get("severity", "") for a in sensor_data["anomalies"]]
        body["status"] = "ALERT:" + ",".join(
            f"{a['metric']}(z={a['z_score']})" for a in sensor_data["anomalies"]
        )[:255]  # ThingSpeak status field max 255 chars

    return body


def send_to_thingspeak(patient_id: str, sensor_data: dict) -> bool:
    """
    POST sensor data to the patient's ThingSpeak channel.
    Returns True on success, False on failure.
    """
    cfg = fetch_patient_thingspeak_cfg(patient_id)
    if not cfg:
        return False  # No channel configured for this patient

    body = _build_thingspeak_payload(cfg["write_api_key"], sensor_data)

    # Need at least one field besides api_key
    if len(body) <= 1:
        log.debug("No field data to send for patient %s, skipping.", patient_id)
        return False

    try:
        resp = requests.post(THINGSPEAK_API_URL, data=body, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        entry_id = result.get("entry_id", 0)
        if entry_id == 0:
            log.warning("ThingSpeak rejected update for patient %s (returned entry_id=0). "
                        "Possible duplicate timestamp or rate limit.", patient_id)
            with _stats_lock:
                _stats["errors"] += 1
            return False

        log.info("ThingSpeak write OK — patient=%s channel=%s entry_id=%s",
                 patient_id, cfg.get("channel_id"), entry_id)
        with _stats_lock:
            _stats["sent"] += 1
        return True

    except requests.HTTPError as exc:
        log.error("ThingSpeak HTTP error for patient %s: %s", patient_id, exc)
    except requests.Timeout:
        log.error("ThingSpeak timeout for patient %s", patient_id)
    except Exception as exc:
        log.exception("Unexpected ThingSpeak error for patient %s: %s", patient_id, exc)

    with _stats_lock:
        _stats["errors"] += 1
    return False


# ══════════════════════════════════════════════════════════════════════════════
# Rate-limited sender thread
# ══════════════════════════════════════════════════════════════════════════════

def _sender_loop():
    """
    Background thread that drains the queue, respecting the 15-second
    ThingSpeak write interval per channel.

    Strategy: "latest-wins buffering"
      - When a reading arrives faster than the interval, we overwrite the
        queued entry with the newest value.
      - When the interval expires, we flush the newest buffered value.
      - This ensures ThingSpeak always receives the most current data
        without ever exceeding the rate limit.
    """
    log.info("ThingSpeak sender thread started (interval=%ds)", WRITE_INTERVAL_SEC)
    while True:
        time.sleep(QUEUE_POLL_INTERVAL)
        now = time.time()

        with _queue_lock:
            candidates = list(_queue.items())

        for patient_id, entry in candidates:
            with _last_sent_lock:
                last = _last_sent.get(patient_id, 0)

            elapsed = now - last
            if elapsed < WRITE_INTERVAL_SEC:
                continue  # Still in cooldown

            # Pop from queue
            with _queue_lock:
                if patient_id not in _queue:
                    continue
                pending = _queue.pop(patient_id)

            wait_ms = int((now - pending["queued_at"]) * 1000)
            log.debug("Flushing queued reading for patient %s (waited %dms)", patient_id, wait_ms)

            success = send_to_thingspeak(patient_id, pending["payload"])
            if success:
                with _last_sent_lock:
                    _last_sent[patient_id] = time.time()


def _enqueue(patient_id: str, payload: dict):
    """
    Add (or replace) the pending payload for a patient.
    If the interval has already elapsed, send immediately.
    """
    with _stats_lock:
        _stats["queued"] += 1

    now = time.time()
    with _last_sent_lock:
        last = _last_sent.get(patient_id, 0)

    elapsed = now - last

    if elapsed >= WRITE_INTERVAL_SEC:
        # Can send right away — no need to queue
        success = send_to_thingspeak(patient_id, payload)
        if success:
            with _last_sent_lock:
                _last_sent[patient_id] = time.time()
    else:
        # Queue the latest value (overwrite any previously queued reading)
        with _queue_lock:
            if patient_id in _queue:
                with _stats_lock:
                    _stats["dropped"] += 1  # Previous queued value superseded
            _queue[patient_id] = {"payload": payload, "queued_at": now}
        remaining = WRITE_INTERVAL_SEC - elapsed
        log.debug("Patient %s queued (%.1fs until next send)", patient_id, remaining)


# ══════════════════════════════════════════════════════════════════════════════
# MQTT — subscribe to both sensor readings AND Data Processor stats
# ══════════════════════════════════════════════════════════════════════════════

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        # Subscribe to raw sensor readings
        sensor_topic = mqtt_config.get("subscribe_topic_sensors", "iothealth/+/sensors")
        client.subscribe(sensor_topic, qos=1)
        # Subscribe to Data Processor rolling stats (for field5/field6)
        stats_topic = mqtt_config.get("subscribe_topic_stats", "iothealth/+/stats")
        client.subscribe(stats_topic, qos=1)
        log.info("MQTT connected. Subscribed to %s and %s", sensor_topic, stats_topic)
    else:
        log.error("MQTT connection failed, code=%d", rc)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        patient_id = payload.get("patient_id") or _patient_from_topic(msg.topic)
        if not patient_id:
            return
        _enqueue(patient_id, payload)
    except json.JSONDecodeError as exc:
        log.error("Bad JSON on %s: %s", msg.topic, exc)
    except Exception as exc:
        log.exception("Error handling MQTT message: %s", exc)


def _patient_from_topic(topic: str) -> str | None:
    parts = topic.split("/")
    return parts[1] if len(parts) >= 2 else None


def setup_mqtt(broker_host: str, broker_port: int) -> mqtt.Client:
    client = mqtt.Client(client_id="thingspeak_forwarder")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_host, broker_port, keepalive=60)
    client.loop_start()
    return client


# ══════════════════════════════════════════════════════════════════════════════
# REST API
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "thingspeak_forwarder"})


@app.get("/status")
def status():
    """Operational metrics for monitoring dashboards."""
    with _stats_lock:
        s = dict(_stats)
    with _queue_lock:
        pending = list(_queue.keys())
    with _last_sent_lock:
        last_sent = {pid: datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                     for pid, ts in _last_sent.items()}
    return jsonify({
        "service":        "thingspeak_forwarder",
        "write_interval": WRITE_INTERVAL_SEC,
        "counters":       s,
        "queue_depth":    len(pending),
        "pending_patients": pending,
        "last_sent":      last_sent,
    })


@app.post("/patients/<patient_id>/flush")
def flush(patient_id: str):
    """
    Manually flush the queued reading for a patient (e.g. for testing).
    Ignores the rate-limit cooldown.
    """
    with _queue_lock:
        entry = _queue.pop(patient_id, None)
    if not entry:
        return jsonify({"flushed": False, "reason": "no pending data"}), 404

    success = send_to_thingspeak(patient_id, entry["payload"])
    if success:
        with _last_sent_lock:
            _last_sent[patient_id] = time.time()
    return jsonify({"flushed": success, "patient_id": patient_id})


@app.delete("/patients/<patient_id>/channel-cache")
def clear_channel_cache(patient_id: str):
    """Force re-fetch of ThingSpeak channel config from the Health Catalog."""
    with _channel_cfg_lock:
        _channel_cfg.pop(patient_id, None)
    return jsonify({"cleared": True, "patient_id": patient_id})


# ══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global mqtt_config, mqtt_client

    log.info("ThingSpeak Forwarder starting …")
    cfg = fetch_catalog_config()
    mqtt_config = cfg.get("mqtt", {})
    broker_host = mqtt_config.get("broker_host", "message_broker")
    broker_port = int(mqtt_config.get("broker_port", 1883))

    mqtt_client = setup_mqtt(broker_host, broker_port)

    # Start rate-limited sender thread
    t = threading.Thread(target=_sender_loop, daemon=True)
    t.start()

    log.info("REST API listening on port %d", REST_PORT)
    app.run(host="0.0.0.0", port=REST_PORT)


if __name__ == "__main__":
    main()
