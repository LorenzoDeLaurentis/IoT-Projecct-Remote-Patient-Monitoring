"""
Sensor Connector Microservice
===============================
Responsibilities:
  - REST Provider: exposes the latest sensor reading per sensorID to other
    services (e.g. Patient-Facing App, Clinician Portal) via read-only GET
    endpoints (GET /health, GET /sensors, GET /sensors/<sensorID>/latest).
    There is no write/POST endpoint.
  - REST Consumer: at startup, fetches broker/topic config
    (fetch_catalog_config) and the list of known patient sensorIDs
    (fetch_known_patients) from the Health Catalog, retrying and degrading
    gracefully (empty config/list, no crash) if the catalog is unreachable.
    The patient list is then refreshed every PATIENT_REFRESH_INTERVAL_SECONDS
    (new, removed and changed patients are applied), and the catalog config
    and the MQTT connection are retried periodically if they failed at
    startup.
  - Data Generator integration: creates one SimulatedSensor instance per
    known sensorID, and runs a background daemon thread
    (run_simulation_loop) that periodically generates a reading for each
    and feeds it into update_reading(). Each simulator's condition is read
    from the Health Catalog (the "condition" field of each patient),
    defaulting to "healthy".
  - MQTT Publisher: every reading that flows through update_reading()
    (currently only from the simulation loop, since the manual POST
    endpoint no longer exists) is also published to the Message Broker, on
    a per-sensor topic derived from the catalog-provided topic pattern,
    degrading gracefully (log warning, keep working) if the broker is
    unavailable.
"""

from __future__ import annotations

import json
import logging
import threading
import time

import cherrypy
import requests

from Data_generator import SimulatedSensor, CONDITION_PROFILES
from MyMQTT import MyMQTT

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SensorConnector] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────
REST_PORT = 5003
SIMULATION_INTERVAL_SECONDS = 5
PATIENT_REFRESH_INTERVAL_SECONDS = 60

# ─── Health Catalog config (fetched at startup, then refreshed) ──────────────
# Populated by fetch_catalog_config() / fetch_known_patients() in main().
# Stay at these empty/default values if the catalog is unreachable at
# startup so the REST provider endpoints keep working regardless.
# run_patient_refresh_loop() retries mqtt_config if it is still empty and
# updates known_sensor_ids every PATIENT_REFRESH_INTERVAL_SECONDS.
mqtt_config: dict = {}
known_sensor_ids: list[str] = []

# ─── MQTT publisher (connected at startup, retried if it failed) ─────────────
# Populated by connect_mqtt_publisher() in main(). Stays None if mqtt_config
# is empty or the broker is unreachable, so update_reading() just skips
# publishing — the REST provider/consumer roles keep working regardless.
# run_patient_refresh_loop() retries the connection while it is None.
mqtt_client: MyMQTT | None = None

# ─── Simulated sensors ───────────────────────────────────────────────────────
# _simulators[sensorID] = SimulatedSensor. Used by both the simulation thread
# and the patient refresh thread, so every access must hold _simulators_lock.
# When both locks are needed, _simulators_lock is always taken BEFORE
# _readings_lock (to avoid deadlocks).
_simulators: dict[str, SimulatedSensor] = {}
_simulators_lock = threading.Lock()

# ─── In-memory latest-reading store ──────────────────────────────────────────
# Structure: _readings[sensorID] = {
#     "sensorID": str, "timestamp": str,
#     "heart_rate": float, "body_temperature": float,
#     "blood_pressure_systolic": float, "blood_pressure_diastolic": float,
# }
_readings: dict[str, dict] = {}
_readings_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# Store (extension point)
# ══════════════════════════════════════════════════════════════════════════════

def update_reading(sensor_id: str, payload: dict):
    """
    Store the latest reading of a sensor in the in-memory store (thread-safe)
    and publish it to MQTT on the sensor's topic, if the publisher is
    connected. Called by the simulation loop for every generated reading.
    """
    with _readings_lock:
        _readings[sensor_id] = payload
    log.debug("Updated latest reading for sensor %s", sensor_id)

    if mqtt_client is not None:
        topic = mqtt_config.get("topics", {}).get("sensors", "iothealth/+/sensors").replace("+", sensor_id)
        try:
            mqtt_client.myPublish(topic, payload)
            log.debug("Published reading for sensor %s to topic %s", sensor_id, topic)
        except Exception as exc:
            log.warning("Failed to publish reading for sensor %s to MQTT: %s", sensor_id, exc)


# ══════════════════════════════════════════════════════════════════════════════
# Data Generator integration (simulation loop)
# ══════════════════════════════════════════════════════════════════════════════

def run_simulation_loop():
    """
    Runs forever in a background thread: every SIMULATION_INTERVAL_SECONDS,
    reads all simulators in _simulators and writes their output into the
    store via update_reading(). If there are no simulators, it just keeps
    sleeping.

    _simulators_lock is held for the whole cycle, so a sensor removed by the
    refresh thread never publishes or stores a reading after its removal.
    """
    while True:
        time.sleep(SIMULATION_INTERVAL_SECONDS)

        with _simulators_lock:
            for sensor_id, sensor in _simulators.items():
                reading = sensor.read()
                update_reading(sensor_id, reading)
                log.debug("Simulated reading generated for sensor %s", sensor_id)

            if _simulators:
                log.info("Simulation cycle complete: %d readings generated", len(_simulators))


def apply_patient_list(patients: dict[str, str]) -> None:
    """
    Sync _simulators with the {sensorID: condition} dict returned by
    fetch_known_patients(): creates simulators for new sensors, removes
    simulators (and their stored reading) for sensors no longer in the
    catalog, and recreates a simulator whose condition changed. Unchanged
    sensors keep their simulator, so an ongoing episode continues.
    Also updates known_sensor_ids.
    """
    global known_sensor_ids

    with _simulators_lock:
        if not patients and _simulators:
            log.warning(
                "Catalog returned no patients — removing all %d simulator(s)",
                len(_simulators),
            )

        for sensor_id in list(_simulators):
            if sensor_id not in patients:
                del _simulators[sensor_id]
                with _readings_lock:
                    _readings.pop(sensor_id, None)
                log.info("Sensor %s removed (no longer in the catalog)", sensor_id)

        for sensor_id, condition in patients.items():
            current = _simulators.get(sensor_id)
            if current is None:
                _simulators[sensor_id] = SimulatedSensor(sensor_id, condition=condition)
                log.info("Sensor %s added with condition '%s'", sensor_id, condition)
            elif current.condition != condition:
                _simulators[sensor_id] = SimulatedSensor(sensor_id, condition=condition)
                log.info(
                    "Sensor %s condition changed from '%s' to '%s'",
                    sensor_id, current.condition, condition,
                )

        known_sensor_ids = sorted(_simulators)


def run_patient_refresh_loop(catalog_url: str) -> None:
    """
    Runs forever in a background thread: every
    PATIENT_REFRESH_INTERVAL_SECONDS, retries the catalog config and the MQTT
    connection if they are still missing, then fetches the patient list
    (single attempt) and applies it with apply_patient_list(). If the
    catalog is unreachable, the current sensor list is kept. Any error is
    logged and the loop keeps running.
    """
    global mqtt_config, mqtt_client

    while True:
        time.sleep(PATIENT_REFRESH_INTERVAL_SECONDS)

        try:
            if mqtt_client is None:
                if not mqtt_config:
                    cfg = fetch_catalog_config(catalog_url, attempts=1)
                    if cfg is not None:
                        mqtt_config = cfg.get("mqtt", {})
                        log.info(
                            "Catalog config retrieved: broker=%s:%s, topics=%s",
                            mqtt_config.get("broker_host"),
                            mqtt_config.get("broker_port"),
                            mqtt_config.get("topics"),
                        )
                if mqtt_config:
                    client = connect_mqtt_publisher(mqtt_config, attempts=1)
                    if client is not None:
                        mqtt_client = client
                        log.info(
                            "MQTT publisher connected to broker %s:%s",
                            mqtt_config.get("broker_host"), mqtt_config.get("broker_port"),
                        )

            patients = fetch_known_patients(catalog_url, attempts=1)
            if patients is None:
                log.warning("Health Catalog unreachable — keeping the current sensor list")
            else:
                apply_patient_list(patients)
        except Exception:
            log.exception("Patient refresh failed")


# ══════════════════════════════════════════════════════════════════════════════
# REST API (CherryPy)
# ══════════════════════════════════════════════════════════════════════════════

class SensorConnectorService:
    exposed = True

    def GET(self, *path, **params):
        if len(path) == 0:
            raise cherrypy.HTTPError(400, "Bad request")

        elif path[0] == "health":
            return json.dumps({"status": "ok", "service": "sensor_connector"})

        elif path[0] == "sensors":
            if len(path) == 1:
                with _readings_lock:
                    sensor_ids = list(_readings.keys())
                return json.dumps({"sensors": sensor_ids})

            elif len(path) == 3 and path[2] == "latest":
                sensor_id = path[1]
                with _readings_lock:
                    reading = _readings.get(sensor_id)
                if reading is None:
                    raise cherrypy.HTTPError(404, f"No reading available for sensor {sensor_id}")
                return json.dumps(reading)

            else:
                raise cherrypy.HTTPError(400, "Bad request")

        else:
            raise cherrypy.HTTPError(400, "Bad request")


# ══════════════════════════════════════════════════════════════════════════════
# Health Catalog bootstrap (REST consumer role)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_catalog_config(catalog_url: str, attempts: int = 10) -> dict | None:
    """
    Retrieve this service's MQTT broker/topic config from the Health Catalog.

    Returns the parsed JSON on success, or None (instead of raising) if the
    catalog is still unreachable after all retries — the caller decides how
    to degrade gracefully.
    """
    for attempt in range(attempts):
        try:
            resp = requests.get(
                f"{catalog_url}/config", params={"service": "sensor_connector"}, timeout=5
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("Catalog not ready (attempt %d/%d): %s", attempt + 1, attempts, exc)
            if attempt < attempts - 1:
                time.sleep(3)
    return None


def fetch_known_patients(catalog_url: str, attempts: int = 10) -> dict[str, str] | None:
    """
    Retrieve the sensorIDs and conditions of all patients currently
    registered in the Health Catalog.

    Returns a dict {sensorID: condition} on success (patients missing a
    sensorID or with sensorID "N/A" are skipped; a missing condition
    defaults to "healthy", and an unknown one is logged and replaced with
    "healthy"), or None (instead of raising) if the catalog is still
    unreachable after all retries.
    """
    for attempt in range(attempts):
        try:
            resp = requests.get(f"{catalog_url}/get_all_patients", timeout=5)
            resp.raise_for_status()
            patients = resp.json()
            conditions = {}
            for p in patients:
                if not p.get("sensorID") or p["sensorID"].strip().upper() == "N/A":
                    continue
                condition = p.get("condition", "healthy")
                if condition not in CONDITION_PROFILES:
                    log.warning(
                        "Unknown condition '%s' for sensor %s — using 'healthy' instead",
                        condition, p["sensorID"],
                    )
                    condition = "healthy"
                conditions[p["sensorID"]] = condition
            return conditions
        except Exception as exc:
            log.warning("Catalog not ready (attempt %d/%d): %s", attempt + 1, attempts, exc)
            if attempt < attempts - 1:
                time.sleep(3)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MQTT Publisher
# ══════════════════════════════════════════════════════════════════════════════

def connect_mqtt_publisher(mqtt_config: dict, attempts: int = 10) -> MyMQTT | None:
    """
    Connect a publisher-only MyMQTT client (notifier=None, never subscribes)
    to the broker described by mqtt_config, matching the MyMQTT convention
    used by time_based_alert.py.

    Returns the started MyMQTT instance on success, or None (instead of
    raising) if mqtt_config is empty or the broker is still unreachable
    after all retries — the caller decides how to degrade gracefully.
    """
    if not mqtt_config:
        log.warning(
            "No mqtt_config available (Health Catalog was unreachable at "
            "startup) — MQTT publishing will be disabled."
        )
        return None

    broker_host = mqtt_config.get("broker_host", "message_broker")
    broker_port = int(mqtt_config.get("broker_port", 1883))

    for attempt in range(attempts):
        try:
            client = MyMQTT("SensorConnector_Publisher", broker_host, broker_port, None)
            client.start()
            return client
        except Exception as exc:
            log.warning("Broker not ready (attempt %d/%d): %s", attempt + 1, attempts, exc)
            if attempt < attempts - 1:
                time.sleep(3)

    log.warning(
        "Could not connect to the MQTT broker after %d attempt(s). Continuing "
        "without MQTT publishing — readings will still be stored via REST.",
        attempts,
    )
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global mqtt_config, known_sensor_ids, mqtt_client

    catalog_url = json.load(open("conf.json"))["catalog_url"]

    cfg = fetch_catalog_config(catalog_url)
    if cfg is None:
        log.warning(
            "Health Catalog unreachable after 10 attempts. Starting Sensor "
            "Connector with an EMPTY mqtt_config — catalog-dependent features "
            "will be unavailable until the catalog becomes reachable."
        )
        mqtt_config = {}
    else:
        mqtt_config = cfg.get("mqtt", {})
        log.info(
            "Catalog config retrieved: broker=%s:%s, topics=%s",
            mqtt_config.get("broker_host"),
            mqtt_config.get("broker_port"),
            mqtt_config.get("topics"),
        )

    mqtt_client = connect_mqtt_publisher(mqtt_config)
    if mqtt_client is not None:
        log.info("MQTT publisher connected to broker %s:%s", mqtt_config.get("broker_host"), mqtt_config.get("broker_port"))

    patients = fetch_known_patients(catalog_url)
    if patients is None:
        log.warning(
            "Health Catalog unreachable after 10 attempts. Starting Sensor "
            "Connector with an EMPTY known_sensor_ids list — catalog-dependent "
            "features will be unavailable until the catalog becomes reachable."
        )
        known_sensor_ids = []
    else:
        known_sensor_ids = list(patients.keys())
        log.info("Known sensor IDs retrieved from catalog: %d found", len(known_sensor_ids))

    apply_patient_list(patients if patients is not None else {})
    with _simulators_lock:
        n_simulators = len(_simulators)
    log.info("%d simulator(s) created", n_simulators)
    if n_simulators == 0:
        log.warning(
            "No known sensorIDs available — the simulation loop will stay idle "
            "until the next successful refresh from the catalog."
        )

    threading.Thread(target=run_simulation_loop, daemon=True).start()
    threading.Thread(target=run_patient_refresh_loop, args=(catalog_url,), daemon=True).start()
    log.info("Patient list will be refreshed every %d seconds", PATIENT_REFRESH_INTERVAL_SECONDS)

    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True,
        }
    }

    cherrypy.config.update({
        'server.socket_host': '0.0.0.0',
        'server.socket_port': REST_PORT,
    })

    log.info("Sensor Connector starting up, REST API listening on port %d", REST_PORT)
    cherrypy.tree.mount(SensorConnectorService(), '/', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == "__main__":
    main()
