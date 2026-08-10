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
  - Data Generator integration: creates one SimulatedSensor instance per
    known sensorID, and runs a background daemon thread
    (run_simulation_loop) that periodically generates a reading for each
    and feeds it into update_reading().
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

from Data_generator import SimulatedSensor
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

# ─── Health Catalog config (fetched once at startup) ─────────────────────────
# Populated by fetch_catalog_config() / fetch_known_patients() in main().
# Stay at these empty/default values if the catalog is unreachable at
# startup so the REST provider endpoints keep working regardless.
mqtt_config: dict = {}
known_sensor_ids: list[str] = []

# ─── MQTT publisher (connected once at startup) ───────────────────────────────
# Populated by connect_mqtt_publisher() in main(). Stays None if mqtt_config
# is empty or the broker is unreachable, so update_reading() just skips
# publishing — the REST provider/consumer roles keep working regardless.
mqtt_client: MyMQTT | None = None

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
    Safely write the latest reading for a sensor into the in-memory store.

    This is the extension point future producers plug into: an MQTT
    on_message callback and/or the data generator loop will call this
    directly once the ingestion path exists. Nothing calls it yet besides
    the temporary manual-testing POST endpoint below.
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

def run_simulation_loop(simulators: dict):
    """
    Runs forever in a background thread: every SIMULATION_INTERVAL_SECONDS,
    reads all simulators and writes their output into the store via
    update_reading(). If simulators is empty, it just keeps sleeping.
    """
    while True:
        time.sleep(SIMULATION_INTERVAL_SECONDS)

        for sensor_id, sensor in simulators.items():
            reading = sensor.read()
            update_reading(sensor_id, reading)
            log.debug("Simulated reading generated for sensor %s", sensor_id)

        if simulators:
            log.info("Simulation cycle complete: %d readings generated", len(simulators))


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

def fetch_catalog_config(catalog_url: str) -> dict | None:
    """
    Retrieve this service's MQTT broker/topic config from the Health Catalog.

    Returns the parsed JSON on success, or None (instead of raising) if the
    catalog is still unreachable after all retries — the caller decides how
    to degrade gracefully.
    """
    for attempt in range(10):
        try:
            resp = requests.get(
                f"{catalog_url}/config", params={"service": "sensor_connector"}, timeout=5
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("Catalog not ready (attempt %d/10): %s", attempt + 1, exc)
            time.sleep(3)
    return None


def fetch_known_patients(catalog_url: str) -> list[str] | None:
    """
    Retrieve the sensorIDs of all patients currently registered in the
    Health Catalog.

    Returns the list of sensorIDs on success (patients missing a sensorID
    are skipped), or None (instead of raising) if the catalog is still
    unreachable after all retries.
    """
    for attempt in range(10):
        try:
            resp = requests.get(f"{catalog_url}/get_all_patients", timeout=5)
            resp.raise_for_status()
            patients = resp.json()
            return [
                p["sensorID"] for p in patients
                if p.get("sensorID") and p["sensorID"].strip().upper() != "N/A"
            ]
        except Exception as exc:
            log.warning("Catalog not ready (attempt %d/10): %s", attempt + 1, exc)
            time.sleep(3)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MQTT Publisher
# ══════════════════════════════════════════════════════════════════════════════

def connect_mqtt_publisher(mqtt_config: dict) -> MyMQTT | None:
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

    for attempt in range(10):
        try:
            client = MyMQTT("SensorConnector_Publisher", broker_host, broker_port, None)
            client.start()
            return client
        except Exception as exc:
            log.warning("Broker not ready (attempt %d/10): %s", attempt + 1, exc)
            time.sleep(3)

    log.warning(
        "Could not connect to the MQTT broker after 10 attempts. Continuing "
        "without MQTT publishing — readings will still be stored via REST."
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
        known_sensor_ids = patients
        log.info("Known sensor IDs retrieved from catalog: %d found", len(known_sensor_ids))

    simulators = {sensor_id: SimulatedSensor(sensor_id) for sensor_id in known_sensor_ids}
    log.info("%d simulator(s) created", len(simulators))
    if not simulators:
        log.warning(
            "No known sensorIDs available — the simulation loop will be idle "
            "until the service is restarted with a reachable catalog."
        )

    threading.Thread(target=run_simulation_loop, args=(simulators,), daemon=True).start()

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
