"""
Sensor Connector Microservice
===============================
Responsibilities:
  - REST Provider: exposes the latest sensor reading per sensorID to other
    services (Data Processor, Clinician Portal, etc.)
  - MQTT Subscriber: consumes real-time sensor readings from the Message
    Broker and keeps the in-memory store up to date. [TODO - next iteration]
  - Health Catalog integration: fetches broker/topic config and the list of
    known patient sensorIDs at startup (REST consumer role).

This file implements the REST provider role plus the Health Catalog REST
consumer bootstrap (mqtt_config / known_sensor_ids). Neither value is
consumed yet — that's future work for the MQTT subscriber and the data
generator integration. The in-memory store and update_reading() are written
as stable extension points so the MQTT subscriber and the data generator can
be plugged in later without refactoring this module.
"""

from __future__ import annotations

import json
import logging
import threading
import time

import cherrypy
import requests

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SensorConnector] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────
REST_PORT = 5003

# ─── Health Catalog config (fetched once at startup) ─────────────────────────
# Populated by fetch_catalog_config() / fetch_known_patients() in main().
# Stay at these empty/default values if the catalog is unreachable at
# startup so the REST provider endpoints keep working regardless.
mqtt_config: dict = {}
known_sensor_ids: list[str] = []

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

    def POST(self, *path, **params):
        # TEMPORARY - for manual testing only, will be removed once the MQTT
        # ingestion path exists.
        if len(path) == 3 and path[0] == "sensors" and path[2] == "latest":
            sensor_id = path[1]
            try:
                payload = json.loads(cherrypy.request.body.read())
            except json.JSONDecodeError:
                raise cherrypy.HTTPError(400, "Invalid JSON in request body")

            update_reading(sensor_id, payload)
            return json.dumps(payload)

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
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global mqtt_config, known_sensor_ids

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
