"""
Sensor Connector Microservice
===============================
Responsibilities:
  - REST Provider: exposes the latest sensor reading per sensorID to other
    services (Data Processor, Clinician Portal, etc.)
  - MQTT Subscriber: consumes real-time sensor readings from the Message
    Broker and keeps the in-memory store up to date. [TODO - next iteration]
  - Health Catalog integration: fetches broker/topic config at startup.
    [TODO - next iteration]

This file implements ONLY the REST provider role for now. The in-memory
store and update_reading() are written as stable extension points so the
MQTT subscriber and the data generator can be plugged in later without
refactoring this module.
"""

from __future__ import annotations

import json
import logging
import threading

import cherrypy

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SensorConnector] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────
REST_PORT = 5003

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
# Entry Point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
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
