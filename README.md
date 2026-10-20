# IoT Platform for Remote Patient Monitoring

A microservice platform that simulates wearable biometric sensors for a group of
patients, publishes their readings over MQTT, stores and analyses them, raises
alerts when a vital sign leaves the patient's thresholds, and reaches patients
through a Telegram bot and clinicians through a Node-RED portal.

All services are Python 3.9 containers (see `Dockerfile`) except the MQTT broker
(Eclipse Mosquitto) and the clinician portal (Node-RED).

| Service (`docker-compose.yml`) | Container | Source | Role | Host port | Technology |
|---|---|---|---|---|---|
| `mqtt-broker` | `message_broker` | `mosquitto.conf` | Message Broker | 1883 | Eclipse Mosquitto |
| `catalog` | `catalog` | `Catalog_Telegram(new version).py` | Health Catalog: patients, doctors, thresholds, reminders, appointments, MQTT config | 8080 | CherryPy, `database.json` |
| `sensor-connector` | `sensor-connector` | `sensor_connector.py`, `Data_generator.py` | Simulated sensors, preprocessing, REST provider of the latest reading, MQTT publisher | 5003 | CherryPy, MyMQTT, NumPy |
| `dataprocessor` | `dataprocessor` | `dataprocessor.py` | Rolling-window statistics, z-score anomalies, trends | none (Flask app on 5002 inside the network) | Flask, paho-mqtt |
| `timeseries-adaptor` | `timeseries-adaptor` | `timeseries_adaptor.py` | Stores every reading in SQLite (`/data/timeseries.db`), history REST API | none (Flask app on 5001 inside the network) | Flask, paho-mqtt, SQLite |
| `thingspeak_forwarder` | `thingspeak_forwarder` | `thingspeak_forwarder.py` | Rate-limited forwarding of readings/stats to ThingSpeak | 5004 | Flask, paho-mqtt |
| `vital-sign-alert` | `vital-sign-alert` | `vital_sign_alert.py` | Compares each reading with the patient's thresholds and publishes alerts | none | MyMQTT |
| `reminder` | `reminder` | `time_based_alert.py` | Medicine and appointment reminders | none | MyMQTT |
| `telegram-bot` | `telegram-bot` | `patient_controll_telegram.py` | Patient interface (alerts, reminders, appointments, latest reading) | none | telepot, MyMQTT |
| `node-red` | `clinician-portal` | `nodred_doctor_portal.json` | Clinician portal | 1880 | Node-RED |

`reminder.py` and `calendarApp.py` are in the repository but are not started by
`docker-compose.yml`. `flows.json` is a separate Node-RED flow that reads
`thingspeak_forwarder:5004/status`.

## Quick start (Docker)

```bash
# Once per machine: the broker volume is declared "external", so compose does not create it
docker volume create mosquitto_data

# Build and start the whole stack
docker compose up --build -d

# Same, but without the Telegram bot (no Telegram notifications).
# Useful when several people run the stack with the same bot token.
docker compose up --build -d --scale telegram-bot=0

# Follow the Sensor Connector (readings, faults, discarded readings)
docker compose logs -f sensor-connector

# Stop and remove the containers
docker compose down
```

The images copy the source code at build time (`COPY . .`), so any code change
needs `--build` to reach the containers.

### Clinician portal setup (Node-RED, once per machine)

A fresh `node_red_data` volume is empty: the portal flow and the dashboard
package are **not** installed automatically. Do these steps once, after the
first `docker compose up`, in this order:

**1. Install Node-RED Dashboard 2.0.** The portal uses the Dashboard 2.0 nodes
(`ui-base`, `ui-page`, `ui-form`, `ui-table`, `ui-chart`, ...), which are not part
of the Node-RED image. Without them the editor shows unknown nodes and the log
says `Waiting for missing types to be registered: ui-base, ...`.

```bash
docker exec clinician-portal npm install --prefix /data @flowfuse/node-red-dashboard
docker restart clinician-portal
```

Or from the editor at http://localhost:1880: menu ☰ → *Manage palette* →
*Install* → search `@flowfuse/node-red-dashboard` → *Install*.

**2. Import the portal flow.** Open http://localhost:1880, menu ☰ → *Import* →
*select a file to import* → choose `nodred_doctor_portal.json` → *Import*, then
click **Deploy** (top right). The flow reaches the catalog and the broker
through `host.docker.internal` (see the file).

**3. Open the portal** at http://localhost:1880/dashboard and log in with a
doctor account from `database.json` (`doctors` → `username` / `password`).

Both the package and the imported flow are stored in the `node_red_data`
volume, so they survive `docker compose down` / `up` (not `down -v`). The
Node-RED image is pinned to `nodered/node-red:5.0.8` in `docker-compose.yml`
(tested with Dashboard 2.0 v1.33.0), so everyone runs the same version.

**Credentials.** `conf.json` holds the Telegram bot token, and `database.json`
holds ThingSpeak write API keys and doctor logins. Never commit real
credentials. Use placeholders in any copy you share.

---

## Sensor side

### Data Generator (`Data_generator.py`)

Every patient wears one simulated sensor, identified by its `sensorID`, which
produces the four vital signs together in one reading.

**Physiological model.** All times are Italian time (`Europe/Rome`), regardless
of the machine or container timezone.

- **Body temperature:** `36.6 + 0.3·sin((h − 12)·π/12)` °C. That is a
  circadian rhythm with a peak around 18:00 and a minimum around 06:00 (`h` is
  the decimal hour). Gaussian sensor noise has σ = 0.05 °C. The result is
  rounded to 2 decimals.
- **Heart rate:** `baseline + 5·sin((h − 12)·π/12) + fever impact + noise`
  (σ = 3 bpm).
  - During a fever episode, the heart rate rises by 12 bpm per °C above 37.5 °C.
  - The result is rounded to an integer.
- **Blood pressure:**
  - Systolic = `sys_baseline + K·(HR − 72) + noise`, with K = 0.5 and σ = 3.
  - Diastolic = `0.67·systolic + noise`, with σ = 2.
  - Both are rounded to integers.

**Patient conditions.** The condition comes from the `condition` field of each
patient in the catalog. Unknown or missing conditions fall back to `healthy`.
`CONDITION_PROFILES` defines 11 profiles:

| Condition | HR mean (bpm) | HR personal spread (bpm) | Systolic baseline (mmHg) | Temperature offset (°C) |
|---|---|---|---|---|
| healthy | 72 | 2.5 | 120 | 0.0 |
| tachycardia | 95 | 5 | 120 | 0.0 |
| bradycardia | 50 | 3 | 120 | 0.0 |
| hypertension | 72 | 3 | 150 | 0.0 |
| hypotension | 72 | 3 | 80 | 0.0 |
| hyperthyroidism | 102 | 5 | 135 | +0.3 |
| hypothyroidism | 52 | 3 | 120 | −0.5 |
| diabetes | 85 | 4 | 145 | 0.0 |
| renal_failure | 90 | 4 | 155 | 0.0 |
| stress_palpitations | 72 | 2.5 | 120 | 0.0 |
| anxiety_palpitations | 72 | 2.5 | 120 | 0.0 |

`stress_palpitations` and `anxiety_palpitations` have a normal baseline. They
differ from `healthy` only in the episodes they can have.

**Personal offsets.** Each patient has a stable "signature" on top of the
condition profile. The offset of each trait is a standard-normal value derived
from SHA-256 of `"<sensorID>:<trait>"`, clamped to ±1.5 σ. The same patient
therefore always gets the same baselines, across restarts, machines and Python
versions. The offsets are scaled as follows:

| Trait | Scale | Max offset |
|---|---|---|
| heart rate | × the condition's `hr_std` | ±1.5 · `hr_std` |
| systolic pressure | × 3.0 mmHg | ±4.5 mmHg |
| temperature | × 0.10 °C | ±0.15 °C |

Noise and episodes stay random.

### Episode engine

On top of the baseline, each sensor can independently start a clinical episode.

- **Shape:** an episode has a random duration and a random intensity (50–120 %
  of the peak). It follows a sine shape: it rises gradually, peaks mid-episode
  and returns to the baseline.
- **Starting:** at each reading, every episode allowed for the condition has a
  given probability of starting.
- **Durations:** these are counted in readings, and one reading is taken every 5 s.

| Episode | Peak effect | Duration demo / realistic (readings) | Start probability per reading, demo / realistic | Allowed hours (realistic mode only) |
|---|---|---|---|---|
| `fever` | +2.0 °C (heart rate follows) | 24–48 / 720–2880 | 1/400 / 1/500000 | any |
| `palpitations_daytime` | +40 bpm | 6–18 / 12–60 | 1/100 / 1/5000 | 08:00–22:00 |
| `palpitations_nocturnal` | +40 bpm | 6–18 / 12–60 | 1/100 / 1/6500 | 22:00–07:00 |
| `hypertensive_crisis` | +45 mmHg systolic | 12–30 / 120–720 | 1/100 / 1/120000 | any |

| Condition | Possible episodes |
|---|---|
| stress_palpitations | fever, palpitations_daytime |
| anxiety_palpitations | fever, palpitations_nocturnal |
| renal_failure | fever, hypertensive_crisis |
| all other conditions | fever |

`SIMULATION_DEMO_MODE` (default `true`) selects the timing:

- **Demo mode** makes episodes frequent and short, and ignores the allowed hours,
  so anomalies show up during a presentation.
- **`false`** uses the realistic frequencies and durations.

To watch the model in a console without Docker, the catalog or MQTT, run
`python Data_generator.py [condition] [interval_seconds]`.

### Sensor Connector (`sensor_connector.py`)

**Startup.**

1. It reads `catalog_url` from `conf.json` (`http://catalog:8080` in Docker).
2. It calls `GET <catalog>/config?service=sensor_connector` to get the MQTT
   section: `broker_host`, `broker_port` and `topics.sensors`.
3. It connects the MQTT publisher.
4. It calls `GET <catalog>/get_all_patients` to get each patient's `sensorID`
   and `condition`. Patients without a `sensorID`, or with `"N/A"`, are skipped.

Each call is retried up to 10 times, 3 s apart. If the catalog or the broker is
still unreachable, the service starts anyway: the REST API works, and the
missing parts are retried later.

**Threads.**

- **Simulation loop:** every 5 s it reads all simulators, applies the simulated
  faults and preprocessing, then stores and publishes the accepted readings.
- **Patient refresh:** every 60 s it retries the catalog config and the MQTT
  connection if they are still missing, then fetches the patient list once.
  - New sensors are added and removed ones are deleted.
  - A sensor whose condition changed gets a new simulator.
  - Unchanged sensors keep their simulator, so an ongoing episode continues.
  - If the catalog is unreachable, the current list is kept.
- **CherryPy REST server:** runs on port 5003.

**REST provider.** All endpoints are read-only. Anything else returns 400.

| Method and path | Response |
|---|---|
| `GET /health` | `{"status": "ok", "service": "sensor_connector"}` |
| `GET /sensors` | `{"sensors": ["sensor01", ...]}`: sensors that have a stored reading |
| `GET /sensors/<sensorID>/latest` | The latest accepted reading, or 404 if there is none |

Example:

```bash
curl http://localhost:5003/sensors/sensor03/latest
```
```json
{"sensorID": "sensor03", "timestamp": "2026-10-20T11:25:37.638916+02:00", "heart_rate": 77, "body_temperature": 36.65, "blood_pressure_systolic": 120, "blood_pressure_diastolic": 82}
```

The Telegram bot reads this endpoint. Its base URL is set by `SENSOR_URL`,
default `http://sensor-connector:5003`.

**MQTT publisher.** Each accepted reading is published as JSON with QoS 2.

- **Topic:** `topics.sensors` from the catalog config, with `+` replaced by the
  sensorID. That gives `iothealth/<sensorID>/sensors`.
- **Client ID:** `SensorConnector_Publisher`.
- **If the broker is down:** the reading is still stored for the REST API.

### Preprocessing

The four biometric channels of a sensor (heart rate, temperature, systolic and
diastolic pressure) are aggregated into one reading with one timestamp. Before a
reading is stored or published, `accept_reading()` checks it in this order. The
first failure discards the reading.

1. **Completeness:** all four parameters are present and numeric, which means
   not `None`, not a boolean and not NaN.
2. **Plausibility:** each value lies within wide physical limits, and the
   diastolic pressure is below the systolic. These limits are not clinical
   thresholds, so abnormal but real values always pass.

   | Parameter | Plausible range |
   |---|---|
   | heart_rate | 20–250 bpm |
   | body_temperature | 30.0–45.0 °C |
   | blood_pressure_systolic | 50–260 mmHg |
   | blood_pressure_diastolic | 30–180 mmHg |

3. **Jump check:** compares the reading with the last accepted reading of the
   same sensor. The maximum change is 60 bpm for heart rate, 2.0 °C for
   temperature, 60 mmHg for systolic and 40 mmHg for diastolic pressure.

   **Re-anchor:** after `MAX_CONSECUTIVE_DISCARDS` = 3 consecutive discards, the
   jump check is skipped, so the next plausible reading becomes the new anchor.
   A real step change, or a wrong anchor, can therefore never lock a sensor out.
   Checks 1 and 2 are never skipped.

A discarded reading is **neither stored nor published**. `/sensors/<id>/latest`
keeps showing the last valid reading. Each discard is logged as one warning,
for example:

```
Reading discarded for sensor03: heart_rate=300 outside plausible range (20-250)
Reading discarded for sensor01: heart_rate=None missing or not numeric
Reading discarded for sensor05: heart_rate=140 jumped by 70 from last valid 70 (max 60)
```

Clinical episodes rise gradually, so they never trigger the jump check, and they
stay inside the physical limits. Statistics over time windows, such as rolling
means, z-scores and trends, are done in the Data Processor (`dataprocessor.py`),
not in the connector. The connector only checks single readings.

### Simulated sensor faults

`FaultScheduler` (in `Data_generator.py`) injects technical faults so that the
preprocessing can be demonstrated. These are sensor errors, not clinical
events, and they are always discarded.

| Fault | Effect on the reading |
|---|---|
| `missing` | One random parameter becomes `null` |
| `spike` | One random parameter gets an impossible value: heart_rate 300, body_temperature 50.0, systolic 320 or diastolic 200 |
| `disconnection` | All four parameters become 0 |

- **One timer for the whole system:**
  - Each fault hits one random sensor, never the same sensor twice in a row
    when more than one exists.
  - A fault lasts 1–3 readings of that sensor and counts as one event.
- **Timing:**

  | | First fault after the Sensor Connector starts | Then one fault every |
  |---|---|---|
  | Demo mode | 1–3 min | 3–4.5 min |
  | Realistic mode | 2–6 h | 6–12 h |

  In demo mode, a 10-minute run gives 2 or 3 faults.

Each fault produces one log line when it starts, followed by one discard line
per corrupted reading:

```
[SENSOR FAULT] sensor03: spike heart_rate=300 (simulated, 2 reading(s))
Reading discarded for sensor03: heart_rate=300 outside plausible range (20-250)
```

To watch them:

```bash
docker compose logs -f sensor-connector | grep -E "SENSOR FAULT|discarded"
```

To disable faults, set `SIMULATION_FAULTS: "false"` in the `sensor-connector`
environment. The commented line is already in `docker-compose.yml`.

### Message Broker and MyMQTT

`mosquitto.conf` sets:

- `listener 1883`
- `allow_anonymous true` (no authentication)
- `persistence true`, with data in `/mosquitto/data/` (the external volume
  `mosquitto_data`)

`MyMQTT.py` is the shared paho-mqtt wrapper. The Sensor Connector,
vital-sign-alert, reminder (`time_based_alert.py`) and telegram-bot use it. The
Data Processor, TimeSeries Adaptor and ThingSpeak forwarder use paho-mqtt
directly, with QoS 1.

| MyMQTT method | Behaviour |
|---|---|
| `MyMQTT(clientID, broker, port, notifier)` | Creates the client with `clean_session=True` and supports paho-mqtt 1.x and 2.x. `notifier.notify(topic, payload)` is called for each received message; pass `None` for a publisher-only client |
| `start()` | Connects and starts the network loop |
| `myPublish(topic, msg)` | Publishes `json.dumps(msg)` with QoS 2 |
| `mySubscribe(topic)` | Subscribes with QoS 2 and remembers the topic (no duplicates) |
| `unsubscribe()` | Unsubscribes from all remembered topics |
| `stop()` | `unsubscribe()`, then stops the loop and disconnects |

Because of `clean_session=True`, the broker forgets the subscriptions when the
connection drops. paho reconnects automatically, and MyMQTT re-subscribes every
remembered topic on each successful (re)connection. Subscribers therefore keep
receiving messages after a broker restart.

---

## MQTT topic map

Built by searching the code for `subscribe`/`publish` calls and the Node-RED
MQTT nodes. ⚠ marks entries that could not be fully verified.

| Topic | Publisher | Subscribers | Payload |
|---|---|---|---|
| `iothealth/<sensorID>/sensors` | sensor-connector (QoS 2) | vital-sign-alert, dataprocessor, timeseries-adaptor, thingspeak_forwarder | One reading (see [Data contract](#data-contract)) |
| `iothealth/<sensorID>/stats` | dataprocessor (QoS 1) | thingspeak_forwarder | `{patient_id, window_seconds, computed_at, stats, anomalies}` (see `dataprocessor.py`) |
| `clinician/patient/<chatID>/alert` | vital-sign-alert, reminder | telegram-bot | Vital-sign alert: `{chatID, sensorID, vital_sign, value, timestamp, msg}`. Reminder: `{chatID, msg}` |
| `iothealth/missing/alerts` | vital-sign-alert | ⚠ no subscriber found | Diagnostic message for an unknown or missing sensorID |
| `clinician/patient/<chatID>/appointments/confirmation` | node-red (topic built in a function node) | telegram-bot | ⚠ see `nodred_doctor_portal.json` |
| `clinician/patient/<chatID>/appointments/rejection` | node-red | telegram-bot | ⚠ see `nodred_doctor_portal.json` |
| `clinician/patient/<chatID>/appointments/cancellation` | ⚠ no publisher found | telegram-bot | n/a |
| `clinician/patient/appointments/request` | telegram-bot | ⚠ no MQTT subscriber found (the portal reads pending requests through the catalog REST API) | see `patient_controll_telegram.py` |
| `clinician/patient/appointments/cancelled` | telegram-bot | ⚠ no MQTT subscriber found | see `patient_controll_telegram.py` |

The catalog config (`database.json` → `config.mqtt.topics`) also declares
`iothealth/+/alerts` and `iothealth/+/reminders`. No service publishes on these
patterns, except `iothealth/missing/alerts`.

Example message captured on `iothealth/sensor06/sensors`:

```json
{"sensorID": "sensor06", "timestamp": "2026-10-20T11:28:17.878488+02:00", "heart_rate": 99, "body_temperature": 36.67, "blood_pressure_systolic": 167, "blood_pressure_diastolic": 110}
```

To watch the live stream from the broker container:

```bash
docker exec message_broker mosquitto_sub -t 'iothealth/+/sensors' -v
```

## Data contract

A reading, as stored by the Sensor Connector, returned by
`/sensors/<id>/latest` and published on `iothealth/<sensorID>/sensors`:

| Field | JSON type | Unit | Notes |
|---|---|---|---|
| `sensorID` | string | n/a | Same as the `<sensorID>` in the topic |
| `timestamp` | **string** | n/a | ISO-8601 with timezone offset, Italian time, e.g. `2026-10-20T11:25:37.638916+02:00`. It is not a number or an epoch |
| `heart_rate` | number (integer) | bpm | Always present and within 20–250 after preprocessing |
| `body_temperature` | number (2 decimals) | °C | Always present and within 30.0–45.0 after preprocessing |
| `blood_pressure_systolic` | number (integer) | mmHg | Always present and within 50–260 after preprocessing |
| `blood_pressure_diastolic` | number (integer) | mmHg | Always present, within 30–180 and below the systolic after preprocessing |

No other fields are present.

## Environment variables

| Variable | Used by | Default | Effect |
|---|---|---|---|
| `SIMULATION_DEMO_MODE` | sensor-connector (`Data_generator.py`) | `true` | `true`: frequent, short episodes and faults, and no hour restrictions. `false`: realistic frequencies |
| `SIMULATION_FAULTS` | sensor-connector (`Data_generator.py`) | `true` | `false` disables the simulated sensor faults |
| `SENSOR_URL` | telegram-bot | `http://sensor-connector:5003` | Base URL of the Sensor Connector REST API |
| `CATALOG_URL` | telegram-bot, `reminder.py` | `catalog_url` in `conf.json` | Base URL of the catalog |
| `HEALTH_CATALOG_URL` | thingspeak_forwarder | `http://catalog:8080` | Base URL of the catalog |

`SIMULATION_DEMO_MODE` and `SIMULATION_FAULTS` accept `1`/`true`/`yes`, case
insensitive. Any other value means false.

`docker-compose.yml` also sets `MQTT_BROKER_HOST`, `MQTT_BROKER_PORT` and
`THINGSPEAK_WRITE_INTERVAL` for thingspeak_forwarder, but the code does not read
them (see `thingspeak_forwarder.py`).

## Known limitations

- **No authentication:**
  - The broker allows anonymous clients.
  - The REST APIs have no authentication or TLS.
  - Doctor passwords are stored in clear text in `database.json`.
- **Simulated data only:** there is no code for a real sensor or a Raspberry Pi
  bridge.
- **Discarded readings leave gaps** in the series: 5 s per reading, up to 15 s
  for a 3-reading fault or a re-anchor. The gaps are not filled or interpolated.
- **Condition changes:** when a patient's condition changes, the jump-check
  anchor is kept. A large baseline change, e.g. hypotension → renal_failure, is
  discarded for 3 readings before the re-anchor.
- **In-memory state:** the latest readings and the preprocessing anchors are
  lost when the Sensor Connector restarts.
- **Catalog data is not persistent:** the catalog writes `database.json` inside
  its container, with no volume. Changes are lost when the container is
  recreated, e.g. by `docker compose up --build`. `time_based_alert.py` reads its
  own image copy of `database.json`, not the catalog API.
- **Log timezone:** container log timestamps are in UTC, while reading
  timestamps are in Italian time.
