import time
import json
from MyMQTT import *


'''
more specifically i want that if the sensor id of a patient show values above or below the threshold it tells me that that specific patient has an alert
'''

# NOTE: In config.json put broker as "broker": "message_broker"
# "broker.hivemq.com" instead of "message_broker" --> broker.hivemq.com is a public, external internet broker. If you use it, your local containers will try to send messages out to the public internet instead of talking to your internal Docker Mosquitto container (message_broker). 
# in config.json now it is localhost cause i want to run it on local terminal otherwise, for docker use message_broker


class VitalSignAlertManager:
    def __init__(self, clientID, broker, port, database_file):
        self.database_file = database_file
        self.clientID = clientID
        self.broker = broker
        self.port = port
        
        # Initialize MQTT client as a subscriber (with a callback method)
        self.mqtt_client = MyMQTT(self.clientID, self.broker, self.port, self)
        self.topic = "iothealth/+/sensors"  # Wildcard topic to capture data from any sensor

    def startClient(self):
        self.mqtt_client.start()
        self.mqtt_client.mySubscribe(self.topic)
        print(f"Vital Sign Alert Manager started and subscribed to '{self.topic}'!")

    def stopClient(self):
        self.mqtt_client.stop()
        print("Vital Sign Alert Manager stopped!")

    def notify(self, topic, payload):
        """
        Callback function triggered automatically when a message arrives 
        on the subscribed MQTT topic.
        """
        try:
            # ADDED
            print(f"\n[DEBUG] Received message on topic: {topic}")
            #print(f"\n[DEBUG] Raw payload type: {type(payload)}")

            if isinstance(payload, bytes):
                payload_str = payload.decode('utf-8')
            else:
                payload_str = payload

            message = json.loads(payload_str)
            # If message is a string (double-encoded), parse again
            if isinstance(message, str):
                message = json.loads(message)

            print(f"[DEBUG] Message content: {message}")

            # Extract sensorID (the key field for matching patients)
            sensor_id = message.get("sensorID")
            if not sensor_id:
                print(f"[WARNING] No sensorID in message: {message}")
                return

            timestamp = message.get("timestamp", time.strftime("%H:%M:%S"))

            # extract vital sign from message
            temp = message.get("body_temperature")
            hr = message.get("heart_rate")
            sys = message.get("blood_pressure_systolic")
            dia = message.get("blood_pressure_diastolic")

            print(f"[DEBUG] Sensor {sensor_id}: HR={hr}, TEMP={temp}, SYS={sys}, DIA={dia}")

        
            # Load latest thresholds from database.json dynamically
            thresholds = self.get_thresholds_for_sensor(sensor_id)
            if not thresholds:
                print(f"[WARNING] No thresholds found for sensor {sensor_id}")
                return 



            #Evaluate each vital sign against its respective threshold boundaries
            self.check_threshold("heart_rate", hr, thresholds.get("heart_rate"), sensor_id, timestamp)
            self.check_threshold("body_temperature", temp, thresholds.get("body_temperature"), sensor_id, timestamp)
            self.check_threshold("blood_pressure_systolic", sys, thresholds.get("blood_pressure_systolic"), sensor_id, timestamp)
            self.check_threshold("blood_pressure_diastolic", dia, thresholds.get("blood_pressure_diastolic"), sensor_id, timestamp)

        # NEW
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse JSON: {e}")
            print(f"[ERROR] Payload was: {payload}")
        except Exception as e:
            print(f"Error processing incoming message: {e}")



    def get_thresholds_for_sensor(self, sensor_id):
        """Helper function to fetch thresholds matching the specific sensorID from database.json"""
        try:
            with open(self.database_file, "r") as f:
                data = json.load(f)
            
            for patient in data.get("patients", []):
                if patient.get("sensorID") == sensor_id:
                    print(f"[DEBUG] Found patient {patient['name']} with sensor {sensor_id}")
                    return patient.get("thresholds")
                
        except Exception as e:
            print(f"[ERROR] Error reading database file: {e}")
        return None

    def check_threshold(self, vital_name, value, limits, sensor_id, timestamp):
        """Compares value against min/max limits and publishes an alert if breached."""
        if value is None or limits is None:
            return

        min_val = limits.get("min")
        max_val = limits.get("max")

        alert_msg = None

        # DA QUI
        if max_val is not None and value > max_val:
            alert_msg = f"[{timestamp}] Alert! Sensor {sensor_id} {vital_name} too high ({value} > max {max_val})"
        elif min_val is not None and value < min_val:
            alert_msg = f"[{timestamp}] Alert! Sensor {sensor_id} {vital_name} too low ({value} < min {min_val})"
        else:
            # Show when value is normal
            print(f"{vital_name}: {value} is normal (range: {min_val}-{max_val})")



        if alert_msg:
            # Find the patient chatID associated with this sensor to target the alert topic correctly
            chat_id = self.get_chat_id_by_sensor(sensor_id)
            if chat_id:
                alert_topic = f"clinician/patient/{chat_id}/alert"
                payload = {
                    "chatID": chat_id,
                    "sensorID": sensor_id,
                    "msg": alert_msg
                }
                self.mqtt_client.myPublish(alert_topic, json.dumps(payload))
                print(f"[ALERT PUBLISHED] {alert_msg}")



    def get_chat_id_by_sensor(self, sensor_id):
        try:
            with open(self.database_file, "r") as f:
                data = json.load(f)
            for patient in data.get("patients", []):
                if patient.get("sensorID") == sensor_id:
                    return patient.get("chatID")
                
        except Exception as e:
            print(f"[ERROR] Error fetching chatID: {e}")
        return None

    

    def runAlert(self):
        self.startClient()
        try:
            print("[INFO] Vital Sign Alert Manager running... waiting for sensor data")

            while True:
                time.sleep(1)  # Keep the main thread alive while background MQTT thread handles callbacks

        except KeyboardInterrupt:
            print("\n[INFO] Shutting down...")
            self.stopClient()


if __name__ == "__main__":
    conf = json.load(open("conf.json"))
    clientID = "VitalSignAlert_Service"
    broker = conf["broker"]
    port = conf["port"]
    database = "database.json"

    print(f"[INFO] Starting VitalSignAlertManager with broker={broker}:{port}")
    manager = VitalSignAlertManager(clientID, broker, port, database)
    manager.runAlert()


'''
class VitalSignAlertService:
    def __init__(self, clientID, broker, port, database_file):
        self.clientID = clientID
        self.database_file = database_file
        
        # Initialize MQTT client
        self.mqttClient = MyMQTT(clientID, broker, port, self)
        
    def startClient(self):
        self.mqttClient.start()
        print("Vital Sign Alert Service started!")
        # Subscribe to the wildcard sensor topic matching your database.json schema
        self.mqttClient.mySubscribe("iothealth/+/sensors")

    def stopClient(self):
        self.mqttClient.stop()
        print("Vital Sign Alert Service stopped!")

    def notify(self, topic, payload):
        """
        This callback triggers automatically whenever the Data_generator publishes new sensor data.
        """
        try:
            # Parse incoming payload (assumes JSON from data generator)
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            data = json.loads(payload)
            
            # Extract sensor details (adjust keys based on your Data_generator.py output structure)
            sensor_id = data.get("sensorID")
            heart_rate = data.get("heart_rate")
            temperature = data.get("temperature")
            sys = data.get("sys")
            dia = data.get("dia")

            # Open database to find the matching patient and their thresholds
            with open(self.database_file, "r") as f:
                db = json.load(f)

            patient = None
            for p in db.get("patients", []):
                if p.get("sensorID") == sensor_id:
                    patient = p
                    break

            if not patient:
                return # No patient linked to this sensor ID

            chat_id = patient["chatID"]
            thresholds = patient.get("thresholds", {})

            # Evaluate each vital sign against thresholds
            alerts = []

            # 1. Heart Rate
            hr_th = thresholds.get("heart_rate", {})
            if heart_rate is not None:
                if hr_th and (heart_rate < hr_th.get("min", 0) or heart_rate > hr_th.get("max", 999)):
                    alerts.append(f"Heart Rate is abnormal: {heart_rate} bpm (allowed: {hr_th.get('min')}-{hr_th.get('max')})")

            # 2. Body Temperature
            temp_th = thresholds.get("body_temperature", {})
            if temperature is not None:
                if temp_th and (temperature < temp_th.get("min", 0) or temperature > temp_th.get("max", 999)):
                    alerts.append(f"Temperature is abnormal: {temperature}°C (allowed: {temp_th.get('min')}-{temp_th.get('max')})")

            # 3. Blood Pressure Systolic
            sys_th = thresholds.get("blood_pressure_systolic", {})
            if sys is not None:
                if sys_th and (sys < sys_th.get("min", 0) or sys > sys_th.get("max", 999)):
                    alerts.append(f"Systolic BP is abnormal: {sys} mmHg (allowed: {sys_th.get('min')}-{sys_th.get('max')})")

            # 4. Blood Pressure Diastolic
            dia_th = thresholds.get("blood_pressure_diastolic", {})
            if dia is not None:
                if dia_th and (dia < dia_th.get("min", 0) or dia > dia_th.get("max", 999)):
                    alerts.append(f"Diastolic BP is abnormal: {dia} mmHg (allowed: {dia_th.get('min')}-{dia_th.get('max')})")

            # If any threshold was violated, publish an alert message
            if alerts:
                alert_topic = f"iothealth/{sensor_id}/alerts"
                alert_payload = {
                    "chatID": chat_id,
                    "name": patient.get("name"),
                    "alerts": alerts
                }
                self.mqttClient.myPublish(alert_topic, json.dumps(alert_payload))
                print(f"[ALERT TRIGGERED] Sent to {alert_topic}: {alerts}")

        except Exception as e:
            print(f"Error processing incoming sensor reading: {e}")

if __name__ == "__main__":
    # Load configuration parameters (similar to your other microservices)
    try:
        with open("database.json", "r") as f:
            db_config = json.load(f)
            broker = db_config["config"]["mqtt"]["broker_host"]
            port = db_config["config"]["mqtt"]["broker_port"]
    except Exception:
        broker = "localhost"  # Fallback if running locally
        port = 1883

    clientID = "VitalSignAlert_Service"
    database = "database.json"

    alert_service = VitalSignAlertService(clientID, broker, port, database)
    alert_service.startClient()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        alert_service.stopClient()

'''






'''


class vitalSignManager:
    def __init__(self, clientID, broker, port, database_file):
        self.database_file = database_file
        self.clientID = clientID
        self.broker = broker
        self.port = port
        # Using MyMQTT as both a subscriber (to listen to sensors) and publisher (to send alerts)
        # Passing 'self' allows MyMQTT to automatically route incoming messages to the notify() method
        self.mqtt_client_publisher = MyMQTT(self.clientID, self.broker, self.port, self)

    def startClient(self):
        self.mqtt_client_publisher.start()
        # Subscribe to the sensor topics published by your data-generator
        self.mqtt_client_publisher.mySubscribe("healthcare/sensors/#")
        print("Vital Sign Alert Manager started and subscribed!")

    def stopClient(self):
        self.mqtt_client_publisher.stop()
        print("Vital Sign Alert Manager stopped!")

    def publishAlert(self, topic, message):
        self.mqtt_client_publisher.myPublish(topic, message)
        current_time = time.strftime("%H:%M")
        print(f"[{current_time}] Alert published to topic '{topic}'. {message['msg']}")

    def notify(self, topic, msg):
        """This function is automatically called by MyMQTT whenever a new sensor reading is published."""
        try:
            # Handle payload format conversion safely
            if isinstance(msg, bytes):
                payload = json.loads(msg.decode("utf-8"))
            elif isinstance(msg, str):
                payload = json.loads(msg)
            else:
                payload = msg

            sensor_type = payload.get("sensor")
            value = payload.get("value")
            patient_id = payload.get("patient_id")

            # Load thresholds live from database.json
            with open(self.database_file, "r") as f:
                data = json.load(f)

            for patient in data.get("patients", []):
                # Match patient by id or chatID depending on what the data generator sends
                if str(patient.get("id")) == str(patient_id) or str(patient.get("chatID")) == str(patient_id):
                    topic = f"clinician/patient/{patient['chatID']}/alert"
                    
                    thresholds = patient.get("thresholds", {})
                    if sensor_type in thresholds:
                        min_limit = thresholds[sensor_type]["min"]
                        max_limit = thresholds[sensor_type]["max"]

                        # Check if value breaks the minimum or maximum boundary
                        if value < min_limit:
                            payload_msg = {
                                "chatID": patient["chatID"],
                                "msg": f"ALERT! Your {sensor_type.replace('_', ' ')} is {value} (TOO LOW). Minimum limit is {min_limit}."
                            }
                            self.publishAlert(topic, payload_msg)

                        elif value > max_limit:
                            payload_msg = {
                                "chatID": patient["chatID"],
                                "msg": f"ALERT! Your {sensor_type.replace('_', ' ')} is {value} (TOO HIGH). Maximum limit is {max_limit}."
                            }
                            self.publishAlert(topic, payload_msg)

        except Exception as e:
            print(f"Error during sensor evaluation: {e}")


    def runAlert(self):
        self.startClient()
        try:
            # Keeps the service running in the background listening for incoming MQTT messages
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.stopClient()

if __name__ == "__main__":
    conf = json.load(open("conf.json"))
    clientID = "VitalSignAlert_Service"
    broker = conf["broker"]
    port = conf["port"]
    database = "database.json"
    manager = vitalSignManager(clientID, broker, port, database)
    manager.runAlert()
'''