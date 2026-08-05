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
