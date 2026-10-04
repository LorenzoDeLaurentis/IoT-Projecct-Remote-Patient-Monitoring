import time
import json
import requests
from MyMQTT import *


'''
more specifically i want that if the sensor id of a patient show values above or below the threshold it tells me that that specific patient has an alert
'''
'''
message sensor1 --> notify() --> check sensor1 --> return
'''

# NOTE: In config.json put broker as "broker": "message_broker"
# "broker.hivemq.com" instead of "message_broker" --> broker.hivemq.com is a public, external internet broker. If you use it, your local containers will try to send messages out to the public internet instead of talking to your internal Docker Mosquitto container (message_broker). 
# in config.json now it is localhost cause i want to run it on local terminal otherwise, for docker use message_broker


class VitalSignAlertManager:
    def __init__(self, clientID, broker, port, catalog_url): # prende dati dal catlaog via REST (versione prima prendeva dati direttamente dal database e quindi creavo database_file al posto di catalog_url)
        #self.database_file = database_file --> versione prima
        self.catalog_url = catalog_url
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




    def get_patient_by_sensor(self, sensor_id):
        '''
        open database.json --> versione vecchia
        take data from database from a REST request from catalog
        check all patients
        compare patient["sensorID"] with ID received
        gives back corresponding patient
        return none if no correspondence is found

        '''
        try:
            # Chiamata REST al Catalog (usa l'endpoint che restituisce il paziente in base al sensore)
            url = f"{self.catalog_url}/patient_by_sensor"
            params = {"sensorID":sensor_id}
            response = requests.get(url, params=params)

            if response.status_code == 200:
                patient_data = response.json()
                return patient_data
            elif response.status_code == 404:
                print(f"[ERROR] Unrecognized sensorID '{sensor_id}'. This sensor is not present.")
                return None
            else:
                print(f"[ERROR] Catalog returned status code {response.status_code}")
                return None

        except Exception as e:
            print(f"[ERROR] Failed to connect to Catalog via REST: {e}")
            return None

    '''
    # PRENDE DATI DA DATABASE
            with open(self.database_file, "r") as f:
                data = json.load(f)

            for patient in data.get("patients", []):
                if patient.get("sensorID") == sensor_id:
                    return patient

        except Exception as e:
            print(f"Error in reading database file: {e}")

        return None
    '''
    

    def notify(self, topic, payload):
        """
        every time it is called, it represents a new sensor read coming from MQTT message of sensor connector
        
        if message 
        {
            "sensorID": "sensor99",
            "heart_rate": 150,
            "body_temperature": 39.2,
            "blood_pressure_systolic": 160,
            "blood_pressure_diastolic": 100
        }
        arrives and if the sensor id exists in database, it does the following things:
        find sensroID in database
        find patient name in database
        take the thresholds
        check the values comparing with the thresholds
        generate alerts if values are not inthresholds boundaries
        """

        print(f"\n[DEBUG] Received message on topic: {topic}")

        if payload is None:
            print("[WARNING Received empty or None payload form MQTT]")
            return
       
        message = None
        try:
            
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')

            if isinstance(payload, str):
                message = payload

            else:
                raise TypeError(f"Unsupported payload type: {type(payload).__name__}")


            # if payload is already in a dictionary
            if isinstance(payload, str):
                message = json.loads(message)


        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse JSON: {e}")
            print(f"[ERROR] Payload was raw: {payload}")
            return
            


        if not isinstance(message, dict):
            raise ValueError(
                f"MQTT message must be a JSON object, got "
                f"{type(message).__name__}"
            )

                

                


        print(f"[DEBUG] Message content: {message}")

        # Extract sensorID (the key field for matching patients)
        sensor_id = message.get("sensorID")
        if not sensor_id:
            missing_message = "missing sensorID"
            print(f"[WARNING] {missing_message}: {message}")

            # MESSAGGIO DIAGNOSTICO SUL TOPIC DEGLI ALERT ??? CIOE???
            self.mqtt_client.myPublish(
                "iothealth/missing/alerts",
                {
                    "status": "error",
                    "message": missing_message,
                    "received_topic": topic,
                    "payload": message
                }
            )
            return

        # looking for patient cossesponding to that sensorID in databse
        patient = self.get_patient_by_sensor(sensor_id)

        if patient is None:
            missing_message = f"missing sensorID: {sensor_id}"
            print(f"[WARNING] {missing_message}")

            self.mqtt_client.myPublish(
                "iothealth/missing/alerts",
                {
                    "status": "error",
                    "message": missing_message,
                    "sensorID": sensor_id,
                    "received_topic": topic
                }
            )
            return

        print(
            f"[DEBUG] Sensor {sensor_id} belongs to patient "
            f"{patient.get('name', 'unknown')}"
        )


        # get patient thresholds form databse
        thresholds = patient.get("thresholds")

            
        if not thresholds:
            print(
                f"[WARNING] Sensor {sensor_id} found but no thresholds are present"
            )
            return



        # extract vital sing 
        timestamp = message.get("timestamp", time.strftime("%H:%M:%S"))

        # extract vital sign from message
        temperature = message.get("body_temperature")
        heart_rate = message.get("heart_rate")
        systolic = message.get("blood_pressure_systolic")
        diastolic = message.get("blood_pressure_diastolic")

        print(f"[DEBUG] Sensor {sensor_id}: "
              f"HR = {heart_rate}, TEMP = {temperature}, SYS = {systolic}, DIA = {diastolic}")


        chat_id = patient.get("chatID")

        #Evaluate each vital sign against its respective threshold boundaries
        self.check_threshold("heart_rate", heart_rate, thresholds.get("heart_rate"), sensor_id, timestamp, chat_id)
        self.check_threshold("body_temperature", temperature, thresholds.get("body_temperature"), sensor_id, timestamp, chat_id)
        self.check_threshold("blood_pressure_systolic", systolic, thresholds.get("blood_pressure_systolic"), sensor_id, timestamp, chat_id)
        self.check_threshold("blood_pressure_diastolic", diastolic, thresholds.get("blood_pressure_diastolic"), sensor_id, timestamp, chat_id)

       





    def check_threshold(self, vital_name, value, limits, sensor_id, timestamp, chat_id):
        """Compares value against min/max limits and publishes an alert if breached."""

        """
        if message contains a sensroID that does not exist in database, the output will be [WARNING] missing sensorID: ID not matching
        and is going to be published to topic iothealth/missing/alerts with payload
        {
            "status": "error",
            "message": "missing sensorID: ID not matching",
            "sensorID": "ID not matching",
            "received_topic": "iothealth/ID not matching/sensors"
        }
        otherwise if sensorID is not in message it will be published
        {
            "status": "error",
            "message": "missing sensorID"
        }
        """

        if value is None:
            print(f"[WARNING] Missing value for {vital_name} from sensor {sensor_id}")
            return

        try:
            value = float(value)
        except(TypeError, ValueError):
            print(f"[WARNING] Invalid value for {vital_name} from sensor {sensor_id}: {value}")
            return

        if limits is None:
            print(f"[WARNING] No limits configured for {vital_name} and sensor {sensor_id}")
            return
        

        min_val = limits.get("min")
        max_val = limits.get("max")
        alert_msg = None


  
        if max_val is not None and value > max_val:
            alert_msg = f"[{timestamp}] Alert! Sensor {sensor_id} {vital_name} too high ({value} > max {max_val})"

        elif min_val is not None and value < min_val:
            alert_msg = f"[{timestamp}] Alert! Sensor {sensor_id} {vital_name} too low ({value} < min {min_val})"

        else:
            # Show when value is normal
            print(f"{vital_name}: {value} is normal (range: {min_val}-{max_val})")
            return
        

        if chat_id is None:
            print(f"[WARNING] No chatID found for sensor {sensor_id}. Alert cannot be sent to the patient")
            return

 
        alert_topic = f"clinician/patient/{chat_id}/alert"
        alert_payload = {
            "chatID": chat_id,
            "sensorID": sensor_id,
            "vital_sign": vital_name,
            "value": value,
            "timestamp": timestamp,
            "msg": alert_msg
        }
        self.mqtt_client.myPublish(alert_topic, alert_payload)
        print(f"[ALERT PUBLISHED] {alert_msg}")




    

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
    catalog_url = conf["catalog_url"]

    print(f"[INFO] Starting VitalSignAlertManager with broker={broker}:{port}")
    manager = VitalSignAlertManager(clientID, broker, port, catalog_url)
    manager.runAlert()
