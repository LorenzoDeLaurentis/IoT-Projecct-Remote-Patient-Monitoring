import numpy as np
import time
from datetime import datetime
import json
from MyMQTT import *


def simulate_temp(is_fever=False):
    """
    Simulation of body temperature based on the current time.
    """
    # Retrieving the current time
    now = datetime.now()
    current_hour = now.hour + now.minute / 60.0 #convert time to decimal, like 14.5 for 14:30
    
    # Circadian model: Average 36.6°C, oscillations of 0.5°C
    # The temperature peaks around 18:00 and is lowest around 6:00 
    #sin has a period of 2pi, diving by 12 gives us a 24-hour cycle, -12 used to shift the peak to 18:00 instead of 12:00
    base_temp = 36.6 + 0.5 * np.sin((current_hour - 12) * np.pi / 12)

    # Adding some Gaussian Noise, due to the sensor - mean 0, standard deviation 0.05)
    noise = np.random.normal(0, 0.05) 
    
    # Fever simulation: If fever is active, add a fixed offset - e.g., +2 degrees
    fever_offset = np.random.normal(2, 0.3)  if is_fever else 0.0

    #Rounding to 2 decimal places to simulate typical sensor output
    return round(base_temp + noise + fever_offset, 2)


def simulate_heart_rate(temp, high_hr_signal=False, fever_mode = False):
    #MAYBE A VARIABLE WITHE THE GENERAL STATUS OF THE PATIENT, LIKE STRESS LEVEL, ACTIVITY LEVEL, OR FEVER STATUS, TO INFLUENCE THE HEART RATE SIMULATION

    # Simulate heart rate with a normal resting range of 60-100 bpm, with some random fluctuations
    now = datetime.now()
    current_hour = now.hour + now.minute / 60.0 

    # Mathematical formula for the HR HR = HRbaseline + HRcircadian + HRfewerimpact + HRnoise
    # HRbaseline: Average heart rate, between 60 and 100 bpm
    # HRcircadian: A circadian rhythm component, with a peak in the afternoon and a trough in the early morning
    # HRfewerimpact: An increase in heart rate due to fever, if active; for every degree of fever, we can add a certain number of bpm 
    # e.g., +10 bpm per degree above 37°C
    # HRnoise: Random fluctuations to simulate natural variability and sensor noise

    hr_circadian = 5 * np.sin((current_hour - 12) * np.pi / 12)  # Peak around 18:00
    temp_diff = max(0, temp - 37.5)  # Only consider the temperature difference if it's above 37.5°C

    hr_fever_impact = temp_diff * 12 if fever_mode else 0  # Adding 10 bpm per degree of fever, only if fever mode is active

    hr_noise = np.random.normal(0, 3) # Random noise with a standard deviation of 3 bpm

    heart_rate = patient_temp_baseline + hr_circadian + hr_fever_impact + hr_noise

    if high_hr_signal:
        heart_rate += 20 # Adding a significant increase to test the alert system

    return round(heart_rate)


def simulate_blood_pressure(heart_rate, high_hr_signal=False):
    # We have to use the HR as an input
    HR_baseline = 72
    sys_noise = np.random.normal(0, 3)
    dia_noise = np.random.normal(0, 2)
    base_sys = 120
    
    if high_hr_signal:
        base_sys += 15  # Pressure raises if HR is high
        
    SYS = base_sys + (K*(heart_rate - HR_baseline)) + sys_noise 
    DIA = (SYS*0.67) + dia_noise

    return round(SYS), round(DIA)








# CLASS TO PUBLISH TO MQTT (vital_sign_alert)
class DataGeneratorPublisher:
    def __init__(self, client_id, broker, port, sensor_id):
        self.client_id = client_id
        self.broker = broker
        self.port = port
        self.sensor_id = sensor_id
        self.mqtt_client = MyMQTT(client_id, broker, port, None)
        self.topic = f"iothealth/{self.sensor_id}/sensors"

    def start(self):
        self.mqtt_client.start()
        #print(f"Data Generator connecte to MQTT broker")

    def stop(self):
        self.mqtt_client.stop()

    def publish_reading(self, temeperature, hearth_rate, sys, dia):
        """Publish sensor reading to MQTT topic"""
        payload = {
            "sensorID": self.sensor_id,
            "timestamp": datetime.now().isoformat(),
            "body_temperature": temperature,
            "heart_rate": heart_rate,
            "blood_pressure_systolic": sys,
            "blood_pressure_diastolic": dia
        }
        
        # Publish to the topic that vital_sign_alert subscribes to
        #topic = f"iothealth/{self.sensor_id}/sensors"
        self.mqtt_client.myPublish(self.topic, json.dumps(payload))


# helper function to check all sensors id
def get_all_sensorID(database_file="database.json"):
    """Reads database.json and extracts all active sensorID"""
    try:
        with open(database_file, "r") as f:
            data = json.load(f)
        
        patients = data.get("patients", data) if isinstance(data, dict) else data
        sensor_ids = [p.get("sensorID") for p in patients if p.get("sensorID")]
        return sensor_ids if sensor_ids else None

    except Exception as e:
        print(f"[ERROR] Could not read sensor IDs from database: {e}")
        return

        






if __name__ == "__main__":
    # TO CONNECT TO MQTT
    conf = json.load(open("conf.json"))
    database_file = "database.json"
    
    sensor_ids = get_all_sensorID(database_file)
    print(f"Found sensor IDs to simulate: {sensor_ids}")

    # initialize and start publisher
    publishers = {}
    for sensor_id in sensor_ids:
        publisher = DataGeneratorPublisher(
            client_id=f"DataGenerator_{sensor_id}",
            broker=conf["broker"],
            port=conf["port"],
            sensor_id=sensor_id 
        )
        publisher.start()
        publishers[sensor_id] = publisher






    #global patient_stress_level

    patient_temp_baseline = 36.6
    patient_temp_baseline = 72 + np.random.normal(0, 3) # Baseline heart rate with some variability, around 73 bpm

    try:
        fever_mode = False
        counter = 0
        fever_cnt= 0
        K = 0.5
    

        print("-- Starting simulated sensor readings --")
        print("Ctrl + C to stop.\n")

        while True:
            # Every 10 readings, we activate the fever mode to simulate a febrile state
            if counter < 15:
                counter += 1
            elif fever_cnt < 5 and counter == 15:
                print("\n[ALERT] Fever mode active!\n")
                fever_mode = True
                fever_cnt += 1
            else: 
                print("\n[ALERT] Fever mode deactivated! Returning to normal state for the next 10 readings.\n")
                fever_mode = False
                counter = 0
                fever_cnt = 0

            high_hr_signal = True if counter % 10 == 0 else False
            if high_hr_signal: print("[TEST] Injecting High HR Signal...")

            timestamp = datetime.now().strftime("%H:%M:%S") #strftime to format the timestamp as HH:MM:SS

            # PUBLISH TO MQTT
            for sensor_id, publisher in publishers.items():


                temperature = simulate_temp(is_fever=fever_mode)
                heart_rate = simulate_heart_rate(temperature, high_hr_signal)
                sys, dia = simulate_blood_pressure(heart_rate, high_hr_signal)

                print(f"[{timestamp}] Sensor lecture: {temperature} °C, Heart Rate: {heart_rate} bpm, Blood Pressure: {sys} / {dia} mmHg")

                publisher.publish_reading(temperature, heart_rate, sys, dia)

            
            # Waitinf 1 sec before the next reading
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nSimulation stopped")
        for publisher in publishers.values():
            publisher.stop()
    