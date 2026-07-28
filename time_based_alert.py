import time
import json
from MyMQTT import *
from datetime import datetime, timedelta # to have appointment reminder a week before

class reminderManager:
    def __init__(self, clientID, broker, port, database_file):
        self.database_file = database_file
        self.clientID = clientID
        self.broker = broker
        self.port = port
        self.mqtt_client_publisher = MyMQTT(self.clientID, self.broker, self.port, None)

    def startClient(self):
        self.mqtt_client_publisher.start()
        print("Reminder Manager Publisher stated!")

    def stopClient(self):
        self.mqtt_client_publisher.stop()
        print("Reminder Manager Publisher stopped!")

    def publishAlert(self, topic, message):
        self.mqtt_client_publisher.myPublish(topic, message)
        #medicine_name = message["msg"].split("Take ")[-1].replace("!", "")
        current_time = time.strftime("%H:%M")
        print(f"[{current_time}] Alert published to topic '{topic}'. {message['msg']}")
        #print(f"[{current_time}] Alert published to topic '{topic}' - Medicine: {medicine_name}")

    def runAlert(self):
        self.startClient()

        try:
            while True:
                current_time = time.strftime("%H:%M")
                current_date = time.strftime("%d-%m-%Y") #day-month-year
                

                try:
                    with open(self.database_file, "r") as f:
                        data = json.load(f)
                    
                    for patient in data.get("patients", []):
                        topic = f"clinician/patient/{patient['chatID']}/alert"

                        # medicien reminders
                        reminders = patient.get("reminders", [])
                        for rem in reminders:
                            if rem["time"] == current_time:
                                #topic = f"clinician/patient/{patient['chatID']}/alert"
                                payload = {
                                    "chatID": patient["chatID"],
                                    "msg": f"Reminder for your medicine! Take {rem['medicine_name']}!"}
                                # Use the class method to publish
                                self.publishAlert(topic, payload)

                        # appointments reminder
                        appointments = patient.get("appointments",[])
                        for appointment in appointments:
                            appointment_date = appointment.get("date")
                            appointment_time = appointment.get("time")

                            # REMINDER A WEEK BEFORE THE REAL APPOINTMENT DATE
                            # remaind an appointment a week before the real appointment date
                            if appointment_date and appointment_time:

                                # convert text string ("02-08-2026") into python datetime object --> needed to subtract days from the date
                                appt_datetime = datetime.strptime(appointment_date, "%d-%m-%Y")

                                # Subtract 7 days to trigger the reminder a week before --> it tells python to look back at 7 days in calendar
                                reminder_date_obj = appt_datetime - timedelta(days=7)

                                # takes the obtained day after the - 7 days and turn it back into string with format day-month-year
                                reminder_date_str = reminder_date_obj.strftime("%d-%m-%Y")

                                # Check if resulting date after - 7 days coincide with actual date and chek that appointment time in database coincide with actual time
                                if reminder_date_str == current_date and appointment_time == current_time:
                                    payload = {
                                        "chatID": patient["chatID"],
                                        "msg": f"You have an upcoming appointment in 1 week (on {appointment_date} at {appointment_time}) with doctor {patient.get('doctor')}. Reason: {appointment.get('reason')}"
                                    }
                                    self.publishAlert(topic, payload)



                            # APPOINTMENT REMINDER WHEN I HAVE IT
                            # Verify if both the date and time match the current system clock
                            if appointment_date == current_date and appointment_time == current_time:
                                payload = {
                                    "chatID": patient["chatID"],
                                    "msg": f"You have an appointment NOW with doctor {patient.get('doctor')}. Reason: {appointment.get('reason')}"
                                }
                                self.publishAlert(topic, payload)



                except Exception as e:
                    print(f"Error during check: {e}")

                time.sleep(60) # Check every 60 seconds
                
        except KeyboardInterrupt:
            self.stopClient()

if __name__ == "__main__":
    conf = json.load(open("conf.json"))
    clientID = "ReminderManager_Service"
    broker = conf["broker"]
    port = conf["port"]
    database = "database.json"
    manager = reminderManager(clientID, broker, port, database)
    manager.runAlert()
        
