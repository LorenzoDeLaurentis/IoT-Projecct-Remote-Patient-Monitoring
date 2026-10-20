# tenere aperto in contemporanea: patient_contoll + reminder + catalog_telegram
import time
import json
import os
import requests

from zoneinfo import ZoneInfo
from MyMQTT import MyMQTT
from datetime import datetime, timedelta
#import telepot
#from telepot.loop import MessageLoop
#from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton

class ReminderManager:
    def __init__(self, broker, port, catalog_url):
        self.broker = broker
        self.port = port
        self.catalog_url = catalog_url
        self.alerted_appointments = set() # stores (chatID, date, reason)
        # Client MQTT per inviare gli alert
        self.client = MyMQTT("ReminderManager_Service", self.broker, self.port, None)
        
    def start(self):
        self.client.start()
        print("Reminder actived")
        
        while True:
            # italian hour (Dockr containers are all in UTC)
            current_time = datetime.now(ZoneInfo("Europe/Rome")).strftime("%H:%M")
            current_date = datetime.now(ZoneInfo("Europe/Rome")).strftime("%d-%m-%Y") # maybe change with %Y-%m-%d
            
            try:
                # reminder presi dal catalog: in Docker ogni container ha la sua copia di database.json
                patients = requests.get(f"{self.catalog_url}/get_all_patients").json()
                
                
                # see the activity every 60s
                print(f"[{current_time}] Checked catalog successfully. Found {len(patients)} patients.", flush=True)
                # flush=True nel print forza l'invio immediato al terminale anche senza configurazioni aggiuntive
                

                for patient in patients:
                    reminders = requests.get(f"{self.catalog_url}/get_reminders?chatID={patient['chatID']}").json()
                    for rem in reminders:
                        # confronto gli orari normalizzati: "8:00" vale come "08:00"
                        try:
                            rem_time = datetime.strptime(str(rem["time"]).strip(), "%H:%M").strftime("%H:%M")
                        except ValueError:
                            continue

                        if rem_time == current_time:
                            topic = f"clinician/patient/{patient['chatID']}/alert"
                            payload = {
                                "chatID": patient["chatID"],
                                "msg": f"Reminder for your medicine! \nTake {rem['medicine_name']}!"
                            }
                            self.client.myPublish(topic, payload)
                            print(f"Alert send for {patient['chatID']}")





                    # APPOINTMENTS REMINDER
                    appt_response = requests.get(f"{self.catalog_url}/get_appointments?chatID={patient['chatID']}").json()
                    appointments = appt_response.get("appointments", [])
                    for appointment in appointments:
                        appointment_date = appointment.get("date")
                        appointment_time = appointment.get("time")

                        if not appointment_date:
                            continue

                        if str(appointment_date).strip() == "TBD":
                            continue

                        

                        #if not appointment_date or not appointment_time:
                        #    continue
                        #if str(appointment_date).strip() == "TBD": #or str(appointment_time).strip() == "TBD":
                        #    continue

                        # Reminder two days before the appointment date
                        try:
                            # convert text string ("02-08-2026") into python datetime object --> needed to subtract days from the date
                            appt_dt = datetime.strptime(str(appointment_date).strip(), "%d-%m-%Y")
                            
                        except ValueError:
                            print(f"Invalid appointment date for {patient['chatID']}: {appointment_date}")
                            continue

                        date_key = appt_dt.strftime("%d-%m-%Y")
                        appointment_key = (patient['chatID'], date_key, appointment.get("reason"))

                        # APPOINTMENT TODAY
                        if date_key == current_date:
                            if appointment_key not in self.alerted_appointments:
                                topic = f"clinician/patient/{patient['chatID']}/alert"
                                payload = {
                                    "chatID": patient["chatID"],
                                    "msg": f"You have an upcoming appointment TODAY with doctor {appt_response.get('doctor')}. Reason: {appointment.get('reason')}"
                                }
                                self.client.myPublish(topic, payload)
                                print(f"2 days appointment alert sent to MQTT for patient {patient['chatID']}")
                                self.alerted_appointments.add(appointment_key)


                        # APPOINTMENT IN 2 DAYS
                        reminder_date = appt_dt - timedelta(days=2)
                        if reminder_date.strftime("%d-%m-%Y") == current_date:
                            if appointment_key not in self.alerted_appointments:
                                topic = f"clinician/patient/{patient['chatID']}/alert"
                                payload = {
                                    "chatID": patient["chatID"],
                                    "msg": f"You have an upcoming appointment in 2 days (on {appointment_date} at {appointment_time}) with doctor {appt_response.get('doctor')}. Reason: {appointment.get('reason')}"
                                }

                                self.client.myPublish(topic,payload)
                                print(f"You have an upcoming appointment in 2 days (on {appointment_date} at {appointment_time}) with doctor {appt_response.get('doctor')}. Reason: {appointment.get('reason')}")
                                self.alerted_appointments.add(appointment_key)
                           
            except Exception as e:
                print(f"Error during check: {e}")

            time.sleep(60) # 60 secondi

if __name__ == "__main__":
    conf = json.load(open("conf.json"))
    # Docker: usa conf.json (http://catalog:8080). Fuori da Docker: CATALOG_URL=http://127.0.0.1:8080
    catalog_url = os.getenv("CATALOG_URL", conf["catalog_url"])
    manager = ReminderManager(conf["broker"], conf["port"], catalog_url)
    manager.start()