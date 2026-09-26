# tenere aperto in contemporanea: patient_contoll + reminder + catalog_telegram
import time
import json
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from MyMQTT import MyMQTT
import telepot
from telepot.loop import MessageLoop
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton

class ReminderManager:
    def __init__(self, broker, port, catalog_url):
        self.broker = broker
        self.port = port
        self.catalog_url = catalog_url
        # Client MQTT per inviare gli alert
        self.client = MyMQTT("ReminderManager_Service", self.broker, self.port, None)
        
    def start(self):
        self.client.start()
        print("Reminder actived")
        
        while True:
            # ora italiana (i container Docker sono in UTC)
            current_time = datetime.now(ZoneInfo("Europe/Rome")).strftime("%H:%M")
            try:
                # reminder presi dal catalog: in Docker ogni container ha la sua copia di database.json
                patients = requests.get(f"{self.catalog_url}/get_all_patients").json()

                for patient in patients:
                    reminders = requests.get(f"{self.catalog_url}/get_reminders?chatID={patient['chatID']}").json()
                    for rem in reminders:
                        if rem["time"] == current_time:
                            topic = f"clinician/patient/{patient['chatID']}/alert"
                            payload = {
                                "chatID": patient["chatID"],
                                "msg": f"Reminder for your medicine! \nTake {rem['medicine_name']}!"
                            }
                            self.client.myPublish(topic, payload)
                            print(f"Alert inviato a MQTT per {patient['chatID']}")

            except Exception as e:
                print(f"Errore durante il controllo: {e}")

            time.sleep(60) # 60 secondi

if __name__ == "__main__":
    conf = json.load(open("conf.json"))
    # Docker: usa conf.json (http://catalog:8080). Fuori da Docker: CATALOG_URL=http://127.0.0.1:8080
    catalog_url = os.getenv("CATALOG_URL", conf["catalog_url"])
    manager = ReminderManager(conf["broker"], conf["port"], catalog_url)
    manager.start()