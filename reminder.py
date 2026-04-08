# tenere aperto in contemporanea: patient_contoll + reminder + catalog_telegram
import time
import json
from MyMQTT import MyMQTT
import telepot
from telepot.loop import MessageLoop
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton

class ReminderManager:
    def __init__(self, broker, port, database_file):
        self.broker = broker
        self.port = port
        self.database_file = database_file
        # Client MQTT per inviare gli alert
        self.client = MyMQTT("ReminderManager_Service", self.broker, self.port, None)
        
    def start(self):
        self.client.start()
        print("Reminder actived")
        
        while True:
            current_time = time.strftime("%H:%M")
            try:
                with open(self.database_file, "r") as f:
                    data = json.load(f)
                
                for patient in data.get("patients", []):
                    reminders = patient.get("reminders", [])
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
    manager = ReminderManager(conf["broker"], conf["port"], "database.json")
    manager.start()