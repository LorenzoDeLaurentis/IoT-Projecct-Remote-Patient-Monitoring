# tenere aperto in contemporanea: patient_contoll + reminder + catalog_telegram
import telepot
from telepot.loop import MessageLoop
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
import json
import requests
import time
from datetime import datetime
import random
from MyMQTT import MyMQTT 

class PatientMonitoringBot:
    def __init__(self, token, catalog_url, broker, port):
        self.tokenBot = token
        self.catalog_url = catalog_url
        self.bot = telepot.Bot(self.tokenBot)
        
        self.pending_registrations = {}
        
        self.client = MyMQTT("TelegramBot_03", broker, port, self)
        self.client.start()

        # ISCRIZIONE AI TOPIC:
        self.client.mySubscribe("clinician/patient/+/alert")
        self.client.mySubscribe("clinician/patient/+/appointments/confirmation")

        MessageLoop(self.bot, {
            'chat': self.on_chat_message,
            'callback_query': self.on_callback_query
        }).run_as_thread()

    # for alerts:
    def notify(self, topic, payload):
        try:
            # Trasforma il payload (stringa) in un dizionario Python
            msg_data = json.loads(payload)
            print(f"MQTT ricevuto su {topic}: {msg_data}")

            # 1. Caso: Conferma Appuntamento da Node-RED
            if "appointments/confirmation" in topic:
                chatID = msg_data.get("chatID")
                date = msg_data.get("date")
                time = msg_data.get("time")
                update = requests.put(f"{self.catalog_url}/update_appointment", json=msg_data)

                self.bot.sendMessage(chatID, f"Your doctor confirmed your appointment on {date} at {time}.")
                print(f"Message sent to {chatID}")

            # 2. Caso: Alert dai sensori (se previsto nel tuo sistema)
            elif "alert" in topic:
                chatID = msg_data.get("chatID")
                testo = msg_data.get("msg", "Attenzione: Alert rilevato!")
                self.bot.sendMessage(chatID, f"ALERT: {testo}")

        except Exception as e:
            print(f"Errore nel processare il messaggio MQTT: {e}")

    def on_chat_message(self, msg):
        content_type, chat_type, chatID = telepot.glance(msg)
        message_text = msg.get('text', '')

        if chatID in self.pending_registrations:
            self.manage_registration(chatID, message_text)
            return

        if message_text == '/start':
            # Verifica chatID 
            try:
                response = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}")
                if response.status_code == 200:
                    user_data = response.json()
                    self.bot.sendMessage(chatID, f"Welcome {user_data['name']}")
                    self.send_main_menu(chatID)
                else:
                    self.bot.sendMessage(chatID, "Welcome! You are not registered. Let's get started.\nWhat's your name? (Name and Surname)")
                    self.pending_registrations[chatID] = {"step": 1}
            except:
                self.bot.sendMessage(chatID, "Error connecting Catalog.")

    # Registration or request
    def manage_registration(self, chatID, text):
        state = self.pending_registrations[chatID]
        # PROFILE INFORMATION:
        if state["step"] == 1:
            state["fullname"] = text
            self.bot.sendMessage(chatID, "What is you date of birthday? (DD/MM/AAAA)")
            state["step"] = 2
        elif state["step"] == 2:
            state["birthdate"] = text
            self.bot.sendMessage(chatID, "What is your doctor name? (Name and Surname)")
            state["step"] = 3
        elif state["step"] == 3:
            state["doctor"] = text
            self.bot.sendMessage(chatID, "Name of your biomedical sensor (es. sensor01):")
            state["step"] = 4
        elif state["step"] == 4:
            state["sensor_id"] = text
            # Registrazione completata:
            new_user = {
                "chatID": chatID,
                "name": state["fullname"],
                "birthdate": state["birthdate"],
                "doctor": state["doctor"],
                "sensorID": state["sensor_id"]
            }
            check_res = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}")
            
            if check_res.status_code == 200:
                requests.put(f"{self.catalog_url}/update_patient", json=new_user)
                self.bot.sendMessage(chatID, "Profile updated successfully!")
            else:
                requests.post(f"{self.catalog_url}/add_patient", json=new_user)
                self.bot.sendMessage(chatID, f"Welcome, {state['fullname']}! Registration complete.")
            
            del self.pending_registrations[chatID]
            self.send_main_menu(chatID)

        # APPOINTMENTS:     c'è il collegamento al dottore, che deve confermare l'appuntamento, per ora è solo registrato
        elif state.get("step") == "waiting_app_reason":
            reason = text
            name = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}").json().get("name", "Unknown")
            mqtt_payload = {
                "patient_name": name,
                "chatID": chatID,
                "reason": reason,
                "status": "pending"
            }
            
            try:
                self.client.myPublish("clinician/patient/appointments/request", mqtt_payload)
                requests.post(f"{self.catalog_url}/add_appointment", json=mqtt_payload)
                print(mqtt_payload)               
                self.bot.sendMessage(chatID, f"Appointmente saved. We requested to your doctor an appointment.")              
                del self.pending_registrations[chatID] 
            except Exception as e:
                self.bot.sendMessage(chatID, f"Errore: {e}")
           
            self.send_main_menu(chatID)

        # REMINDERS:
        elif state.get("step") == "waiting_rem_data":
            try:
                med, t = text.split(",")
                payload = {"chatID": chatID, "medicine_name": med.strip(), "time": t.strip()}
                requests.post(f"{self.catalog_url}/add_reminder", json=payload)
                self.bot.sendMessage(chatID, "Reminder saved!")
                del self.pending_registrations[chatID]
            except:
                self.bot.sendMessage(chatID, "Format error. Try again (medicine, HH:MM):")
            self.send_main_menu(chatID)

    def send_main_menu(self, chatID):
        buttons = [
            [InlineKeyboardButton(text="Vitals", callback_data='vitals'),
             InlineKeyboardButton(text="Reminders", callback_data='reminders')],
            [InlineKeyboardButton(text="Appointments", callback_data='appointments'),
             InlineKeyboardButton(text="Stats", callback_data='stats')],
            [InlineKeyboardButton(text="Alerts", callback_data='alerts'),
             InlineKeyboardButton(text="Profile", callback_data='profile')]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        self.bot.sendMessage(chatID, text=f"What do you need to do?\n(In the menu you can see what each button do) ", reply_markup=keyboard)

    def on_callback_query(self, msg):
        query_id, chatID, query_data = telepot.glance(msg, flavor='callback_query')
        
        # VITALS:      (da collegare al sensore, per ora dati random)
        if query_data == 'vitals':
            data = {
                "heartrate" : random.randint(60, 100),
                "temperature": round(random.uniform(35.5, 37)),
                "blood_pressure": f"{random.randint(115, 130)}/{random.randint(80, 85)}"
            }
            msg = (f"Latest Vital Signs:\n\n"
                    f"Heart rate: {data['heartrate']} bpm\n"
                    f"Temperature: {data['temperature']} °C\n"
                    f"Blood Pressure: {data['blood_pressure']} mmHg")
            self.bot.sendMessage(chatID, msg, parse_mode='Markdown')
            self.send_main_menu(chatID)
            '''
            # collegamento al sensore:
            res = requests.get(f"{self.catalog_url}/get_latest_vitals?sensorID={sensorID}")
            
            if res.status_code == 200:
                data = res.json()
                msg = (f"Latest Vital Signs:\n\n"
                    f"Heart rate: {data['heartrate']} bpm\n"
                    f"Temperature: {data['temperature']} °C\n"
                    f"Blood Pressure: {data['blood_pressure']} mmHg")
                self.bot.sendMessage(chatID, msg, parse_mode='Markdown')
            else:
                self.bot.sendMessage(chatID, "Dati non disponibili al momento.")
            '''
        
        # APPOINTMENTS:
        elif query_data == 'appointments':
            res = requests.get(f"{self.catalog_url}/get_appointments?chatID={chatID}")
            data = res.json()
            doctor = data.get("doctor")
            appointments = data.get("appointments", [])
            
            if not appointments:
                text = f"No appointments found with doctor {doctor}."
            else:
                text = f"Your appointments with doctor {doctor}:\n" + "\n".join([f"Appointment on {a}" for a in appointments])
            
            buttons = [
                    [InlineKeyboardButton(text="Book New", callback_data='app_create')],
                    [InlineKeyboardButton(text="Delete All", callback_data='app_delete')],
                    [InlineKeyboardButton(text="Main Menu", callback_data='main_menu')]]
            self.bot.sendMessage(chatID, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        elif query_data == 'app_create':
            self.bot.sendMessage(chatID, "What is the reason for the appointment?")
            self.pending_registrations[chatID] = {"step": "waiting_app_reason"}
        elif query_data == 'app_delete':  # avvisare anche il dottore? per ora solo cancellazione lato bot
            url = f"{self.catalog_url}/delete_all_appointments?chatID={chatID}"
            try:
                res = requests.delete(url)
                if res.status_code == 200:
                    self.bot.sendMessage(chatID, "All appointments have been deleted!")
                else:
                    self.bot.sendMessage(chatID, "Try again.")
            except Exception as e:
                self.bot.sendMessage(chatID, f"Connection error: {e}")
            self.send_main_menu(chatID)  
            
        # REMINDERS:   (finito)
        elif query_data == "reminders":
            res = requests.get(f"{self.catalog_url}/get_reminders?chatID={chatID}")
            reminders = res.json()
            
            if not reminders:
                text = "No reminders found."
            else:
                text = "Your reminders:\n" + "\n".join([f"{r['medicine_name']} at {r['time']}" for r in reminders])
            
            buttons = [[InlineKeyboardButton(text="Create", callback_data='rem_create'),
                        InlineKeyboardButton(text="Delete All", callback_data='rem_delate')],
                         [InlineKeyboardButton(text="Main Menu", callback_data='main_menu')]]
            self.bot.sendMessage(chatID, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        elif query_data == "rem_create":
            self.bot.sendMessage(chatID, "Enter: medicine_name, HH:MM (example: Aspirin, 08:30)")
            self.pending_registrations[chatID] = {"step": "waiting_rem_data"}            
        elif query_data == "rem_delate":
            url = f"{self.catalog_url}/delete_all_reminders?chatID={chatID}"
            try:
                res = requests.delete(url)
                if res.status_code == 200:
                    self.bot.sendMessage(chatID, "All reminders have been deleted!")
                else:
                    self.bot.sendMessage(chatID, "Try again.")
            except Exception as e:
                self.bot.sendMessage(chatID, f"Connection error: {e}")
            self.send_main_menu(chatID)
        
        # TRENDS SETTIMANALI: (da fare)
        elif query_data == 'stats':
            # da fare
            self.bot.sendMessage(chatID, "Ecco il tuo trend settimanale: [Link ThingSpeak]")
        
        # ALERTS: (creare gli alert del sensore o gli alert inviati direttamente dal dottore)
        elif query_data == 'alerts':
            res = requests.get(f"{self.catalog_url}/get_alerts?chatID={chatID}")
            alerts = res.json()
            
            if not alerts:
                text = "No alerts found."
            else:
                text = "Your alerts:\n" + "\n".join([f"{a['message']} at {a['timestamp']}" for a in alerts])
            
            buttons = [[InlineKeyboardButton(text="Main Menu", callback_data='main_menu')]]
            self.bot.sendMessage(chatID, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


        # PROFILE:     (finito)  
        elif query_data == 'profile':
            self.bot.sendMessage(chatID, "Ecco i tuoi dati registrati nel sistema:")
            try:
                res = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}")
                if res.status_code == 200:
                    user = res.json()
                    msg = (f"Name: {user['name']}\n"
                           f"Birthdate: {user['birthdate']}\n"
                           f"Doctor: {user['doctor']}\n"
                           f"Sensor ID: {user['sensorID']}")
                    
                    buttons = [
                        [InlineKeyboardButton(text="Edit Profile", callback_data='edit_profile')],
                        [InlineKeyboardButton(text="Main Menu", callback_data='main_menu')]]
                    
                    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
                    self.bot.sendMessage(chatID, msg, parse_mode='Markdown', reply_markup=keyboard)
                else:
                    self.bot.sendMessage(chatID, "Profile not found. Please register again.")
            except Exception as e:
                self.bot.sendMessage(chatID, f"Connection error: {e}")
        elif query_data == 'edit_profile':
            self.bot.sendMessage(chatID, "Let's update your profile. What's your name? (Name and Surname)")
            self.pending_registrations[chatID] = {"step": 1}
        elif query_data == 'main_menu':
            self.send_main_menu(chatID)


if __name__ == "__main__":
    config = json.load(open("conf.json"))
    
    bot = PatientMonitoringBot(
        token=config["token"],
        catalog_url=config["catalog_url"],
        broker=config["broker"],
        port=config["port"]
    )
    
    
    while True:
        time.sleep(10)