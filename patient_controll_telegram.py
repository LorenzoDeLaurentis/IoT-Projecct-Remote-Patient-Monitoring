# tenere aperto in contemporanea: patient_contoll + reminder + catalog_telegram
import telepot
from telepot.loop import MessageLoop
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
import json
import os
import requests
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from MyMQTT import MyMQTT

class PatientMonitoringBot:
    def __init__(self, token, catalog_url, broker, port):
        self.tokenBot = token
        self.catalog_url = catalog_url
        # Docker: http://sensor-connector:5003. Fuori da Docker: SENSOR_URL=http://127.0.0.1:5003
        self.sensor_url = os.getenv("SENSOR_URL", "http://sensor-connector:5003")
        self.bot = telepot.Bot(self.tokenBot)
        
        self.pending_registrations = {}

        # Comandi di testo -> callback_data dei pulsanti
        self.commands = {
            "/vitals": "vitals",
            "/reminders": "reminders",
            "/appointments": "appointments",
            "/stats": "stats",
            "/alerts": "alerts",
            "/profile": "profile"
        }

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
                app_time = msg_data.get("time")

                # Il catalog _02 salva data/ora solo nel ramo "target_reason":
                # recupero la reason del primo appuntamento pending
                res = requests.get(f"{self.catalog_url}/get_appointments?chatID={chatID}")
                apps = res.json().get("appointments", []) if res.status_code == 200 else []
                pending = next((a for a in apps if isinstance(a, dict) and a.get("status") == "pending"), None)
                if pending is None:
                    print(f"Nessun appuntamento pending per {chatID}")
                    return

                body = {
                    "target_reason": pending.get("reason"),
                    "new_date": date,
                    "new_time": app_time,
                    "new_status": "confirmed"
                }
                update = requests.put(f"{self.catalog_url}/update_appointment/{chatID}", json=body)
                if update.status_code != 200:
                    print(f"update_appointment fallito: {update.status_code} {update.text}")
                    return

                conferma = f"Your doctor confirmed your appointment on {date} at {app_time}."
                self.bot.sendMessage(chatID, conferma)
                # la conferma resta visibile anche nella sezione Alerts
                self.save_alert(chatID, conferma)
                print(f"Message sent to {chatID}")

            # 2. Caso: Alert dai sensori (se previsto nel tuo sistema)
            elif "alert" in topic:
                chatID = msg_data.get("chatID")
                testo = msg_data.get("msg", "Attenzione: Alert rilevato!")
                # Reminder della pastiglia: messaggio con i pulsanti di risposta
                if "Reminder for your medicine" in testo:
                    self.send_medicine_reminder(chatID, testo)
                else:
                    self.bot.sendMessage(chatID, f"ALERT: {testo}")

        except Exception as e:
            print(f"Errore nel processare il messaggio MQTT: {e}")

    # Salva un alert nel profilo del paziente sul catalog (campo "alerts")
    def save_alert(self, chatID, message):
        user = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}").json()
        alerts = user.get("alerts", [])
        alerts.append({
            "message": message,
            "timestamp": datetime.now(ZoneInfo("Europe/Rome")).strftime("%d-%m-%Y %H:%M")
        })
        res = requests.put(f"{self.catalog_url}/update_general_info", json={"chatID": chatID, "alerts": alerts})
        if res.status_code != 200:
            print(f"Salvataggio alert fallito: {res.status_code} {res.text}")

    # Reminder della pastiglia con i pulsanti Taken / Not taken / Passed
    def send_medicine_reminder(self, chatID, testo):
        buttons = [[InlineKeyboardButton(text="Taken", callback_data='med_taken'),
                    InlineKeyboardButton(text="Not taken", callback_data='med_not_taken'),
                    InlineKeyboardButton(text="Passed", callback_data='med_passed')]]
        self.bot.sendMessage(chatID, f"ALERT: {testo}", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

    # Risposta al reminder: Taken/Passed chiudono, Not taken lo rimanda dopo 5 minuti
    def manage_medicine_answer(self, chatID, message, answer):
        # tolgo i pulsanti dal messaggio a cui ha risposto
        try:
            self.bot.editMessageReplyMarkup((chatID, message['message_id']), reply_markup=None)
        except Exception as e:
            print(f"[reminder] {type(e).__name__}: {e}")

        if answer == 'med_not_taken':
            testo = message.get('text', '').replace("ALERT: ", "", 1)
            threading.Timer(300, self.send_medicine_reminder, args=(chatID, testo)).start()
            self.bot.sendMessage(chatID, "The reminder will be resent in 5 minutes")

        self.send_main_menu(chatID)

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
                    self.bot.sendMessage(chatID, f"Welcome {user_data.get('name', '')}")
                    self.send_main_menu(chatID)
                else:
                    self.bot.sendMessage(chatID, "Welcome! You are not registered. Let's get started.\nWhat's your name? (Name and Surname)")
                    self.pending_registrations[chatID] = {"step": 1}
            except Exception as e:
                print(f"[/start] {type(e).__name__}: {e}")   # causa reale
                self.bot.sendMessage(chatID, f"Error connecting Catalog ({type(e).__name__}).")

        # Comandi di testo: stessa azione del pulsante corrispondente
        elif message_text in self.commands:
            self.manage_action(chatID, self.commands[message_text])

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
            # Check: il medico deve esistere nel catalog
            try:
                doctors = requests.get(f"{self.catalog_url}/get_doctors").json()
            except Exception as e:
                print(f"[get_doctors] {type(e).__name__}: {e}")
                self.bot.sendMessage(chatID, f"Error connecting Catalog ({type(e).__name__}). Write your doctor name again:")
                return
            doctor = next((d for d in doctors if d["name"].strip().lower() == text.strip().lower()), None)
            if doctor is None:
                names = ", ".join(d["name"] for d in doctors)
                self.bot.sendMessage(chatID, f"Doctor not found. Available doctors: {names}\nWhat is your doctor name? (Name and Surname)")
                return
            state["doctor"] = doctor["name"]   # salvato come scritto nel catalog
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
            try:
                check_res = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}")

                if check_res.status_code == 200:
                    res = requests.put(f"{self.catalog_url}/update_general_info", json=new_user)
                    ok_msg = "Profile updated successfully!"
                else:
                    res = requests.post(f"{self.catalog_url}/add_patient", json=new_user)
                    ok_msg = f"Welcome, {state['fullname']}! Registration complete."

                if res.status_code == 200:
                    self.bot.sendMessage(chatID, ok_msg)
                else:
                    print(f"Registrazione fallita: {res.status_code} {res.text}")
                    self.bot.sendMessage(chatID, "Error saving profile. Try again.")
            except Exception as e:
                print(f"[registration] {type(e).__name__}: {e}")
                self.bot.sendMessage(chatID, f"Error connecting Catalog ({type(e).__name__}).")
            
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
                catalog_body = {**mqtt_payload, "date": "TBD", "time": "TBD"}
                res = requests.post(f"{self.catalog_url}/add_appointment", json=catalog_body)
                print(mqtt_payload)
                if res.status_code == 200:
                    self.bot.sendMessage(chatID, "Appointment saved. We requested to your doctor an appointment.")
                else:
                    print(f"add_appointment fallito: {res.status_code} {res.text}")
                    self.bot.sendMessage(chatID, "Error saving appointment. Try again.")
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

    # True se la data è precedente a oggi (TBD o formati sconosciuti restano visibili)
    def is_past_appointment(self, a):
        date = a.get("date") if isinstance(a, dict) else a
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(str(date), fmt).date() < datetime.now().date()
            except ValueError:
                pass
        return False

    def on_callback_query(self, msg):
        query_id, chatID, query_data = telepot.glance(msg, flavor='callback_query')
        # conferma il click a Telegram, cosi il pulsante non resta evidenziato
        self.bot.answerCallbackQuery(query_id)

        # Pulsanti del reminder della pastiglia
        if query_data in ('med_taken', 'med_not_taken', 'med_passed'):
            self.manage_medicine_answer(chatID, msg['message'], query_data)
            return

        self.manage_action(chatID, query_data)

    # Azioni comuni a pulsanti e comandi di testo
    def manage_action(self, chatID, query_data):
        # VITALS:      (collegato al sensor_connector)
        if query_data == 'vitals':
            try:
                # sensorID del paziente dal catalog, poi ultima lettura dal sensor_connector
                user = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}").json()
                sensorID = user.get("sensorID")
                res = requests.get(f"{self.sensor_url}/sensors/{sensorID}/latest")

                if res.status_code == 200:
                    data = res.json()
                    msg = (f"Latest Vital Signs:\n\n"
                            f"Heart rate: {data['heart_rate']} bpm\n"
                            f"Temperature: {data['body_temperature']} °C\n"
                            f"Blood Pressure: {data['blood_pressure_systolic']}/{data['blood_pressure_diastolic']} mmHg")
                    self.bot.sendMessage(chatID, msg, parse_mode='Markdown')
                else:
                    self.bot.sendMessage(chatID, "Dati non disponibili al momento.")
            except Exception as e:
                self.bot.sendMessage(chatID, f"Connection error: {e}")
            self.send_main_menu(chatID)

        # APPOINTMENTS:
        elif query_data == 'appointments':
            res = requests.get(f"{self.catalog_url}/get_appointments?chatID={chatID}")
            data = res.json()
            doctor = data.get("doctor")
            # nasconde gli appuntamenti con data precedente a oggi
            appointments = [a for a in data.get("appointments", []) if not self.is_past_appointment(a)]
            
            if not appointments:
                text = f"No appointments found with doctor {doctor}."
            else:
                lines = []
                for a in appointments:
                    if isinstance(a, dict):
                        lines.append(f"{a.get('date', 'TBD')} {a.get('time', 'TBD')} - {a.get('reason', '')} ({a.get('status', '')})")
                    else:
                        lines.append(f"Appointment on {a}")
                text = f"Your appointments with doctor {doctor}:\n" + "\n".join(lines)
            
            buttons = [
                    [InlineKeyboardButton(text="Book New", callback_data='app_create')],
                    [InlineKeyboardButton(text="Delete All", callback_data='app_delete')],
                    [InlineKeyboardButton(text="Main Menu", callback_data='main_menu')]]
            self.bot.sendMessage(chatID, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        elif query_data == 'app_create':
            self.bot.sendMessage(chatID, "What is the reason for the appointment?")
            self.pending_registrations[chatID] = {"step": "waiting_app_reason"}
        elif query_data == 'app_delete':  # avvisare anche il dottore? per ora solo cancellazione lato bot
            try:
                res = requests.put(f"{self.catalog_url}/update_general_info",
                                   json={"chatID": chatID, "appointments": []})
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
            self.send_main_menu(chatID)
        
        # ALERTS: (creare gli alert del sensore o gli alert inviati direttamente dal dottore)
        elif query_data == 'alerts':
            # gli alert sono salvati nel profilo del paziente (vedi save_alert)
            res = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}")
            alerts = res.json().get("alerts", []) if res.status_code == 200 else []
            
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
                    self.send_main_menu(chatID)
            except Exception as e:
                self.bot.sendMessage(chatID, f"Connection error: {e}")
                self.send_main_menu(chatID)
        elif query_data == 'edit_profile':
            self.bot.sendMessage(chatID, "Let's update your profile. What's your name? (Name and Surname)")
            self.pending_registrations[chatID] = {"step": 1}
        elif query_data == 'main_menu':
            self.send_main_menu(chatID)


if __name__ == "__main__":
    config = json.load(open("conf.json"))

    # Docker: usa conf.json (http://catalog:8080). Fuori da Docker: CATALOG_URL=http://127.0.0.1:8080
    catalog_url = os.getenv("CATALOG_URL", config["catalog_url"])
    print(f"Catalog URL: {catalog_url}")

    bot = PatientMonitoringBot(
        token=config["token"],
        catalog_url=catalog_url,
        broker=config["broker"],
        port=config["port"]
    )
    
    
    while True:
        time.sleep(10)