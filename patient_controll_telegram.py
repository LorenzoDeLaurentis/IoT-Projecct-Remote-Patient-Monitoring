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
        # Tracks which appointment index a patient is in the process of cancelling
        # {chatID: appointment_index}
        self.pending_cancellations = {}

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
        self.client.mySubscribe("clinician/patient/+/appointments/rejection")
        self.client.mySubscribe("clinician/patient/+/appointments/cancellation")

        MessageLoop(self.bot, {
            'chat': self.on_chat_message,
            'callback_query': self.on_callback_query
        }).run_as_thread()

    # for alerts:
    def notify(self, topic, payload):
        try:
            # Trasforma il payload (stringa) in un dizionario Python
            msg_data = json.loads(payload)
            # vital_sign_alert codifica il messaggio due volte: se è ancora una stringa, decodifico di nuovo
            if isinstance(msg_data, str):
                msg_data = json.loads(msg_data)
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

            # 2. Caso: Rifiuto della richiesta di appuntamento da Node-RED
            elif "appointments/rejection" in topic:
                chatID = msg_data.get("chatID")
                reason = msg_data.get("reason")
                motivation = msg_data.get("motivation", "")

                # la richiesta passa a "rejected": esce dai pending e dalla lista Appointments
                body = {"target_reason": reason, "new_status": "rejected"}
                update = requests.put(f"{self.catalog_url}/update_appointment/{chatID}", json=body)
                if update.status_code != 200:
                    print(f"update_appointment fallito: {update.status_code} {update.text}")
                    return

                rifiuto = f"Your doctor rejected your appointment request \"{reason}\". Reason: {motivation}"
                self.bot.sendMessage(chatID, rifiuto)
                # il rifiuto resta visibile anche nella sezione Alerts
                self.save_alert(chatID, rifiuto)
                print(f"Rejection sent to {chatID}")

            # 3. Caso: Cancellazione confermata dal dottore (Node-RED)
            elif "appointments/cancellation" in topic:
                chatID = msg_data.get("chatID")
                reason = msg_data.get("reason")
                cancelled_by = msg_data.get("cancelled_by", "doctor")

                # Mark the appointment as cancelled in the catalog
                body = {"target_reason": reason, "new_status": "cancelled"}
                update = requests.put(f"{self.catalog_url}/update_appointment/{chatID}", json=body)
                if update.status_code != 200:
                    print(f"update_appointment (cancellation) fallito: {update.status_code} {update.text}")
                    return

                testo = f"Your appointment \"{reason}\" has been cancelled by your doctor."
                self.bot.sendMessage(chatID, testo)
                self.save_alert(chatID, testo)
                print(f"Cancellation notification sent to {chatID}")

            # 4. Caso: Alert dai sensori (se previsto nel tuo sistema)
            elif "alert" in topic:
                chatID = msg_data.get("chatID")
                testo = msg_data.get("msg", "Attenzione: Alert rilevato!")
                # Reminder della pastiglia: messaggio con i pulsanti di risposta
                if "Reminder for your medicine" in testo:
                    self.send_medicine_reminder(chatID, testo)
                else:
                    # Disattivato per la prova: gli ALERT dei sensori non vengono mandati su Telegram (i reminder si)
                    # self.bot.sendMessage(chatID, f"ALERT: {testo}")
                    print(f"ALERT sensore non inviato (disattivato): {chatID} {testo}")

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
                    self.bot.sendMessage(chatID, "Welcome! You are not registered. Let's get started.\nWhat's your name? (name and surname)")
                    self.pending_registrations[chatID] = {"step": 1}
            except Exception as e:
                print(f"[/start] {type(e).__name__}: {e}")   # causa reale
                self.bot.sendMessage(chatID, f"Error connecting to the catalog ({type(e).__name__}).")

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
            self.bot.sendMessage(chatID, "What is your doctor name? (name and surname)")
            state["step"] = 3
        elif state["step"] == 3:
            # Check: il medico deve esistere nel catalog
            try:
                doctors = requests.get(f"{self.catalog_url}/get_doctors").json()
            except Exception as e:
                print(f"[get_doctors] {type(e).__name__}: {e}")
                self.bot.sendMessage(chatID, f"Error connecting to the catalog ({type(e).__name__}). Write your doctor name again:")
                return
            doctor = next((d for d in doctors if d["name"].strip().lower() == text.strip().lower()), None)
            if doctor is None:
                names = ", ".join(d["name"] for d in doctors)
                self.bot.sendMessage(chatID, f"Doctor not found. Available doctors: {names}\nWhat is your doctor name? (name and surname)")
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
                self.bot.sendMessage(chatID, f"Error connecting to the catalog ({type(e).__name__}).")
            
            del self.pending_registrations[chatID]
            self.send_main_menu(chatID)

        # APPOINTMENTS:     c'è il collegamento al dottore, che deve confermare l'appuntamento, per ora è solo registrato
        elif state.get("step") == "waiting_app_reason":
            reason = text
            # tolgo subito lo stato: se qualcosa va storto il prossimo messaggio non viene preso di nuovo come motivo
            del self.pending_registrations[chatID]

            try:
                name = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}").json().get("name", "Unknown")
                mqtt_payload = {
                    "patient_name": name,
                    "chatID": chatID,
                    "reason": reason,
                    "status": "pending"
                }
                self.client.myPublish("clinician/patient/appointments/request", mqtt_payload)
                catalog_body = {**mqtt_payload, "date": "TBD", "time": "TBD"}
                res = requests.post(f"{self.catalog_url}/add_appointment", json=catalog_body)
                print(mqtt_payload)
                if res.status_code == 200:
                    self.bot.sendMessage(chatID, "Appointment saved. We requested to your doctor an appointment.")
                else:
                    print(f"add_appointment fallito: {res.status_code} {res.text}")
                    self.bot.sendMessage(chatID, "Error saving appointment. Try again.")
            except Exception as e:
                self.bot.sendMessage(chatID, f"Error: {e}")
           
            self.send_main_menu(chatID)

        # REMINDERS:
        elif state.get("step") == "waiting_rem_data":
            parts = [p.strip() for p in text.split(",")]
            # l'orario può essere scritto prima o dopo la medicina ("Aspirin, 08:30" oppure "08:30, Aspirin")
            times = [p for p in parts if self.normalize_time(p)]
            meds = [p for p in parts if not self.normalize_time(p)]
            if len(parts) != 2 or len(times) != 1 or len(meds) != 1:
                # formato sbagliato: resto in attesa di un nuovo tentativo
                self.bot.sendMessage(chatID, "Format error. Try again (medicine, HH:MM):")
                return

            del self.pending_registrations[chatID]
            # orario sempre HH:MM (8:00 -> 08:00), se no reminder.py non lo trova
            payload = {"chatID": chatID, "medicine_name": meds[0], "time": self.normalize_time(times[0])}
            try:
                res = requests.post(f"{self.catalog_url}/add_reminder", json=payload)
                # prima diceva sempre "Reminder saved!" anche se il catalog dava errore
                if res.status_code == 200:
                    self.bot.sendMessage(chatID, "Reminder saved!")
                else:
                    print(f"add_reminder fallito: {res.status_code} {res.text}")
                    self.bot.sendMessage(chatID, "Error saving reminder. Try again.")
            except Exception as e:
                print(f"[add_reminder] {type(e).__name__}: {e}")
                self.bot.sendMessage(chatID, f"Error connecting to the catalog ({type(e).__name__}).")
            self.send_main_menu(chatID)

    def _get_cancellable_appointments(self, chatID):
        """
        Returns the list of appointments that can be cancelled:
        future appointments that are either pending or confirmed.
        Rejected and already-cancelled ones are excluded.
        """
        res = requests.get(f"{self.catalog_url}/get_appointments?chatID={chatID}")
        if res.status_code != 200:
            return []
        all_apps = res.json().get("appointments", [])
        return [
            a for a in all_apps
            if isinstance(a, dict)
            and not self.is_past_appointment(a)
            and a.get("status") not in ("rejected", "cancelled")
        ]

    def show_appointments_for_cancellation(self, chatID):
        """
        Sends a message listing each cancellable appointment as its own
        inline button. Tapping a button triggers 'app_cancel_<index>'.
        """
        apps = self._get_cancellable_appointments(chatID)
        if not apps:
            self.bot.sendMessage(chatID, "You have no appointments that can be cancelled.")
            self.send_main_menu(chatID)
            return

        buttons = []
        for i, a in enumerate(apps):
            label = (
                f"{a.get('date','TBD')} {a.get('time','TBD')} "
                f"– {a.get('reason','')} ({a.get('status','')})"
            )
            buttons.append([InlineKeyboardButton(text=label, callback_data=f"app_cancel_{i}")])

        buttons.append([InlineKeyboardButton(text="Back", callback_data="appointments")])
        self.bot.sendMessage(
            chatID,
            "Tap the appointment you want to cancel:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )

    def cancel_appointment(self, chatID, index: int):
        """
        Cancels a specific appointment by its position in the cancellable list.
        Steps:
          1. Fetch the cancellable appointments and pick the one at `index`.
          2. Update its status to 'cancelled' in the catalog.
          3. Publish an MQTT message so the clinician portal (Node-RED) is notified.
          4. Save the cancellation as an alert and confirm to the patient.
        """
        apps = self._get_cancellable_appointments(chatID)

        if index < 0 or index >= len(apps):
            self.bot.sendMessage(chatID, "Appointment not found. It may have already been cancelled.")
            self.send_main_menu(chatID)
            return

        appointment = apps[index]
        reason = appointment.get("reason", "Unknown")
        date   = appointment.get("date", "TBD")
        time_  = appointment.get("time", "TBD")

        # 1. Update catalog
        try:
            body = {"target_reason": reason, "new_status": "cancelled"}
            res = requests.put(f"{self.catalog_url}/update_appointment/{chatID}", json=body)
            if res.status_code != 200:
                self.bot.sendMessage(chatID, "Could not cancel the appointment. Please try again.")
                print(f"cancel_appointment catalog update failed: {res.status_code} {res.text}")
                self.send_main_menu(chatID)
                return
        except Exception as e:
            self.bot.sendMessage(chatID, f"Connection error: {e}")
            self.send_main_menu(chatID)
            return

        # 2. Notify the doctor via MQTT so Node-RED can update the clinician portal
        try:
            patient_name = requests.get(
                f"{self.catalog_url}/search_patient?chatID={chatID}"
            ).json().get("name", "Unknown")

            mqtt_payload = {
                "chatID":        chatID,
                "patient_name":  patient_name,
                "reason":        reason,
                "date":          date,
                "time":          time_,
                "cancelled_by":  "patient",
            }
            self.client.myPublish("clinician/patient/appointments/cancelled", mqtt_payload)
            print(f"Cancellation MQTT published for patient {chatID}: {reason}")
        except Exception as e:
            # MQTT failure is non-fatal: catalog is already updated
            print(f"[cancel_appointment] MQTT publish failed: {e}")

        # 3. Confirm to patient and save as alert
        confirmation = f"Appointment \"{reason}\" on {date} at {time_} has been cancelled."
        self.bot.sendMessage(chatID, f"{confirmation}")
        self.save_alert(chatID, confirmation)
        self.send_main_menu(chatID)

    # Lista dei reminder del paziente (GET get_reminders)
    def _get_reminders(self, chatID):
        res = requests.get(f"{self.catalog_url}/get_reminders?chatID={chatID}")
        return res.json() if res.status_code == 200 else []

    # Ogni reminder come pulsante: toccandolo parte 'rem_delete_<index>'
    def show_reminders_for_deletion(self, chatID):
        reminders = self._get_reminders(chatID)
        if not reminders:
            self.bot.sendMessage(chatID, "You have no reminders to delete.")
            self.send_main_menu(chatID)
            return

        buttons = []
        for i, r in enumerate(reminders):
            label = f"{r.get('medicine_name', '')} at {r.get('time', '')}"
            buttons.append([InlineKeyboardButton(text=label, callback_data=f"rem_delete_{i}")])
        buttons.append([InlineKeyboardButton(text="Back", callback_data="reminders")])
        self.bot.sendMessage(chatID, "Tap the reminder you want to delete:",
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

    # Elimina un solo reminder (quello scelto, anche se ci sono medicine con lo stesso nome a orari diversi):
    # rimando al catalog la lista senza quel reminder (PUT update_general_info)
    def delete_reminder(self, chatID, index: int):
        reminders = self._get_reminders(chatID)
        if index < 0 or index >= len(reminders):
            self.bot.sendMessage(chatID, "Reminder not found. It may have already been deleted.")
            self.send_main_menu(chatID)
            return

        removed = reminders.pop(index)
        try:
            res = requests.put(f"{self.catalog_url}/update_general_info",
                               json={"chatID": chatID, "reminders": reminders})
            if res.status_code != 200:
                print(f"delete_reminder fallito: {res.status_code} {res.text}")
                self.bot.sendMessage(chatID, "Could not delete the reminder. Please try again.")
                self.send_main_menu(chatID)
                return
        except Exception as e:
            self.bot.sendMessage(chatID, f"Connection error: {e}")
            self.send_main_menu(chatID)
            return

        self.bot.sendMessage(chatID, f"Reminder \"{removed.get('medicine_name', '')}\" at {removed.get('time', '')} has been deleted.")
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

    # Orario del reminder in formato HH:MM ("8:00" -> "08:00"), None se non è un orario valido
    def normalize_time(self, text):
        try:
            return datetime.strptime(text.strip(), "%H:%M").strftime("%H:%M")
        except ValueError:
            return None

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
        # Se il paziente non e' registrato il catalog risponde 404: invece di andare in errore chiedo di registrarsi
        try:
            check = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}")
        except Exception as e:
            print(f"[manage_action] {type(e).__name__}: {e}")
            self.bot.sendMessage(chatID, f"Error connecting to the catalog ({type(e).__name__}).")
            return
        if check.status_code != 200:
            self.bot.sendMessage(chatID, "You are not registered yet. Send /start to register.")
            return

        # VITALS:      (collegato al sensor_connector)
        if query_data == 'vitals':
            try:
                # sensorID del paziente dal catalog, poi ultima lettura dal sensor_connector
                user = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}").json()
                sensorID = user.get("sensorID")
                res = requests.get(f"{self.sensor_url}/sensors/{sensorID}/latest")

                if res.status_code == 200:
                    data = res.json()
                    msg = (f"Latest vital signs:\n\n"
                            f"Heart rate: {data['heart_rate']} bpm\n"
                            f"Temperature: {data['body_temperature']} °C\n"
                            f"Blood pressure: {data['blood_pressure_systolic']}/{data['blood_pressure_diastolic']} mmHg")
                    self.bot.sendMessage(chatID, msg, parse_mode='Markdown')
                else:
                    self.bot.sendMessage(chatID, "Data not available at the moment.")
            except Exception as e:
                self.bot.sendMessage(chatID, f"Connection error: {e}")
            self.send_main_menu(chatID)

        # APPOINTMENTS:
        elif query_data == 'appointments':
            res = requests.get(f"{self.catalog_url}/get_appointments?chatID={chatID}")
            data = res.json()
            doctor = data.get("doctor")
            # nasconde gli appuntamenti con data precedente a oggi e le richieste rifiutate dal medico
            appointments = [a for a in data.get("appointments", [])
                            if not self.is_past_appointment(a) and not (isinstance(a, dict) and a.get("status") == "rejected")]
            
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
                    [InlineKeyboardButton(text="Book new",    callback_data='app_create')],
                    [InlineKeyboardButton(text="Cancel one",  callback_data='app_cancel_select')],
                    [InlineKeyboardButton(text="Delete all",  callback_data='app_delete')],
                    [InlineKeyboardButton(text="Main menu",   callback_data='main_menu')]]
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

        # CANCEL A SPECIFIC APPOINTMENT:
        elif query_data == 'app_cancel_select':
            # Show each cancellable appointment as a tappable button
            self.show_appointments_for_cancellation(chatID)

        # NB: escludo 'app_cancel_confirm_...' (inizia anche lui con 'app_cancel_'), se no il "Yes" finiva qui
        elif query_data.startswith('app_cancel_') and not query_data.startswith('app_cancel_confirm_'):
            # e.g. 'app_cancel_0', 'app_cancel_1', ...
            try:
                index = int(query_data.split('app_cancel_')[1])
            except (IndexError, ValueError):
                self.bot.sendMessage(chatID, "Invalid selection. Please try again.")
                self.send_main_menu(chatID)
                return

            # Ask for confirmation before actually cancelling
            apps = self._get_cancellable_appointments(chatID)
            if index < 0 or index >= len(apps):
                self.bot.sendMessage(chatID, "Appointment not found.")
                self.send_main_menu(chatID)
                return

            a = apps[index]
            confirm_text = (
                f"Are you sure you want to cancel this appointment?\n\n"
                f"Date:   {a.get('date', 'TBD')}\n"
                f"Time:   {a.get('time', 'TBD')}\n"
                f"Reason: {a.get('reason', '')}\n"
                f"Status: {a.get('status', '')}"
            )
            buttons = [
                [InlineKeyboardButton(text="Yes, cancel it",  callback_data=f"app_cancel_confirm_{index}"),
                 InlineKeyboardButton(text="No, go back",     callback_data="app_cancel_select")]
            ]
            self.bot.sendMessage(chatID, confirm_text,
                                 reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

        elif query_data.startswith('app_cancel_confirm_'):
            # Patient confirmed — proceed with cancellation
            try:
                index = int(query_data.split('app_cancel_confirm_')[1])
            except (IndexError, ValueError):
                self.bot.sendMessage(chatID, "Invalid selection. Please try again.")
                self.send_main_menu(chatID)
                return
            self.cancel_appointment(chatID, index)  
            
        # REMINDERS:   (finito)
        elif query_data == "reminders":
            res = requests.get(f"{self.catalog_url}/get_reminders?chatID={chatID}")
            reminders = res.json()
            
            if not reminders:
                text = "No reminders found."
            else:
                text = "Your reminders:\n" + "\n".join([f"{r['medicine_name']} at {r['time']}" for r in reminders])
            
            buttons = [[InlineKeyboardButton(text="Create", callback_data='rem_create')],
                       [InlineKeyboardButton(text="Delete one", callback_data='rem_delete_select')],
                       [InlineKeyboardButton(text="Delete all", callback_data='rem_delate')],
                       [InlineKeyboardButton(text="Main menu", callback_data='main_menu')]]
            self.bot.sendMessage(chatID, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

        # DELETE ONE REMINDER: stesso schema della cancellazione degli appuntamenti
        elif query_data == 'rem_delete_select':
            # ogni reminder diventa un pulsante
            self.show_reminders_for_deletion(chatID)

        # NB: escludo 'rem_delete_confirm_...' (inizia anche lui con 'rem_delete_'), come per gli appuntamenti
        elif query_data.startswith('rem_delete_') and not query_data.startswith('rem_delete_confirm_'):
            # es. 'rem_delete_0', 'rem_delete_1', ...
            try:
                index = int(query_data.split('rem_delete_')[1])
            except (IndexError, ValueError):
                self.bot.sendMessage(chatID, "Invalid selection. Please try again.")
                self.send_main_menu(chatID)
                return

            # prima di eliminare chiedo conferma
            reminders = self._get_reminders(chatID)
            if index < 0 or index >= len(reminders):
                self.bot.sendMessage(chatID, "Reminder not found.")
                self.send_main_menu(chatID)
                return

            r = reminders[index]
            confirm_text = (
                f"Are you sure you want to delete this reminder?\n\n"
                f"Medicine: {r.get('medicine_name', '')}\n"
                f"Time:     {r.get('time', '')}"
            )
            buttons = [
                [InlineKeyboardButton(text="Yes, delete it", callback_data=f"rem_delete_confirm_{index}"),
                 InlineKeyboardButton(text="No, go back", callback_data="rem_delete_select")]
            ]
            self.bot.sendMessage(chatID, confirm_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

        elif query_data.startswith('rem_delete_confirm_'):
            # il paziente ha confermato: elimino
            try:
                index = int(query_data.split('rem_delete_confirm_')[1])
            except (IndexError, ValueError):
                self.bot.sendMessage(chatID, "Invalid selection. Please try again.")
                self.send_main_menu(chatID)
                return
            self.delete_reminder(chatID, index)
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
        
        # TRENDS SETTIMANALI: link al canale ThingSpeak del paziente (campo "thingspeak" nel catalog)
        elif query_data == 'stats':
            try:
                user = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}").json()
                channel_id = (user.get("thingspeak") or {}).get("channel_id")
                if channel_id:
                    buttons = [[InlineKeyboardButton(text="Open ThingSpeak chart",
                                                     url=f"https://thingspeak.com/channels/{channel_id}")]]
                    self.bot.sendMessage(chatID, "Here is your weekly trend:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
                else:
                    self.bot.sendMessage(chatID, "Your ThingSpeak channel is not configured yet.")
            except Exception as e:
                self.bot.sendMessage(chatID, f"Connection error: {e}")
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
            
            buttons = [[InlineKeyboardButton(text="Main menu", callback_data='main_menu')]]
            self.bot.sendMessage(chatID, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


        # PROFILE:     (finito)  
        elif query_data == 'profile':
            self.bot.sendMessage(chatID, "Here is your data registered in the system:")
            try:
                res = requests.get(f"{self.catalog_url}/search_patient?chatID={chatID}")
                if res.status_code == 200:
                    user = res.json()
                    msg = (f"Name: {user['name']}\n"
                           f"Birthdate: {user['birthdate']}\n"
                           f"Doctor: {user['doctor']}\n"
                           f"Sensor ID: {user['sensorID']}")
                    
                    buttons = [
                        [InlineKeyboardButton(text="Edit profile", callback_data='edit_profile')],
                        [InlineKeyboardButton(text="Main menu", callback_data='main_menu')]]
                    
                    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
                    # senza parse_mode='Markdown': un "_" nel nome o nel sensorID (es. sensor_01) faceva fallire il messaggio
                    self.bot.sendMessage(chatID, msg, reply_markup=keyboard)
                else:
                    self.bot.sendMessage(chatID, "Profile not found. Please register again.")
                    self.send_main_menu(chatID)
            except Exception as e:
                self.bot.sendMessage(chatID, f"Connection error: {e}")
                self.send_main_menu(chatID)
        elif query_data == 'edit_profile':
            self.bot.sendMessage(chatID, "Let's update your profile. What's your name? (name and surname)")
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