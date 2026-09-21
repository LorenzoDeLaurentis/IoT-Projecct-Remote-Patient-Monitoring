# tenere aperto in contemporanea: patient_contoll + reminder + catalog_telegram
import cherrypy
import json

class CatalogService:
    exposed = True

    def __init__(self, database_file):
        self.database_file = database_file
        # Carica i dati dal file JSON all'avvio
        with open(self.database_file, "r") as f:
            self.data = json.load(f)

    def GET(self, *path, **params):
        if len(path) == 0:
            return "Catalog is online!"
        elif path[0] == "all_patients":
            return json.dumps(self.data.get("patients", []))
        # REGISTRATION:
        elif path[0] == "search_patient":
            chatID = int(params.get("chatID"))
            for patient in self.data["patients"]:
                if patient["chatID"] == chatID:
                    return json.dumps(patient)
            raise cherrypy.HTTPError(404, "Paziente non trovato")
        # REMINDERS:
        elif path[0] == "get_reminders":
            chatID = int(params.get("chatID"))
            for p in self.data["patients"]:
                if p["chatID"] == chatID:
                    return json.dumps(p.get("reminders", []))
            return json.dumps([])
        
        # APPOINTMENTS su Telegram:
        elif path[0] == "get_appointments":
            chatID = int(params.get("chatID"))
            for p in self.data["patients"]:
                if p["chatID"] == chatID:
                    response = {
                        "doctor": p.get("doctor"),
                        "appointments": p.get("appointments", [])
                    }
                    return json.dumps(response)
            return json.dumps([])
        
        # APPOINTMENTS su Node-RED:
        elif path[0] == "get_pending_requests":
            pending = []
            for p in self.data["patients"]:
                for app in p.get("appointments", []):
                    # Verifica che sia un dizionario e che sia pending
                    if isinstance(app, dict) and app.get("status") == "pending":
                        pending.append({
                            "name": p["name"],
                            "chatID": p["chatID"],
                            "reason": app.get("reason", "N/A")
                        })
            return json.dumps(pending)

        # MY CALENDAR:
        elif path[0] == "get_confirmed_appointments":
            confirmed = []
            for p in self.data["patients"]:
                appointments_list = p.get("appointments", [])
                for app in appointments_list:
                    # CASO 1: Nuovo formato (dizionario con status)
                    if isinstance(app, dict) and app.get("status") == "confirmed":
                        confirmed.append({
                            "date": app.get("date", "TBD"),
                            "time": app.get("time", "TBD"),
                            "name": p["name"]
                        })
                        
                    # CASO 2: Vecchio formato (solo la stringa della data)
                    elif isinstance(app, str):
                        confirmed.append({
                            "date": app,
                            "time": "TBD",
                            "name": p["name"]
                        })

            # IF NO APPOINTMENTS:
            if len(confirmed) == 0:
                nessun_appuntamento = [{
                    "date": "-",
                    "time": "-",
                    "name": "No appointments"
                }]
                return json.dumps(nessun_appuntamento)
            return json.dumps(confirmed)
        
        # Ritorna tutti i pazienti (filtrabili per dottore se passi il parametro)
        elif path[0] == "get_all_patients":
            # Questa variabile per ora è "None", ma in futuro Node-RED
            # potrà passargli il nome del medico dopo il login
            doctor_name = params.get("doctor_name", None) 
            
            patient_list = []
            for p in self.data["patients"]:
                # Se un dottore è specificato, mostra solo i suoi, 
                # altrimenti mostrali tutti
                if doctor_name is None or p.get("doctor") == doctor_name:
                    patient_list.append({
                        "name": p["name"],
                        "birthdate": p.get("birthdate", "N/A"),
                        "sensorID": p.get("sensorID", "N/A"),
                        "chatID": p["chatID"]
                    })
            return json.dumps(patient_list)

    def POST(self, *path, **params):
        # REGISTRATION:
        if path[0] == "add_patient":
            body = cherrypy.request.body.read()
            new_patient = json.loads(body)
            
            # Aggiunge il paziente alla lista e salva su file
            self.data["patients"].append(new_patient)
            with open(self.database_file, "w") as f:
                json.dump(self.data, f, indent=4)
            return "OK"
        
        # REMINDERS:
        elif path[0] == "add_reminder":
            body = json.loads(cherrypy.request.body.read())
            chatID = int(body["chatID"])
            new_rem = {"medicine_name": body["medicine_name"], "time": body["time"]}
            
            for p in self.data["patients"]:
                if p["chatID"] == chatID:
                    if "reminders" not in p: p["reminders"] = []
                    p["reminders"].append(new_rem)
                with open(self.database_file, "w") as f:
                    json.dump(self.data, f, indent=4)
                    
            self.save_database() # La tua funzione che scrive su database.json
            return "OK"

        # APPOINTMENTS:
        elif path[0] == "add_appointment":
            body = json.loads(cherrypy.request.body.read())
            chatID = int(body["chatID"])
            new_app = {
                "date": "TBD",
                "reason": body["reason"],
                "time": "TBD",
                "status": "pending"
            }
            for p in self.data["patients"]:
                if p["chatID"] == chatID:
                    if "appointments" not in p: p["appointments"] = []
                    p["appointments"].append(new_app)
                    break
            
            with open(self.database_file, "w") as f:
                json.dump(self.data, f, indent=4)
            return "OK"

        #  Per NOD-RED: ritorna tutti i pazienti 
        elif path[0] == "get_all_patients":
            patient_list = []
            for p in self.data["patients"]:
                patient_list.append({
                    "name": p["name"],
                    "birthdate": p.get("birthdate", "N/A"),
                    "sensorID": p.get("sensorID", "N/A"),
                    "chatID": p["chatID"]
                })
            return json.dumps(patient_list)

    def PUT(self, *path, **params):
        # DA RIEVEDERE:
        if path[0] == "update_patient":
            body = json.loads(cherrypy.request.body.read())
            new_data = body
            chatID = new_data["chatID"]
            
            found = False
            for i, patient in enumerate(self.data["patients"]):
                if patient["chatID"] == chatID:
                    old_reminders = patient.get("reminders", [])
                    old_appointments = patient.get("appointments", [])
                    self.data["patients"][i] = new_data
                    self.data["patients"][i]["reminders"] = old_reminders
                    self.data["patients"][i]["appointments"] = old_appointments
                    found = True
                    break
            
            if found:
                with open(self.database_file, "w") as f:
                    json.dump(self.data, f, indent=4)
                return "OK"
            else:
                raise cherrypy.HTTPError(404, "Patient not found")
        
        elif path[0] == "update_appointment":
            body = json.loads(cherrypy.request.body.read())
            chatID = body.get("chatID")
            
            success = False
            # Look for the patient in the database
            for patient in self.data["patients"]:
                if patient["chatID"] == chatID:
                    # Look for the pending appointment to confirm it
                    for app in patient.get("appointments", []):
                        if app["status"] == "pending":
                            app["date"] = body.get("date")
                            app["time"] = body.get("time")
                            app["status"] = "confirmed"
                            success = True
                            break # Stop once updated
            
            if success:
                json.dump(self.data, open(self.database_file, "w"), indent=4)
                return "OK"
            
            raise cherrypy.HTTPError(404, "No pending appointment found for this patient.")

    def DELETE(self, *path, **params):
        if path[0] == "delete_all_reminders":
            chatID = int(params.get("chatID"))
            
            found = False
            for p in self.data["patients"]:
                if p["chatID"] == chatID:
                    p["reminders"] = []
                    found = True
                    break
            
            if found:
                # Salviamo il file database.json
                with open(self.database_file, "w") as f:
                    json.dump(self.data, f, indent=4)
                return "OK"
            else:
                raise cherrypy.HTTPError(404, "Patient not found")

if __name__ == "__main__":
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True,
        }
    }
    cherrypy.config.update({'server.socket_host': "0.0.0.0", 'server.socket_port': 8080})
    cherrypy.tree.mount(CatalogService("database.json"), '/', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()