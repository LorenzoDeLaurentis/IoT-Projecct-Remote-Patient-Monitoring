# tenere aperto in contemporanea: patient_contoll + reminder + catalog_telegram

############################ PER HEALTH CATALOG #############à########################
import cherrypy
import json
import logging



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Catalog] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)



class CatalogService:
    exposed = True

    def __init__(self, database_file):
        self.database_file = database_file
        # Carica i dati dal file JSON all'avvio
        with open(self.database_file, "r") as f:
            self.data = json.load(f)

        # It writes an informational log message to your console or terminal confirming that the Catalog successfully started up and read the target database file 
        # (database_file). It helps with debugging and tracking service health when the application boots.
        log.info("Catalog loaded from %s", database_file) 


    def save_database(self):
        #Save in-memory data to database.json
        try:
            with open(self.database_file, "w") as f:
                json.dump(self.data, f, indent=4)
            log.info("Database saved successfully")
        except Exception as e:
            log.error("Error saving database: %s", e)
            raise cherrypy.HTTPError(500, f"Database save failed: {e}")

    def find_patient_by_id(self, patient_id):
        #Find patient by chatID
        for patient in self.data.get("patients", []):
            if str(patient.get("chatID")) == str(patient_id):
                return patient
        return None
    


    # In distributed IoT architectures, microservices (like DataProcessor or VitalSignAlert) need to know foundational system 
    # information without hardcoding it, such as the MQTT broker's IP address, port, and topic schemas. 
    # This endpoint acts as a central configuration registry where any microservice can query the catalog to dynamically fetch its required connection parameters.
    def config(self, service_name = None):
        # Returns MQTT broker configuration for microservices
        # What returns:
        '''
        {
            "mqtt":{
                "broker_host":"message_broker"
                "broker_port":1883
                "topics":{
                    "sensors":"iothealth/+/sensors"
                    "alerts":"iothealth/+/alerts"
                    and so on
                }
            }
        }
        '''

        try:
            config_data = self.data.get("config", {})
            mqtt_config = config_data.get("mqtt", {})

            if not mqtt_config:
                log.warning("No MQTT config found in database")
                raise cherrypy.HTTPError(404, "MQTT configuration not found in catalog")

            log.info("Config requested by service: %s", service_name or "unknown")
            return {"mqtt": mqtt_config}

        except Exception as e:
            log.error("Error saving database: %s", e)
            raise cherrypy.HTTPError(500, f"Database save failed: {e}")




    def threshold(self):
        return {
            "heart_rate": {"min": 60, "max": 100},
            "body_temperature": {"min": 36.0, "max": 37.5},
            "blood_pressure_systolic": {"min": 90, "max": 140},
            "blood_pressure_diastolic": {"min": 60, "max": 90}
        }
    

    # NEW
    '''
    def threshold(self, patient_id = None):
        
        # COMMENT
        GET /patients/{patient_id}/thresholds
        Returns vital sign thresholds and baselines for a patient.
        Used by: dataprocessor, vital_sign_alert for comparison logic.

        RETURNS
        {
            "patient_id":"id"
            "threshold":{
            "heart_rate":{
                    "min": value,
                    "max": value
                }
            }
        }
        # END COMMENT

        if not patient_id:
            raise cherrypy.HTTPError(400, "patient_id parameter required")
        
        try:
            patient = self.find_patient_by_id(patient_id)
            if not patient:
                log.warning("Patient %s not found", patient_id)
                raise cherrypy.HTTPError(404, f"Patient {patient_id} not found")
            
            # Get patient specific thresholds or use defaults
            patient_thresholds = patient.get("thresholds", {})

            default_th = {
                "heart_rate": {
                    "min": 60,
                    "max": 100,
                },

                "body_temperature": {
                    "min": 36.0,
                    "max": 37.5,
                },

                "blood_pressure_systolic": {
                    "min": 90,
                    "max": 140,
                },

                "blood_pressure_diastolic": {
                    "min": 60,
                    "max": 90,
                }
            }

            #threshold_values = self.threshold.get("thresholds", default_th)

            # Use patient_thresholds if it has values, otherwise fill missing parts with default_th
            threshold_values = default_th
            if patient_thresholds:
                # Merge patient specific settings over defaults if needed, or just assign directly:
                threshold_values = patient_thresholds

            log.info("Thresholds retrieved for patient %s", patient_id)
            return {
                "patient_id": patient_id,
                "threshold_values": threshold_values,
            }
        
        except cherrypy.HTTPError:
            raise
        except Exception as e:
            log.error("Error fetching thresholds for %s: %s", patient_id, e)
            raise cherrypy.HTTPError(500, f"Thresholds fetch failed: {e}")
    '''


    # NEW
    '''
    def update_thresholds(self, patient_id = None):
        
        # COMMENT
        PUT /patients/{patient_id}/thresholds
        Update vital sign thresholds for a patient.
        NOTE: only doctors/admin can call this --> I HAVE TO FIND A WAY TO CHECK IF IT IS A DOCTOR OR NOT THAT IS CALLING IT

        REQUEST BODY:
        {
            "thresholds":{
                "heart_rate":{
                    "min":value,
                    "max":value
                }
            }
        }
        #END COMMENT

        if cherrypy.request.method != "PUT":
            raise cherrypy.HTTPError(405, "Method not allowed. Use PUT.")
        
        if not patient_id:
            raise cherrypy.HTTPError(400, "patient_id parameter required")
        
        try:
            body = cherrypy.request.json
            
            patient = self.find_patient_by_id(patient_id)
            if not patient:
                raise cherrypy.HTTPError(404, f"Patient {patient_id} not found")

            # update thresholds
            if "thresholds" not in patient:
                patient["thresholds"] = {}
            
            patient["thresholds"].update(body)
            self.save_database()

            log.info("thresholds updated for patient %s", patient_id)
            return{
                "status":"success",
                "patient_id":patient_id,
                "message":"Threshold updated successfully"
            }
        
        except cherrypy.HTTPError:
            raise
        except json.JSONDecodeError:
            raise cherrypy.HTTPError(400, "Invalid JSON in request body")
        except Exception as e:
            log.error("Error updating thresholds for %s: %s", patient_id, e)
            raise cherrypy.HTTPError(500, f"Thresholds update failed: {e}")
    '''




    def GET(self, *path, **params):
        if len(path) == 0:
            return "Catalog is online!"
        
        # NEW
        # Properly hooks into the config method you wrote earlier, allowing other microservices to pull system-wide parameters dynamically
        elif path[0] == "config":
            service_name = params.get("service", "unkonwn")
            return json.dumps(self.config(service_name))

        
        

        elif len(path) != 0 and path[0].lower == "all_patients":
            return json.dumps(self.data.get("patients", []))
        
        
        # REGISTRATION
        elif len(path) > 0 and path[0] == "search_patient":
            chatID = int(params.get("chatID"))
            for patient in self.data["patients"]:
                if patient["chatID"] == chatID:
                    return json.dumps(patient)
            raise cherrypy.HTTPError(404, "Patient not found")
        
        # REMINDERS
        elif len(path) != 0 and path[0] == "get_reminders":
            chatID = int(params.get("chatID"))

            found = False
            for p in self.data["patients"]:
                if p["chatID"] == chatID:
                    found = p
                    break

            # if patient don't exist
            if not found:
                raise cherrypy.HTTPError(404, "Patient not found")
            
            # if patient exist by no threshold
            reminders = found.get("reminders")
            if not reminders:
                raise cherrypy.HTTPError(500, "Internal Server Error: Reminders data missing for parient")
            
            return json.dumps(reminders)
                    
        
        
        # THRESHOLDS --> MAYBE USE THIS INSTEAD OF NEW ONE
        elif len(path)!=0 and path[0] == "get_thresholds":
            sensorID = params.get("sensorID")
            chatID = params.get("chatID")

            found = False
            for p in self.data["patients"]:
                if (sensorID and p.get("sensorID") == sensorID) or (chatID and str(p.get("chatID")) == str(chatID)):
                    found = p
                    break
                
            # if patient don't exist
            if not found:
                raise cherrypy.HTTPError(404, "Patient not found")
            
            # if patient exist by no threshold
            thresholds = found.get("thresholds")
            if not thresholds:
                raise cherrypy.HTTPError(500, "Internal Server Error: Threshold data missing for parient")
            
            return json.dumps(thresholds)
        


        # APPOINTMENTS with Telegram
        elif len(path)!= 0  and path[0] == "get_appointments":
            chatID = int(params.get("chatID"))
            for p in self.data["patients"]:
                if p["chatID"] == chatID:
                    response = {
                        "doctor": p.get("doctor"),
                        "appointments": p.get("appointments", [])
                    }
                    return json.dumps(response)
            raise cherrypy.HTTPError(404, "Patient not found")
        

        # APPOINTMENTS with Node-RED:
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



        # MY CALENDAR: MAYBE DIVIDE IT PER DOCTORS

        #DoctorX[
        #    {

        #        "date": "2026-04-22",
        #        "time": "19:32",
        #        "name": "Diana Giulia Cheban"

        #    },
        #    {

        #        "date": "2026-04-22",
        #        "time": "19:32",
        #        "name": "Diana Giulia Cheban"
        #    },

        #]
  
        elif  path[0] == "get_confirmed_appointments":
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
        
        else:
            raise cherrypy.HTTPError(400, "Bad request")








    def POST(self, *path, **params):
        # REGISTRATION: add a new patient
        if path[0] == "add_patient":

            raw_body = cherrypy.request.body.read()
            body = json.loads(raw_body)


            # Extract basic info (adjust keys to match your exact request body structure)
            chat_id = int(body.get("chatID"))
            name = body.get("name")
            birthdate = body.get("birthdate", "N/A")
            doctor = body.get("doctor", "N/A")
            sensor_id = body.get("sensorID", "N/A")

            # Check if patient already exists to avoid duplicates
            for p in self.data["patients"]:
                if p.get("chatID") == chat_id:
                    return "Patient with this ID alreay exist"
                    #raise cherrypy.HTTPError(400, "Patient with this chatID already exists")
                

            # Automatically assign default thresholds using the helper method
            new_patient = {
                "chatID": chat_id,
                "name": name,
                "birthdate": birthdate,
                "doctor": doctor,
                "sensorID": sensor_id,
                "thresholds": self.threshold(), 
                "reminders": [],
                "appointments": []
            }

            self.data["patients"].append(new_patient)
            self.save_database()
            

            
            



            '''
            # Aggiunge il paziente alla lista e salva su file
            self.data["patients"].append(new_patient)
            with open(self.database_file, "w") as f:
                json.dump(self.data, f, indent=4)
            return "OK"
            '''
        
        # REMINDERS: #TO DO: if i don't write time write N/A
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
                "date": body["date"],
                "reason": body["reason"],
                "time": body["time"],
                "status": body["status"]
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

        '''
        # Update thresholds --> NEW
        if len(path) >= 2 and path[0] == "patients" and path[2] == "thresholds":
            patient_id = path[1]
            return json.dumps(self.update_thresholds(patient_id))
        '''

        ########################### NUOVO PUT ##################################
        
        if path[0] == "update_general_info":
            change_body = cherrypy.request.body.read()
            change_body = json.loads(change_body)
            chatID = int(change_body["chatID"])

            found = False
            for patient in self.data["patients"]:
                if patient["chatID"] == chatID: # if the rule name is the same, I can change the rules
                    patient.update(change_body)
                    found = True

            if found:
                with open(self.database_file, "w") as f:
                    json.dump(self.data, f, indent=4)
                return "OK"
            else:
                raise cherrypy.HTTPError(404, "Patient not found")
            


        
        elif path[0] == "update_reminder":
            chatID = int(path[1])
            change_body = cherrypy.request.body.read()
            change_body = json.loads(change_body)

            target_medicine = change_body.get("medicine_name") # The current medicine name to find
            new_medicine_name = change_body.get("new_medicine_name") # The updated medicine name (optional)
            new_time = change_body.get("new_time")
            
            patient_found = False
            reminder_updated = False
            for patient in self.data["patients"]:
                if patient["chatID"] == chatID: # if the rule name is the same, I can change the rules
                    patient_found = True

                    for reminder in patient["reminders"]:
                        if reminder["medicine_name"] == target_medicine:
                            if new_medicine_name:
                                reminder["medicine_name"] = new_medicine_name
                                reminder_updated = True
                            if new_time:
                                reminder["time"] = new_time
                                reminder_updated = True
                            
            if reminder_updated:
                with open(self.database_file, "w") as f:
                    json.dump(self.data, f, indent=4)
                return "OK"
            elif not patient_found:
                raise cherrypy.HTTPError(404, "Patient not found")
            else:
                raise cherrypy.HTTPError(404, "Reminder with that medicine name not found")





        # use as a key the reason. But what if i have more appointmens in different days with the same reason?
        # !!!!!! TO DO: maybe do a check with the date: if reason is the same and also the date is the same, then this is the element to modify !!!!!!!
        elif path[0] == "update_appointment":
            chatID = int(path[1])
            change_body = cherrypy.request.body.read()
            change_body = json.loads(change_body)
        
            target_reason = change_body.get("target_reason") # The current medicine name to find --> write in body "target_reason":"name in database". It is the key to access to correct appointment
            new_date = change_body.get("new_date") # The updated medicine name 
            new_reason = change_body.get("new_reason") # updated reason
            new_time = change_body.get("new_time") # updated time
            new_status = change_body.get("new_status") # updated status
                    
            patient_found = False
            appointment_updated = False
            for patient in self.data["patients"]:
                if patient["chatID"] == chatID: # if the rule name is the same, I can change the rules
                    patient_found = True
        
                    for appointment in patient["appointments"]:
                        if appointment["reason"] == target_reason:
                            if new_date:
                                appointment["date"] = new_date
                                appointment_updated = True
                            if new_time:
                                appointment["time"] = new_time
                                appointment_updated = True
                            if new_reason:
                                appointment["reason"] = new_reason
                                appointment_updated = True
                            if new_status:
                                appointment["status"] = new_status
                                appointment_updated = True
                                    
            if appointment_updated:
                with open(self.database_file, "w") as f:
                    json.dump(self.data, f, indent=4)
                return "Database updated"
            elif not patient_found:
                raise cherrypy.HTTPError(404, "Patient not found")
            else:
                raise cherrypy.HTTPError(404, "Reminder with that date is not found")






    def DELETE(self, *path, **params):
        # Delete the entire patient profile
        if path[0] == "delete_patient":
            chatID = int(path[1])

            found = False
            for patient in self.data["patients"]:
                if patient["chatID"] == chatID:
                    self.data["patients"].remove(patient)
                    found = True
        
            if found:
                with open(self.database_file, "w") as f:
                    json.dump(self.data, f, indent=4)
                return "OK"
            else:
                raise cherrypy.HTTPError(404, "Patient not found")

        # delate reminder or all reminders or a soecific one
        elif path[0] == "delete_reminder":
            chatID = int(path[1])
            target_medicine = params.get("target_medicine")
        
            found_patient = False
            deltete_reminder = False
            for patient in self.data["patients"]:
                if patient["chatID"] == chatID:
                    found_patient = True

                    # clean all reminders
                    if len(params) == 0:
                        patient["reminders"] = []
                        deltete_reminder = True 

                    # remove a specific reminder
                    elif len(params) != 0:
                        for reminder in patient["reminders"]:
                            if target_medicine.strip().lower() == reminder["medicine_name"].strip().lower():
                                patient["reminders"].remove(reminder)
                                deltete_reminder = True
                                break
                            
        
            if deltete_reminder:
                with open(self.database_file, "w") as f:
                    json.dump(self.data, f, indent=4)
                return "OK"
            elif not found_patient:
                raise cherrypy.HTTPError(404, "Patient not found")
            else:
                raise cherrypy.HTTPError(404, "Reminder with that medicine name not found")


        # delate appointment --> same logic as reminder. WHAT DO I HAVE TO USE AS A KEY?
        #  WHAT I WANT TO DO IS DO THE ACCESS TO A SPECIFIC PATIENT, SEE ALL THE APPOINTMENTS AND CLICK ON DELATE ON THE APPOINTMENT I WANT TO CANCEL OUT


        


            



if __name__ == "__main__":
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True,
        }
    }

    # creates an instance of your web handler class
    cherrypy.tree.mount(CatalogService("database.json"), '/', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()

