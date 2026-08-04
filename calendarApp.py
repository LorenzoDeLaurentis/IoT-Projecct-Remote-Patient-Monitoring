from flask import Flask, Response, render_template
import json
from datetime import datetime
from collections import OrderedDict #to keep the key order written in code equal to the one displayed

'''
Without Flask, Python just runs scripts in the terminal or background (like your MQTT reminder manager). 
It doesn't know how to talk to a web browser or show visual windows. Flask gives your Python project a "voice" and a "face" on the web.

What flask do:
    - act as a local web serve. When you run calendar.py, Flask starts a local server on your computer
    - serves HTML pages. When you type that URL into your browser, Flask finds your calendar.html file and sends it to your browser so you can actually see the visual calendar
    - handles data requests. Visual calendar needs to know what appointments are in database.json file. The browser cannot read files directly from your computer's hard drive
        so Flask acts as the middleman: 
            browser asks Flask: "Give me the appointments."
            Flask reads database.json using Python.
            Flask sends that data back to the browser in a clean format (JSON), which the calendar uses to display the events

NOTE: JSON dictionaries don't have a guaranteed order for their keys. When Python converts a dictionary to JSON and sends it to your browser or when a browser client displays it, the keys
      might appear in a different order than the one typed in code.
      To have keys appears in a specific order when viewed in frontend, use 'collections.OrderedDict'
'''


# Initialize the Flask application
# creates an istance of the flask class. This object acts as central registry for views, url and configurations
app = Flask(__name__)

# when shows the output of the code, Flask order the keys in aphabetical order --> to prevent this behaviour, tell Flask to not sort in this way
#app.config["JSON_SORT_KEYS"] = False --> DOESN'T WORK

# helper function to load data from JSON file
def load_data():
    with open("database.json", "r") as file:
        return json.load(file) # converts contents of json file into standard python dictionary




# Define the route for the homepage ("/")
# It tells Flask that whenever a user visits your website's root URL (e.g., http://localhost:5000/), it should execute the function immediately.
@app.route("/")
# This view function returns a simple string. Flask automatically takes this string, sends it to the user's browser, and displays it as a webpage.
def home():
    #data = load_data() --> PROVE
    #return f"Loaded {len(data['patients'])} from database" --> PROVE
    return render_template('Calendar.html')


# Define another route
# Route to return the entire JSON data as an API endpoint
@app.route("/appointments", methods=["GET"])
def get_appointments():
    data = load_data()

    patient_appointment = []

    for patient in data.get("patients", []): # write like this and not like data["patients"] to prevent crash if a patient don't have "appointments":
        for app in patient.get("appointments", []):

            
            raw_date = str(app.get("date", ""))
            formatted_date = raw_date

            try:
                d_m_y_date = datetime.strptime(raw_date, "%d-%m-%Y")
                formatted_date = d_m_y_date.strftime("%Y-%m-%d")

            except ValueError:
                try:
                    # Fallback in case it's already YYYY-MM-DD
                    d_m_y_date = datetime.strptime(raw_date, "%Y-%m-%d")
                    formatted_date = d_m_y_date.strftime("%Y-%m-%d")
                except ValueError:
                    pass


            time_string = app.get("time", "00:00")
            start_datetime = f"{formatted_date}T{time_string}"

            # Format data for the frontend calendar
            # Use standard curly braces {} instead of a list of tuples []
            info = {
                "title": f"Appointment: {app.get('reason', 'Unknown')} ({patient.get('name', 'Unknown')})",
                "start": start_datetime,  # Required by FullCalendar
                "reason": app.get('reason', 'Unknown'),
                "doctor": patient.get("doctor", "Unknown"),
                "patient_name": patient.get("name", "Unknown"),
                "date": raw_date,
                "time": time_string,
            }

            patient_appointment.append(info)
            #info = OrderedDict(info)

    # 3. Serialize list of dictionaries to JSON string and encode to bytes properly
    json_data = json.dumps(patient_appointment, sort_keys=False)
    return Response(json_data.encode("utf-8"), mimetype="application/json")
    #json_data = json.dumps(patient_appointment, sort_keys=False)
    #return Response(patient_appointment) # use Response to bypass the aphabetical ordering given in default by Flask. Use this since 
                                # STANDARD RETURN (not necessary in modern browser): return Response(json_data, mimetype="application/json") --> tells what kind of data are sent
# jsonify converts python dictionary in a json 



# Run the application
# Starts the local development server. Setting debug=True means the server will automatically restart whenever you save changes to your code, and it will show detailed error messages if something goes wrong.
if __name__ == "__main__":
    app.run(debug=True)


'''

app = Flask(__name__)
DATABASE_FILE = "database.json"

@app.route('/')
def index():
    return render_template('calendar.html')

@app.route('/api/appointments', methods=['GET'])
def get_appointments():
    try:
        with open(DATABASE_FILE, "r") as f:
            data = json.load(f)
        
        all_appointments = []
        for patient in data.get("patients", []):
            for appt in patient.get("appointments", []):
                # Format data for the frontend calendar
                all_appointments.append({
                    "title": f"Dr. {appt.get('doctor')} ({appt.get('reason')})",
                    "date": appt.get("date"), # Format should be YYYY-MM-DD for standard calendars
                    "time": appt.get("time")
                })
        return jsonify(all_appointments)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)  

'''