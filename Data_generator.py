import numpy as np
import time
from datetime import datetime

def simulate_temp(is_fever=False):
    """
    Simulation of body temperature based on the current time.
    """
    # 1. Retrieving the current time
    now = datetime.now()
    current_hour = now.hour + now.minute / 60.0 #convert time to decimal, like 14.5 for 14:30
    
    # 2. Circadian model: Average 36.6°C, oscillations of 0.5°C
    # The temperature peaks around 18:00 and is lowest around 6:00 
    #sin has a period of 2pi, diving by 12 gives us a 24-hour cycle, -12 used to shift the peak to 18:00 instead of 12:00
    base_temp = 36.6 + 0.5 * np.sin((current_hour - 12) * np.pi / 12)

    # 3. Adding some Gaussian Noise, due to the sensor - mean 0, standard deviation 0.05)
    noise = np.random.normal(0, 0.05) 
    
    # 4. Fever simulation: If fever is active, add a fixed offset - e.g., +2 degrees
    fever_offset = np.random.normal(2, 0.3)  if is_fever else 0.0

    #Rounding to 2 decimal places to simulate typical sensor output
    return round(base_temp + noise + fever_offset, 2)

# Example usage: Simulate continuous sensor readings every second
print("-- Starting simulated temperature sensor readings --")
print("Ctrl + C to stop.\n")

def simulate_heart_rate(temp, high_hr_signal=False):
    #MAYBE A VARIABLE WITHE THE GENERAL STATUS OF THE PATIENT, LIKE STRESS LEVEL, ACTIVITY LEVEL, OR FEVER STATUS, TO INFLUENCE THE HEART RATE SIMULATION

    # Simulate heart rate with a normal resting range of 60-100 bpm, with some random fluctuations
    now = datetime.now()
    current_hour = now.hour + now.minute / 60.0 

    # Mathematical formula for the HR HR = HRbaseline + HRcircadian + HRfewerimpact + HRnoise
    # HRbaseline: Average heart rate, between 60 and 100 bpm
    # HRcircadian: A circadian rhythm component, with a peak in the afternoon and a trough in the early morning
    # HRfewerimpact: An increase in heart rate due to fever, if active; for every degree of fever, we can add a certain number of bpm 
    # e.g., +10 bpm per degree above 37°C
    # HRnoise: Random fluctuations to simulate natural variability and sensor noise

    hr_circadian = 5 * np.sin((current_hour - 12) * np.pi / 12)  # Peak around 18:00
    temp_diff = max(0, temp - 37.5)  # Only consider the temperature difference if it's above 37.5°C

    hr_fever_impact = temp_diff * 12 if fever_mode else 0  # Adding 10 bpm per degree of fever, only if fever mode is active

    hr_noise = np.random.normal(0, 3) # Random noise with a standard deviation of 3 bpm

    heart_rate = patient_temp_baseline + hr_circadian + hr_fever_impact + hr_noise

    if high_hr_signal:
        heart_rate += 20 # Adding a significant increase to test the alert system

    return round(heart_rate)

""" 
def simulate_blood_pressure():
    # Simulate blood pressure with a normal range of around 120/80 mmHg, with some random fluctuations
    systolic = np.random.normal(120, 10)  # Average systolic pressure with some variability
    diastolic = np.random.normal(80, 5)    # Average diastolic pressure with some variability
    return round(systolic), round(diastolic) """


if __name__ == "__main__":
    global patient_stress_level

    patient_temp_baseline = 36.6
    patient_temp_baseline = 72 + np.random.normal(0, 3) # Baseline heart rate with some variability, around 73 bpm

    try:
        fever_mode = False
        counter = 0
        fever_cnt= 0
        cnt_hr = 0
        stop = 0

        while stop < 2:
            # Every 10 readings, we activate the fever mode to simulate a febrile state
            
            if counter < 15:
                counter += 1
            elif fever_cnt < 5 and counter == 15:
                print("\n[ALERT] Fever mode active!\n")
                fever_mode = True
                fever_cnt += 1
            else: 
                print("\n[ALERT] Fever mode deactivated! Returning to normal state for the next 10 readings.\n")
                fever_mode = False
                counter = 0
                fever_cnt = 0
                stop += 1

            high_hr_signal = True if counter % 10 == 0 else False
            if high_hr_signal: print("[TEST] Injecting High HR Signal...")

            temp = simulate_temp(is_fever=fever_mode)
            freq= simulate_heart_rate(temp, high_hr_signal)
            timestamp = datetime.now().strftime("%H:%M:%S") #strftime to format the timestamp as HH:MM:SS
            
            print(f"[{timestamp}] Sensor lecture: {temp} °C, Heart Rate: {freq} bpm")
            
            # Waitinf 1 sec before the next reading
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nSimulation stopped")