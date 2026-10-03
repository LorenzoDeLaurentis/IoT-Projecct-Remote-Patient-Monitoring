import logging
import math
import os
import random
import numpy as np
import time
from datetime import datetime

log = logging.getLogger("DataGenerator")

def simulate_temp(is_fever=False, temp_offset=0.0):
    """
    Simulation of body temperature based on the current time.
    """
    # Retrieving the current time
    now = datetime.now()
    current_hour = now.hour + now.minute / 60.0 #convert time to decimal, like 14.5 for 14:30
    
    # Circadian model: Average 36.6°C, oscillations of 0.5°C
    # The temperature peaks around 18:00 and is lowest around 6:00 
    #sin has a period of 2pi, diving by 12 gives us a 24-hour cycle, -12 used to shift the peak to 18:00 instead of 12:00
    base_temp = 36.6 + 0.5 * np.sin((current_hour - 12) * np.pi / 12)

    # Adding some Gaussian Noise, due to the sensor - mean 0, standard deviation 0.05)
    noise = np.random.normal(0, 0.05) 
    
    # Fever simulation: If fever is active, add a fixed offset - e.g., +2 degrees
    fever_offset = np.random.normal(2, 0.3)  if is_fever else 0.0

    #Rounding to 2 decimal places to simulate typical sensor output
    return round(base_temp + noise + fever_offset + temp_offset, 2)


def simulate_heart_rate(temp, high_hr_signal=False, fever_mode=False, hr_baseline=72):
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

    heart_rate = hr_baseline + hr_circadian + hr_fever_impact + hr_noise

    if high_hr_signal:
        heart_rate += 20 # Adding a significant increase to test the alert system

    return round(heart_rate)


def simulate_blood_pressure(heart_rate, high_hr_signal=False, K=0.5, sys_baseline=120):
    # We have to use the HR as an input
    HR_baseline = 72
    sys_noise = np.random.normal(0, 3)
    dia_noise = np.random.normal(0, 2)
    base_sys = sys_baseline

    if high_hr_signal:
        base_sys += 15  # Pressure raises if HR is high
        
    SYS = base_sys + (K*(heart_rate - HR_baseline)) + sys_noise 
    DIA = (SYS*0.67) + dia_noise

    return round(SYS), round(DIA)


# Demo mode makes episodes frequent (one every few minutes per patient) and
# ignores time-of-day restrictions, so anomalies are visible during a short
# presentation. Set the environment variable SIMULATION_DEMO_MODE=false to use
# realistic frequencies and durations.
DEMO_MODE = os.getenv("SIMULATION_DEMO_MODE", "true").strip().lower() in ("1", "true", "yes")


# Baseline profiles: 4 "pure" single-variable conditions (useful to test one
# alert in isolation) plus 4 named diseases that shift more than one vital at
# once, mirroring real comorbidity patterns. Diabetes has no direct vital-sign
# signature in this system (no glucose sensor), so it is approximated by the
# hypertension + mild tachycardia pattern common in decompensated type-2 diabetes.
# "stress_palpitations" and "anxiety_palpitations" have a normal baseline and
# differ from "healthy" only in the episodes they can experience (see CONDITION_EPISODES).
CONDITION_PROFILES = {
    "healthy":              {"hr_mean": 72,  "hr_std": 3, "sys_baseline": 120, "temp_offset": 0.0},
    "tachycardia":          {"hr_mean": 95,  "hr_std": 5, "sys_baseline": 120, "temp_offset": 0.0},
    "bradycardia":          {"hr_mean": 50,  "hr_std": 3, "sys_baseline": 120, "temp_offset": 0.0},
    "hypertension":         {"hr_mean": 72,  "hr_std": 3, "sys_baseline": 150, "temp_offset": 0.0},
    "hypotension":          {"hr_mean": 72,  "hr_std": 3, "sys_baseline": 80,  "temp_offset": 0.0},
    "hyperthyroidism":      {"hr_mean": 102, "hr_std": 5, "sys_baseline": 135, "temp_offset": 0.3},
    "hypothyroidism":       {"hr_mean": 52,  "hr_std": 3, "sys_baseline": 120, "temp_offset": -0.5},
    "diabetes":             {"hr_mean": 85,  "hr_std": 4, "sys_baseline": 145, "temp_offset": 0.0},
    "renal_failure":        {"hr_mean": 90,  "hr_std": 4, "sys_baseline": 155, "temp_offset": 0.0},
    "stress_palpitations":  {"hr_mean": 72,  "hr_std": 3, "sys_baseline": 120, "temp_offset": 0.0},
    "anxiety_palpitations": {"hr_mean": 72,  "hr_std": 3, "sys_baseline": 120, "temp_offset": 0.0},
}

# Episode types. Deltas are the PEAK change applied on top of the patient's
# baseline. Durations are in number of readings (one reading every few seconds).
# prob_* is the probability, at each reading, that the episode starts.
# hours (optional) restricts when the episode can start: (start_hour, end_hour),
# wrapping around midnight if start > end. Ignored in demo mode.
EPISODE_TYPES = {
    "fever": {
        "temp_delta": 2.0, "hr_delta": 0, "sys_delta": 0,
        "duration_demo": (24, 48), "duration_real": (720, 2880),
        "prob_demo": 1 / 400, "prob_real": 1 / 500000,
        "hours": None,
    },
    "palpitations_daytime": {
        "temp_delta": 0.0, "hr_delta": 40, "sys_delta": 0,
        "duration_demo": (6, 18), "duration_real": (12, 60),
        "prob_demo": 1 / 40, "prob_real": 1 / 5000,
        "hours": (8, 22),
    },
    "palpitations_nocturnal": {
        "temp_delta": 0.0, "hr_delta": 40, "sys_delta": 0,
        "duration_demo": (6, 18), "duration_real": (12, 60),
        "prob_demo": 1 / 40, "prob_real": 1 / 6500,
        "hours": (22, 7),
    },
    "hypertensive_crisis": {
        "temp_delta": 0.0, "hr_delta": 0, "sys_delta": 45,
        "duration_demo": (12, 30), "duration_real": (120, 720),
        "prob_demo": 1 / 50, "prob_real": 1 / 120000,
        "hours": None,
    },
}

# Episodes each condition can experience. Conditions not listed here can only
# have a fever.
CONDITION_EPISODES = {
    "stress_palpitations":  ["fever", "palpitations_daytime"],
    "anxiety_palpitations": ["fever", "palpitations_nocturnal"],
    "renal_failure":        ["fever", "hypertensive_crisis"],
}
DEFAULT_EPISODES = ["fever"]


class SimulatedSensor:
    """
    Stateful wrapper around simulate_temp/simulate_heart_rate/simulate_blood_pressure.

    The `condition` parameter selects a constant baseline profile from
    CONDITION_PROFILES that shapes the sensor's heart rate, blood pressure,
    and temperature baselines.

    On top of the baseline, the sensor can randomly experience episodes from
    EPISODE_TYPES, limited to those allowed for its condition by
    CONDITION_EPISODES (DEFAULT_EPISODES otherwise). Each episode has a random
    duration and intensity and follows a sine shape: values rise gradually,
    peak in the middle and return to baseline at the end. Episodes are drawn
    independently for each sensor, so patients are never synchronized.

    DEMO_MODE controls episode frequency and duration, and whether the
    time-of-day restrictions of an episode type are applied.
    """

    def __init__(self, sensor_id, condition="healthy"):
        self.sensor_id = sensor_id
        self.condition = condition
        profile = CONDITION_PROFILES.get(condition, CONDITION_PROFILES["healthy"])
        self.hr_baseline = profile["hr_mean"] + np.random.normal(0, profile["hr_std"])
        self.sys_baseline = profile["sys_baseline"]
        self.temp_offset = profile["temp_offset"]
        self.K = 0.5
        self.allowed_episodes = CONDITION_EPISODES.get(condition, DEFAULT_EPISODES)
        self.active_episode = None  # dict with "name", "tick", "duration", "amplitude" when active

    def _hour_allowed(self, hours):
        if DEMO_MODE or hours is None:
            return True
        h = datetime.now().hour
        start, end = hours
        if start <= end:
            return start <= h < end
        # Window wraps around midnight
        return h >= start or h < end

    def _maybe_start_episode(self):
        candidates = list(self.allowed_episodes)
        random.shuffle(candidates)
        for name in candidates:
            spec = EPISODE_TYPES[name]
            if not self._hour_allowed(spec["hours"]):
                continue
            prob = spec["prob_demo"] if DEMO_MODE else spec["prob_real"]
            if random.random() < prob:
                duration = random.randint(*spec["duration_demo" if DEMO_MODE else "duration_real"])
                amplitude = random.uniform(0.5, 1.2)
                self.active_episode = {"name": name, "tick": 0, "duration": duration, "amplitude": amplitude}
                log.info("Sensor %s (%s): episode '%s' started (duration %d readings, intensity %.0f%%)",
                         self.sensor_id, self.condition, name, duration, amplitude * 100)
                break

    def _episode_effect(self):
        """Return (name, intensity) for the current reading and advance the episode."""
        if self.active_episode is None:
            return None, 0.0
        e = self.active_episode
        # Sine shape: gradual rise, peak mid-episode, back to baseline at the end
        intensity = e["amplitude"] * math.sin(math.pi * (e["tick"] + 0.5) / e["duration"])
        e["tick"] += 1
        name = e["name"]
        if e["tick"] >= e["duration"]:
            log.info("Sensor %s (%s): episode '%s' ended", self.sensor_id, self.condition, name)
            self.active_episode = None
        return name, intensity

    def read(self):
        if self.active_episode is None:
            self._maybe_start_episode()

        name, intensity = self._episode_effect()
        spec = EPISODE_TYPES.get(name, {})
        is_fever = name == "fever"

        # The fever increase goes through temp_offset (is_fever=False) so it
        # follows the gradual episode shape instead of a fixed random offset.
        temperature = simulate_temp(is_fever=False,
                                    temp_offset=self.temp_offset + spec.get("temp_delta", 0.0) * intensity)
        # fever_mode=is_fever keeps the coupling: heart rate rises with fever temperature
        heart_rate = simulate_heart_rate(temperature, False, is_fever,
                                         self.hr_baseline + spec.get("hr_delta", 0) * intensity)
        sys, dia = simulate_blood_pressure(heart_rate, False, self.K,
                                           self.sys_baseline + spec.get("sys_delta", 0) * intensity)

        return {
            "sensorID": self.sensor_id,
            "timestamp": datetime.now().isoformat(),
            "heart_rate": heart_rate,
            "body_temperature": temperature,
            "blood_pressure_systolic": sys,
            "blood_pressure_diastolic": dia,
        }


if __name__ == "__main__":
    #global patient_stress_level

    patient_temp_baseline = 72 + np.random.normal(0, 3) # Baseline heart rate with some variability, around 73 bpm

    try:
        fever_mode = False
        counter = 0
        fever_cnt= 0
        K = 0.5

        print("-- Starting simulated sensor readings --")
        print("Ctrl + C to stop.\n")

        while True:
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

            high_hr_signal = True if counter % 10 == 0 else False
            if high_hr_signal: print("[TEST] Injecting High HR Signal...")

            timestamp = datetime.now().strftime("%H:%M:%S") #strftime to format the timestamp as HH:MM:SS
            temperature = simulate_temp(is_fever=fever_mode)
            heart_rate = simulate_heart_rate(temperature, high_hr_signal, fever_mode, patient_temp_baseline)
            sys, dia = simulate_blood_pressure(heart_rate, high_hr_signal, K)
            print(f"[{timestamp}] Sensor lecture: {temperature} °C, Heart Rate: {heart_rate} bpm, Blood Pressure: {sys} / {dia} mmHg")
            
            # Waitinf 1 sec before the next reading
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nSimulation stopped")