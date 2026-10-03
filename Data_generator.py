import hashlib
import logging
import math
import os
import random
import sys
import numpy as np
import time
from datetime import datetime
from statistics import NormalDist

log = logging.getLogger("DataGenerator")

def simulate_temp(temp_offset=0.0):
    """
    Simulation of body temperature based on the current time.

    temp_offset carries both the condition offset and the effect of any
    active episode (e.g. fever), as computed by SimulatedSensor.
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

    #Rounding to 2 decimal places to simulate typical sensor output
    return round(base_temp + noise + temp_offset, 2)


def simulate_heart_rate(temp, fever_mode=False, hr_baseline=72):
    # Simulate heart rate with a normal resting range of 60-100 bpm, with some random fluctuations
    now = datetime.now()
    current_hour = now.hour + now.minute / 60.0 

    # Mathematical formula for the HR HR = HRbaseline + HRcircadian + HRfewerimpact + HRnoise
    # HRbaseline: Average heart rate, between 60 and 100 bpm
    # HRcircadian: A circadian rhythm component, with a peak in the afternoon and a trough in the early morning
    # HRfewerimpact: An increase in heart rate due to fever, if active; for every degree of fever, we can add a certain number of bpm 
    # i.e., +12 bpm per degree above 37.5°C
    # HRnoise: Random fluctuations to simulate natural variability and sensor noise

    hr_circadian = 5 * np.sin((current_hour - 12) * np.pi / 12)  # Peak around 18:00
    temp_diff = max(0, temp - 37.5)  # Only consider the temperature difference if it's above 37.5°C

    hr_fever_impact = temp_diff * 12 if fever_mode else 0  # Adding 12 bpm per degree above 37.5°C, only if fever mode is active

    hr_noise = np.random.normal(0, 3) # Random noise with a standard deviation of 3 bpm

    heart_rate = hr_baseline + hr_circadian + hr_fever_impact + hr_noise

    return round(heart_rate)


def simulate_blood_pressure(heart_rate, K=0.5, sys_baseline=120):
    # We have to use the HR as an input
    HR_baseline = 72
    sys_noise = np.random.normal(0, 3)
    dia_noise = np.random.normal(0, 2)
    base_sys = sys_baseline

    SYS = base_sys + (K*(heart_rate - HR_baseline)) + sys_noise 
    DIA = (SYS*0.67) + dia_noise

    return round(SYS), round(DIA)


# Demo mode makes episodes frequent (one every few minutes per patient) and
# ignores time-of-day restrictions, so anomalies are visible during a short
# presentation. Set the environment variable SIMULATION_DEMO_MODE=false to use
# realistic frequencies and durations.
DEMO_MODE = os.getenv("SIMULATION_DEMO_MODE", "true").strip().lower() in ("1", "true", "yes")


# Personal offsets: each patient has a stable "signature" derived from its
# sensorID, so the same patient always gets the same baselines (across
# restarts, machines and Python versions).
PERSONAL_SYS_STD = 4.0     # mmHg, spread of the personal systolic offset
PERSONAL_TEMP_STD = 0.15   # °C, spread of the personal temperature offset
PERSONAL_Z_LIMIT = 2.0     # personal offsets are clamped to ±2 standard deviations


def personal_offset(sensor_id: str, trait: str) -> float:
    """
    Return a deterministic standard-normal value (mean 0, std 1) for the given
    sensor and trait, clamped to ±PERSONAL_Z_LIMIT.

    SHA-256 is used because, unlike Python's built-in hash(), its result is the
    same at every run and on every machine. The trait is part of the hashed
    text so that the offsets of different vital signs are independent.
    """
    digest = hashlib.sha256(f"{sensor_id}:{trait}".encode("utf-8")).digest()
    u = (int.from_bytes(digest[:8], "big") + 0.5) / 2**64   # uniform in (0, 1)
    z = NormalDist().inv_cdf(u)                             # standard normal
    return max(-PERSONAL_Z_LIMIT, min(PERSONAL_Z_LIMIT, z))


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
        "prob_demo": 1 / 100, "prob_real": 1 / 5000,
        "hours": (8, 22),
    },
    "palpitations_nocturnal": {
        "temp_delta": 0.0, "hr_delta": 40, "sys_delta": 0,
        "duration_demo": (6, 18), "duration_real": (12, 60),
        "prob_demo": 1 / 100, "prob_real": 1 / 6500,
        "hours": (22, 7),
    },
    "hypertensive_crisis": {
        "temp_delta": 0.0, "hr_delta": 0, "sys_delta": 45,
        "duration_demo": (12, 30), "duration_real": (120, 720),
        "prob_demo": 1 / 100, "prob_real": 1 / 120000,
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
    and temperature baselines. The baselines combine the condition profile
    with stable personal offsets derived from the sensorID (personal_offset),
    so a patient keeps the same "signature" across restarts and condition
    changes, while noise and episodes stay random.

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
        self.hr_baseline = profile["hr_mean"] + personal_offset(sensor_id, "hr") * profile["hr_std"]
        self.sys_baseline = profile["sys_baseline"] + personal_offset(sensor_id, "sys") * PERSONAL_SYS_STD
        self.temp_offset = profile["temp_offset"] + personal_offset(sensor_id, "temp") * PERSONAL_TEMP_STD
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

        # The fever increase is applied through temp_offset so it follows the
        # gradual episode shape.
        temperature = simulate_temp(temp_offset=self.temp_offset + spec.get("temp_delta", 0.0) * intensity)
        # fever_mode=is_fever keeps the coupling: heart rate rises with fever temperature
        heart_rate = simulate_heart_rate(temperature, fever_mode=is_fever,
                                         hr_baseline=self.hr_baseline + spec.get("hr_delta", 0) * intensity)
        sys, dia = simulate_blood_pressure(heart_rate, K=self.K,
                                           sys_baseline=self.sys_baseline + spec.get("sys_delta", 0) * intensity)

        return {
            "sensorID": self.sensor_id,
            "timestamp": datetime.now().isoformat(),
            "heart_rate": heart_rate,
            "body_temperature": temperature,
            "blood_pressure_systolic": sys,
            "blood_pressure_diastolic": dia,
        }


if __name__ == "__main__":
    # Console demo of the episode engine (no Catalog, no MQTT, no network).
    # Usage: python Data_generator.py [condition] [interval_seconds]
    condition = sys.argv[1] if len(sys.argv) > 1 else "healthy"
    if condition not in CONDITION_PROFILES:
        print(f"Unknown condition '{condition}'. Available conditions:")
        for name in sorted(CONDITION_PROFILES):
            print(f"  - {name}")
        sys.exit(1)

    try:
        interval_seconds = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    except ValueError:
        interval_seconds = -1
    if interval_seconds <= 0:
        print("interval_seconds must be a positive number")
        sys.exit(1)

    # Make the episode start/end log messages visible on the console
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

    print("-- Simulated sensor demo --")
    print(f"Condition: {condition}")
    print(f"Demo mode: {'on' if DEMO_MODE else 'off'}")
    print(f"Allowed episodes: {', '.join(CONDITION_EPISODES.get(condition, DEFAULT_EPISODES))}")
    print("Ctrl+C to stop.\n")

    sensor = SimulatedSensor("demo_sensor", condition)
    try:
        while True:
            r = sensor.read()
            timestamp = datetime.now().strftime("%H:%M:%S")
            marker = f"  <- {sensor.active_episode['name']}" if sensor.active_episode is not None else ""
            print(f"[{timestamp}] HR: {r['heart_rate']} bpm, Temp: {r['body_temperature']} °C, "
                  f"BP: {r['blood_pressure_systolic']}/{r['blood_pressure_diastolic']} mmHg{marker}")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nSimulation stopped")