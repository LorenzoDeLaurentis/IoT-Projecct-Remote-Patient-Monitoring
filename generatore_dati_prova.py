import numpy as np
import time
from datetime import datetime

def get_simulated_temp(is_fever=False):
    """
    Simula la temperatura corporea basata sull'ora attuale.
    """
    # 1. Recuperiamo l'ora attuale (es. 14.5 per le 14:30)
    now = datetime.now()
    current_hour = now.hour + now.minute / 60.0
    
    # 2. Modello Circadiano: Media 36.6°C, oscillazione di 0.5°C
    # Il picco è impostato verso le ore 18:00
    base_temp = 36.6 + 0.5 * np.sin((current_hour - 12) * np.pi / 12)
    
    # 3. Aggiunta di Rumore Gaussiano (media 0, deviazione standard 0.05)
    # Simula la piccola imprecisione del sensore fisico
    noise = np.random.normal(0, 0.05)
    
    # 4. Logica di Anomalia (Febbre)
    # Se attivata, aggiunge un offset fisso (es. +2 gradi)
    fever_offset = 2.0 if is_fever else 0.0
    
    return round(base_temp + noise + fever_offset, 2)

# --- TEST DI ESECUZIONE ---
print("--- Avvio Simulatore Sensore Biometrico (Lorenzo) ---")
print("Premi Ctrl+C per fermare.\n")

try:
    fever_mode = False
    counter = 0
    
    while True:
        # Ogni 10 letture simuliamo l'insorgenza della febbre per testare il sistema
        counter += 1
        if counter == 10:
            print("\n[ALERT] Simulazione stato febbrile attivata!")
            fever_mode = True
        
        temp = get_simulated_temp(is_fever=fever_mode)
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        print(f"[{timestamp}] Lettura Sensore: {temp} °C")
        
        # Aspettiamo 1 secondo tra una lettura e l'altra
        time.sleep(1)

except KeyboardInterrupt:
    print("\nSimulazione interrotta dall'utente.")