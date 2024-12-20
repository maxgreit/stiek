import sys
import os
import logging
import azure.functions as func
import pyodbc
import time
from datetime import datetime, timedelta

# Voeg paden toe aan sys.path voor dagelijkse, maandelijkse en werknemers scripts
sys.path.append(os.path.join(os.path.dirname(__file__), 'cost_management'))
from cost_management.main import main as cost_management_main

# Voeg paden toe aan sys.path voor dagelijkse, maandelijkse en werknemers scripts
sys.path.append(os.path.join(os.path.dirname(__file__), 'looncomponenten'))
from looncomponenten.actief_main import main as looncomponenten_actief_main

# Configureer logging
logging.basicConfig(level=logging.INFO)

# Azure Function App object
app = func.FunctionApp()

# Dagelijkse run (elke dag om 4 uur 's nachts)
@app.function_name(name="StiekCostManagement")
@app.schedule(schedule="0 40 4 * * *", arg_name="stiekCostTimer", run_on_startup=False, use_monitor=True)
def StiekCostManagement(stiekCostTimer: func.TimerRequest) -> None:
    
    run_script(cost_management_main, "Stiek Cost Management")

# Wekelijkse run
@app.function_name(name="StiekLooncomponenten")
@app.schedule(schedule="0 0 12 * * 1", arg_name="stiekLoonTimer", run_on_startup=False, use_monitor=True)
def StiekLooncomponenten(stiekLoonTimer: func.TimerRequest) -> None:
    
    run_script(looncomponenten_actief_main, "Stiek Looncomponenten")

def run_script(script_main_function, script_type):
    try:
        logging.info(f"Start {script_type} script")
        
        # Begin tijdstip van het script
        start_time = time.time()
        
        # Log het begin van de hoofdfunctie
        logging.info(f"{script_type} | Uitvoeren van de main-functie")
        script_main_function()

        # Log het succesvol afronden van de main-functie
        logging.info(f"{script_type} | Main-functie succesvol afgerond")

        # Eind tijdstip van het script
        end_time = time.time()
        duration = timedelta(seconds=(end_time - start_time))
        
        # Log de totale looptijd van het script
        logging.info(f"{script_type} script is succesvol uitgevoerd in {str(duration)}")
        
    except Exception as e:
        # Log specifieke fouten en het type script waar de fout optrad
        logging.error(f"FOUTMELDING | {script_type} script is mislukt: {e}")
        logging.info(f"{script_type} | Script geëindigd met fouten")
    finally:
        # Dit deel wordt altijd uitgevoerd, ook als er een fout is
        logging.info(f"Einde van {script_type} script")