from fases_modules.database import connect_to_database
from fases_modules.log import log
import pandas as pd
import logging
import time

def fetch_current_contract_dict(cursor):
    # Voer de query uit om het hoogste ScriptID op te halen
    query = 'SELECT ID, Werknemer, Contracttype, Actief FROM Plaatsing'
    cursor.execute(query)
    
    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()

    # Extract de connectiestrings uit de resultaten
    contract_dict = {row[0]: (row[1], row[2], row[3]) for row in rows}  
    
    return contract_dict

def create_phase_df(klant_connection_string, greit_connection_string, klant, bron, script, script_id):
    max_retries = 3
    retry_delay = 5
    
    try:
        database_conn = connect_to_database(klant_connection_string)
    except Exception as e:
        logging.info(f"Verbinding met database mislukt, foutmelding: {e}")
    if database_conn:
        logging.info(f"Verbinding met database opnieuw geslaagd")
        cursor = database_conn.cursor()
        contract_dict = None
        for attempt in range(max_retries):
            try:
                contract_dict = fetch_current_contract_dict(cursor)
                if contract_dict:
                    break
            except Exception as e:
                time.sleep(retry_delay)
        database_conn.close()
        
        if contract_dict:

            # Start logging
            log(greit_connection_string, klant, bron, f"Ophalen contract dictionary gelukt", script, script_id)
        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Ophalen contract dictionary mislukt na meerdere pogingen")
            log(greit_connection_string, klant, bron, f"FOUTMELDING | Ophalen contract dictionary mislukt na meerdere pogingen", script, script_id)
    else:
        # Foutmelding logging
        print(f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen", script, script_id)
        return pd.DataFrame()
    
    # DataFrame maken van de contract_dict met de kolommen ID, Contracttype en Werknemer
    contract_df = pd.DataFrame.from_dict(contract_dict, orient='index', columns=['Werknemer', 'Contracttype', 'Actief']).reset_index()
    contract_df.rename(columns={'index': 'ID'}, inplace=True)
    
    return contract_df