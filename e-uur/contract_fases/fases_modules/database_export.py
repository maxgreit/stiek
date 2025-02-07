from fases_modules.database import connect_to_database
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

def create_phase_df(klant_connection_string):
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
            logging.info(f"Ophalen contract dictionary gelukt")
        else:
            # Foutmelding logging
            logging.error(f"Ophalen contract dictionary mislukt na meerdere pogingen")

    else:
        # Foutmelding logging
        logging.error(f"Verbinding met database mislukt na meerdere pogingen")

        return pd.DataFrame()
    
    # DataFrame maken van de contract_dict met de kolommen ID, Contracttype en Werknemer
    contract_df = pd.DataFrame.from_dict(contract_dict, orient='index', columns=['Werknemer', 'Contracttype', 'Actief']).reset_index()
    contract_df.rename(columns={'index': 'ID'}, inplace=True)
    
    return contract_df