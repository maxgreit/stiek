from looncomponenten_modules.database import empty_and_fill_table, fetch_looncomponenten_data_from_table, fetch_plaatsing_data_from_table
from looncomponenten_modules.config import determine_script_id, create_connection_dict
from looncomponenten_modules.actief_selenium import looncomponenten_ophalen
from looncomponenten_modules.type_mapping import apply_conversion
from looncomponenten_modules.log import end_log, setup_logging
from looncomponenten_modules.env_tool import env_check
import logging
import time
import os

def main():
    
    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    klant = "Stiek"
    script = "Looncomponenten Actief"
    bron = 'E-Uur'
    start_time = time.time()

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    euurusername = os.getenv('EUURUSERNAME')
    euurpassword = os.getenv('EUURPASSWORD')
    euururl = os.getenv('EUURURL')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string)
    
    # Set up logging (met database logging)
    db_handler = setup_logging(greit_connection_string, klant, bron, script, script_id)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string)

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            
            if klantnaam == "Stiek":            
                # Start logging
                bron = 'E-Uur'

                # Ophalen plaatsing data
                plaatsing_df = fetch_plaatsing_data_from_table(klant_connection_string, "Plaatsing")

                for id, werknemer, actief in zip(plaatsing_df['ID'], plaatsing_df['Werknemer'], plaatsing_df['Actief']):
                    if actief == True:
                        # Looncomponenten ophalen
                        looncomponent_df = looncomponenten_ophalen(euururl, euurusername, euurpassword, id, werknemer)

                        # Kolommen type conversie
                        converted_df = apply_conversion(looncomponent_df)
                        
                        # Data overdracht
                        empty_and_fill_table(converted_df, "Looncomponenten", id, klant_connection_string)
                        
                    else:
                        logging.error(f"Looncomponenten ophalen voor {id} {werknemer} is niet actief")
                        
    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)
    
    # Logging afhandelen
    db_handler.flush_logs()
    logging.shutdown()

if __name__ == "__main__":
    main()