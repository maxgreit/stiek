
from looncomponenten_modules.database import empty_and_fill_table, fetch_looncomponenten_data_from_table, fetch_plaatsing_data_from_table
from looncomponenten_modules.config import determine_script_id, create_connection_dict
from looncomponenten_modules.inactief_selenium import looncomponenten_ophalen
from looncomponenten_modules.type_mapping import apply_conversion
from looncomponenten_modules.env_tool import env_check
from looncomponenten_modules.log import log, end_log
import logging
import time
import os

def main():
    
    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    klant = "Stiek"
    script = "Looncomponenten Inactief"
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
    script_id = determine_script_id(greit_connection_string, klant, bron, script)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string, klant, bron, script, script_id)

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            
            if klantnaam == "Stiek":            
                # Start logging
                bron = 'E-Uur'

                # Ophalen plaatsing data
                plaatsing_df = fetch_plaatsing_data_from_table(klant_connection_string, "Plaatsing", greit_connection_string, klant, bron, script, script_id)

                # Ophalen huidige looncomponenten data
                looncomponenten_df = fetch_looncomponenten_data_from_table(klant_connection_string, "Looncomponenten", greit_connection_string, klant, bron, script, script_id)

                # Houd alleen IDs over die niet in looncomponenten data zit
                df = plaatsing_df[~plaatsing_df['ID'].isin(looncomponenten_df['ID'])]

                for id, werknemer, actief in zip(df['ID'], df['Werknemer'], df['Actief']):
                    if actief == False:
                        
                        # Looncomponenten ophalen
                        looncomponent_df = looncomponenten_ophalen(euururl, euurusername, euurpassword, id, werknemer, greit_connection_string, klant, script, script_id, bron)

                        # Kolommen type conversie
                        converted_df = apply_conversion(looncomponent_df, greit_connection_string, klant, bron, script, script_id)
                        
                        # Data overdracht
                        empty_and_fill_table(converted_df, "Looncompnenten", id, klant_connection_string, greit_connection_string, klant, bron, script, script_id)

                    else:
                        print(f"Looncomponenten ophalen voor {id} {werknemer} is actief")
                        log(greit_connection_string, klant, bron, f"Looncomponenten ophalen voor {id} {werknemer} is actief", script, script_id)

    except Exception as e:
        print(f"Er is een fout opgetreden: {e}")
        log(greit_connection_string, klant, bron, f"Er is een fout opgetreden: {e}", script, script_id)

    # Eindtijd logging
    end_log(start_time, greit_connection_string, klant, bron, script, script_id)

if __name__ == "__main__":
    main()