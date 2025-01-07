from feedback_modules.config import determine_script_id, create_connection_dict
from feedback_modules.database import write_to_database, clear_table
from feedback_modules.google_sheet import get_google_sheet_data
from feedback_modules.type_mapping import apply_conversion
from feedback_modules.mapping import map_columns
from feedback_modules.env_tool import env_check
from feedback_modules.log import log, end_log
import pandas as pd
import logging
import time
import os

def main():
    
    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    klant = "Stiek"
    script = "Google Sheet | Feedback | Dagelijks"
    bron = 'Python'
    start_time = time.time()
    
    # Omgevingsvariabelen
    credentials_file_path = os.getenv('CREDENTIALS_FILE_PATH')
    username = os.getenv('GEBRUIKERSNAAM')
    sheet_name = os.getenv('SHEET_NAME')
    sheet_url = os.getenv('SHEET_URL')
    database = os.getenv('DATABASE')
    password = os.getenv('PASSWORD')
    server = os.getenv('SERVER')
    tabelnaam = "Feedback"
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string, klant, bron, script)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string, klant, bron, script, script_id)

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Stiek": 

                df = get_google_sheet_data(sheet_url, sheet_name, credentials_file_path)

                if df.empty:
                    print("Geen data beschikbaar in de Google Sheet.")
                else:
                    print(df.head())
                
                # Mapping toepassen
                mapped_df = map_columns(df)
                
                # Kolommen type conversie
                converted_df = apply_conversion(mapped_df, tabelnaam, greit_connection_string, klant, bron, script, script_id)

                # Data leeghalen en toeschrijven
                clear_table(klant_connection_string, tabelnaam)
                write_to_database(converted_df, tabelnaam, klant_connection_string, batch_size=1000)
                print("Data is succesvol opgeslagen in de database.")
                
    except Exception as e:
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Script mislukt: {e}", script, script_id, tabelnaam)
    
    end_log(start_time, greit_connection_string, klant, bron, script, script_id)

if __name__ == "__main__":
    main()
        