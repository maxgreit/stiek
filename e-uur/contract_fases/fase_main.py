
from fases_modules.config import determine_script_id, create_connection_dict
from fases_modules.column_management import apply_transformation
from fases_modules.database_export import create_phase_df
from fases_modules.type_mapping import apply_conversion
from fases_modules.database import fill_table
from fases_modules.env_tool import env_check
from fases_modules.log import log, end_log
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    klant = "Stiek"
    script = "E-Uur | Contract Fases | Dagelijks"
    bron = 'Python'
    start_time = time.time()

    # Omgevingsvariabelen
    tabelnaam = "Contract_fases"
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string, klant, bron, script)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string, klant, bron, script, script_id)

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Stiek":      

                # Dataframe ophalen uit database
                df = create_phase_df(klant_connection_string, greit_connection_string, klant, bron, script, script_id)

                # Kolommen type conversie
                converted_df = apply_conversion(df, tabelnaam, greit_connection_string, klant, bron, script, script_id)

                # Voeg datumtijd kolom toe
                transformed_df = apply_transformation(converted_df)
                
                # Dataframe toevoegen aan database
                fill_table(transformed_df, tabelnaam, klant_connection_string, greit_connection_string, klant, bron, script, script_id)
                
    except Exception as e:
        print(f"FOUTMELDING | Script mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Script mislukt: {e}", script, script_id, tabelnaam)

    # Eindtijd logging
    end_log(start_time, greit_connection_string, klant, bron, script, script_id)

if __name__ == "__main__":
    main()
