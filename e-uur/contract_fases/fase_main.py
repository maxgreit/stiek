
from fases_modules.config import determine_script_id, create_connection_dict
from fases_modules.column_management import apply_transformation
from fases_modules.database_export import create_phase_df
from fases_modules.type_mapping import apply_conversion
from fases_modules.log import end_log, setup_logging
from fases_modules.database import fill_table
from fases_modules.env_tool import env_check
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    klant = "Stiek"
    script = "Contract Fases"
    bron = 'E-Uur'
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
    script_id = determine_script_id(greit_connection_string)
    
    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string)

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Stiek":      

                # Dataframe ophalen uit database
                df = create_phase_df(klant_connection_string)

                # Kolommen type conversie
                converted_df = apply_conversion(df, tabelnaam)

                # Voeg datumtijd kolom toe
                transformed_df = apply_transformation(converted_df)
                
                # Dataframe toevoegen aan database
                fill_table(transformed_df, tabelnaam, klant_connection_string)
                
    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)

if __name__ == "__main__":
    main()
