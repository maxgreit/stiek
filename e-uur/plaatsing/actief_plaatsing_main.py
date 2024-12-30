from plaatsing_modules.excel_processing import get_df_from_excel, delete_excel_file
from plaatsing_modules.config import determine_script_id, create_connection_dict
from plaatsing_modules.selenium import actieve_plaatsing_bestand_opslaan
from plaatsing_modules.database import empty_and_fill_table
from plaatsing_modules.type_mapping import apply_conversion
from plaatsing_modules.table_mapping import apply_mapping
from plaatsing_modules.env_tool import env_check
from plaatsing_modules.log import log, end_log
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    klant = "Stiek"
    script = "E-Uur | Plaatsing Actief | Wekelijks"
    bron = 'Python'
    start_time = time.time()

    # Omgevingsvariabelen
    tabelnaam = "Plaatsing"
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
                
                # Plaatsing bestand opslaan
                actieve_plaatsing_bestand_opslaan(euururl, euurusername, euurpassword, greit_connection_string, klant, script, script_id, bron)    

                # DataFrame uit Excel maken
                df, file_path = get_df_from_excel(greit_connection_string, klant, script, script_id)

                # Kolommen type conversie
                converted_df = apply_conversion(df, tabelnaam, greit_connection_string, klant, bron, script, script_id)

                # Data transformatie
                transformed_df = apply_mapping(converted_df, tabelnaam, greit_connection_string, klant, bron, script, script_id)
                
                # Data overdracht
                empty_and_fill_table(transformed_df, tabelnaam, klant_connection_string, greit_connection_string, klant, bron, script, script_id)

                # Excel verwijderen
                delete_excel_file(file_path, greit_connection_string, klant, bron, script, script_id)
                
    except Exception as e:
        print(f"FOUTMELDING | Script mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Script mislukt: {e}", script, script_id, tabelnaam)

    # Eindtijd logging
    end_log(start_time, greit_connection_string, klant, bron, script, script_id)

if __name__ == "__main__":
    main()
