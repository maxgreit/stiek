from uren_modules.excel_processing import get_df_from_excel, delete_excel_file
from uren_modules.config import determine_script_id, create_connection_dict
from uren_modules.selenium import urenrapportage_bestand_opslaan
from uren_modules.database import empty_and_fill_table
from uren_modules.type_mapping import apply_conversion
from uren_modules.table_mapping import apply_mapping
from uren_modules.log import end_log, setup_logging
from uren_modules.env_tool import env_check
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    klant = "Stiek"
    script = "Urenrapportage"
    bron = 'E-Uur'
    start_time = time.time()

    # Omgevingsvariabelen
    tabelnaam = "Urenregistratie"
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    euurusername = os.getenv('EUURUSERNAME')
    euurpassword = os.getenv('EUURPASSWORD')
    base_dir = os.getenv("BASE_DIR")
    euururl = os.getenv('EUURURL')
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
                # Bron
                bron = "E-Uur"
                
                # Urenrapportage bestand opslaan
                urenrapportage_bestand_opslaan(euururl, euurusername, euurpassword, base_dir)

                # DataFrame uit Excel maken
                df, detail, begindatum, einddatum, file_path = get_df_from_excel(base_dir)

                # Kolommen type conversie
                converted_df = apply_conversion(df, tabelnaam)

                # Data transformatie
                transformed_df = apply_mapping(converted_df, tabelnaam)
                
                # Data overdracht
                empty_and_fill_table(transformed_df, tabelnaam, klant_connection_string, detail, begindatum, einddatum)

                # Excel verwijderen
                delete_excel_file(file_path)
                
    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)

if __name__ == "__main__":
    main()
