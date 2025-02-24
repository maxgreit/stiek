from ontbrekend_modules.excel_processing import get_df_from_excel, delete_excel_file
from ontbrekend_modules.config import determine_script_id, create_connection_dict
from ontbrekend_modules.selenium import ontbrekende_uren_bestand_opslaan
from ontbrekend_modules.column_selection import column_selection
from ontbrekend_modules.database import empty_and_fill_table
from ontbrekend_modules.type_mapping import apply_conversion
from ontbrekend_modules.table_mapping import apply_mapping
from ontbrekend_modules.log import end_log, setup_logging
from ontbrekend_modules.env_tool import env_check
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    klant = "Stiek"
    script = "Ontbrekende Uren"
    bron = 'E-Uur'
    start_time = time.time()

    # Omgevingsvariabelen
    tabelnaam = "Ontbrekende_uren"
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
    db_handler = setup_logging(greit_connection_string, klant, bron, script, script_id)

    # Connectie dictionary maken
    connection_dict = create_connection_dict(greit_connection_string)

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Stiek":      
                # Start logging
                bron = 'E-Uur'

                # Ontbrekende uren opslaan
                ontbrekende_uren_bestand_opslaan(euururl, euurusername, euurpassword, base_dir)

                # DataFrame uit Excel maken
                df, file_path = get_df_from_excel(base_dir)

                # Kolommen type conversie
                converted_df = apply_conversion(df, tabelnaam)

                # Data transformatie
                transformed_df = apply_mapping(converted_df, tabelnaam)
                
                # Selecteer specifieke kolommen
                selected_df = column_selection(transformed_df)
                
                # Data overdracht
                empty_and_fill_table(selected_df, tabelnaam, klant_connection_string)

                # Excel verwijderen
                delete_excel_file(file_path)
                
    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)
    
    # Logging afhandelen
    db_handler.flush_logs()
    logging.shutdown()

if __name__ == "__main__":
    main()
