
from modules.database import DatabaseManager
from modules.type_mapping import TypeMapper
from modules.config import ConfigManager
from modules.env_tool import env_check
from modules.log import LoggerManager
import pandas as pd
import logging
import time
import os


def main():
    """
    Hoofdfunctie voor het ophalen en verwerken van contractfases uit de database.
    """
    
    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    klant = "Stiek"
    script = "Contract Fases"
    bron = 'E-Uur'
    start_time = time.time()
    tabelnaam = "Contract_fases"
    source_table = "Plaatsingen"

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # ConfigManager initialiseren
    config_manager = ConfigManager(greit_connection_string)

    # Script ID bepalen
    script_id = config_manager.determine_script_id()
    
    # Logger configuratie
    logger_config = {
        'conn_str': greit_connection_string,
        'customer': klant,
        'source': bron,
        'script': script,
        'script_id': script_id,
        'buffer_size': 10,
        'flush_interval': 30,
        'log_level': logging.INFO
    }
    
    # Initialiseer LoggerManager
    logger_manager = LoggerManager(logger_config)
    logger_manager.start_log()

    # Connectie dictionary maken
    connection_dict = config_manager.create_connection_dict()

    # Initialiseer de class-based modules
    type_mapper = TypeMapper()

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Stiek":
                logging.info(f"Start verwerking voor klant: {klantnaam}")

                # DatabaseManager initialiseren
                database_manager = DatabaseManager(klant_connection_string)

                # Dataframe ophalen uit database
                df = database_manager.fetch_contract_phase_data(source_table, only_active=True)
                if df is None or df.empty:
                    logging.error("Geen contractfases opgehaald uit de database.")
                    continue

                # Kolommen type conversie
                converted_df = type_mapper.apply_conversion(df, tabelnaam)
                if converted_df is None or converted_df.empty:
                    logging.error("Type conversie mislukt of geen data na conversie.")
                    continue
                
                # Dataframe toevoegen aan database
                success = database_manager.fill_table(converted_df, tabelnaam, batch_size=1000)
                if success:
                    logging.info("Data succesvol overgedragen naar database.")
                else:
                    logging.error("Data overdragen naar database mislukt.")


    except Exception as e:
        logging.error(f"Script mislukt: {e}")
        raise
    finally:
        # Eindtijd logging en cleanup
        logger_manager.end_log()
        logger_manager.close()

if __name__ == "__main__":
    main()
