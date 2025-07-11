from modules.selenium import EuurLoonPerPlaatsingDownloader
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
    Hoofdfunctie voor het ophalen en verwerken van looncomponenten uit E-Uur.
    """
    
    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    klant = "Stiek"
    script = "Loon"
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
    loon_downloader = EuurLoonPerPlaatsingDownloader()

    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            if klantnaam == "Stiek":
                logging.info(f"Start verwerking voor klant: {klantnaam}")

                # DatabaseManager initialiseren
                database_manager = DatabaseManager(klant_connection_string)

                # Ophalen plaatsing data (actief én inactief)
                plaatsing_df = database_manager.fetch_plaatsing_data("Plaatsingen")
                plaatsingen_lijst = plaatsing_df[['ID', 'Werknemer']].to_dict('records')

                # Ophalen bestaande loon-ID's
                bestaande_loon_ids = database_manager.fetch_loon_ids("Loon", "ID")

                # Filter plaatsingen waarvoor nog geen loondata is
                nieuwe_plaatsingen = [p for p in plaatsingen_lijst if p['ID'] not in bestaande_loon_ids]

                if not nieuwe_plaatsingen:
                    logging.info("Alle plaatsingen hebben al loondata. Geen actie nodig.")
                else:
                    # Download looncomponenten voor alleen de nieuwe plaatsingen
                    logging.info("Start download van looncomponenten uit E-Uur voor nieuwe plaatsingen")
                    looncomponenten_df = loon_downloader.download_loon_per_plaatsing(
                        euururl, euurusername, euurpassword, nieuwe_plaatsingen
                    )
                    
                    if looncomponenten_df is not None and not looncomponenten_df.empty:
                        # Directe verwerking van DataFrame
                        logging.info("Start type conversie")
                        converted_df = type_mapper.apply_conversion(looncomponenten_df, "Loon")
                        if converted_df is not None:
                            logging.info("Type conversie succesvol voltooid")
                            database_manager.clear_and_fill_table(converted_df, "Loon", id_column="ID", batch_size=1000)
                            logging.info("Data succesvol overgedragen naar database")
                        else:
                            logging.error("Type conversie mislukt")
                    else:
                        logging.error("Geen looncomponenten opgehaald voor nieuwe plaatsingen.")
    except Exception as e:
        logging.error(f"Script mislukt: {e}")
        raise
    finally:
        # Eindtijd logging en cleanup
        logger_manager.end_log()
        logger_manager.close()

if __name__ == "__main__":
    main()