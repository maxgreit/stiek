from modules.selenium import EuurUrenRapportageDownloader
from modules.excel_processing import ExcelProcessor
from modules.database import DatabaseManager
from modules.type_mapping import TypeMapper
from modules.config import ConfigManager
from modules.env_tool import env_check
from modules.log import LoggerManager
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import logging
import time
import os

def main(start_datum_override=None, eind_datum_override=None):
    """
    Hoofdfunctie voor het ophalen en verwerken van urenrapportages uit E-Uur.
    """
    
    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    klant = "Stiek"
    script = "Uren rapportage"
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
    base_dir = os.getenv("BASE_DIR")
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
    excel_processor = ExcelProcessor(base_dir)
    type_mapper = TypeMapper()
    download_dir = os.path.join(base_dir, "stiek/file")
    uren_downloader = EuurUrenRapportageDownloader(base_dir, download_dir)
    
    # Verwerk aangepaste datums indien meegegeven
    start_datum_obj = None
    eind_datum_obj = None
    rapportage_type = 'standaard'

    if start_datum_override and eind_datum_override:
        try:
            start_datum_obj = datetime.strptime(start_datum_override, '%d-%m-%Y')
            eind_datum_obj = datetime.strptime(eind_datum_override, '%d-%m-%Y')
            rapportage_type = 'custom'
            logging.warning(f"LET OP: Aangepaste datums worden gebruikt: {start_datum_override} tot {eind_datum_override}")
        except ValueError:
            logging.error("Ongeldig datumformaat in hardcoded datums. Gebruik dd-mm-jjjj. Script wordt gestopt.")
            return
    
    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            
            if klantnaam == "Stiek":            
                logging.info(f"Start verwerking voor klant: {klantnaam}")

                # DatabaseManager initialiseren
                database_manager = DatabaseManager(klant_connection_string)

                # Urenrapportage ophalen
                logging.info("Start download van urenrapportage uit E-Uur")
                success = uren_downloader.download_urenrapportage(
                    euururl, 
                    euurusername, 
                    euurpassword, 
                    rapportage_type=rapportage_type,
                    start_datum=start_datum_obj,
                    eind_datum=eind_datum_obj
                )
                
                if success:
                    logging.info("Urenrapportage succesvol gedownload")
                    
                    # Zoek het gedownloade bestand op basis van een patroon
                    logging.info("Zoeken naar gedownload Excel-bestand...")
                    uren_file_dir = download_dir
                    bestands_patroon = r"Urenrapportage_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.xlsx"
                    filepath, datum_groepen = excel_processor.find_file_by_pattern(uren_file_dir, bestands_patroon)

                    if filepath and datum_groepen:
                        # Datums uit bestandsnaam halen
                        begindatum_str, einddatum_str = datum_groepen
                        begindatum = datetime.strptime(begindatum_str, "%Y-%m-%d").date()
                        einddatum = datetime.strptime(einddatum_str, "%Y-%m-%d").date()
                        logging.info(f"Urenrapportage gevonden voor periode: {begindatum} tot {einddatum}")

                        # DataFrame uit Excel maken met nieuwe ExcelProcessor
                        logging.info("Start Excel verwerking")
                        result = excel_processor.get_df_from_excel(custom_filepath=filepath)
                    
                        if result is not None:
                            df, file_path = result
                            logging.info(f"Excel bestand succesvol verwerkt: {file_path}")

                            # Kolommen type conversie met TypeMapper
                            logging.info("Start type conversie")
                            converted_df = type_mapper.apply_conversion(df, "UrenRapportage")
                            
                            if converted_df is not None:
                                logging.info("Type conversie succesvol voltooid")
                                
                                # Verwijder eerst de bestaande data voor de betreffende periode
                                date_column_name = "Datum"
                                if database_manager.delete_by_date_range("UrenRapportage", date_column_name, begindatum, einddatum):
                                
                                    # Voeg de nieuwe data toe
                                    database_manager.fill_table(converted_df, "UrenRapportage")
                                    logging.info("Data succesvol overgedragen naar database")
                                    
                                    # Excel verwijderen met ExcelProcessor
                                    excel_processor.delete_excel_file(file_path)
                                    logging.info("Excel bestand verwijderd")
                                else:
                                    logging.error("Verwijderen van bestaande data voor de periode is mislukt. Database niet bijgewerkt.")

                            else:
                                logging.error("Type conversie mislukt")
                        else:
                            logging.error("Excel verwerking mislukt")
                    else:
                        logging.error("Geen urenrapportage bestand gevonden dat overeenkomt met het patroon.")
                else:
                    logging.error("Urenrapportage download mislukt")
                        
    except Exception as e:
        logging.error(f"Script mislukt: {e}", exc_info=True)
        raise

    finally:
        # Eindtijd logging en cleanup
        logger_manager.end_log()
        logger_manager.close()

if __name__ == "__main__":
    main()
    """loop_start_date = datetime(2023, 1, 1)
    loop_end_date = datetime(2025, 6, 1)  # Stopt na de periode die in mei 2025 eindigt
    current_start_date = loop_start_date

    while current_start_date < loop_end_date:
        # Bepaal de einddatum van de periode van 2 maanden
        current_end_date = current_start_date + relativedelta(months=2) - relativedelta(days=1)

        # Formatteer de datums naar het dd-mm-jjjj formaat
        start_str = current_start_date.strftime('%d-%m-%Y')
        end_str = current_end_date.strftime('%d-%m-%Y')

        print(f"--- Start verwerking van periode: {start_str} tot {end_str} ---")
        main(start_datum_override=start_str, eind_datum_override=end_str)
        print(f"--- Verwerking van periode: {start_str} tot {end_str} VOLTOOID ---")
        
        # Ga naar het begin van de volgende periode van 2 maanden
        current_start_date += relativedelta(months=2)"""