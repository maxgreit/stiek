from modules.selenium import EuurOntbrekendeUrenDownloader
from modules.excel_processing import ExcelProcessor
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
    Hoofdfunctie voor het ophalen en verwerken van ontbrekende uren uit E-Uur.
    """
    
    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    klant = "Stiek"
    script = "OntbrekendeUren"
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
        'log_level': logging.WARNING
    }
    
    # Initialiseer LoggerManager
    logger_manager = LoggerManager(logger_config)
    logger_manager.start_log()

    # Connectie dictionary maken
    connection_dict = config_manager.create_connection_dict()

    # Initialiseer de class-based modules
    excel_processor = ExcelProcessor(base_dir, "stiek/file/Ontbrekende urenbriefjes.xlsx")
    type_mapper = TypeMapper()
    download_dir = os.path.join(base_dir, "stiek/file")
    ontbrekende_uren_downloader = EuurOntbrekendeUrenDownloader(base_dir, download_dir)
    
    try:
        for klantnaam, (klant_connection_string, type) in connection_dict.items():
            
            if klantnaam == "Stiek":            
                logging.info(f"Start verwerking voor klant: {klantnaam}")

                # DatabaseManager initialiseren
                database_manager = DatabaseManager(klant_connection_string)

                # Looncomponenten ophalen met nieuwe class-based downloader
                logging.info("Start download van looncomponenten uit E-Uur")
                success = ontbrekende_uren_downloader.download_ontbrekende_uren(euururl, euurusername, euurpassword)
                
                if success:
                    logging.info("Looncomponenten succesvol gedownload")
                    
                    # DataFrame uit Excel maken met nieuwe ExcelProcessor
                    logging.info("Start Excel verwerking")
                    result = excel_processor.get_df_from_excel()
                    
                    if result is not None:
                        df, file_path = result
                        logging.info(f"Excel bestand succesvol verwerkt: {file_path}")
                        
                    # Kolommen type conversie met TypeMapper
                        logging.info("Start type conversie")
                        converted_df = type_mapper.apply_conversion(df, "OntbrekendeUren")
                        
                        if converted_df is not None:
                            logging.info("Type conversie succesvol voltooid")
                            
                            # Excel verwijderen met ExcelProcessor
                            excel_processor.delete_excel_file(file_path)
                            logging.info("Excel bestand verwijderd")
                            
                            database_manager.clear_and_fill_table(converted_df, "OntbrekendeUren", batch_size=1000)
                            logging.info("Data succesvol overgedragen naar database")
                            
                        else:
                            logging.error("Type conversie mislukt")
                    else:
                        logging.error("Excel verwerking mislukt")
                else:
                    logging.error("Looncomponenten download mislukt")
                        
    except Exception as e:
        logging.error(f"Script mislukt: {e}")
        raise

    finally:
        # Eindtijd logging en cleanup
        logger_manager.end_log()
        logger_manager.close()

if __name__ == "__main__":
    main()