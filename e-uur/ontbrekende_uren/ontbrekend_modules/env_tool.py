from dotenv import load_dotenv
import logging
import os

def env_check(base_dir):
    env_path = os.path.join(base_dir, '.env')
    
    if os.path.exists(env_path):
            load_dotenv()
            print("Lokaal draaien: .env bestand gevonden en geladen.")
            logging.info("Lokaal draaien: .env bestand gevonden en geladen.")
    else:
        logging.info("Draaien in productieomgeving (Azure): .env bestand niet gevonden.")
        print("Draaien in productieomgeving (Azure): .env bestand niet gevonden.")