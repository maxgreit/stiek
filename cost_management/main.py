import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from cost_management_modules.access_token import get_access_token
from cost_management_modules.database import connect_to_database, write_to_database, clear_table
from cost_management_modules.log import log
from cost_management_modules.config import fetch_script_id, fetch_all_connection_strings
from cost_management_modules.type_mapping import convert_column_types, kosten_typing
from cost_management_modules.table_mapping import transform_columns, kosten
import pandas as pd
import time
import logging

def main():

    if os.path.exists("/Users/maxrood/werk/greit/klanten/stiek/.env"):
        load_dotenv()
        print("Lokaal draaien: .env bestand gevonden en geladen.")
        logging.info("Lokaal draaien: .env bestand gevonden en geladen.")
    else:
        logging.info("Draaien in productieomgeving (Azure): .env bestand niet gevonden.")

    # Logging configuratie
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Definiëren van script
    script = "Cost Management"
    klant = "Stiek"

    # Leg de starttijd vast
    start_time = time.time()

    # Aantal retries instellen
    max_retries = 3
    retry_delay = 5

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'

    # Verbindingsstring
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    logging.info(f"Verbindingsstring: {greit_connection_string}")

    try:
        database_conn = connect_to_database(greit_connection_string)
    except Exception as e:
        print(f"Databaseverbinding is niet tot stand gekomen. Foutmelding: {e}")
        exit(1)

    # Ophalen van script_id
    if database_conn:
        try:
            cursor = database_conn.cursor()
            logging.info("Probeer script_id op te halen uit de database...")
            latest_script_id = fetch_script_id(cursor)

            if latest_script_id is not None:
                script_id = latest_script_id + 1
                logging.info(f"Script_id succesvol opgehaald: {latest_script_id}. Nieuw script_id: {script_id}")
            else:
                logging.error("Geen script_id gevonden in de database. Controleer of de tabel gegevens bevat.")
                database_conn.close()
                exit(1)

        except Exception as e:
            logging.error(f"Fout bij het ophalen van script_id: {e}")
            database_conn.close()
            exit(1)
    else:
        logging.error("Databaseverbinding is niet tot stand gekomen. Script wordt beëindigd.")
        exit(1)
    
    # Start logging
    bron = 'python'
    try:
        log(greit_connection_string, klant, bron, f"Script gestart", script, script_id)
        logging.info("Script gestart logging succesvol.")
    except Exception as e:
        logging.error(f"FOUTMELDING | Script gestart logging mislukt: {e}")
        exit(1)

    # Verbinding maken met database
    try:
        database_conn = connect_to_database(greit_connection_string)
    except Exception as e:
        print(f"FOUTMELDING | Verbinding met database mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Verbinding met database mislukt: {e}", script, script_id)
        logging.error(f"FOUTMELDING | Verbinding met database mislukt: {e}")
        return
    if database_conn:
        cursor = database_conn.cursor()
        connection_dict = None
        for attempt in range(max_retries):
            try:
                connection_dict = fetch_all_connection_strings(cursor)
                if connection_dict:
                    break
            except Exception as e:
                time.sleep(retry_delay)
        database_conn.close()
        if connection_dict:

            # Start logging
            log(greit_connection_string, klant, bron, f"Ophalen connectiestrings gestart", script, script_id)
        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen")
            log(greit_connection_string, klant, bron, f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen", script, script_id)
    else:
        # Foutmelding logging
        print(f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen", script, script_id)

    logging.info(f"Connection_dict: {connection_dict}")

    # Genereer nieuw access token
    print("Genereer nieuw access token")
    log(greit_connection_string, klant, bron, "Genereer nieuw access token", script, script_id)
    try:
        bearer_token = get_access_token()
    except Exception as e:
        print(f"Fout bij het genereren van een nieuw access token: {e}")
        log(greit_connection_string, klant, bron, f"Fout bij het genereren van een nieuw access token: {e}", script, script_id)
        exit(1)

    # Vul deze variabelen in met je eigen gegevens
    subscription_id = os.getenv('SUBSCRIPTION_ID')

    # API URL voor het opvragen van kosten
    cost_url = f'https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01'

    # Bereken de startdatum als de eerste dag van de vorige maand
    start_date = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
    # Bereken de einddatum als de huidige datum
    end_date = datetime.now().strftime("%Y-%m-%d")

    # JSON-body voor het opvragen van de kosten voor de huidige maand per dag
    cost_body = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date,
            "to": end_date
        },
        "dataset": {
            "granularity": "Daily",
            "aggregation": {
                "totalCost": {
                    "name": "PreTaxCost",
                    "function": "Sum"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": "ServiceName"
                }
            ]
        }
    }

    # Headers voor het verzoek
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }

    # Maak een POST-verzoek om de kosten op te vragen
    try:
        print("Maak een POST-verzoek om de kosten op te vragen")
        logging.info("Maak een POST-verzoek om de kosten op te vragen")
        log(greit_connection_string, klant, bron, "Maak een POST-verzoek om de kosten op te vragen", script, script_id)
        response = requests.post(cost_url, json=cost_body, headers=headers)
    except Exception as e:
        print(f"Fout bij het opvragen van de kosten: {e}")
        logging.error(f"Fout bij het opvragen van de kosten: {e}")
        log(greit_connection_string, klant, bron, f"Fout bij het opvragen van de kosten: {e}", script, script_id)
        exit(1)

    # Controleer of de gegevens succesvol zijn opgevraagd
    if response.status_code == 200:
        print('Kosteninformatie ontvangen')
        logging.info("Kosteninformatie ontvangen")
        log(greit_connection_string, klant, bron, "Kosteninformatie ontvangen", script, script_id)
        json_data = response.json()
    else:
        print('Fout bij het ophalen van kosteninformatie:', response.status_code)
        print(response.text)
        logging.error(f"Fout bij het ophalen van kosteninformatie: {response.status_code}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Fout bij het ophalen van kosteninformatie: {response.status_code}", script, script_id)
        exit(1)

    # Maak een DataFrame van de gegevens
    column_names = [col["name"] for col in json_data["properties"]["columns"]]
    rows = json_data["properties"]["rows"]
    df = pd.DataFrame(rows, columns=column_names)

    # Voeg Klant kolom toe
    df['Klant'] = klant

    # Kolom mapping
    column_mapping = {
        'Kosten': kosten,
    }

    # Tabel mapping
    for tabel, mapping in column_mapping.items():
            # Transformeer de kolommen
            try:
                df = transform_columns(df, mapping)
                print(f"Kolommen getransformeerd")
                logging.info("Kolommen getransformeerd")
                log(greit_connection_string, klant, bron, f"Mapping van kolommen correct uitgevoerd", script, script_id, tabel)
            except Exception as e:
                print(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                logging.error(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen transformeren mislukt: {e}", script, script_id, tabel)
                return

    # Kolom typing
    column_typing = {
        'Kosten': kosten_typing,
    }

    # Update typing van kolommen
    for tabel, typing in column_typing.items():
            # Type conversie
            try:
                df = convert_column_types(df, typing)
                print(f"Kolommen type conversie")
                logging.info("Kolommen type conversie")
                log(greit_connection_string, klant, bron, f"Kolommen type conversie correct uitgevoerd", script, script_id, tabel)
            except Exception as e:
                print(f"FOUTMELDING | Kolommen type conversie mislukt: {e}")
                logging.error(f"FOUTMELDING | Kolommen type conversie mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen type conversie mislukt: {e}", script, script_id, tabel)
                return
    
    # Tabel leeg maken
    try:
        clear_table(greit_connection_string, tabel, klant)
        print(f"Tabel {tabel} leeg gemaakt vanaf begin van deze maand")
        logging.info(f"Tabel {tabel} leeg gemaakt vanaf begin van deze maand")
        log(greit_connection_string, klant, bron, f"Tabel {tabel} leeg gemaakt vanaf begin van deze maand", script, script_id, tabel)
    except Exception as e:
        print(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")
        logging.error(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel leeg maken mislukt: {e}", script, script_id, tabel)
        return
    
    # Tabel vullen
    try:
        print(f"Volledige lengte {tabel}: ", len(df))
        logging.info(f"Volledige lengte {tabel}: {len(df)}")
        log(greit_connection_string, klant, bron, f"Volledige lengte {tabel}: {len(df)}", script, script_id,  tabel)
        added_rows_count = write_to_database(df, tabel, greit_connection_string)
        print(f"Tabel {tabel} gevuld")
        logging.info(f"Tabel {tabel} gevuld")
        log(greit_connection_string, klant, bron, f"Tabel gevuld met {added_rows_count} rijen", script, script_id,  tabel)
    except Exception as e:
        print(f"FOUTMELDING | Tabel vullen mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel vullen mislukt: {e}", script, script_id,  tabel)
        return
    
    # Eindtijd logging
    bron = 'python'
    eindtijd = time.time()
    tijdsduur = timedelta(seconds=(eindtijd - start_time))
    tijdsduur_str = str(tijdsduur).split('.')[0]
    log(greit_connection_string, klant, bron, f"Script gestopt in {tijdsduur_str}", script, script_id)
    logging.info(f"Script gestopt in {tijdsduur_str}")
    print(f"Script gestopt in {tijdsduur_str}")

if __name__ == '__main__':
    main()