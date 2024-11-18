
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import time
from urenrapportage_verwerking_modules.log import log
from urenrapportage_verwerking_modules.database import connect_to_database, write_to_database, clear_table
from urenrapportage_verwerking_modules.config import fetch_configurations, fetch_script_id, fetch_all_connection_strings
from urenrapportage_verwerking_modules.excel_processing import process_excel_file, get_file_path, clean_excel, delete_excel_file
from urenrapportage_verwerking_modules.table_mapping import transform_columns, urenregistratie
from urenrapportage_verwerking_modules.type_mapping import convert_column_types, urenregistratie_typing
import pandas as pd
import logging

def main():
    if os.path.exists("/Users/maxrood/werk/greit/klanten/stiek/project_dashboard/urenrapportage_verwerking/.env"):
        load_dotenv()
        print("Lokaal draaien: .env bestand gevonden en geladen.")
        logging.info("Lokaal draaien: .env bestand gevonden en geladen.")
    else:
        logging.info("Draaien in productieomgeving (Azure): .env bestand niet gevonden.")

    # Definiëren van script
    script = "Urenrapportage verwerking"
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
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # ScriptID ophalen
    database_conn = connect_to_database(greit_connection_string)
    if database_conn:
        cursor = database_conn.cursor()
        latest_script_id = fetch_script_id(cursor)
        database_conn.close()

        if latest_script_id:
            script_id = latest_script_id + 1
        else:
            script_id = 1

    # Start logging
    bron = 'python'
    log(greit_connection_string, klant, bron, f"Script gestart", script, script_id)

    # Verbinding maken met database
    database_conn = connect_to_database(greit_connection_string)
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

    for klantnaam, (klant_connection_string, type) in connection_dict.items():
        # Skip de klant als type niet gelijk is aan 1
        if type != 1:
            print(f"Skip {klantnaam}")
            log(greit_connection_string, klant, bron, f"Skip {klantnaam}", script, script_id)
            continue

        if klantnaam == "Stiek":      

            # Excel bestand ophalen
            bron = 'excel'
            try:
                filepath, detail, begindatum, einddatum = get_file_path()
            except Exception as e:
                print(f"FOUTMELDING | Excel bestand ophalen mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Excel bestand ophalen mislukt: {e}", script, script_id)
                return

            # Excel bestand opschonen
            try:
                status = clean_excel(filepath)
                if status == "already_cleaned":
                    print("Het bestand was al opgeschoond, doorgaan met de rest van het script.")
            except Exception as e: 
                print(f"FOUTMELDING | Excel bestand opschonen mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Excel bestand opschonen mislukt: {e}", script, script_id)
                return

            # Voer Excel transformatie uit
            try:
                df = process_excel_file(filepath, greit_connection_string, klant, bron, script, script_id)
            except Exception as e:
                print(f"FOUTMELDING | Excel verwerking mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Excel verwerking mislukt: {e}", script, script_id)

            # Dataframe check
            if df is None:
                print(f"FOUTMELDING | Geen DataFrame geretourneerd")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Geen DataFrame geretourneerd", script, script_id)
                return
            if df.empty:
                print(f"FOUTMELDING | DataFrame is leeg")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | DataFrame is leeg", script, script_id)
                return

            # Kolom typing
            column_typing = {
                'Urenregistratie': urenregistratie_typing,
            }

            # Update typing van kolommen
            for tabel, typing in column_typing.items():
                    # Type conversie
                    try:
                        df = convert_column_types(df, typing)
                        print(f"Kolommen type conversie")
                        log(greit_connection_string, klant, bron, f"Kolommen type conversie correct uitgevoerd", script, script_id, tabel)
                    except Exception as e:
                        print(f"FOUTMELDING | Kolommen type conversie mislukt: {e}")
                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen type conversie mislukt: {e}", script, script_id, tabel)
                        return

            # Kolom mapping
            column_mapping = {
                'Urenregistratie': urenregistratie,
            }

            # Tabel mapping
            for mapping_table, mapping in column_mapping.items():
                    # Transformeer de kolommen
                    try:
                        df = transform_columns(df, mapping)
                        print(f"Kolommen getransformeerd")
                        log(greit_connection_string, klant, bron, f"Mapping van kolommen correct uitgevoerd", script, script_id, tabel)
                    except Exception as e:
                        print(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen transformeren mislukt: {e}", script, script_id, tabel)
                        return

            # Tabel leeg maken
            try:
                clear_table(klant_connection_string, tabel, begindatum, einddatum)
                print(f"Tabel {tabel} leeg gemaakt van {begindatum} tot {einddatum}")
                log(greit_connection_string, klant, bron, f"Tabel {tabel} leeg gemaakt van {begindatum} tot {einddatum}", script, script_id, tabel)
            except Exception as e:
                print(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel leeg maken mislukt: {e}", script, script_id, tabel, detail)
                return
            
            # Tabel vullen
            try:
                print(f"Volledige lengte {tabel}: ", len(df))
                log(greit_connection_string, klant, bron, f"Volledige lengte {tabel}: {len(df)}", script, script_id, tabel)
                added_rows_count = write_to_database(df, tabel, klant_connection_string)
                print(f"Tabel {tabel} gevuld met {detail}")
                log(greit_connection_string, klant, bron, f"Tabel gevuld met {added_rows_count} rijen met {detail}", script, script_id,  tabel)
            except Exception as e:
                print(f"FOUTMELDING | Tabel vullen mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel vullen mislukt: {e}", script, script_id,  tabel)
                return

            # Excel verwijderen
            try:
                delete_excel_file(filepath)
                print(f"Bestand succesvol verwijderd: {filepath}")
                log(greit_connection_string, klant, bron, f"Bestand succesvol verwijderd: {filepath}", script, script_id,  tabel)
            except Exception as e:
                print(f"FOUTMELDING | Bestand verwijderen mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Bestand verwijderen mislukt: {e}", script, script_id,  tabel)
                return

    # Eindtijd logging
    bron = 'python'
    eindtijd = time.time()
    tijdsduur = timedelta(seconds=(eindtijd - start_time))
    tijdsduur_str = str(tijdsduur).split('.')[0]
    log(greit_connection_string, klant, bron, f"Script gestopt in {tijdsduur_str}", script, script_id)
    print(f"Script gestopt in {tijdsduur_str}")

if __name__ == "__main__":
    main()
