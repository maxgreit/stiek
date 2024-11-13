from looncomponenten_modules.log import log
from looncomponenten_modules.database import connect_to_database, write_to_database, clear_table, fetch_looncomponenten_data_from_table, fetch_plaatsing_data_from_table
from looncomponenten_modules.config import fetch_script_id, fetch_all_connection_strings
from looncomponenten_modules.type_mapping import convert_column_types, looncomponent_typing
from looncomponenten.looncomponenten_modules.actief_selenium import looncomponenten_ophalen
import os
from dotenv import load_dotenv
import time
import pandas as pd
from datetime import timedelta

def main():
    # Laden van .env bestand als het lokaal aanwezig is
    if os.path.exists("/Users/maxrood/werk/greit/klanten/stiek/.env"):
        load_dotenv()
        print("Lokaal draaien: .env bestand gevonden en geladen.")

    # Definiëren van script
    script = "Looncomponenten ophalen"
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
    euurusername = os.getenv('EUURUSERNAME')
    euurpassword = os.getenv('EUURPASSWORD')
    euururl = os.getenv('EUURURL')
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
    bron = 'Python'
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
            # Start logging
            bron = 'Azure SQL Database'

            # Ophalen Plaatsing Data
            try:
                log(greit_connection_string, klant, bron, f"Ophalen plaatsing data gestart", script, script_id)
                plaatsing_dict = fetch_plaatsing_data_from_table(klant_connection_string, "Plaatsing")
            except Exception as e:
                print(f"FOUTMELDING | Ophalen plaatsing data mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Ophalen plaatsing data mislukt: {e}", script, script_id)

            # Data omzetten naar DataFrame
            log(greit_connection_string, klant, bron, f"Data omzetten naar DataFrame gestart", script, script_id)
            df = pd.DataFrame.from_dict(plaatsing_dict, orient='index').reset_index()
            df.columns = ['ID', 'Werknemer', 'Actief']

            # Ophalen huidige looncomponenten data
            try:
                log(greit_connection_string, klant, bron, f"Ophalen huidige looncomponenten data gestart", script, script_id)
                looncomponenten_dict = fetch_looncomponenten_data_from_table(klant_connection_string, "Looncomponenten")
            except Exception as e:
                print(f"FOUTMELDING | Ophalen huidige looncomponenten data mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Ophalen huidige looncomponenten data mislukt: {e}", script, script_id)

            # Data omzetten naar DataFrame
            log(greit_connection_string, klant, bron, f"Data omzetten naar DataFrame gestart", script, script_id)
            df_looncomponenten = pd.DataFrame.from_dict(looncomponenten_dict, orient='index').reset_index()
            df_looncomponenten.columns = ['ID', 'Werknemer', 'Actief']

            # Houd alleen IDs over die niet in looncomponenten data zit
            df = df[~df['ID'].isin(df_looncomponenten['ID'])]

            for id, werknemer, actief in zip(df['ID'], df['Werknemer'], df['Actief']):
                if actief == True:
                    bron = 'Selenium'
                    log(greit_connection_string, klant, bron, f"Looncomponenten ophalen voor {id} {werknemer}", script, script_id)
                    try:
                        df = looncomponenten_ophalen(euururl, euurusername, euurpassword, id, werknemer, greit_connection_string, klant, script, script_id)
                    except Exception as e:
                        print(f"FOUTMELDING | Looncomponenten ophalen mislukt voor {id} {werknemer}: {e}")
                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Looncomponenten ophalen mislukt voor {id} {werknemer}: {e}", script, script_id)
                    
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
                        'Looncomponenten': looncomponent_typing,
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
                    
                    # Tabel leeg maken
                    try:
                        rows_deleted = clear_table(klant_connection_string, tabel, id)
                        print(f"Tabel {tabel} leeg gemaakt, {rows_deleted} rijen verwijderd")
                        log(greit_connection_string, klant, bron, f"Tabel {tabel} leeg gemaakt, {rows_deleted} rijen verwijderd", script, script_id, tabel)
                    except Exception as e:
                        print(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")
                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel leeg maken mislukt: {e}", script, script_id, tabel)
                        return
                    
                    # Tabel vullen
                    try:
                        print(f"Volledige lengte {tabel}: ", len(df))
                        log(greit_connection_string, klant, bron, f"Volledige lengte {tabel}: {len(df)}", script, script_id,  tabel)
                        added_rows_count = write_to_database(df, tabel, klant_connection_string)
                        print(f"Tabel {tabel} gevuld")
                        log(greit_connection_string, klant, bron, f"Tabel gevuld met {added_rows_count} rijen", script, script_id,  tabel)
                    except Exception as e:
                        print(f"FOUTMELDING | Tabel vullen mislukt: {e}")
                        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel vullen mislukt: {e}", script, script_id,  tabel)
                        return

                else:
                    print(f"Looncomponenten ophalen voor {id} {werknemer} is niet actief")
                    log(greit_connection_string, klant, bron, f"Looncomponenten ophalen voor {id} {werknemer} is niet actief", script, script_id)

    # Eindtijd logging
        bron = 'Python'
        eindtijd = time.time()
        tijdsduur = timedelta(seconds=(eindtijd - start_time))
        tijdsduur_str = str(tijdsduur).split('.')[0]
        log(greit_connection_string, klant, bron, f"Script gestopt in {tijdsduur_str}", script, script_id)
        print(f"Script gestopt in {tijdsduur_str}")

if __name__ == "__main__":
    main()