import pyodbc
import time
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine, event, text
import urllib
from uren_modules.log import log

def connect_to_database(connection_string):
    # Retries en delays
    max_retries = 3
    retry_delay = 5
    
    # Pogingen doen om connectie met database te maken
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            return conn
        except Exception as e:
            print(f"Fout bij poging {attempt + 1} om verbinding te maken: {e}")
            if attempt < max_retries - 1:  # Wacht alleen als er nog pogingen over zijn
                time.sleep(retry_delay)
    
    # Als het na alle pogingen niet lukt, return None
    print("Kan geen verbinding maken met de database na meerdere pogingen.")
    return None

def clear_table(connection_string, table, begindatum, einddatum):
    try:
        # Maak verbinding met de database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        # Probeer de tabel leeg te maken met DELETE, gefilterd op de datum tussen de gegeven begindatum en einddatum
        try:
            cursor.execute(f"""
                DELETE FROM {table}
                WHERE Datum >= ? AND Datum <= ?
            """, (begindatum, einddatum))
            rows_deleted = cursor.rowcount  # Houd het aantal verwijderde rijen bij
        except pyodbc.Error as e:
            print(f"DELETE FROM {table} voor de opgegeven periode (van {begindatum} tot {einddatum}) failed: {e}")
            rows_deleted = 0
        
        # Commit de transactie
        connection.commit()
        print(f"Leeggooien succesvol uitgevoerd voor tabel {table}. Aantal verwijderde rijen: {rows_deleted}.")
    except pyodbc.Error as e:
        print(f"Fout bij het leeggooien van tabel {table}: {e}")
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()

def write_to_database(df, tabel, connection_string, batch_size=1000):
    db_params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={db_params}", fast_executemany=True)

    total_rows = len(df)
    rows_added = 0
    
    try:
        # Werk in batches
        for start in range(0, total_rows, batch_size):
            batch_df = df.iloc[start:start + batch_size]
            # Schrijf direct naar de database
            batch_df.to_sql(tabel, con=engine, index=False, if_exists="append", schema="dbo")
            rows_added += len(batch_df)
            print(f"{rows_added} rijen toegevoegd aan de tabel tot nu toe...")
        
        print(f"DataFrame succesvol toegevoegd/bijgewerkt in de tabel: {tabel}")
    except Exception as e:
        print(f"Fout bij het toevoegen naar de database: {e}")


def empty_and_fill_table(df, tabelnaam, klant_connection_string, greit_connection_string, klant, bron, script, script_id, detail, begindatum, einddatum):
    # Tabel leeg maken
    try:
        clear_table(klant_connection_string, tabelnaam, begindatum, einddatum)
        print(f"Tabel {tabelnaam} leeg gemaakt van {begindatum} tot {einddatum}")
        log(greit_connection_string, klant, bron, f"Tabel {tabelnaam} leeg gemaakt van {begindatum} tot {einddatum}", script, script_id, tabelnaam)
    except Exception as e:
        print(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel leeg maken mislukt: {e}", script, script_id, tabelnaam, detail)
        return
    
    # Tabel vullen
    try:
        print(f"Volledige lengte {tabelnaam}: ", len(df))
        log(greit_connection_string, klant, bron, f"Volledige lengte {tabelnaam}: {len(df)}", script, script_id, tabelnaam)
        added_rows_count = write_to_database(df, tabelnaam, klant_connection_string)
        print(f"Tabel {tabelnaam} gevuld met {detail}")
        log(greit_connection_string, klant, bron, f"Tabel gevuld met {added_rows_count} rijen met {detail}", script, script_id,  tabelnaam)
    except Exception as e:
        print(f"FOUTMELDING | Tabel vullen mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel vullen mislukt: {e}", script, script_id,  tabelnaam)
        return



