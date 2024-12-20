from sqlalchemy import create_engine, event, text
from fases_modules.log import log
from datetime import datetime
import sqlalchemy
import urllib
import pyodbc
import time

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

def clear_table(connection_string, table, id_list):
    try:
        # Maak verbinding met de database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        rows_deleted = 0
        
        # Itereer door de lijst met ID's en verwijder voor elke ID de overeenkomstige rij
        for id_value in id_list:
            try:
                cursor.execute(f"""
                    DELETE FROM {table}
                    WHERE ID = ?
                """, (id_value,))
                rows_deleted += cursor.rowcount  # Houd het aantal verwijderde rijen bij
            except pyodbc.Error as e:
                print(f"Verwijderen van ID {id_value} in tabel {table} mislukt: {e}")

        # Commit de transactie
        connection.commit()
        print(f"Leeggooien succesvol uitgevoerd voor tabel {table}. Aantal verwijderde rijen: {rows_deleted}.")
    except pyodbc.Error as e:
        print(f"Fout bij het leeggooien van tabel {table}: {e}")
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()
    
    return rows_deleted

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

    return rows_added


def fill_table(df, tabelnaam, klant_connection_string, greit_connection_string, klant, bron, script, script_id):
    
    # Tabel vullen
    try:
        print(f"Volledige lengte {tabelnaam}: ", len(df))
        log(greit_connection_string, klant, bron, f"Volledige lengte {tabelnaam}: {len(df)}", script, script_id,  tabelnaam)
        added_rows_count = write_to_database(df, tabelnaam, klant_connection_string)
        print(f"Tabel {tabelnaam} gevuld")
        log(greit_connection_string, klant, bron, f"Tabel gevuld met {added_rows_count} rijen", script, script_id,  tabelnaam)
    except Exception as e:
        print(f"FOUTMELDING | Tabel vullen mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel vullen mislukt: {e}", script, script_id,  tabelnaam)
        return



