from sqlalchemy import create_engine
import pandas as pd
import logging
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

def clear_table(connection_string, table, id):
    try:
        # Maak verbinding met de database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        rows_deleted = 0
        
        # Itereer door de lijst met ID's en verwijder voor elke ID de overeenkomstige rij
        try:
            cursor.execute(f"""
                DELETE FROM {table}
                WHERE ID = ?
            """, (id,))
            rows_deleted += cursor.rowcount  # Houd het aantal verwijderde rijen bij
        except pyodbc.Error as e:
            logging.error(f"Verwijderen van ID {id} in tabel {table} mislukt: {e}")

        # Commit de transactie
        connection.commit()
        logging.info(f"Leeggooien succesvol uitgevoerd voor tabel {table}. Aantal verwijderde rijen: {rows_deleted}.")
    except pyodbc.Error as e:
        logging.error(f"Fout bij het leeggooien van tabel {table}: {e}")
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
        
        logging.info(f"DataFrame succesvol toegevoegd/bijgewerkt in de tabel: {tabel}")
    except Exception as e:
        logging.error(f"Fout bij het toevoegen naar de database: {e}")
    
    return rows_added

def empty_and_fill_table(df, tabel, id, klant_connection_string):
    # Tabel leeg maken
    try:
        rows_deleted = clear_table(klant_connection_string, tabel, id)
        logging.info(f"Tabel {tabel} leeg gemaakt, {rows_deleted} rijen verwijderd")

    except Exception as e:
        logging.error(f"Tabel leeg maken mislukt: {e}")
        return
    
    # Tabel vullen
    try:
        added_rows_count = write_to_database(df, tabel, klant_connection_string)
        logging.info(f"Tabel {tabel} gevuld met {added_rows_count} rijen")
    except Exception as e:
        logging.error(f"Tabel vullen mislukt: {e}")
        return
    
    
def fetch_plaatsing_data_from_table(connection_string, table_name):
    try:
        # Verbinding maken met de database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query om alle data op te halen uit de opgegeven tabel
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)

        # Resultaten ophalen
        rows = cursor.fetchall()

        # Controleer of er resultaten zijn
        if not rows:
            print("Geen data gevonden.")
            return {}

        # Extract de configuraties en waarden, waarbij de bron de sleutel is
        plaatsing_dict = {}
        for row in rows:
            ID = row[0]  
            werknemer = row[6]      
            actief = row[5]
            
            # Je kunt de waardes toevoegen aan het dictionary
            plaatsing_dict[ID] = {
                'Werknemer': werknemer,
                'Actief': actief
            }

        # Verbinding sluiten
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Fout bij het ophalen van data uit de tabel {table_name}: {e}")
        return None

    # Data omzetten naar DataFrame
    try:
        logging.info("Data omzetten naar DataFrame gestart")
        df = pd.DataFrame.from_dict(plaatsing_dict, orient='index').reset_index()
        df.columns = ['ID', 'Werknemer', 'Actief']
        return df
    except Exception as e:
        logging.error(f"Data omzetten naar DataFrame mislukt: {e}")
        return None

def fetch_looncomponenten_data_from_table(connection_string, table_name):
    try:
        # Verbinding maken met de database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Query om alle data op te halen uit de opgegeven tabel
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)

        # Resultaten ophalen
        rows = cursor.fetchall()

        # Controleer of er resultaten zijn
        if not rows:
            print("Geen data gevonden.")
            return {}

        # Extract de configuraties en waarden, waarbij de bron de sleutel is
        looncomponenten_dict = {}
        for row in rows:
            ID = row[0]  
            looncomponent = row[1]      
            loon = row[2]
        
            # Je kunt de waardes toevoegen aan het dictionary
            looncomponenten_dict[ID] = {
                'Looncomponent': looncomponent,
                'Loon': loon
            }

        # Verbinding sluiten
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Fout bij het ophalen van data uit de tabel {table_name}: {e}")
        return None

    # Data omzetten naar DataFrame
    try:
        logging.info("Data omzetten naar DataFrame gestart")
        df = pd.DataFrame.from_dict(looncomponenten_dict, orient='index').reset_index()
        df.columns = ['ID', 'Werknemer', 'Actief']
        return df
    except Exception as e:
        logging.error(f"FOUTMELDING | Data omzetten naar DataFrame mislukt: {e}")
        return None