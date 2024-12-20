from datetime import datetime, timedelta
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

def log(logging_connection_string, klant, bron, log, script, scriptid, tabel=None):
    # Actuele datum en tijd ophalen
    datumtijd = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Probeer verbinding te maken met de database
    logging_conn = connect_to_database(logging_connection_string)

    # Foutmelding geven indien connectie niet gemaakt kan worden
    if logging_conn is None:
        print("Kan geen verbinding maken met de logging database na 3 pogingen.")
        return  # Stop de functie als er geen verbinding is

    try:
        # Verbinding maken met database
        cursor = logging_conn.cursor()

        # Query om waarden toe te voegen aan de Logging tabel met parameterbinding
        insert_query = """
        INSERT INTO Logging (Datumtijd, Klant, Log, Bron, Tabel, Script, ScriptID)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        # Voer de INSERT-query uit met parameterbinding
        cursor.execute(insert_query, (datumtijd, klant, log, bron, tabel, script, scriptid))
        logging_conn.commit() 

    except Exception as e:
        print(f"Fout bij het toevoegen van waarden: {e}")

    finally:
        # Sluit connectie als die is gemaakt
        if logging_conn:
            logging_conn.close()
            
def end_log(start_time, greit_connection_string, klant, bron, script, script_id):
    bron = 'Python'
    eindtijd = time.time()
    tijdsduur = timedelta(seconds=(eindtijd - start_time))
    tijdsduur_str = str(tijdsduur).split('.')[0]
    log(greit_connection_string, klant, bron, f"Script gestopt in {tijdsduur_str}", script, script_id)
    print(f"Script gestopt in {tijdsduur_str}")