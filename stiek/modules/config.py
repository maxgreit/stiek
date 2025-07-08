import logging
import pyodbc
import time

class ConfigManager():
    def __init__(self, greit_connection_string):
        self.greit_connection_string = greit_connection_string

    def _connect_to_database(self, connection_string):
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

    def _fetch_current_script_id(self, cursor):
        # Voer de query uit om het hoogste ScriptID op te halen
        query = 'SELECT MAX(Script_ID) FROM Logboek'
        cursor.execute(query)
        
        # Verkrijg het resultaat
        highest_script_id = cursor.fetchone()[0]

        return highest_script_id

    def determine_script_id(self):
        try:
            database_conn = self._connect_to_database(self.greit_connection_string)
        except Exception as e:
            logging.info(f"Verbinding met database mislukt, foutmelding: {e}")
        if database_conn:
            logging.info(f"Verbinding met database geslaagd")
            cursor = database_conn.cursor()
            latest_script_id = self._fetch_current_script_id(cursor)
            logging.info(f"ScriptID: {latest_script_id}")
            database_conn.close()

        if latest_script_id:
            script_id = latest_script_id + 1
        else:
            script_id = 1
            
        logging.info(f"ScriptID: {script_id}")
        
        return script_id
    
    def _fetch_all_connection_strings(self, cursor):
        # Voer de query uit om alle connectiestrings op te halen
        query = 'SELECT * FROM Klanten'
        cursor.execute(query)
        
        # Verkrijg alle rijen uit de resultaten
        rows = cursor.fetchall()
        
        # Extract de connectiestrings uit de resultaten
        connection_dict = {row[1]: (row[2], row[3]) for row in rows}  
        return connection_dict
        
    def create_connection_dict(self):
        max_retries = 3
        retry_delay = 5
        
        try:
            database_conn = self._connect_to_database(self.greit_connection_string)
        except Exception as e:
            logging.info(f"Verbinding met database mislukt, foutmelding: {e}")
        if database_conn:
            logging.info(f"Verbinding met database opnieuw geslaagd")
            cursor = database_conn.cursor()
            connection_dict = None
            for attempt in range(max_retries):
                try:
                    connection_dict = self._fetch_all_connection_strings(cursor)
                    if connection_dict:
                        break
                except Exception as e:
                    time.sleep(retry_delay)
            database_conn.close()
            if connection_dict:
                logging.info(f"Ophalen connectiestrings gelukt")
            else:
                # Foutmelding logging
                logging.error(f"Ophalen connectiestrings mislukt na meerdere pogingen")
        else:
            # Foutmelding logging
            logging.error(f"Verbinding met database mislukt na meerdere pogingen")
        
        logging.info("Configuratie dictionary opgehaald")
        
        return connection_dict
    
    def _fetch_configurations(self, cursor):
        # Voer de query uit om alle configuraties op te halen
        query = 'SELECT * FROM Configuratie'
        cursor.execute(query)

        # Verkrijg alle rijen uit de resultaten
        rows = cursor.fetchall()
        
        # Controleer of er resultaten zijn
        if not rows:
            logging.error("Geen configuraties gevonden.")
            return {}

        # Extract de configuraties en waarden, waarbij de bron de sleutel is
        configuratie_dict = {}
        for row in rows:
            configuratie = row[1]  # Kolom 'Configuratie' (index 1)
            waarde = row[2]         # Kolom 'Waarde' (index 2)
            bron = row[3]           # Kolom 'Bron' (index 3)

            # Voeg configuratie en waarde toe onder de bron
            if bron not in configuratie_dict:
                configuratie_dict[bron] = {}
            configuratie_dict[bron][configuratie] = waarde

        return configuratie_dict
        
        