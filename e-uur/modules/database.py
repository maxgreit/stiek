from sqlalchemy import create_engine
import pandas as pd
import logging
import urllib
import pyodbc
import time


class DatabaseManager:
    """
    Een class voor het beheren van database operaties zoals schrijven en verwijderen van data.
    """
    
    def __init__(self, connection_string, max_retries=3, retry_delay=5):
        """
        Initialiseer de DatabaseManager.
        
        Args:
            connection_string: Database connection string
            max_retries: Maximum aantal pogingen voor database connectie
            retry_delay: Delay tussen pogingen in seconden
        """
        self.connection_string = connection_string
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(__name__)
    
    def connect_to_database(self):
        """
        Maak verbinding met de database met retry mechanisme.
        
        Returns:
            pyodbc.Connection: Database connectie of None bij fout
        """
        for attempt in range(self.max_retries):
            try:
                conn = pyodbc.connect(self.connection_string)
                self.logger.info("Database verbinding succesvol")
                return conn
            except Exception as e:
                self.logger.warning(f"Fout bij poging {attempt + 1} om verbinding te maken: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
        
        self.logger.error("Kan geen verbinding maken met de database na meerdere pogingen.")
        return None
    
    def clear_table(self, table):
        """
        Maak een tabel compleet leeg.
        
        Args:
            table: Naam van de tabel
            
        Returns:
            bool: True als succesvol, False bij fout
        """
        try:
            connection = self.connect_to_database()
            if not connection:
                return False
                
            cursor = connection.cursor()
            
            cursor.execute(f"DELETE FROM {table}")
            rows_deleted = cursor.rowcount
            connection.commit()
            
            cursor.close()
            connection.close()
            
            self.logger.info(f"Tabel {table} compleet leeggemaakt, {rows_deleted} rijen verwijderd")
            return True
            
        except Exception as e:
            self.logger.error(f"Fout bij leegmaken van tabel {table}: {e}")
            return False
    
    def delete_rows_by_ids(self, table, id_column, ids):
        """
        Verwijder rijen uit een tabel op basis van een lijst van ID's.
        
        Args:
            table (str): Naam van de tabel.
            id_column (str): Naam van de ID kolom.
            ids (list): Lijst met ID's om te verwijderen.
            
        Returns:
            bool: True als succesvol, False bij fout.
        """
        if not ids:
            self.logger.info(f"Geen ID's opgegeven om te verwijderen uit tabel {table}.")
            return True
            
        try:
            connection = self.connect_to_database()
            if not connection:
                return False
                
            cursor = connection.cursor()
            
            # Verdeel de operatie in chunks om limieten op het aantal parameters te vermijden
            chunk_size = 500
            rows_deleted_total = 0
            
            unique_ids = list(set(ids))

            for i in range(0, len(unique_ids), chunk_size):
                chunk_ids = unique_ids[i:i + chunk_size]
                
                placeholders = ', '.join(['?'] * len(chunk_ids))
                sql = f"DELETE FROM {table} WHERE [{id_column}] IN ({placeholders})"
                
                cursor.execute(sql, chunk_ids)
                rows_deleted = cursor.rowcount
                rows_deleted_total += rows_deleted if rows_deleted > -1 else 0
                
            connection.commit()
            
            cursor.close()
            connection.close()
            
            self.logger.info(f"{rows_deleted_total} rijen verwijderd uit tabel {table} op basis van ID's in kolom {id_column}.")
            return True
            
        except Exception as e:
            self.logger.error(f"Fout bij conditioneel verwijderen uit tabel {table}: {e}")
            return False

    def delete_by_date_range(self, table, date_column, start_date, end_date):
        """
        Verwijder rijen uit een tabel op basis van een datumbereik.
        
        Args:
            table (str): Naam van de tabel.
            date_column (str): Naam van de datumkolom.
            start_date (date): Begindatum van de periode.
            end_date (date): Einddatum van de periode.
            
        Returns:
            bool: True als succesvol, False bij fout.
        """
        try:
            connection = self.connect_to_database()
            if not connection:
                return False
                
            cursor = connection.cursor()
            
            sql = f"DELETE FROM {table} WHERE [{date_column}] BETWEEN ? AND ?"
            
            cursor.execute(sql, start_date, end_date)
            rows_deleted = cursor.rowcount
            
            connection.commit()
            
            cursor.close()
            connection.close()
            
            self.logger.info(f"{rows_deleted} rijen verwijderd uit tabel {table} voor periode {start_date} t/m {end_date}.")
            return True
            
        except Exception as e:
            self.logger.error(f"Fout bij verwijderen op datumbereik uit tabel {table}: {e}")
            return False

    def fill_table(self, df, table, batch_size=1000):
        """
        Vul een tabel met data uit een DataFrame.
        
        Args:
            df: Pandas DataFrame met data
            table: Naam van de doel tabel
            batch_size: Grootte van de batches voor schrijven
            
        Returns:
            bool: True als succesvol, False bij fout
        """
        try:
            db_params = urllib.parse.quote_plus(self.connection_string)
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={db_params}", fast_executemany=True)

            total_rows = len(df)
            rows_added = 0
            
            # Werk in batches
            for start in range(0, total_rows, batch_size):
                batch_df = df.iloc[start:start + batch_size]
                batch_df.to_sql(table, con=engine, index=False, if_exists="append", schema="dbo")
                rows_added += len(batch_df)
                self.logger.info(f"{rows_added} rijen toegevoegd aan tabel {table}")
            
            self.logger.info(f"Tabel {table} succesvol gevuld met {rows_added} rijen")
            return True
            
        except Exception as e:
            self.logger.error(f"Fout bij vullen van tabel {table}: {e}")
            return False
    
    def clear_and_fill_table(self, df, table, id_column=None, batch_size=1000):
        """
        Maak een tabel leeg en vul deze met nieuwe data.
        Indien id_column is opgegeven, worden alleen de rijen verwijderd waarvan
        de ID's in de meegegeven DataFrame voorkomen, voordat de nieuwe data wordt ingevoegd.
        
        Args:
            df: Pandas DataFrame met nieuwe data
            table: Naam van de tabel
            id_column (str, optional): Naam van de ID kolom voor conditioneel verwijderen.
            batch_size: Grootte van de batches voor schrijven
            
        Returns:
            bool: True als succesvol, False bij fout
        """
        if id_column:
            if id_column not in df.columns:
                self.logger.error(f"ID kolom '{id_column}' niet gevonden in DataFrame.")
                return False
            
            ids_to_delete = df[id_column].dropna().unique().tolist()
            
            if not self.delete_rows_by_ids(table, id_column, ids_to_delete):
                return False
        else:
            if not self.clear_table(table):
                return False
        
        return self.fill_table(df, table, batch_size)

    def fetch_plaatsing_data(self, table_name="Plaatsingen"):
        """
        Haal alle plaatsingen op uit de opgegeven tabel.
        Args:
            table_name (str): Naam van de tabel (default: 'Plaatsingen')
        Returns:
            pd.DataFrame: DataFrame met kolommen ['ID', 'Werknemer', 'Actief']
        """
        try:
            conn = self.connect_to_database()
            if not conn:
                self.logger.error("Geen databaseverbinding voor ophalen plaatsingen.")
                return pd.DataFrame()
            cursor = conn.cursor()
            query = f"SELECT ID, Werknemer, Actief FROM {table_name}"
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                self.logger.warning(f"Geen plaatsingen gevonden in tabel {table_name}.")
                cursor.close()
                conn.close()
                return pd.DataFrame(columns=["ID", "Werknemer", "Actief"])
            plaatsing_dict = {}
            for row in rows:
                ID = row[0]
                werknemer = row[1]
                actief = row[2]
                plaatsing_dict[ID] = {
                    'Werknemer': werknemer,
                    'Actief': actief
                }
            cursor.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"Fout bij het ophalen van data uit de tabel {table_name}: {e}")
            return pd.DataFrame()
        try:
            self.logger.info("Data omzetten naar DataFrame gestart")
            df = pd.DataFrame.from_dict(plaatsing_dict, orient='index').reset_index()
            df.columns = ['ID', 'Werknemer', 'Actief']
            return df
        except Exception as e:
            self.logger.error(f"Data omzetten naar DataFrame mislukt: {e}")
            return pd.DataFrame()

    def fetch_contract_phase_data(self, table_name="Plaatsingen", only_active=False):
        """
        Haal alle contractfases op uit de opgegeven tabel.
        Args:
            table_name (str): Naam van de tabel (default: 'Plaatsingen')
            only_active (bool): Indien True, alleen actieve plaatsingen ophalen
        Returns:
            pd.DataFrame: DataFrame met kolommen ['ID', 'Werknemer', 'Contracttype', 'Actief']
        """
        max_retries = self.max_retries
        retry_delay = self.retry_delay
        contract_dict = None
        try:
            conn = self.connect_to_database()
            if not conn:
                self.logger.error("Geen databaseverbinding voor ophalen contractfases.")
                return pd.DataFrame()
            cursor = conn.cursor()
            for attempt in range(max_retries):
                try:
                    query = f"SELECT ID, Werknemer, Contracttype, Actief FROM {table_name}"
                    if only_active:
                        query += " WHERE Actief = 1"
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    contract_dict = {row[0]: (row[1], row[2], row[3]) for row in rows}
                    if contract_dict:
                        break
                except Exception as e:
                    self.logger.warning(f"Fout bij poging {attempt+1} contractfases ophalen: {e}")
                    time.sleep(retry_delay)
            cursor.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"Fout bij het ophalen van data uit de tabel {table_name}: {e}")
            return pd.DataFrame()
        if not contract_dict:
            self.logger.error(f"Ophalen contract dictionary mislukt na meerdere pogingen")
            return pd.DataFrame()
        try:
            contract_df = pd.DataFrame.from_dict(contract_dict, orient='index', columns=['Werknemer', 'Contracttype', 'Actief']).reset_index()
            contract_df.rename(columns={'index': 'ID'}, inplace=True)
            self.logger.info("Contractfases succesvol opgehaald en omgezet naar DataFrame")
            return contract_df
        except Exception as e:
            self.logger.error(f"Data omzetten naar DataFrame mislukt: {e}")
            return pd.DataFrame()

    def fetch_loon_ids(self, table_name="Loon", id_column="ID"):
        """
        Haal alle unieke ID's op uit de opgegeven Loon-tabel.
        Args:
            table_name (str): Naam van de tabel (default: 'Loon')
            id_column (str): Naam van de ID kolom (default: 'ID')
        Returns:
            list: Lijst met unieke ID's
        """
        try:
            conn = self.connect_to_database()
            if not conn:
                self.logger.error("Geen databaseverbinding voor ophalen loon-ID's.")
                return []
            cursor = conn.cursor()
            query = f"SELECT DISTINCT [{id_column}] FROM {table_name}"
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            ids = [row[0] for row in rows if row[0] is not None]
            return ids
        except Exception as e:
            self.logger.error(f"Fout bij het ophalen van loon-ID's uit {table_name}: {e}")
            return []