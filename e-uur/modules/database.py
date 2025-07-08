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