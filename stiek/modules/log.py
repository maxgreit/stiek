from datetime import timedelta, datetime
from contextlib import contextmanager
import logging
import pyodbc
import time

class BufferedDatabaseHandler(logging.Handler):
    """
    Een efficiënte logging handler die logs buffert en in batches naar de database schrijft.
    """
    
    def __init__(self, conn_str, customer, source, script, script_id, 
                 buffer_size = 100, flush_interval = 30):
        """
        Initialiseer de BufferedDatabaseHandler.
        
        Args:
            conn_str: Database connection string
            customer: Klantnaam
            source: Bron van de logs
            script: Scriptnaam
            script_id: Script ID
            buffer_size: Aantal logs voordat automatisch geflushed wordt
            flush_interval: Tijd in seconden voordat automatisch geflushed wordt
        """
        super().__init__()
        self.conn_str = conn_str
        self.customer = customer
        self.source = source
        self.script = script
        self.script_id = script_id
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.log_buffer = []
        self.last_flush_time = time.time()

    def emit(self, record):
        """
        Voeg een logbericht toe aan de buffer en flush indien nodig.
        """
        try:
            log_message = self.format(record)
            # Maak bericht schoner door alleen het laatste deel na '-' te nemen
            log_message = log_message.split('-')[-1].strip()
            created_at = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')

            log_entry = (
                record.levelname, 
                log_message, 
                created_at, 
                self.customer, 
                self.source, 
                self.script, 
                self.script_id
            )
            self.log_buffer.append(log_entry)

            # Auto-flush op basis van buffer grootte of tijd
            current_time = time.time()
            if (len(self.log_buffer) >= self.buffer_size or 
                current_time - self.last_flush_time >= self.flush_interval):
                self.flush_logs()
                self.last_flush_time = current_time
                
        except Exception as e:
            # Fallback naar console logging bij fouten
            print(f"Fout in BufferedDatabaseHandler.emit: {e}")

    def flush_logs(self):
        """
        Schrijf alle buffered logs in één keer naar de database.
        
        Returns:
            bool: True als succesvol, False bij fout
        """
        if not self.log_buffer:
            return True

        try:
            with pyodbc.connect(self.conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """INSERT INTO Logboek 
                           (Niveau, Bericht, Datumtijd, Klant, Bron, Script, Script_ID) 
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        self.log_buffer
                    )
                    conn.commit()
            
            self.log_buffer.clear()
            return True
            
        except Exception as e:
            print(f"Fout bij flush_logs: {e}")
            return False

    def close(self):
        """
        Zorg ervoor dat alle logs worden geflushed voordat het script stopt.
        """
        self.flush_logs()
        super().close()


class LoggerManager:
    """
    Een manager class voor het configureren en beheren van logging.
    """
    
    def __init__(self, config):
        """
        Initialiseer de LoggerManager.
        
        Args:
            config: Configuratie dictionary met logging instellingen
        """
        self.config = config
        self.db_handler = None
        self.start_time = None
        self._setup_logging()
    
    def _setup_logging(self):
        """
        Configureer de logging setup.
        """
        logger = logging.getLogger()
        logger.setLevel(self.config.get('log_level', logging.INFO))
        
        # Verwijder bestaande handlers om duplicaten te voorkomen
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Console formatter
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s', 
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler toevoegen
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # Database handler
        if all(key in self.config for key in ['conn_str', 'customer', 'source', 'script', 'script_id']):
            self.db_handler = BufferedDatabaseHandler(
                conn_str=self.config['conn_str'],
                customer=self.config['customer'],
                source=self.config['source'],
                script=self.config['script'],
                script_id=self.config['script_id'],
                buffer_size=self.config.get('buffer_size', 100),
                flush_interval=self.config.get('flush_interval', 30)
            )
            self.db_handler.setFormatter(console_formatter)
            logger.addHandler(self.db_handler)
        
        logging.info("Logboek is geconfigureerd.")
    
    def start_log(self):
        """
        Start het logging proces en log de starttijd.
        
        Returns:
            float: Starttijd timestamp
        """
        self.start_time = time.time()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Forceer start bericht naar console, ongeacht logging niveau
        print(f"=== SCRIPT GESTART OM {current_time} ===")
        logging.info(f"Script gestart om {current_time}")
        return self.start_time
    
    def end_log(self):
        """
        Log de eindtijd en totale uitvoeringstijd.
        """
        if self.start_time is None:
            logging.warning("start_log() is niet aangeroepen voordat end_log() werd aangeroepen")
            return
            
        end_time = time.time()
        total_time = timedelta(seconds=(end_time - self.start_time))
        total_time_str = str(total_time).split('.')[0]
        
        # Forceer eind bericht naar console, ongeacht logging niveau
        print(f"=== SCRIPT VOLTOOID IN {total_time_str} ===")
        logging.info(f"Script voltooid in {total_time_str}")
    
    def flush_database_logs(self):
        """
        Forceer een flush van database logs.
        
        Returns:
            bool: True als succesvol, False bij fout
        """
        if self.db_handler:
            return self.db_handler.flush_logs()
        return True
    
    def close(self):
        """
        Sluit alle logging handlers netjes af.
        """
        if self.db_handler:
            self.db_handler.close()
        
        # Sluit alle andere handlers
        logger = logging.getLogger()
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)


# Backwards compatibility functies
def setup_logging(conn_str, klant, bron, script, script_id, 
                 log_file = 'app.log', log_level = logging.INFO):
    """
    Backwards compatibility functie voor bestaande code.
    
    Args:
        conn_str: Database connection string
        klant: Klantnaam
        bron: Bron van de logs
        script: Scriptnaam
        script_id: Script ID
        log_file: Log bestand pad
        log_level: Logging niveau
        
    Returns:
        BufferedDatabaseHandler: De database handler
    """
    config = {
        'conn_str': conn_str,
        'customer': klant,
        'source': bron,
        'script': script,
        'script_id': script_id,
        'log_file': log_file,
        'log_level': log_level
    }
    
    manager = LoggerManager(config)
    return manager.db_handler


def start_log():
    """
    Backwards compatibility functie voor start_log.
    
    Returns:
        float: Starttijd timestamp
    """
    return time.time()


def end_log(start_time):
    """
    Backwards compatibility functie voor end_log.
    
    Args:
        start_time: Starttijd timestamp
    """
    if start_time is None:
        return
        
    end_time = time.time()
    total_time = timedelta(seconds=(end_time - start_time))
    total_time_str = str(total_time).split('.')[0]
    logging.info(f"Script voltooid in {total_time_str}")


# Context manager voor automatische cleanup
@contextmanager
def logging_context(config):
    """
    Context manager voor automatische logging setup en cleanup.
    
    Args:
        config: Configuratie dictionary voor LoggerManager
        
    Yields:
        LoggerManager: De geconfigureerde logger manager
    """
    manager = LoggerManager(config)
    try:
        yield manager
    finally:
        manager.close()
