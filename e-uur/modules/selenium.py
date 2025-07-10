from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
from pathlib import Path
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import logging
import time
import pandas as pd

class SeleniumManager:
    """
    Een efficiënte manager voor Selenium web automation taken.
    """
    
    def __init__(self, config):
        """
        Initialiseer de SeleniumManager.
        
        Args:
            config: Configuratie dictionary met Selenium instellingen
        """
        self.config = config
        self.driver = None
        self.wait = None
        self.download_dir = Path(config.get('download_dir', 'downloads'))
        self.timeout = config.get('timeout', 10)
        self.headless = config.get('headless', True)
        
        # Zorg ervoor dat download directory bestaat
        self.download_dir.mkdir(parents=True, exist_ok=True)
    
    def _setup_chrome_options(self):
        """
        Configureer Chrome opties voor optimalisatie.
        
        Returns:
            Chrome options object
        """
        options = Options()
        
        # Performance optimalisaties
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")  # Sneller laden
        options.add_argument("--disable-javascript")  # Alleen indien niet nodig
        
        if self.headless:
            options.add_argument("--headless")
        
        # Download voorkeuren
        prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.notifications": 2  # Disable notifications
        }
        options.add_experimental_option("prefs", prefs)
        
        return options
    
    def _create_driver(self):
        """
        Maak en configureer de Chrome driver.
        
        Returns:
            Geconfigureerde Chrome driver
        """
        options = self._setup_chrome_options()
        service = Service(ChromeDriverManager().install())
        
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_window_size(1920, 1080)
        
        return driver
    
    def _wait_for_element(self, by, value, timeout = None):
        """
        Wacht tot een element beschikbaar is.
        
        Args:
            by: Locator strategy
            value: Locator value
            timeout: Timeout in seconden
            
        Returns:
            Het gevonden element
        """
        timeout = timeout or self.timeout
        return WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
    
    def _wait_for_clickable(self, by, value, timeout = None):
        """
        Wacht tot een element klikbaar is.
        
        Args:
            by: Locator strategy
            value: Locator value
            timeout: Timeout in seconden
            
        Returns:
            Het klikbare element
        """
        timeout = timeout or self.timeout
        return WebDriverWait(self.driver, timeout).until(
            EC.element_to_be_clickable((by, value))
        )
    
    def _safe_click(self, element, description):
        """
        Veilig klikken op een element met error handling.
        
        Args:
            element: Het element om op te klikken
            description: Beschrijving voor logging
            
        Returns:
            True als succesvol, False bij fout
        """
        try:
            element.click()
            logging.info(f"Succesvol geklikt: {description}")
            return True
        except Exception as e:
            logging.error(f"Fout bij klikken op {description}: {e}")
            return False
    
    def _safe_send_keys(self, element, keys, description):
        """
        Veilig tekst invoeren in een element.
        
        Args:
            element: Het element om tekst in te voeren
            keys: De tekst om in te voeren
            description: Beschrijving voor logging
            
        Returns:
            True als succesvol, False bij fout
        """
        try:
            element.clear()
            element.send_keys(keys)
            logging.info(f"Succesvol tekst ingevoerd: {description}")
            return True
        except Exception as e:
            logging.error(f"Fout bij invoeren tekst in {description}: {e}")
            return False
    
    def _wait_for_download(self, filename, timeout = 120):
        """
        Wacht tot een bestand is gedownload.
        
        Args:
            filename: Naam van het bestand
            timeout: Timeout in seconden
            
        Returns:
            True als bestand gedownload is, False bij timeout
        """
        file_path = self.download_dir / filename
        start_time = time.time()
        
        while not file_path.exists():
            if time.time() - start_time > timeout:
                logging.error(f"Download timeout voor {filename}")
                return False
            time.sleep(1)
        
        logging.info(f"Bestand {filename} succesvol gedownload")
        return True
    
    def _rename_downloaded_file(self, old_filename, new_filename):
        """
        Hernoem een gedownload bestand.
        
        Args:
            old_filename: Originele bestandsnaam
            new_filename: Nieuwe bestandsnaam
            
        Returns:
            True als succesvol hernoemd, False bij fout
        """
        try:
            old_path = self.download_dir / old_filename
            new_path = self.download_dir / new_filename
            
            if old_path.exists():
                old_path.rename(new_path)
                logging.info(f"Bestand succesvol hernoemd naar: {new_filename}")
                return True
            else:
                logging.error("Gedownload bestand niet gevonden!")
                return False
        except Exception as e:
            logging.error(f"Fout bij hernoemen bestand: {e}")
            return False
    
    def start_session(self):
        """
        Start een nieuwe browser sessie.
        
        Returns:
            True als succesvol gestart, False bij fout
        """
        try:
            self.driver = self._create_driver()
            self.wait = WebDriverWait(self.driver, self.timeout)
            logging.info("Browser sessie succesvol gestart")
            return True
        except Exception as e:
            logging.error(f"Fout bij starten browser sessie: {e}")
            return False
    
    def navigate_to(self, url):
        """
        Navigeer naar een URL.
        
        Args:
            url: De URL om naar toe te navigeren
            
        Returns:
            True als succesvol, False bij fout
        """
        try:
            self.driver.get(url)
            logging.info(f"Succesvol genavigeerd naar: {url}")
            return True
        except Exception as e:
            logging.error(f"Fout bij navigeren naar {url}: {e}")
            return False
    
    def login(self, username, password):
        """
        Voer login uit.
        
        Args:
            username: Gebruikersnaam
            password: Wachtwoord
            
        Returns:
            True als succesvol ingelogd, False bij fout
        """
        try:
            # Wacht op login velden
            username_field = self._wait_for_element(By.NAME, "username")
            password_field = self._wait_for_element(By.NAME, "password")
            
            # Vul inloggegevens in
            if not self._safe_send_keys(username_field, username, "gebruikersnaam"):
                return False
            if not self._safe_send_keys(password_field, password, "wachtwoord"):
                return False
            
            # Klik op login knop
            login_button = self._wait_for_element(By.NAME, "euur")
            if not self._safe_click(login_button, "login knop"):
                return False
            
            # Wacht op dashboard
            self._wait_for_element(By.CSS_SELECTOR, "div[data-id='dashboard']")
            logging.info("Succesvol ingelogd")
            return True
            
        except Exception as e:
            logging.error(f"Fout bij inloggen: {e}")
            return False
    
    def navigate_to_start_menu(self):
        """
        Navigeer naar het start menu.
        
        Returns:
            True als succesvol, False bij fout
        """
        try:
            # Klik op Start knop
            start_button = self._wait_for_clickable(
                By.CSS_SELECTOR, "button.start-menu.akyla-widget-button"
            )
            if not self._safe_click(start_button, "Start knop"):
                return False
            
            logging.info("Succesvol naar start menu genavigeerd")
            return True
            
        except Exception as e:
            logging.error(f"Fout bij navigeren naar start menu: {e}")
            return False
    
    def download_excel(self, filename = "download.xlsx"):
        """
        Download het Excel bestand.
        
        Args:
            filename: Naam van het te downloaden bestand
            
        Returns:
            True als succesvol gedownload, False bij fout
        """
        try:
            # Klik op Excel knop
            excel_button = self._wait_for_clickable(
                By.XPATH, '(//button[@title="Excel" and @data-controller="excel"])[2]'
            )
            if not self._safe_click(excel_button, "Excel download knop"):
                return False
            
            # Wacht op download
            return self._wait_for_download(filename)
            
        except Exception as e:
            logging.error(f"Fout bij downloaden Excel bestand: {e}")
            return False
    
    def close_session(self):
        """
        Sluit de browser sessie.
        """
        if self.driver:
            try:
                self.driver.quit()
                logging.info("Browser sessie gesloten")
            except Exception as e:
                logging.error(f"Fout bij sluiten browser sessie: {e}")
            finally:
                self.driver = None
                self.wait = None


class EuurLooncomponentenDownloader:
    """
    Specifieke class voor het downloaden van looncomponenten uit E-Uur.
    """
    
    def __init__(self, base_dir, download_dir, headless = True):
        """
        Initialiseer de E-Uur looncomponenten downloader.
        
        Args:
            base_dir: Basis directory voor downloads
            download_dir: Specifieke download directory (optioneel)
            headless: Of de browser in headless mode moet draaien
        """
        self.base_dir = Path(base_dir)
        self.download_dir = Path(download_dir)
        
        logging.info(f"EeurLooncomponentenDownloader geïnitialiseerd")
        logging.info(f"Base directory: {self.base_dir}")
        logging.info(f"Download directory: {self.download_dir}")
        logging.info(f"Download directory bestaat: {self.download_dir.exists()}")
        
        config = {
            'download_dir': str(self.download_dir),
            'headless': headless,
            'timeout': 10
        }
        
        self.selenium_manager = SeleniumManager(config)
    
    def navigate_to_looncomponenten(self):
        """
        Navigeer naar de looncomponenten pagina.
        
        Returns:
            True als succesvol, False bij fout
        """
        try:
            logging.info("Start navigatie naar looncomponenten")
            
            # Navigeer naar start menu
            logging.info("Navigeer naar start menu...")
            if not self.selenium_manager.navigate_to_start_menu():
                logging.error("Navigatie naar start menu mislukt")
                return False
            
            # Klik op Applicatiebeheer
            logging.info("Klik op Applicatiebeheer...")
            applicatiebeheer = self.selenium_manager._wait_for_clickable(
                By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Applicatiebeheer']"
            )
            if not self.selenium_manager._safe_click(applicatiebeheer, "Applicatiebeheer"):
                logging.error("Klikken op Applicatiebeheer mislukt")
                return False
            
            # Klik op Looncomponenten
            logging.info("Klik op Looncomponenten...")
            looncomponenten = self.selenium_manager._wait_for_clickable(
                By.XPATH, "//div[@class='awjat-m-item' and text()='Looncomponenten']"
            )
            if not self.selenium_manager._safe_click(looncomponenten, "Looncomponenten"):
                logging.error("Klikken op Looncomponenten mislukt")
                return False
            
            logging.info("Succesvol genavigeerd naar looncomponenten")
            return True
            
        except Exception as e:
            logging.error(f"Fout bij navigeren naar looncomponenten: {e}")
            return False
    
    def download_looncomponenten(self, euururl, euurusername, euurpassword):
        """
        Download looncomponenten uit E-Uur.
        
        Args:
            euururl: E-Uur URL
            euurusername: Gebruikersnaam
            euurpassword: Wachtwoord
            
        Returns:
            True als succesvol gedownload, False bij fout
        """
        logging.info("Start looncomponenten download proces")
        logging.info(f"E-Uur URL: {euururl}")
        logging.info(f"Download directory: {self.download_dir}")
        
        try:
            # Start browser sessie
            logging.info("Start browser sessie...")
            if not self.selenium_manager.start_session():
                logging.error("Start browser sessie mislukt")
                return False
            
            # Navigeer naar E-Uur
            logging.info("Navigeer naar E-Uur...")
            if not self.selenium_manager.navigate_to(euururl):
                logging.error("Navigatie naar E-Uur mislukt")
                return False
            
            # Log in
            logging.info("Log in op E-Uur...")
            if not self.selenium_manager.login(euurusername, euurpassword):
                logging.error("Login mislukt")
                return False
            
            # Navigeer naar looncomponenten
            logging.info("Navigeer naar looncomponenten pagina...")
            if not self.navigate_to_looncomponenten():
                logging.error("Navigatie naar looncomponenten pagina mislukt")
                return False
            
            # Download Excel bestand
            logging.info("Start download van Excel bestand...")
            if not self.selenium_manager.download_excel("Looncomponent.xlsx"):
                logging.error("Download Excel bestand mislukt")
                return False
            
            # Controleer of bestand daadwerkelijk is gedownload
            expected_file = self.download_dir / "Looncomponent.xlsx"
            logging.info(f"Verwacht bestand: {expected_file}")
            logging.info(f"Bestand bestaat na download: {expected_file.exists()}")
            if expected_file.exists():
                logging.info(f"Bestand grootte: {expected_file.stat().st_size} bytes")
            
            logging.info("Looncomponenten download proces succesvol voltooid")
            return True
            
        except Exception as e:
            logging.error(f"Onverwachte fout tijdens download proces: {e}")
            return False
        finally:
            logging.info("Sluit browser sessie...")
            self.selenium_manager.close_session()


class EuurOntbrekendeUrenDownloader:
    """
    Specifieke class voor het downloaden van ontbrekende uren uit E-Uur.
    """
    
    def __init__(self, base_dir, download_dir, headless = True):
        """
        Initialiseer de E-Uur ontbrekende uren downloader.
        
        Args:
            base_dir: Basis directory voor downloads
            download_dir: Specifieke download directory (optioneel)
            headless: Of de browser in headless mode moet draaien
        """
        self.base_dir = Path(base_dir)
        self.download_dir = Path(download_dir)
        
        config = {
            'download_dir': str(self.download_dir),
            'headless': headless,
            'timeout': 10
        }
        
        self.selenium_manager = SeleniumManager(config)
    
    def navigate_to_ontbrekende_uren(self):
        """
        Navigeer naar de ontbrekende uren pagina.
        
        Returns:
            True als succesvol, False bij fout
        """
        try:
            # Navigeer naar start menu
            if not self.selenium_manager.navigate_to_start_menu():
                return False
            
            # Klik op Urenbriefjes
            urenbriefjes = self.selenium_manager._wait_for_clickable(
                By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Urenbriefjes']"
            )
            if not self.selenium_manager._safe_click(urenbriefjes, "Urenbriefjes"):
                return False
            
            # Klik op Ontbrekende urenbriefjes
            ontbrekende_uren = self.selenium_manager._wait_for_clickable(
                By.XPATH, "//div[@class='awjat-m-item' and text()='Ontbrekende urenbriefjes']"
            )
            if not self.selenium_manager._safe_click(ontbrekende_uren, "Ontbrekende urenbriefjes"):
                return False
            
            logging.info("Succesvol genavigeerd naar ontbrekende uren")
            return True
            
        except Exception as e:
            logging.error(f"Fout bij navigeren naar ontbrekende uren: {e}")
            return False
    
    def download_ontbrekende_uren(self, euururl, euurusername, euurpassword):
        """
        Download ontbrekende uren uit E-Uur.
        
        Args:
            euururl: E-Uur URL
            euurusername: Gebruikersnaam
            euurpassword: Wachtwoord
            
        Returns:
            True als succesvol gedownload, False bij fout
        """
        logging.info("Start ontbrekende uren download proces")
        
        try:
            # Start browser sessie
            if not self.selenium_manager.start_session():
                return False
            
            # Navigeer naar E-Uur
            if not self.selenium_manager.navigate_to(euururl):
                return False
            
            # Log in
            if not self.selenium_manager.login(euurusername, euurpassword):
                return False
            
            # Navigeer naar ontbrekende uren
            if not self.navigate_to_ontbrekende_uren():
                return False
            
            # Download Excel bestand
            if not self.selenium_manager.download_excel("Ontbrekende urenbriefjes.xlsx"):
                return False
            
            logging.info("Ontbrekende uren download proces succesvol voltooid")
            return True
            
        except Exception as e:
            logging.error(f"Onverwachte fout tijdens download proces: {e}")
            return False
        finally:
            self.selenium_manager.close_session()


class EuurPlaatsingDownloader:
    """
    Specifieke class voor het downloaden van plaatsingen uit E-Uur.
    Ondersteunt zowel actieve als inactieve plaatsingen.
    """
    
    def __init__(self, base_dir, download_dir, headless = True):
        """
        Initialiseer de E-Uur plaatsing downloader.
        
        Args:
            base_dir: Basis directory voor downloads
            download_dir: Specifieke download directory (optioneel)
            headless: Of de browser in headless mode moet draaien
        """
        self.base_dir = Path(base_dir)
        self.download_dir = Path(download_dir)
        
        config = {
            'download_dir': str(self.download_dir),
            'headless': headless,
            'timeout': 10
        }
        
        self.selenium_manager = SeleniumManager(config)
    
    def navigate_to_plaatsing(self, plaatsing_type):
        """
        Navigeer naar de plaatsing pagina.
        
        Args:
            plaatsing_type: 'actief' of 'inactief'
            
        Returns:
            True als succesvol, False bij fout
        """
        try:
            # Navigeer naar start menu
            if not self.selenium_manager.navigate_to_start_menu():
                return False
            
            # Klik op Plaatsingen
            plaatsingen = self.selenium_manager._wait_for_clickable(
                By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Plaatsingen']"
            )
            if not self.selenium_manager._safe_click(plaatsingen, "Plaatsingen"):
                return False
            
            # Klik op de juiste optie (Overzicht voor actief, Inactief voor inactief)
            if plaatsing_type.lower() == 'actief':
                optie = self.selenium_manager._wait_for_clickable(
                    By.XPATH, "//div[@class='awjat-m-item' and text()='Overzicht']"
                )
                if not self.selenium_manager._safe_click(optie, "Overzicht"):
                    return False
            elif plaatsing_type.lower() == 'inactief':
                optie = self.selenium_manager._wait_for_clickable(
                    By.XPATH, "//div[@class='awjat-m-item' and text()='Inactief']"
                )
                if not self.selenium_manager._safe_click(optie, "Inactief"):
                    return False
            else:
                logging.error(f"Ongeldig plaatsing type: {plaatsing_type}. Gebruik 'actief' of 'inactief'")
                return False
            
            logging.info(f"Succesvol genavigeerd naar {plaatsing_type} plaatsingen")
            return True
            
        except Exception as e:
            logging.error(f"Fout bij navigeren naar {plaatsing_type} plaatsingen: {e}")
            return False
    
    def download_plaatsing(self, euururl, euurusername, euurpassword, plaatsing_type):
        """
        Download plaatsingen uit E-Uur.
        
        Args:
            euururl: E-Uur URL
            euurusername: Gebruikersnaam
            euurpassword: Wachtwoord
            plaatsing_type: 'actief' of 'inactief'
            
        Returns:
            True als succesvol gedownload, False bij fout
        """
        logging.info(f"Start {plaatsing_type} plaatsingen download proces")
        
        try:
            # Start browser sessie
            if not self.selenium_manager.start_session():
                return False
            
            # Navigeer naar E-Uur
            if not self.selenium_manager.navigate_to(euururl):
                return False
            
            # Log in
            if not self.selenium_manager.login(euurusername, euurpassword):
                return False
            
            # Navigeer naar plaatsingen
            if not self.navigate_to_plaatsing(plaatsing_type):
                return False
            
            # Download Excel bestand
            if not self.selenium_manager.download_excel("Plaatsing.xlsx"):
                return False
            
            logging.info(f"{plaatsing_type.capitalize()} plaatsingen download proces succesvol voltooid")
            return True
            
        except Exception as e:
            logging.error(f"Onverwachte fout tijdens {plaatsing_type} plaatsingen download proces: {e}")
            return False
        finally:
            self.selenium_manager.close_session()


class EuurUrenRapportageDownloader:
    """
    Specifieke class voor het downloaden van urenrapportages uit E-Uur.
    Ondersteunt zowel standaard als een-maand rapportages.
    """
    
    def __init__(self, base_dir, download_dir, headless = True):
        """
        Initialiseer de E-Uur urenrapportage downloader.
        
        Args:
            base_dir: Basis directory voor downloads
            download_dir: Specifieke download directory (optioneel)
            headless: Of de browser in headless mode moet draaien
        """
        self.base_dir = Path(base_dir)
        self.download_dir = Path(download_dir)
        
        config = {
            'download_dir': str(self.download_dir),
            'headless': headless,
            'timeout': 10
        }
        
        self.selenium_manager = SeleniumManager(config)
    
    def navigate_to_urenrapportage(self):
        """
        Navigeer naar de urenrapportage pagina.
        
        Returns:
            True als succesvol, False bij fout
        """
        try:
            # Navigeer naar start menu
            if not self.selenium_manager.navigate_to_start_menu():
                return False
            
            # Klik op Rapportage
            rapportage = self.selenium_manager._wait_for_clickable(
                By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Rapportage']"
            )
            if not self.selenium_manager._safe_click(rapportage, "Rapportage"):
                return False
            
            # Klik op Urenrapportage
            urenrapportage = self.selenium_manager._wait_for_clickable(
                By.XPATH, "//div[@class='awjat-m-item' and text()='Urenrapportage']"
            )
            if not self.selenium_manager._safe_click(urenrapportage, "Urenrapportage"):
                return False
            
            logging.info("Succesvol genavigeerd naar urenrapportage")
            return True
            
        except Exception as e:
            logging.error(f"Fout bij navigeren naar urenrapportage: {e}")
            return False
    
    def _setup_date_filters(self, rapportage_type, custom_start_date=None, custom_end_date=None):
        """
        Stel datum filters in voor de urenrapportage. Aangepaste datums hebben voorrang.
        
        Args:
            rapportage_type: 'standaard' of 'een_maand'
            custom_start_date (datetime, optional): Aangepaste startdatum.
            custom_end_date (datetime, optional): Aangepaste einddatum.
            
        Returns:
            Tuple van (start_datum, eind_datum) als datetime objecten
        """
        if custom_start_date and custom_end_date:
            logging.info(f"Gebruik van aangepaste datums: {custom_start_date.strftime('%d-%m-%Y')} tot {custom_end_date.strftime('%d-%m-%Y')}")
            return custom_start_date, custom_end_date

        if rapportage_type.lower() == 'standaard':
            # Vorige maand tot huidige maand
            start_datum = datetime.now().replace(day=1) - relativedelta(months=1)
            eind_datum = datetime.now().replace(day=1) + relativedelta(months=1) - timedelta(days=1)
        elif rapportage_type.lower() == 'een_maand':
            # Huidige maand
            start_datum = datetime.now().replace(day=1)
            eind_datum = datetime.now().replace(day=1) + relativedelta(months=1) - timedelta(days=1)
        else:
            logging.error(f"Ongeldig rapportage type: {rapportage_type}")
            return None, None
        
        logging.info(f"Gebruik van standaard rapportage type '{rapportage_type}': {start_datum.strftime('%d-%m-%Y')} tot {eind_datum.strftime('%d-%m-%Y')}")
        return start_datum, eind_datum
    
    def _apply_date_filters(self, start_datum, eind_datum):
        """
        Pas datum filters toe op de urenrapportage.
        
        Args:
            start_datum: Start datum object
            eind_datum: Eind datum object
            
        Returns:
            True als succesvol, False bij fout
        """
        try:
            # Klik op filter knop
            filter_button = self.selenium_manager._wait_for_clickable(
                By.XPATH, "(//i[@class='toggle down fa fa-filter'])[2]"
            )
            if not self.selenium_manager._safe_click(filter_button, "filter knop"):
                return False
            
            # Wacht een kort moment totdat de filter-sectie is geopend.
            time.sleep(1)

            # Vul datums in met JavaScript en trigger een 'change' event.
            # We targeten het tweede element ([1]), gebaseerd op de werkende logica van het oude script.
            start_date_str = start_datum.strftime('%d-%m-%Y')
            self.selenium_manager.driver.execute_script(
                f"let el = document.getElementsByName('date[start]')[1]; el.value = '{start_date_str}'; el.dispatchEvent(new Event('change'));"
            )
            logging.info(f"Startdatum ingesteld op: {start_date_str}")
            
            end_date_str = eind_datum.strftime('%d-%m-%Y')
            self.selenium_manager.driver.execute_script(
                f"let el = document.getElementsByName('date[end]')[1]; el.value = '{end_date_str}'; el.dispatchEvent(new Event('change'));"
            )
            logging.info(f"Einddatum ingesteld op: {end_date_str}")
            
            # Klik op zoek knop
            search_button = self.selenium_manager._wait_for_clickable(
                By.XPATH, '(//button[@title="Zoeken" and contains(@class, "akyla-widget-button")])[2]'
            )
            if not self.selenium_manager._safe_click(search_button, "zoek knop"):
                return False
            
            # Wacht even voor de resultaten
            time.sleep(10)
            
            logging.info("Datum filters succesvol toegepast")
            return True
            
        except Exception as e:
            logging.error(f"Fout bij toepassen datum filters: {e}")
            return False
    
    def download_urenrapportage(self, euururl, euurusername, euurpassword, rapportage_type='standaard', start_datum=None, eind_datum=None):
        """
        Download urenrapportage uit E-Uur.
        
        Args:
            euururl: E-Uur URL
            euurusername: Gebruikersnaam
            euurpassword: Wachtwoord
            rapportage_type: 'standaard' of 'een_maand'
            start_datum (datetime, optional): Aangepaste startdatum.
            eind_datum (datetime, optional): Aangepaste einddatum.
            
        Returns:
            True als succesvol gedownload, False bij fout
        """
        logging.info(f"Start {rapportage_type} urenrapportage download proces")
        
        try:
            # Start browser sessie
            if not self.selenium_manager.start_session():
                return False
            
            # Navigeer naar E-Uur
            if not self.selenium_manager.navigate_to(euururl):
                return False
            
            # Log in
            if not self.selenium_manager.login(euurusername, euurpassword):
                return False
            
            # Navigeer naar urenrapportage
            if not self.navigate_to_urenrapportage():
                return False
            
            # Bepaal datums
            start_datum_obj, eind_datum_obj = self._setup_date_filters(rapportage_type, start_datum, eind_datum)
            if start_datum_obj is None or eind_datum_obj is None:
                return False
            
            # Pas datum filters toe
            if not self._apply_date_filters(start_datum_obj, eind_datum_obj):
                return False
            
            # Download Excel bestand
            default_filename = "Urenrapportage.xlsx"
            if not self.selenium_manager.download_excel(default_filename):
                return False
            
            # Hernoem bestand met datums
            new_filename = f"Urenrapportage_{start_datum_obj.strftime('%Y-%m-%d')}_{eind_datum_obj.strftime('%Y-%m-%d')}.xlsx"
            if not self.selenium_manager._rename_downloaded_file(default_filename, new_filename):
                return False
            
            logging.info(f"{rapportage_type.capitalize()} urenrapportage download proces succesvol voltooid")
            return True
            
        except Exception as e:
            logging.error(f"Onverwachte fout tijdens {rapportage_type} urenrapportage download proces: {e}")
            return False
        finally:
            self.selenium_manager.close_session()


class EuurLoonPerPlaatsingDownloader:
    """
    Class voor het ophalen van looncomponenten per plaatsing (actief én inactief) uit E-Uur.
    """
    def __init__(self, headless=True):
        self.headless = headless

    def _login_and_navigate(self, selenium_manager, euururl, euurusername, euurpassword):
        if not selenium_manager.start_session():
            return False
        if not selenium_manager.navigate_to(euururl):
            return False
        if not selenium_manager.login(euurusername, euurpassword):
            return False
        return True

    def _navigate_to_plaatsingen(self, selenium_manager):
        # Navigeer naar start menu
        if not selenium_manager.navigate_to_start_menu():
            return False
        # Klik op Plaatsingen
        plaatsingen = selenium_manager._wait_for_clickable(
            By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Plaatsingen']"
        )
        if not selenium_manager._safe_click(plaatsingen, "Plaatsingen"):
            return False
        # Klik op Overzicht (actief) - altijd naar Overzicht, want daar staan alle plaatsingen
        overzicht = selenium_manager._wait_for_clickable(
            By.XPATH, "//div[@class='awjat-m-item' and text()='Overzicht']"
        )
        if not selenium_manager._safe_click(overzicht, "Overzicht"):
            return False
        return True

    def _zoek_en_klik_op_plaatsing(self, selenium_manager, plaatsing_id):
        max_attempts = 10
        attempts = 0
        while attempts < max_attempts:
            try:
                rij = WebDriverWait(selenium_manager.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, f"//tr[@module='confirmedassignmentmerger' and @objectid='{plaatsing_id}']"))
                )
                rij.click()
                return True
            except Exception:
                try:
                    next_button = selenium_manager.driver.find_element(By.CSS_SELECTOR, "i.pager.fa-angle-right")
                    next_button.click()
                    time.sleep(1)
                except Exception:
                    break
                attempts += 1
        return False

    def _lees_looncomponenten_tabel(self, selenium_manager, plaatsing_id, werknemer):
        try:
            WebDriverWait(selenium_manager.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//tr[@tablename='AssignmentcomponentTable']"))
            )
        except Exception:
            return pd.DataFrame()
        looncomponenten_data = []
        try:
            table_rows = selenium_manager.driver.find_elements(By.XPATH, "//tr[@tablename='AssignmentcomponentTable']")
            for rij in table_rows:
                try:
                    cells = rij.find_elements(By.TAG_NAME, "td")
                    if len(cells) > 1:
                        looncomponent = cells[0].find_element(By.CLASS_NAME, "value").text
                        loon = cells[6].find_element(By.CLASS_NAME, "value").text
                        looncomponenten_data.append({
                            'ID': plaatsing_id,
                            'Looncomponent': looncomponent,
                            'Loon': loon,
                            'Werknemer': werknemer
                        })
                except Exception:
                    continue
        except Exception:
            pass
        return pd.DataFrame(looncomponenten_data)

    def download_loon_per_plaatsing(self, euururl, euurusername, euurpassword, plaatsingen_lijst):
        """
        Haal looncomponenten op voor alle plaatsingen in de lijst.
        Args:
            euururl: E-Uur URL
            euurusername: Gebruikersnaam
            euurpassword: Wachtwoord
            plaatsingen_lijst: lijst van dicts met minimaal 'ID' en 'Werknemer'
        Returns:
            DataFrame met alle looncomponenten
        """
        alle_data = []
        for plaatsing in plaatsingen_lijst:
            plaatsing_id = plaatsing['ID']
            werknemer = plaatsing.get('Werknemer', '')
            selenium_manager = SeleniumManager({'headless': self.headless, 'timeout': 10})
            try:
                if not self._login_and_navigate(selenium_manager, euururl, euurusername, euurpassword):
                    continue
                if not self._navigate_to_plaatsingen(selenium_manager):
                    continue
                if not self._zoek_en_klik_op_plaatsing(selenium_manager, plaatsing_id):
                    logging.warning(f"Plaatsing {plaatsing_id} niet gevonden")
                    continue
                df = self._lees_looncomponenten_tabel(selenium_manager, plaatsing_id, werknemer)
                if not df.empty:
                    alle_data.append(df)
                    logging.info(f"Data van plaatsing {plaatsing_id} succesvol toegevoegd aan de dataframe.")
            finally:
                selenium_manager.close_session()
        if alle_data:
            return pd.concat(alle_data, ignore_index=True)
        return pd.DataFrame()