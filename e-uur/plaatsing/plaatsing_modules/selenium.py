from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
import logging
import time
import os

def actieve_plaatsing_bestand_opslaan(euururl, euurusername, euurpassword, base_dir):
    logging.info(f"Plaatsingen ophalen")
                
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")  # Disables GPU hardware acceleration (optioneel)
    chrome_options.add_argument("--headless")  # Zet de browser in headless mode
    chrome_options.add_argument("--no-sandbox")  # Zorgt ervoor dat je script kan draaien in een beveiligde omgeving (optioneel)
    
    # Instellen van downloadopties
    download_dir = os.path.join(base_dir, "e-uur/plaatsing/plaatsing_file")
    
    # Instellen van downloadopties
    prefs = {
        "download.default_directory": download_dir,  # Pas dit pad aan naar de gewenste map
        "download.prompt_for_download": False,  # Geen download prompt
        "download.directory_upgrade": True,  # Upgrade de directory indien nodig
        "safebrowsing.enabled": True  # Zorg ervoor dat Safe Browsing ingeschakeld is
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Stap 1: WebDriver configureren met deze opties
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)  # Pas aan naar de gewenste grootte

    try:
        # Stap 2: Open de gewenste URL
        try:
            driver.get(euururl)
            print("Stap 2: URL geopend")
        except Exception as e:
            logging.error(f"Fout bij het openen van de URL: {str(e)}")

        # Stap 3: Wacht tot de pagina is geladen
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
            print("Stap 3: Pagina geladen, gebruikersnaam veld gevonden")
        except Exception as e:
            logging.error(f"Fout bij het wachten tot de pagina is geladen: {str(e)}")

        # Stap 4: Vul inloggegevens in
        try:
            username = driver.find_element(By.NAME, "username")
            password = driver.find_element(By.NAME, "password")
            username.send_keys(euurusername)
            password.send_keys(euurpassword)
            print("Stap 4: Inloggegevens ingevoerd")
        except Exception as e:
            logging.error(f"Fout bij het invullen van inloggegevens: {str(e)}")

        # Stap 5: Klik op de inlogknop
        try:
            login_button = driver.find_element(By.NAME, "euur")
            login_button.click()
            print("Stap 5: Inlogknop aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de inlogknop: {str(e)}")

        # Stap 6: Wacht tot inloggen voltooid is
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-id='dashboard']")))
            print("Stap 6: Inloggen voltooid, dashboard geladen")
        except Exception as e:
            logging.error(f"Fout bij het wachten tot inloggen voltooid is: {str(e)}")

        # Stap 7: Klik op de 'Start' knop
        try:
            start_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.start-menu.akyla-widget-button"))
            )
            start_button.click()
            print("Stap 7: 'Start' knop aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de 'Start' knop: {str(e)}")

        # Stap 8: Klik op de 'Plaatsingen' element
        try:
            plaatsingen_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Plaatsingen']"))
            )
            plaatsingen_element.click()
            print("Stap 8: 'Plaatsingen' element aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op het 'Plaatsingen' element: {str(e)}")

        # Stap 9: klik op de specifieke optie | Overzicht
        try:
            inactief_option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-m-item' and text()='Overzicht']"))
            )
            inactief_option.click()
            print("Stap 9: 'Overzicht' optie aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de 'Overzicht' optie: {str(e)}")
        
        # Stap 11: Klik op de Excel-knop om te downloaden
        try:
            excel_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '(//button[@title="Excel" and @data-controller="excel"])[2]'))
            )
            excel_button.click()
            print("Excel-knop succesvol aangeklikt!")
            
            # Wachten op het downloaden van het bestand
            filename = "Plaatsing.xlsx"  # Pas aan
            timeout = 30
            start_time = time.time()

            while not os.path.exists(os.path.join(download_dir, filename)):
                if time.time() - start_time > timeout:
                    logging.error("Download duurde te lang!")
                    break
                time.sleep(1)

            if os.path.exists(os.path.join(download_dir, filename)):
                logging.info(f"Bestand {filename} succesvol gedownload!")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de Excel-knop: {str(e)}")

    finally:
        # Stap 11: Sluit de browser
        try:
            driver.quit()
            print("Stap 11: Browser gesloten")
        except Exception as e:
            logging.error(f"Fout bij het sluiten van de browser: {str(e)}")

def inactieve_plaatsing_bestand_opslaan(euururl, euurusername, euurpassword, base_dir):
    logging.info(f"Plaatsingen ophalen")
                
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")  # Disables GPU hardware acceleration (optioneel)
    chrome_options.add_argument("--headless")  # Zet de browser in headless mode
    chrome_options.add_argument("--no-sandbox")  # Zorgt ervoor dat je script kan draaien in een beveiligde omgeving (optioneel)
    
    # Instellen van downloadopties
    download_dir = os.path.join(base_dir, "e-uur/plaatsing/plaatsing_file")
    
    # Instellen van downloadopties
    prefs = {
        "download.default_directory": download_dir,  # Pas dit pad aan naar de gewenste map
        "download.prompt_for_download": False,  # Geen download prompt
        "download.directory_upgrade": True,  # Upgrade de directory indien nodig
        "safebrowsing.enabled": True  # Zorg ervoor dat Safe Browsing ingeschakeld is
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Stap 1: WebDriver configureren met deze opties
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)  # Pas aan naar de gewenste grootte

    try:
        # Stap 2: Open de gewenste URL
        try:
            driver.get(euururl)
            print("Stap 2: URL geopend")
        except Exception as e:
            logging.error(f"Fout bij het openen van de URL: {str(e)}")

        # Stap 3: Wacht tot de pagina is geladen
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
            print("Stap 3: Pagina geladen, gebruikersnaam veld gevonden")
        except Exception as e:
            logging.error(f"Fout bij het wachten tot de pagina is geladen: {str(e)}")

        # Stap 4: Vul inloggegevens in
        try:
            username = driver.find_element(By.NAME, "username")
            password = driver.find_element(By.NAME, "password")
            username.send_keys(euurusername)
            password.send_keys(euurpassword)
            print("Stap 4: Inloggegevens ingevoerd")
        except Exception as e:
            logging.error(f"Fout bij het invullen van inloggegevens: {str(e)}")

        # Stap 5: Klik op de inlogknop
        try:
            login_button = driver.find_element(By.NAME, "euur")
            login_button.click()
            print("Stap 5: Inlogknop aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de inlogknop: {str(e)}")

        # Stap 6: Wacht tot inloggen voltooid is
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-id='dashboard']")))
            print("Stap 6: Inloggen voltooid, dashboard geladen")
        except Exception as e:
            logging.error(f"Fout bij het wachten tot inloggen voltooid is: {str(e)}")

        # Stap 7: Klik op de 'Start' knop
        try:
            start_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.start-menu.akyla-widget-button"))
            )
            start_button.click()
            print("Stap 7: 'Start' knop aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de 'Start' knop: {str(e)}")

        # Stap 8: Klik op de 'Plaatsingen' element
        try:
            plaatsingen_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Plaatsingen']"))
            )
            plaatsingen_element.click()
            print("Stap 8: 'Plaatsingen' element aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op het 'Plaatsingen' element: {str(e)}")

        # Stap 9: klik op de specifieke optie | Inactief
        try:
            inactief_option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-m-item' and text()='Inactief']"))
            )
            inactief_option.click()
            print("Stap 9: 'Inactief' optie aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de 'Inactief' optie: {str(e)}")
        
        # Stap 11: Klik op de Excel-knop om te downloaden
        try:
            excel_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '(//button[@title="Excel" and @data-controller="excel"])[2]'))
            )
            excel_button.click()
            print("Excel-knop succesvol aangeklikt!")
            
            # Wachten op het downloaden van het bestand
            filename = "Plaatsing.xlsx"  # Pas aan
            timeout = 30
            start_time = time.time()

            while not os.path.exists(os.path.join(download_dir, filename)):
                if time.time() - start_time > timeout:
                    logging.error("Download duurde te lang!")
                    break
                time.sleep(1)

            if os.path.exists(os.path.join(download_dir, filename)):
                logging.info(f"Bestand {filename} succesvol gedownload!")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de Excel-knop: {str(e)}")

    finally:
        # Stap 11: Sluit de browser
        try:
            driver.quit()
            print("Stap 11: Browser gesloten")
        except Exception as e:
            logging.error(f"Fout bij het sluiten van de browser: {str(e)}")