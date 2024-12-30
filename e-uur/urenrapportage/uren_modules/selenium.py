from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from uren_modules.log import log
import pandas as pd
import time
import os

def urenrapportage_bestand_opslaan(euururl, euurusername, euurpassword, greit_connection_string, klant, script, scriptid, bron):
    log(greit_connection_string, klant, bron, f"Urenrapportage ophalen", script, scriptid)
                
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")  # Disables GPU hardware acceleration (optioneel)
    chrome_options.add_argument("--headless")  # Zet de browser in headless mode
    chrome_options.add_argument("--no-sandbox")  # Zorgt ervoor dat je script kan draaien in een beveiligde omgeving (optioneel)
    
    # Instellen van downloadopties
    prefs = {
        "download.default_directory": "/Users/maxrood/werk/greit/klanten/stiek/e-uur/urenrapportage/uren_file",  # Pas dit pad aan naar de gewenste map
        "download.prompt_for_download": False,  # Geen download prompt
        "download.directory_upgrade": True,  # Upgrade de directory indien nodig
        "safebrowsing.enabled": True  # Zorg ervoor dat Safe Browsing ingeschakeld is
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Stap 1: WebDriver configureren met deze opties
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        # Stap 2: Open de gewenste URL
        try:
            driver.get(euururl)
            print("Stap 2: URL geopend")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het openen van de URL: {str(e)}", script, scriptid)

        # Stap 3: Wacht tot de pagina is geladen
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
            print("Stap 3: Pagina geladen, gebruikersnaam veld gevonden")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het wachten tot de pagina is geladen: {str(e)}", script, scriptid)

        # Stap 4: Vul inloggegevens in
        try:
            username = driver.find_element(By.NAME, "username")
            password = driver.find_element(By.NAME, "password")
            username.send_keys(euurusername)
            password.send_keys(euurpassword)
            print("Stap 4: Inloggegevens ingevoerd")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het inloggen: {str(e)}", script, scriptid)

        # Stap 5: Klik op de inlogknop
        try:
            login_button = driver.find_element(By.NAME, "euur")
            login_button.click()
            print("Stap 5: Inlogknop aangeklikt")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het klikken op de inlogknop: {str(e)}", script, scriptid)

        # Stap 6: Wacht tot inloggen voltooid is
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-id='dashboard']")))
            print("Stap 6: Inloggen voltooid, dashboard geladen")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het wachten tot inloggen voltooid is: {str(e)}", script, scriptid)

        # Stap 7: Klik op de 'Start' knop
        try:
            start_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.start-menu.akyla-widget-button"))
            )
            start_button.click()
            print("Stap 7: 'Start' knop aangeklikt")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het klikken op de 'Start' knop: {str(e)}", script, scriptid)

        # Stap 8: Klik op de 'Rapportage' element
        try:
            plaatsingen_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Rapportage']"))
            )
            plaatsingen_element.click()
            print("Stap 8: 'Rapportage' element aangeklikt")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het klikken op het 'Rapportage' element: {str(e)}", script, scriptid)

        # Stap 9: klik op de specifieke optie | Urenrapportage
        try:
            inactief_option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-m-item' and text()='Urenrapportage']"))
            )
            inactief_option.click()
            print("Stap 9: 'Urenrapportage' optie aangeklikt")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het klikken op de 'Urenrapportage' optie: {str(e)}", script, scriptid)
        
        # Stap 10: Klik op de filter knop
        try:
            filter_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "(//i[@class='toggle down fa fa-filter'])[2]"))
            )
            filter_button.click()
            print("Tweede filterknop succesvol aangeklikt!")
        except Exception as e:
            print(f"Fout bij het klikken op de tweede filterknop: {e}")
            
        # Start- en einddatum bepalen
        start_datum = datetime.now().replace(day=1) - relativedelta(months=1)
        eind_datum = datetime.now().replace(day=1) + relativedelta(months=1) - timedelta(days=1)
            
        # Stap 11: Start Datum invoeren
        try:
            # Zoek alle elementen met de naam 'date[start]'
            date_inputs = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.NAME, "date[start]"))
            )
            # Selecteer de tweede (index 1)
            date_input = date_inputs[1]
            date_input.clear()  # Wis het veld (indien nodig)
            date_input.send_keys(f"{start_datum.strftime('%d-%m-%Y')}")  # Vul hier de gewenste datum in
            print("Startdatum succesvol ingevuld!")
        except Exception as e:
            print(f"Fout bij het invullen van de startdatum: {e}")

        # Stap 12: Eind Datum invoeren
        try:
            # Zoek alle elementen met de naam 'date[end]'
            date_inputs = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.NAME, "date[end]"))
            )
            # Selecteer de tweede (index 1)
            date_input = date_inputs[1]
            date_input.clear()  # Wis het veld (indien nodig)
            date_input.send_keys(f"{eind_datum.strftime('%d-%m-%Y')}")  # Vul hier de gewenste datum in
            print("Einddatum succesvol ingevuld!")
        except Exception as e:
            print(f"Fout bij het invullen van de einddatum: {e}")
            
        # Stap 13: Klik op de zoek knop
        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '(//button[@title="Zoeken" and contains(@class, "akyla-widget-button")])[2]'))
            )
            search_button.click()
            print("Tweede zoek-knop succesvol aangeklikt!")
        except Exception as e:
            print(f"Fout bij het klikken op de tweede zoek-knop: {e}")
        
        time.sleep(10)
        
        # Stap 11: Klik op de Excel-knop om te downloaden
        try:
            excel_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '(//button[@title="Excel" and @data-controller="excel"])[2]'))
            )
            excel_button.click()
            print("Excel-knop succesvol aangeklikt!")
            
            # Wachten op het downloaden van het bestand
            download_dir = "/Users/maxrood/werk/greit/klanten/stiek/e-uur/urenrapportage/uren_file"
            default_filename = "Urenrapportage.xlsx"  # Pas aan
            new_filename = f"Urenrapportage_{start_datum.strftime('%Y-%m-%d')}_{eind_datum.strftime('%Y-%m-%d')}.xlsx"
            timeout = 30
            start_time = time.time()

            while not os.path.exists(os.path.join(download_dir, default_filename)):
                if time.time() - start_time > timeout:
                    log(greit_connection_string, klant, bron, "Download duurde te lang!", script, scriptid)
                    break
                time.sleep(1)

            # Hernoemen van het bestand
            if os.path.exists(os.path.join(download_dir, default_filename)):
                new_file_path = os.path.join(download_dir, new_filename)
                os.rename(
                    os.path.join(download_dir, default_filename),
                    new_file_path
                )
                print(f"Bestand succesvol hernoemd naar: {new_filename}")
                log(greit_connection_string, klant, bron, f"Bestand succesvol hernoemd naar: {new_filename}", script, scriptid)
            else:
                print("Gedownload bestand niet gevonden!")
                log(greit_connection_string, klant, bron, "Gedownload bestand niet gevonden!", script, scriptid)
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het klikken of downloaden: {str(e)}", script, scriptid)

    finally:
        # Stap 11: Sluit de browser
        try:
            driver.quit()
            print("Stap 11: Browser gesloten")
        except Exception as e:
            log(greit_connection_string, klant, bron, f"Fout bij het sluiten van de browser: {str(e)}", script, scriptid)