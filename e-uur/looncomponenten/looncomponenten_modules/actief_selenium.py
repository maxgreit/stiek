from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium import webdriver
import pandas as pd
import logging

def looncomponenten_ophalen(euururl, euurusername, euurpassword, target_object_id, werknemer):
    logging.info(f"Looncomponenten ophalen voor {id} {werknemer}")
                
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")  # Disables GPU hardware acceleration (optioneel)
    chrome_options.add_argument("--headless")  # Zet de browser in headless mode
    chrome_options.add_argument("--no-sandbox")  # Zorgt ervoor dat je script kan draaien in een beveiligde omgeving (optioneel)
    
    # Stap 1: Browser driver instellen en configureren
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)  # Pas aan naar de gewenste grootte

    def go_to_next_page():
        """Functie om naar de volgende pagina te navigeren door de 'Volgende' knop te klikken."""
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "i.pager.fa-angle-right")
            next_button.click()
            print("Stap: Volgende pagina geladen")
        except Exception as e:
            print(f"Fout bij het klikken op de volgende knop: {str(e)}")

    try:
        # Stap 2: Open de gewenste URL
        try:
            driver.get(euururl)
            print("Stap 2: URL geopend")
        except Exception as e:
            logging.error("Fout bij het openen van de URL: {str(e)}")

        # Stap 3: Wacht tot de pagina is geladen
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
            print("Stap 3: Pagina geladen, gebruikersnaam veld gevonden")
        except Exception as e:
            logging.error("Fout bij het wachten tot de pagina is geladen: {str(e)}")

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

        # Stap 8: Wacht totdat het element 'Plaatsingen' zichtbaar en klikbaar is
        try:
            plaatsingen_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-sub-m-item' and text()='Plaatsingen']"))
            )
            plaatsingen_element.click()
            print("Stap 8: 'Plaatsingen' element aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op het 'Plaatsingen' element: {str(e)}")

        # **Nieuwe stap**: Klik op de 'Inactief' optie na het klikken op Plaatsingen
        try:
            inactief_option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-m-item' and text()='Overzicht']"))
            )
            inactief_option.click()
            print("Stap 9: 'Overzicht' optie aangeklikt")
        except Exception as e:
            logging.error(f"Fout bij het klikken op de 'Overzicht' optie: {str(e)}")

        # Stap 9: Zoek de specifieke rij op basis van objectid
        try:
            rij = None
            max_attempts = 10  # Maximaal aantal pogingen
            attempts = 0  # Teller voor pogingen
            
            while rij is None and attempts < max_attempts:
                try:
                    # Zoek de rij op basis van de objectid
                    rij = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//tr[@module='confirmedassignmentmerger' and @objectid='{target_object_id}']"))
                    )
                    # Als de rij gevonden is, klik erop
                    rij.click()
                    print(f"Stap 9: Geïnteresseerd in object met ID: {target_object_id}")
                
                except TimeoutException:
                    print(f"Object met ID {target_object_id} niet gevonden op deze pagina.")
                    attempts += 1
                    if attempts < max_attempts:
                        print(f"Probeer het opnieuw. Poging {attempts}/{max_attempts}")
                        go_to_next_page()
                    else:
                        logging.error(f"Maximaal aantal pogingen bereikt. Object met ID {target_object_id} niet gevonden na {max_attempts} pogingen.")
                        break
                
                # Wacht tot de pagina is geladen nadat de volgende pagina is aangeklikt
                if attempts < max_attempts:  # Voorkomt dat er nog wordt gewacht als het maximum is bereikt
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//tr[@module='confirmedassignmentmerger']")))

        except Exception as e:
            logging.error(f"Fout bij het zoeken naar de specifieke rij: {str(e)}")

        # Stap 10: Wacht totdat de tabel opnieuw is geladen na het klikken
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//tr[@tablename='AssignmentcomponentTable']"))
            )
            print("Stap 10: Tabel opnieuw geladen")
        except Exception as e:
            logging.error(f"Fout bij het wachten tot de tabel opnieuw is geladen: {str(e)}")

        # Verkrijg de rijen opnieuw na het laden
        try:
            table_rows = driver.find_elements(By.XPATH, "//tr[@tablename='AssignmentcomponentTable']")
            print("Stap 11: Rijen verkregen")
        except Exception as e: 
            logging.error(f"Fout bij het verkrijgen van de rijen: {str(e)}")
        
        # Lijst om de data op te slaan
        looncomponenten_data = []

        try:
            for rij in table_rows:
                try:
                    # Verkrijg de cellen van de huidige rij
                    cells = rij.find_elements(By.TAG_NAME, "td")
                    
                    # Als de rij de verwachte aantal cellen heeft, extracteer de data
                    if len(cells) > 1:
                        looncomponent = cells[0].find_element(By.CLASS_NAME, "value").text
                        loon = cells[6].find_element(By.CLASS_NAME, "value").text
                        
                        # Voeg de data toe aan de lijst
                        looncomponenten_data.append({
                            'ID': target_object_id,
                            'Looncomponent': looncomponent,
                            'Loon': loon,
                            'Werknemer': werknemer
                        })
                
                except Exception as e:
                    logging.error(f"Fout bij het verkrijgen van de rij: {str(e)}")
                    continue

        except Exception as e:
            logging.error(f"Fout bij het verkrijgen van de rijen: {str(e)}")

        # Zet de lijst om naar een DataFrame
        df = pd.DataFrame(looncomponenten_data)

    except TimeoutException:
        logging.error("Element kon niet op tijd worden gevonden, controleer je selectors en probeer opnieuw.")

        df = pd.DataFrame()  # Lege DataFrame in geval van fout

    finally:
        # Stap 11: Sluit de browser
        try:
            driver.quit()
            print("Stap 11: Browser gesloten")
        except Exception as e:
            logging.error(f"Fout bij het sluiten van de browser: {str(e)}")
    
    # Dataframe check
    if df is None:
        logging.error("Geen DataFrame geretourneerd")

        return
    if df.empty:
        logging.info("DataFrame is leeg")

        return       
     
    return df