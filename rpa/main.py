from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException

import time
import os
import datetime
from dotenv import load_dotenv

if os.path.exists("/Users/maxrood/werk/greit/klanten/stiek/cost_management/.env"):
        load_dotenv()
        print("Lokaal draaien: .env bestand gevonden en geladen.")

# Variabelen definiëren
username = os.getenv('EUURUSERNAME')
password = os.getenv('EUURPASSWORD')

# Stap 1: Browser driver instellen en configureren
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.maximize_window()

try:
    # Stap 2: Open de gewenste URL
    driver.get("https://stiek.flexportal.eu")
    print("Stap 2: URL geopend")

    # Stap 3: Wacht tot de pagina is geladen
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
    print("Stap 3: Pagina geladen, gebruikersnaam veld gevonden")

    # Stap 4: Vul inloggegevens in
    username = driver.find_element(By.NAME, "username")
    password = driver.find_element(By.NAME, "password")
    username.send_keys(username)
    password.send_keys(password)
    print("Stap 4: Inloggegevens ingevoerd")

    # Stap 5: Klik op de inlogknop
    login_button = driver.find_element(By.NAME, "euur")
    login_button.click()
    print("Stap 5: Inlogknop aangeklikt")

    # Stap 6: Wacht tot inloggen voltooid is
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-id='dashboard']")))
    print("Stap 6: Inloggen voltooid, dashboard geladen")

    # Stap 7: Klik op de 'Start' knop
    start_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.start-menu.akyla-widget-button"))
    )
    start_button.click()
    print("Stap 7: 'Start' knop aangeklikt")

    # Stap 8: Klik op de 'Rapportage' knop
    report_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'awjat-sub-m-item') and text()='Rapportage']"))
    )
    report_button.click()
    print("Stap 8: 'Rapportage' knop aangeklikt")

    # Stap 9: Klik op de 'Urenrapportage' knop
    timecard_report_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//div[@class='awjat-m-item' and @data-url='timecardreport']"))
    )
    timecard_report_button.click()
    print("Stap 9: 'Urenrapportage' knop aangeklikt")

    # Stap 10: Wacht even om ervoor te zorgen dat de download voltooid is
    time.sleep(5)
    print("Stap 10: Wacht tot de actie voltooid is")
    print("Volgende stap...")

    # Stap 11: Vul de eerste datum in het zoekveld in
    search_input = driver.find_element(By.CSS_SELECTOR, "input[data-gtm-form-interact-field-id='9']")

    # Klik op het veld voordat je de datum invoert om het interactief te maken
    search_input.click()
    start_date = datetime.date(2024, 10, 1)
    
    try:
        # Probeer de datum in te voeren
        search_input.send_keys(start_date.strftime("%Y-%m-%d"))
        print(f"Stap 11: Datumveld ingevuld - {start_date.strftime('%Y-%m-%d')}")
    except ElementNotInteractableException:
        # Als het veld niet interactief is, probeer de waarde direct via JavaScript in te stellen
        driver.execute_script("arguments[0].value = arguments[1];", search_input, start_date.strftime("%Y-%m-%d"))
        print(f"Stap 11: Datumveld ingevuld met JavaScript - {start_date.strftime('%Y-%m-%d')}")

    # Stap 12: Klik op de zoekknop (vergrootglas)
    search_button = driver.find_element(By.CSS_SELECTOR, "a.search > i.fa-search")
    search_button.click()
    print("Stap 12: Zoekknop aangeklikt")

    # Eventueel een tweede datum invullen
    # Je zou hier een tweede datum kunnen invoeren door dezelfde stappen herhalen en vervolgens weer op de zoekknop te klikken.

    # Stap 13: Wacht even om de actie te voltooien (optioneel)
    time.sleep(5)
    print("Stap 13: Actie voltooid")

except TimeoutException:
    print("Een element kon niet op tijd worden gevonden, controleer je selectors en probeer opnieuw.")
finally:
    # Stap 10: Sluit de browser
    driver.quit()
