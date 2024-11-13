from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import time
import datetime
import os

def zoek_element(driver, by, value, max_retries=5, wait_time=2):
    for poging in range(max_retries):
        try:
            element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((by, value)))
            return element
        except (TimeoutException, NoSuchElementException):
            print(f"Element niet gevonden, poging {poging + 1} van {max_retries}... Wachten en opnieuw proberen.")
            time.sleep(wait_time)  # Wachten voordat je opnieuw probeert
    raise TimeoutException("Het element kon niet worden gevonden na meerdere pogingen.")

# Stap 1: Browser driver instellen en configureren
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.maximize_window()

try:
    # Stap 2: Open de gewenste URL
    driver.get("https://stiek.flexportal.eu")
    print("Stap 2: URL geopend")

    # Stap 3: Wacht tot de pagina is geladen en het gebruikersnaamveld zichtbaar is
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
    print("Stap 3: Pagina geladen, gebruikersnaam veld gevonden")

    # Stap 4: Vul inloggegevens in
    username = driver.find_element(By.NAME, "username")
    password = driver.find_element(By.NAME, "password")
    username.send_keys("max@greit.nl")
    password.send_keys("frt6kma.vdz5EKD1trf")
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

    # Stap 10: Wacht even om ervoor te zorgen dat de pagina is geladen
    time.sleep(5)
    print("Stap 10: Wacht tot de actie voltooid is")

    # Stap 10: Zorg dat het juiste tabblad is geselecteerd
    urenrapportage_tab = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'tab') and text()='Urenrapportage']"))
    )
    urenrapportage_tab.click()
    print("Stap 10: 'Urenrapportage' tabblad geselecteerd")

    # Stap 13: Zoek naar de 'Excel' download knop
    download_button = zoek_element(driver, By.XPATH, "//button[@data-action='atexport' and @data-controller='excel']")
    download_button.click()
    print("Stap 13: 'Excel' download knop aangeklikt")

    # Stap 14: Wacht even om ervoor te zorgen dat de download voltooid is
    time.sleep(10)
    print("Stap 14: Wacht tot de download voltooid is")

    # Stap 15: Bestand hernoemen naar 'test123.xlsx'
    download_path = os.path.expanduser("~/Downloads")
    original_filename = max([f for f in os.listdir(download_path)], key=lambda x: os.path.getctime(os.path.join(download_path, x)))
    original_filepath = os.path.join(download_path, original_filename)
    new_filepath = os.path.join(download_path, "test123.xlsx")
    os.rename(original_filepath, new_filepath)
    print("Stap 15: Bestand hernoemd naar 'test123.xlsx'")

except TimeoutException:
    print("Een element kon niet op tijd worden gevonden, controleer je selectors en probeer opnieuw.")
except ElementNotInteractableException:
    print("Het element is niet interactief, probeer handmatig te scrollen of wacht langer totdat het element beschikbaar is.")
finally:
    # Stap 16: Sluit de browser
    driver.quit()
    print("Stap 16: Browser gesloten")
