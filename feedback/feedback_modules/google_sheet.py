from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import gspread


def get_google_sheet_data(sheet_url, sheet_name, credentials_file_path):
    print("A")
    # Definieer de scope voor Google Sheets API
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    print("B")
    print(credentials_file_path)
    # Maak de credentials aan
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file_path, scope)
    except FileNotFoundError:
        print(f"De credentials bestand '{credentials_file_path}' bestaat niet.")
    print("C")
    try:
        client = gspread.authorize(creds)
    except Exception as e:
        print(f"Fout bij het authorizen van de Google Sheets API: {e}")
    print("D")
    # Open de Google Sheet
    try:
        sheet = client.open_by_url(sheet_url).worksheet(sheet_name)
    except Exception as e:
        print(f"Fout bij het openen van de Google Sheet: {e}")
    print("E")
    # Haal alle gegevens op en laad ze in een Pandas DataFrame
    data = sheet.get_all_records()
    print("F")
    df = pd.DataFrame(data)
    print("G")
    return df