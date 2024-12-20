import requests
from dotenv import load_dotenv, set_key
import os

def get_access_token():

    load_dotenv()

    # Vul deze variabelen in met je eigen gegevens
    tenant_id = os.getenv('TENANT_ID')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')

    # URL om het token op te halen
    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'

    # De gegevens voor het aanvragen van het token
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://management.azure.com/.default'
    }

    # Maak een POST-verzoek om het token te verkrijgen
    response = requests.post(token_url, data=token_data)

    # Controleer of het token is verkregen
    if response.status_code == 200:
        access_token = response.json().get('access_token')
        print('Bearer Token verkregen:', access_token)

    else:
        print('Fout bij het ophalen van het token:', response.status_code)
        print(response.text)

    return access_token
