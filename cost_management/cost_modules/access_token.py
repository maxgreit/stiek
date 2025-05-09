import requests

def get_access_token(tenant_id, client_id, client_secret):

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
