from datetime import datetime, timedelta
from cost_modules.log import log
import pandas as pd
import requests

def generate_cost_dataframe(subscription_id, klant, bron, script, script_id, greit_connection_string, bearer_token):
    
    # API URL voor het opvragen van kosten
    cost_url = f'https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01'

    # Bereken de startdatum als de eerste dag van de vorige maand
    start_date = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
    # Bereken de einddatum als de huidige datum
    end_date = datetime.now().strftime("%Y-%m-%d")

    # JSON-body voor het opvragen van de kosten voor de huidige maand per dag
    cost_body = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date,
            "to": end_date
        },
        "dataset": {
            "granularity": "Daily",
            "aggregation": {
                "totalCost": {
                    "name": "PreTaxCost",
                    "function": "Sum"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": "ServiceName"
                }
            ]
        }
    }

    # Headers voor het verzoek
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }

    # Maak een POST-verzoek om de kosten op te vragen
    try:
        print("Maak een POST-verzoek om de kosten op te vragen")
        log(greit_connection_string, klant, bron, "Maak een POST-verzoek om de kosten op te vragen", script, script_id)
        response = requests.post(cost_url, json=cost_body, headers=headers)
    except Exception as e:
        print(f"Fout bij het opvragen van de kosten: {e}")
        log(greit_connection_string, klant, bron, f"Fout bij het opvragen van de kosten: {e}", script, script_id)
        exit(1)

    # Controleer of de gegevens succesvol zijn opgevraagd
    if response.status_code == 200:
        print('Kosteninformatie ontvangen')
        log(greit_connection_string, klant, bron, "Kosteninformatie ontvangen", script, script_id)
        json_data = response.json()
    else:
        print('Fout bij het ophalen van kosteninformatie:', response.status_code)
        print(response.text)
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Fout bij het ophalen van kosteninformatie: {response.status_code}", script, script_id)
        exit(1)

    # Maak een DataFrame van de gegevens
    column_names = [col["name"] for col in json_data["properties"]["columns"]]
    rows = json_data["properties"]["rows"]
    df = pd.DataFrame(rows, columns=column_names)

    # Voeg Klant kolom toe
    df['Klant'] = klant
    
    return df