import pandas as pd
import requests
import logging

def generate_cost_dataframe(subscription_id, klant, bearer_token, start_datum, eind_datum):
    
    # Printen van begin en eind datum
    logging.info(f"Ophalen cost dataframe vanaf {start_datum} tot {eind_datum}")
    
    # API URL voor het opvragen van kosten
    cost_url = f'https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01'

    # JSON-body voor het opvragen van de kosten voor de huidige maand per dag
    cost_body = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_datum,
            "to": eind_datum
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
        logging.info("Maak een POST-verzoek om de kosten op te vragen")
        response = requests.post(cost_url, json=cost_body, headers=headers)
    except Exception as e:
        logging.error(f"Fout bij het opvragen van de kosten: {e}")
        exit(1)

    # Controleer of de gegevens succesvol zijn opgevraagd
    if response.status_code == 200:
        logging.info('Kosteninformatie ontvangen')
        json_data = response.json()
    else:
        logging.error('Fout bij het ophalen van kosteninformatie:', response.status_code)
        exit(1)

    # Maak een DataFrame van de gegevens
    column_names = [col["name"] for col in json_data["properties"]["columns"]]
    rows = json_data["properties"]["rows"]
    df = pd.DataFrame(rows, columns=column_names)

    # Voeg Klant kolom toe
    df['Klant'] = klant
    
    return df