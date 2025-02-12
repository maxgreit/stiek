from cost_modules.database import apply_clearing_and_writing
from cost_modules.type_mapping import apply_type_conversion
from cost_modules.table_mapping import apply_transformation
from cost_modules.cost_api import generate_cost_dataframe
from cost_modules.access_token import get_access_token
from cost_modules.config import determine_script_id
from cost_modules.log import end_log, setup_logging
from dateutil.relativedelta import relativedelta
from cost_modules.env_tool import env_check
from datetime import datetime, timedelta
import logging
import time
import os

def main():
    
    env_check()

    # Script configuratie
    klant = "Stiek"
    script = "Cost Management"
    bron = 'Azure'
    start_time = time.time()

    # Verbindingsinstellingen
    tenant_id = os.getenv('TENANT_ID')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    subscription_id = os.getenv('SUBSCRIPTION_ID')
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    tabel = "Kosten"
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string)

    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)

    try:
        # Access token genereren
        bearer_token = get_access_token(tenant_id, client_id, client_secret)

        # Start datum en eind datum bepalen
        vandaag = datetime.today()
        start_datum = (vandaag - relativedelta(months=1)).replace(day=1)  # Begin vorige maand
        eind_datum = (vandaag + relativedelta(months=1)).replace(day=1) - timedelta(days=1)  # Eind deze maand
        start_datum = start_datum.strftime('%Y-%m-%d')
        eind_datum = eind_datum.strftime('%Y-%m-%d')

        # Creëer cost dataframe
        df = generate_cost_dataframe(subscription_id, klant, bearer_token, start_datum, eind_datum)

        # Transformeer kolommen
        df = apply_transformation(df)

        # Verander data types
        df = apply_type_conversion(df)
        
        # Leeghalen en toeschrijven van data
        apply_clearing_and_writing(greit_connection_string, df, tabel, klant, start_datum, eind_datum)
        
    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)

if __name__ == '__main__':
    main()