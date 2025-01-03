from cost_modules.database import apply_clearing_and_writing
from cost_modules.type_mapping import apply_type_conversion
from cost_modules.table_mapping import apply_transformation
from cost_modules.cost_api import generate_cost_dataframe
from cost_modules.access_token import get_access_token
from cost_modules.config import determine_script_id
from cost_modules.env_tool import env_check
from cost_modules.log import log, end_log
import time
import os

def main():
    
    env_check()

    # Script configuratie
    klant = "Stiek"
    script = "Azure | Cost Management"
    bron = 'Python'
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
    script_id = determine_script_id(greit_connection_string, klant, bron, script)

    try:
        # Access token genereren
        bearer_token = get_access_token(tenant_id, client_id, client_secret)

        # Creëer cost dataframe
        df = generate_cost_dataframe(subscription_id, klant, bron, script, script_id, greit_connection_string, bearer_token)

        # Transformeer kolommen
        df = apply_transformation(df, greit_connection_string, klant, bron, script, script_id)

        # Verander data types
        df = apply_type_conversion(greit_connection_string, klant, bron, script, script_id, df)
        
        # Leeghalen en toeschrijven van data
        apply_clearing_and_writing(greit_connection_string, df, tabel, klant, bron, script, script_id)

    except Exception as e:
        print(f"FOUTMELDING | {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | {e}", script, script_id, tabel)
    
    # Eind logging
    end_log(start_time, greit_connection_string, klant, bron, script, script_id)

if __name__ == '__main__':
    main()