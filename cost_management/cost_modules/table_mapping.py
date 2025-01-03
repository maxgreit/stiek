from cost_modules.log import log

kosten =   {
    "PreTaxCost": "Kosten",
    "UsageDate": "Datum",
    "ServiceName": "Service",
    "Currency": "Valuta"
}

def transform_columns(df, column_mapping):
    # Controleer of de DataFrame leeg is
    
    if df.empty:
        # Retourneer een melding en None
        print("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Hernoem de kolommen
    df = df.rename(columns=column_mapping)

    return df

def apply_transformation(df, greit_connection_string, klant, bron, script, script_id):
    # Kolom mapping
    column_mapping = {
        'Kosten': kosten,
    }

    # Tabel mapping
    for tabel, mapping in column_mapping.items():
        # Transformeer de kolommen
        try:
            df = transform_columns(df, mapping)
            print(f"Kolommen getransformeerd")
            log(greit_connection_string, klant, bron, f"Mapping van kolommen correct uitgevoerd", script, script_id, tabel)
        except Exception as e:
            print(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
            log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen transformeren mislukt: {e}", script, script_id, tabel)
            return None
    
    return df