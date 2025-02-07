import logging

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
        logging.error("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Hernoem de kolommen
    df = df.rename(columns=column_mapping)

    return df

def apply_transformation(df):
    # Kolom mapping
    column_mapping = {
        'Kosten': kosten,
    }

    # Tabel mapping
    for tabel, mapping in column_mapping.items():
        # Transformeer de kolommen
        try:
            df = transform_columns(df, mapping)
            logging.info(f"Kolommen getransformeerd")
        except Exception as e:
            logging.error(f"Kolommen transformeren mislukt: {e}")
            return None
    
    return df