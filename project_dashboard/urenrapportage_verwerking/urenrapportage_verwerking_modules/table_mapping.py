urenregistratie =   {
    "Aantal uren": "Aantal_uren",
    "Laatst gewijzigd": "Laatst_gewijzigd",
    "werknemer": "Werknemer"
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