plaatsing =   {
    "Datum aangemaakt": "Datum_aangemaakt",
    "werknemer": "Werknemer",
    "Id": "ID"
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