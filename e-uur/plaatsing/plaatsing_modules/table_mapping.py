import logging

plaatsing =   {
    "Datum aangemaakt": "Datum_aangemaakt",
    "werknemer": "Werknemer",
    "Id": "ID",
    "Gemiddelde werkweek": "Gemiddelde_werkweek"
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


def apply_mapping(df, tabelnaam):
    # Kolom mapping
    column_mapping = {
    'Plaatsing': plaatsing,
    }

    # Tabel mapping
    for mapping_table, mapping in column_mapping.items():
        if tabelnaam == mapping_table:

            # Transformeer de kolommen
            try:
                transformed_df = transform_columns(df, mapping)
                logging.info(f"Kolommen getransformeerd")
                
                return transformed_df
            except Exception as e:
                logging.error(f"Kolommen transformeren mislukt: {e}")