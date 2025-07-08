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
                
                # Beperk tot gewenste kolommen
                gewenste_kolommen = [
                    "ID",
                    "Functie",
                    "Startdatum",
                    "Einddatum",
                    "Datum_aangemaakt",
                    "Actief",
                    "Werknemer",
                    "Inlener",
                    "Contracttype",
                    "Sector",
                    "Bemiddelaar",
                    "Gemiddelde_werkweek",
                    "Callcenter",
                    "Branche",
                    "Accountmanager",
                    "Recruiter",
                    "Bron"
                ]
                transformed_df = select_columns(transformed_df, gewenste_kolommen)
                
                return transformed_df
            except Exception as e:
                logging.error(f"Kolommen transformeren mislukt: {e}")

def select_columns(df, kolomnamen):
    """
    Beperk de DataFrame tot de opgegeven kolommen.
    Kolommen die niet bestaan worden genegeerd.
    """
    bestaande_kolommen = [kolom for kolom in kolomnamen if kolom in df.columns]
    return df[bestaande_kolommen]