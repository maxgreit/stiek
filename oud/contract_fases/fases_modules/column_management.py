from datetime import datetime

def add_datetime_column(df):
    # Huidige datum en tijd
    current_datetime = datetime.now()
    
    df['Datumtijd'] = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    
    return df

def apply_transformation(df):
    
    # Voeg datumtijd kolom toe
    df = add_datetime_column(df)
    
    # Filter op Actief = TRUE
    df = df[df['Actief'] == True]
    
    # Haal Actief kolom weg
    df = df.drop(columns=['Actief'])
    
    return df
