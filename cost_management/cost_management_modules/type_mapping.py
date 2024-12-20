import pandas as pd
from decimal import Decimal
import numpy as np

kosten_typing =   {
    "Klant": "nvarchar",
    "Kosten": "decimal",
    "Datum": "date",
    "Service": "nvarchar",
    "Valuta": "nvarchar",
}

def convert_column_types(df, column_types):
    pd.set_option('future.no_silent_downcasting', True)
    
    for column, dtype in column_types.items():
        if column in df.columns:
            try:
                if dtype == 'int':
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    invalid_values = df[column].isnull()
                    if invalid_values.any():
                        ongeldige_waarden = df[column][invalid_values].unique()
                        print(f"Waarschuwing: {len(ongeldige_waarden)} ongeldige waarden gevonden in kolom '{column}': {ongeldige_waarden}, deze worden vervangen door 0.")
                        df[column] = df[column].fillna(0)
                    df[column] = df[column].astype(int)
                elif dtype == 'nvarchar':
                    df[column] = df[column].astype(str)
                elif dtype == 'decimal':
                    df[column] = pd.to_numeric(df[column], errors='coerce').apply(lambda x: round(x, 2) if pd.notna(x) else None)
                elif dtype == 'bit':
                    df[column] = df[column].apply(lambda x: bool(x) if x in [0, 1] else x == -1)
                elif dtype == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce', format='%Y%m%d').dt.date
                elif dtype == 'datetime':
                    df[column] = pd.to_datetime(df[column], errors='coerce', format='%Y%m%d')
                elif dtype == 'bigint':
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    invalid_values = df[column].isnull()
                    if invalid_values.any():
                        ongeldige_waarden = df[column][invalid_values].unique()
                        print(f"Waarschuwing: {len(ongeldige_waarden)} ongeldige waarden gevonden in kolom '{column}': {ongeldige_waarden}, deze worden vervangen door 0.")
                        df[column] = df[column].fillna(0)
                    df[column] = df[column].astype('int64')
                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
        else:
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df

