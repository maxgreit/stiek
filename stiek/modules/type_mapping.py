import pandas as pd
import logging

class TypeMapper:
    """
    Een class voor het beheren en toepassen van type mappings voor verschillende tabellen.
    """
    
    def __init__(self):
        """
        Initialiseer de TypeMapper met alle beschikbare type mappings.
        """
        self._type_mappings = {
            'Looncomponenten': {
                "Id": "bigint",
                "Componenttype": "nvarchar",
                "Omschrijving": "nvarchar",
                "Klasse": "nvarchar",
                "Prioriteit": "int",
                "Percentage": "decimal",
                "Globaal": "bit",
                "Bruto": "bit",
                "Uitzonderlijk": "bit",
                "Extern nummer": "decimal",
                "Eenheid": "nvarchar",
                "Accordering leverancier verplicht": "bit",
                "Bijlageverplichting": "bit",
                "Componentsoort": "nvarchar",
                "Fasetelling": "bit",
                "Uitsluiten van facturatie": "bit",
                "Uitsluiten van margefacturatie": "bit",
            },
            'OntbrekendeUren': {
                "Periode": "nvarchar",
                "Inlener": "nvarchar",
                "werknemer": "nvarchar",
                "Afdeling": "nvarchar",
                "Plaatsing": "nvarchar",
                "Begindatum": "date",
                "Einddatum": "date",
                "Assignmenttype": "nvarchar",
                "Leverancierseenheid": "nvarchar",
                "Kostenplaatsnaam": "nvarchar",
                "E-mail flexkracht": "nvarchar",
                "Mobiel flexkracht": "nvarchar",
                "E-mail accordeerder": "nvarchar",
                "Mobiel accordeerder": "nvarchar",
            },
            'Plaatsingen': {
                "Id": "bigint",
                "Extern nummer backoffice": "nvarchar",
                "Functie": "nvarchar",
                "Startdatum": "date",
                "Einddatum": "date",
                "Referentie": "nvarchar",
                "Gebruik projecten": "nvarchar",
                "Datum aangemaakt": "datetime",
                "Actief": "bit",
                "Actie": "bit",
                "Exportstatus": "nvarchar",
                "Extern nummer frontoffice": "nvarchar",
                "werknemer": "nvarchar",
                "Inlener": "nvarchar",
                "Autorisatie-eenheid": "nvarchar",
                "Contracttype": "nvarchar",
                "Sector": "nvarchar",
                "Ontrafelingsprofiel": "nvarchar",
                "Kostenplaats": "nvarchar",
                "Gemiddelde werkweek": "decimal",
                "Bemiddelaar": "nvarchar",
                "Accountmanager": "nvarchar",
                "Recruiter": "nvarchar",
                "Bron": "nvarchar",
                "Callcenter": "bit",
                "Branche": "nvarchar",
            },
            'UrenRapportage': {
                "Datum": "date",
                "Jaar": "int",
                "Periodenummer": "int",
                "Aantal uren": "decimal",
                "Starttijd": "time",
                "Eindtijd": "time",
                "Pauze": "time",
                "Project": "nvarchar",
                "Projectnaam": "nvarchar",
                "Projectcode": "nvarchar",
                "Status": "nvarchar",
                "Accountmanager": "int",
                "Tarief": "decimal",
                "Urensoort": "nvarchar",
                "Componenttype": "nvarchar",
                "Componenteenheid": "nvarchar",
                "Urensoortcode": "int",
                "Functie": "nvarchar",
                "Plaatsingscode": "nvarchar",
                "Referentie": "nvarchar",
                "Plaatsing": "bigint",
                "Afdeling": "nvarchar",
                "werknemer": "nvarchar",
                "Flexkrachtcode": "nvarchar",
                "Inlener": "nvarchar",
                "Inlenercode": "nvarchar",
                "Laatst gewijzigd": "datetime",
                "Periode": "nvarchar",
                "Exportstatus": "nvarchar",
                "Id": "bigint",
                "Exportdatum": "datetime",
                "Leverancierseenheid": "nvarchar",
                "Naam rekeninghouder": "nvarchar",
                "Factuurdatum": "datetime",
                "Gemiddelde werkweek": "decimal",
                "Bemiddelaar": "nvarchar",
                "Recruiter": "int",
                "Bron": "int",
                "Callcenter": "bit",
                "Branche": "int",
            }
        }
    
    def get_type_mapping(self, table_name):
        """
        Haal de type mapping op voor een specifieke tabel.
        
        Args:
            table_name: De naam van de tabel
            
        Returns:
            Dict met kolomnamen en hun types, of None als tabel niet bestaat
        """
        return self._type_mappings.get(table_name)
    
    def add_type_mapping(self, table_name, type_mapping):
        """
        Voeg een nieuwe type mapping toe voor een tabel.
        
        Args:
            table_name: De naam van de tabel
            type_mapping: Dictionary met kolomnamen en hun types
        """
        self._type_mappings[table_name] = type_mapping
        logging.info(f"Type mapping toegevoegd voor tabel: {table_name}")
    
    def get_available_tables(self):
        """
        Haal een lijst op van alle beschikbare tabellen.
        
        Returns:
            List met tabelnamen
        """
        return list(self._type_mappings.keys())
    
    def convert_column_types(self, df, column_types):
        """
        Converteer kolom types in een DataFrame volgens de opgegeven type mapping.
        
        Args:
            df: Het DataFrame om te converteren
            column_types: Dictionary met kolomnamen en hun gewenste types
            
        Returns:
            Het geconverteerde DataFrame
            
        Raises:
            ValueError: Bij fouten tijdens type conversie
        """
        pd.set_option('future.no_silent_downcasting', True)
        
        for column, dtype in column_types.items():
            if column in df.columns:
                try:
                    df[column] = self._convert_single_column(df[column], column, dtype)
                except ValueError as e:
                    raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
            else:
                raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
        
        return df
    
    def _convert_single_column(self, column_data, column_name, dtype):
        """
        Converteer een enkele kolom naar het opgegeven type.
        
        Args:
            column_data: De kolom data om te converteren
            column_name: De naam van de kolom (voor logging)
            dtype: Het gewenste datatype
            
        Returns:
            De geconverteerde kolom
        """
        if dtype == 'int':
            return self._convert_to_int(column_data, column_name)
        elif dtype == 'nvarchar':
            return column_data.astype(str)
        elif dtype == 'decimal':
            return pd.to_numeric(column_data, errors='coerce').apply(
                lambda x: round(x, 2) if pd.notna(x) else None
            )
        elif dtype == 'bit':
            return column_data.apply(
                lambda x: True if str(x).strip().lower() in ['ja', 'true', '1'] else 
                         (False if str(x).strip().lower() in ['nee', 'false', '0'] else None)
            )
        elif dtype == 'date':
            return pd.to_datetime(column_data, errors='coerce', dayfirst=True).dt.date
        elif dtype == 'datetime':
            return pd.to_datetime(column_data, errors='coerce', dayfirst=True)
        elif dtype == 'time':
            # Converteert naar datetime en pakt alleen de tijdcomponent.
            # 'coerce' zet foute waarden om in NaT (Not a Time), die worden als None behandeld.
            return pd.to_datetime(column_data, errors='coerce').dt.time
        elif dtype == 'bigint':
            return self._convert_to_bigint(column_data, column_name)
        else:
            raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column_name}'.")
    
    def _convert_to_int(self, column_data, column_name):
        """
        Converteer een kolom naar integer type met foutafhandeling.
        """
        converted = pd.to_numeric(column_data, errors='coerce')
        invalid_values = converted.isnull()
        
        if invalid_values.any():
            ongeldige_waarden = column_data[invalid_values].unique()
            logging.warning(
                f"Waarschuwing: {len(ongeldige_waarden)} ongeldige waarden gevonden in kolom '{column_name}': "
                f"{ongeldige_waarden}, deze worden vervangen door 0."
            )
            converted = converted.fillna(0)
        
        return converted.astype(int)
    
    def _convert_to_bigint(self, column_data, column_name):
        """
        Converteer een kolom naar bigint type met foutafhandeling.
        """
        converted = pd.to_numeric(column_data, errors='coerce')
        invalid_values = converted.isnull()
        
        if invalid_values.any():
            ongeldige_waarden = column_data[invalid_values].unique()
            logging.warning(
                f"Waarschuwing: {len(ongeldige_waarden)} ongeldige waarden gevonden in kolom '{column_name}': "
                f"{ongeldige_waarden}, deze worden vervangen door 0."
            )
            converted = converted.fillna(0)
        
        return converted.astype('int64')
    
    def apply_conversion(self, df, table_name):
        """
        Pas type conversie toe op een DataFrame voor een specifieke tabel.
        
        Args:
            df: Het DataFrame om te converteren
            table_name: De naam van de tabel (bepaalt welke type mapping wordt gebruikt)
            
        Returns:
            Het geconverteerde DataFrame, of None bij fout
        """
        # Haal de juiste type mapping op
        column_types = self.get_type_mapping(table_name)
        
        if column_types is None:
            logging.error(f"Geen type mapping gevonden voor tabel: {table_name}")
            logging.info(f"Beschikbare tabellen: {self.get_available_tables()}")
            return None
        
        # Voer type conversie uit
        try:
            df = self.convert_column_types(df, column_types)
            logging.info(f"Type conversie succesvol toegepast voor tabel: {table_name}")
            return df
        except Exception as e:
            logging.error(f"Type conversie mislukt voor tabel '{table_name}': {e}")
            return None


# Globale instantie voor backwards compatibility
_type_mapper = TypeMapper()


# Backwards compatibility functies
def convert_column_types(df, column_types):
    """
    Backwards compatibility functie voor convert_column_types.
    
    Args:
        df: Het DataFrame om te converteren
        column_types: Dictionary met kolomnamen en hun types
        
    Returns:
        Het geconverteerde DataFrame
    """
    return _type_mapper.convert_column_types(df, column_types)


def apply_conversion(df, table_name):
    """
    Backwards compatibility functie voor apply_conversion.
    
    Args:
        df: Het DataFrame om te converteren
        table_name: De naam van de tabel
        
    Returns:
        Het geconverteerde DataFrame, of None bij fout
    """
    return _type_mapper.apply_conversion(df, table_name)


# Behoud de oude variabele voor backwards compatibility
looncomponent_typing = _type_mapper.get_type_mapping('Looncomponenten')
