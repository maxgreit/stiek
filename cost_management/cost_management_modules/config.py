def fetch_script_id(cursor):
    # Voer de query uit om het hoogste ScriptID op te halen
    query = 'SELECT MAX(ScriptID) FROM Logging'
    cursor.execute(query)
    
    # Verkrijg het resultaat
    highest_script_id = cursor.fetchone()[0]

    return highest_script_id

def fetch_configurations(cursor):
    # Voer de query uit om alle configuraties op te halen
    query = 'SELECT * FROM Configuratie'
    cursor.execute(query)

    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()
    
    # Controleer of er resultaten zijn
    if not rows:
        print("Geen configuraties gevonden.")
        return {}

    # Extract de configuraties en waarden, waarbij de bron de sleutel is
    connection_dict = {}
    for row in rows:
        configuratie = row[1]  # Kolom 'Configuratie' (index 1)
        waarde = row[2]         # Kolom 'Waarde' (index 2)
        bron = row[3]           # Kolom 'Bron' (index 3)

        # Voeg configuratie en waarde toe onder de bron
        if bron not in connection_dict:
            connection_dict[bron] = {}
        connection_dict[bron][configuratie] = waarde

    return connection_dict

def fetch_all_connection_strings(cursor):
    # Voer de query uit om alle connectiestrings op te halen
    query = 'SELECT * FROM Klanten'
    cursor.execute(query)
    
    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()
    
    # Extract de connectiestrings uit de resultaten
    connection_dict = {row[1]: (row[2], row[3]) for row in rows}  
    return connection_dict