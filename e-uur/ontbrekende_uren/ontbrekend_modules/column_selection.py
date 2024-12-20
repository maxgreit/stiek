def column_selection(df):
    # Gewenste kolommen selecteren
    kolommen = [
        'Periode',
        'Inlener',
        'Werknemer'
    ]
    
    selected_df = df[kolommen]
    
    return selected_df