def map_columns(df):
    mapping = {
        "Timestamp": "Timestamp",
        "Email Address": "Email_Address",
        "Naam": "Naam",
        "E-mailadres": "Emailadres",
        "Om welk tabblad gaat het?": "Tabblad",
        "Betreft het een foutmelding of een suggestie?": "Type_Feedback",
        "Wat is je feedback?": "Feedback",
        "Wat loopt er fout?": "Foutmelding",
        "Wat is je suggestie?": "Suggestie",
        "Wil je iets anders aangeven?": "Anders",
        "Status": "Status",
        "ID": "ID"
    }
    
    # Renaming columns based on mapping
    mapped_df = df.rename(columns=mapping)
    return mapped_df
