import os
import json
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Constantes / noms des feuilles Google Sheets
FEUILLE_BRUT = "Data clients bruts"
FEUILLE_ENCODE = "Data client encoded"

# Variables d'environnement attendues
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# Fichiers de référence pour encodage
SECTEUR_REFERENCE_FILE = "secteur_reference.xlsx"
PROFESSION_REFERENCE_FILE = "profession_reference.xlsx"

def get_credentials():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    return Credentials.from_service_account_info(creds_dict, scopes=scopes)

def get_sheets_service():
    creds = get_credentials()
    return build('sheets', 'v4', credentials=creds)

def lire_feuille(service, feuille):
    result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=feuille).execute()
    values = result.get('values', [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    data_rows = values[1:]
    max_len = len(header)
    fixed_rows = []
    for row in data_rows:
        if len(row) < max_len:
            row += [''] * (max_len - len(row))
        elif len(row) > max_len:
            row = row[:max_len]
        fixed_rows.append(row)
    return pd.DataFrame(fixed_rows, columns=header)

def update_feuille(service, feuille, df):
    valeurs = [df.columns.tolist()] + df.astype(str).values.tolist()
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=feuille,
        valueInputOption='RAW',
        body={'values': valeurs}
    ).execute()

def encodage_personnalise(df):
    df = df.copy()
    # Exemple : encodage ordinal d'une colonne 'sexe'
    if 'sexe' in df.columns:
        df['sexe'] = df['sexe'].map({'Homme': 1, 'Femme': 0}).fillna(-1).astype(int)

    # Encodage binaire sur colonne 'marie'
    if 'marie' in df.columns:
        df['marie'] = df['marie'].map({'Oui': 1, 'Non': 0}).fillna(-1).astype(int)

    # One-hot simplifié sur 'type_contrat' par exemple
    if 'type_contrat' in df.columns:
        dummies = pd.get_dummies(df['type_contrat'], prefix='type_contrat')
        df = pd.concat([df, dummies], axis=1)
        df.drop(columns=['type_contrat'], inplace=True)

    # Encodage via fichiers de référence
    if 'secteur_d_activite' in df.columns and os.path.exists(SECTEUR_REFERENCE_FILE):
        ref_secteur = pd.read_excel(SECTEUR_REFERENCE_FILE)
        dict_secteur = dict(zip(ref_secteur['code'], ref_secteur['valeur']))
        df['secteur_d_activite'] = df['secteur_d_activite'].map(dict_secteur).fillna(-1).astype(int)

    if 'profession' in df.columns and os.path.exists(PROFESSION_REFERENCE_FILE):
        ref_prof = pd.read_excel(PROFESSION_REFERENCE_FILE)
        dict_prof = dict(zip(ref_prof['code'], ref_prof['valeur']))
        df['profession'] = df['profession'].map(dict_prof).fillna(-1).astype(int)

    # Marquer la date/heure de l'encodage
    from datetime import datetime
    df['horodateur'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Champ "encoded" à jour
    df['encoded'] = 'oui'

    return df

def run_encodage():
    print("Début de l'encodage...")

    sheets_service = get_sheets_service()

    df_brut = lire_feuille(sheets_service, FEUILLE_BRUT)
    if df_brut.empty or 'encoded' not in df_brut.columns:
        print("Pas de données brutes ou colonne 'encoded' absente.")
        return "Aucune donnée à encoder"

    df_brut['encoded'] = df_brut['encoded'].fillna('').astype(str).str.strip().str.lower()
    df_a_encoder = df_brut[(df_brut['encoded'] == '') | (df_brut['encoded'] == 'non')]

    if df_a_encoder.empty:
        print("Aucun client à encoder.")
        return "Aucun client à encoder"

    # Réaliser l'encodage personnalisé
    df_encode = encodage_personnalise(df_a_encoder)

    # Charger la feuille encodée existante
    df_encode_existante = lire_feuille(sheets_service, FEUILLE_ENCODE)
    if df_encode_existante.empty:
        df_final = df_encode
    else:
        # Concaténer en évitant doublons par 'cli'
        df_final = pd.concat([df_encode_existante, df_encode], ignore_index=True)
        df_final.drop_duplicates(subset=['cli'], keep='last', inplace=True)

    # Mettre à jour la feuille 'Data client encoded'
    update_feuille(sheets_service, FEUILLE_ENCODE, df_final)

    # Mettre à jour la colonne 'encoded' dans la feuille brut à 'oui' pour les clients encodés
    df_brut.loc[df_brut['cli'].isin(df_encode['cli']), 'encoded'] = 'oui'
    update_feuille(sheets_service, FEUILLE_BRUT, df_brut)

    print("Encodage terminé.")
    return "✅ Encodage terminé"

if __name__ == "__main__":
    run_encodage()
