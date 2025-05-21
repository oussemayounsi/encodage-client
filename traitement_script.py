import os
import io
import pandas as pd
import numpy as np
import joblib
import shap
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Constantes / noms des feuilles Google Sheets
FEUILLE_DATA = "Data client encoded"
FEUILLE_RESULTATS = "resultat_modele_RF"
FEUILLE_SHAP = "SHAP_par_client"

# Variables d'environnement attendues
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
GOOGLE_DRIVE_FILE_ID_MODELE = os.getenv("GOOGLE_DRIVE_FILE_ID_MODELE")
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")

def get_credentials():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive.readonly']
    return Credentials.from_service_account_info(creds_dict, scopes=scopes)

def get_sheets_service():
    creds = get_credentials()
    return build('sheets', 'v4', credentials=creds)

def get_drive_service():
    creds = get_credentials()
    return build('drive', 'v3', credentials=creds)

def load_model_from_drive():
    drive_service = get_drive_service()
    request = drive_service.files().get_media(fileId=GOOGLE_DRIVE_FILE_ID_MODELE)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    model = joblib.load(fh)
    return model

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

def append_feuille(service, feuille, df_append):
    df_exist = lire_feuille(service, feuille)
    df_final = pd.concat([df_exist, df_append], ignore_index=True)
    valeurs = [df_final.columns.tolist()] + df_final.astype(str).values.tolist()
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=feuille,
        valueInputOption='RAW',
        body={'values': valeurs}
    ).execute()

def update_data_client_encoded(service, df_encoded):
    valeurs = [df_encoded.columns.tolist()] + df_encoded.astype(str).values.tolist()
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=FEUILLE_DATA,
        valueInputOption='RAW',
        body={'values': valeurs}
    ).execute()

def predict_and_report(df, model):
    exclude_cols = ['traite', 'horodateur']
    features_cols = [col for col in df.columns if col not in exclude_cols + ['cli', 'decision_systeme']]

    # S'assurer que les colonnes existent dans le modèle
    valid_features = [col for col in features_cols if col in model.feature_names_in_]

    X = df[valid_features].replace({',': '.'}, regex=True).astype(float)

    df = df.copy()
    df['PD'] = model.predict_proba(X)[:, 1]
    df['decision_modele'] = (df['PD'] < 0.7).astype(int)
    df['comparison'] = df['decision_systeme'].astype(str) + " vs " + df['decision_modele'].astype(str)

    return df, valid_features

def compute_shap(df, model, features_cols):
    X = df[features_cols].replace({',': '.'}, regex=True).astype(float)
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)

    if isinstance(shap_values, list) and len(shap_values) == 2:
        shap_class_1 = shap_values[1]
    elif isinstance(shap_values, np.ndarray) and shap_values.ndim == 3:
        shap_class_1 = shap_values[:, :, 1]
    elif isinstance(shap_values, np.ndarray) and shap_values.ndim == 2:
        shap_class_1 = shap_values
    else:
        raise ValueError("Format des SHAP values non reconnu.")

    shap_df = pd.DataFrame(shap_class_1, columns=features_cols)
    shap_df.insert(0, 'cli', df['cli'].values)
    shap_df.insert(1, 'PD', df['PD'].values)

    for col in shap_df.columns:
        if col != 'cli':
            shap_df[col] = shap_df[col].astype(float).map(
                lambda x: ('{0:.5f}'.format(x)).rstrip('0').rstrip('.').replace('.', ',')
            )
    return shap_df

def run_traitement():
    print("Début du traitement...")

    sheets_service = get_sheets_service()
    df = lire_feuille(sheets_service, FEUILLE_DATA)

    if df.empty or 'traite' not in df.columns:
        print("Aucune donnée à traiter.")
        return "Aucune donnée à traiter"

    df['traite'] = df['traite'].fillna('').astype(str).str.strip().str.lower()
    df_to_process = df[(df['traite'] == '') | (df['traite'] == 'non')]

    if df_to_process.empty:
        print("Aucun client à traiter.")
        return "Aucun client à traiter"

    model = load_model_from_drive()

    df_resultats, features_cols = predict_and_report(df_to_process, model)
    shap_df = compute_shap(df_resultats, model, features_cols)

    colonnes_resultats = [col for col in df.columns if col not in ['traite', 'horodateur']]
    for col in ['PD', 'decision_modele', 'comparison']:
        if col not in colonnes_resultats:
            colonnes_resultats.append(col)

    df_resultats_export = df_resultats[colonnes_resultats]
    df_resultats_export['PD'] = df_resultats_export['PD'].map(
        lambda x: ('{0:.6f}'.format(float(x))).rstrip('0').rstrip('.').replace('.', ',') if pd.notnull(x) else ''
    ).astype(str)

    append_feuille(sheets_service, FEUILLE_RESULTATS, df_resultats_export)

    feuille_existante = lire_feuille(sheets_service, FEUILLE_SHAP)
    if feuille_existante.empty:
        colonnes_shap_final = shap_df.columns.tolist()
    else:
        colonnes_shap_final = [col for col in feuille_existante.columns if col in shap_df.columns]
        if 'cli' not in colonnes_shap_final:
            colonnes_shap_final.insert(0, 'cli')
        if 'PD' not in colonnes_shap_final:
            colonnes_shap_final.append('PD')

    shap_df = shap_df[colonnes_shap_final]
    append_feuille(sheets_service, FEUILLE_SHAP, shap_df)

    df.loc[df['cli'].isin(df_resultats['cli']), 'traite'] = 'oui'
    update_data_client_encoded(sheets_service, df)

    print("Traitement terminé.")
    return "✅ Traitement terminé"

if __name__ == "__main__":
    run_traitement()
