from flask import Flask, jsonify
<<<<<<< HEAD
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

app = Flask(__name__)

def colnum_to_excel(col_num):
    letters = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters

def encoder_clients():
    # Authentification Google Sheets via variable d'environnement
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not credentials_json:
        raise Exception("La variable d'environnement 'GOOGLE_CREDENTIALS_JSON' n'est pas définie.")
    
    credentials_dict = json.loads(credentials_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)

    # Chargement des données brutes
    spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1rgUGd1PdOWI0rvKz1tTDZp3qbNvq8du_-7IGAhQinak/edit?usp=sharing")
    ws_raw = spreadsheet.worksheet("Data client brut")
    df = pd.DataFrame(ws_raw.get_all_records())
    df.columns = df.columns.str.strip().str.lower()

    # Sélection des lignes à encoder
    df_to_encode = df[df["encoded"].str.lower() != "oui"].copy()
    if df_to_encode.empty:
        return "✅ Aucun nouveau client à encoder."

    # Référentiels
    secteur_ref = pd.DataFrame(spreadsheet.worksheet("secteur_reference").get_all_records())
    profession_ref = pd.DataFrame(spreadsheet.worksheet("profession_reference").get_all_records())
    secteur_ref.columns = secteur_ref.columns.str.strip().str.lower()
    profession_ref.columns = profession_ref.columns.str.strip().str.lower()
    secteur_dict = dict(zip(secteur_ref["secteur_d_activite"], secteur_ref["code_secteur"]))
    profession_dict = dict(zip(profession_ref["profession"], profession_ref["code_profession"]))

    # Encodage
    df_to_encode["decision_systeme"] = df_to_encode["decision_systeme"].str.strip().str.lower().map({
        "refus système": 0, "accord système": 1, "demande etude": 2
    })
    df_to_encode["type_du_client"] = df_to_encode["type_du_client"].str.strip().str.lower().map({
        "entreprise": 0, "particulier": 1
    })
    df_to_encode["nationalite"] = df_to_encode["nationalite"].str.strip().str.lower().map({
        "etranger": 0, "tunisie": 1
    })
    df_to_encode["pays_de_residence"] = df_to_encode["pays_de_residence"].str.strip().str.lower().map({
        "etranger": 0, "tunisie": 1
    })
    df_to_encode["regime_matrimonial"] = df_to_encode["regime_matrimonial"].str.strip().str.lower().map({
        "indéfini": 0, "separation de biens": 1, "communaute des biens": 2
    })
    df_to_encode["statut_marital"] = df_to_encode["statut_marital"].str.strip().str.lower().map({
        "celibataire": 1, "marie": 2, "divorce": 3, "veuf": 4
    })
    df_to_encode["logement"] = df_to_encode["logement"].str.strip().str.lower().map({"non": 0, "oui": 1}).fillna(0).astype(int)
    df_to_encode["prive_public"] = df_to_encode["prive_public"].str.strip().str.lower().map({"prive": 0, "etatique": 1}).fillna(0).astype(int)
    df_to_encode["genre"] = df_to_encode["genre"].str.strip().str.lower().map({"femme": 0, "homme": 1}).fillna(0).astype(int)

    # Variables dérivées
    df_to_encode["statut_professionel"] = df_to_encode["statut_professionel"].str.strip().str.lower()
    df_to_encode["retraite"] = (df_to_encode["statut_professionel"] == "retraite").astype(int)
    df_to_encode["cdi"] = df_to_encode["statut_professionel"].isin(["cdi", "cdi non titulaire", "cdi titulaire"]).astype(int)
    df_to_encode["titulaire"] = (df_to_encode["statut_professionel"] == "cdi titulaire").astype(int)
    df_to_encode["cdd"] = (df_to_encode["statut_professionel"] == "cdd").astype(int)
    df_to_encode["stagiaire"] = (df_to_encode["statut_professionel"] == "stagiaire").astype(int)
    df_to_encode["liberale"] = df_to_encode["statut_professionel"].isin(["independant", "independant   conventionne", "artisan"]).astype(int)
    df_to_encode.drop(columns=["statut_professionel"], inplace=True)

    df_to_encode["secteur_d_activite"] = df_to_encode["secteur_d_activite"].map(secteur_dict).fillna(0).astype(int)
    df_to_encode["profession"] = df_to_encode["profession"].map(profession_dict).fillna(0).astype(int)

    # Revenu total
    df_to_encode["revenu"] = df_to_encode.get("salaire", 0).fillna(0) + df_to_encode.get("totalautrerevenu", 0).fillna(0)
    df_to_encode.drop(columns=["salaire", "totalautrerevenu"], errors="ignore", inplace=True)

    if "encoded" in df_to_encode.columns:
        df_to_encode.drop(columns=["encoded"], inplace=True)

    # Enregistrement
    encoded_ws_name = "Data client encoded"
    try:
        encoded_ws = spreadsheet.worksheet(encoded_ws_name)
        existing_df = pd.DataFrame(encoded_ws.get_all_records())
        df_to_append = df_to_encode[~df_to_encode["cli"].isin(existing_df["cli"])]
    except gspread.exceptions.WorksheetNotFound:
        encoded_ws = spreadsheet.add_worksheet(title=encoded_ws_name, rows="1000", cols=str(len(df_to_encode.columns)))
        df_to_append = df_to_encode.copy()

    if df_to_append.empty:
        return "✅ Tous les clients encodés sont déjà présents."

    current_values = encoded_ws.get_all_values()
    start_row = len(current_values) + 1

    if len(current_values) == 0:
        encoded_ws.update(f"A1", [df_to_append.columns.tolist()])
        start_row = 2

    required_rows = start_row + len(df_to_append) - 1
    if required_rows > encoded_ws.row_count:
        encoded_ws.add_rows(required_rows - encoded_ws.row_count)

    start_col_letter = "A"
    end_col_letter = colnum_to_excel(df_to_append.shape[1])
    cell_range = f"{start_col_letter}{start_row}:{end_col_letter}{start_row + len(df_to_append) - 1}"
    encoded_ws.update(values=df_to_append.values.tolist(), range_name=cell_range)

    # Mise à jour colonne "encoded"
    cli_list = df_to_append["cli"].tolist()
    cli_col = df["cli"].tolist()
    encoded_col_index = df.columns.get_loc("encoded") + 1

    for row_index, cli_value in enumerate(cli_col, start=2):
        if cli_value in cli_list:
            ws_raw.update_cell(row_index, encoded_col_index, "oui")

    return f"✅ {len(df_to_append)} clients encodés avec succès."

@app.route("/", methods=["GET"])
def run():
    try:
        result = encoder_clients()
        return jsonify({"message": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
=======
from script_encodage import run_encodage
from traitement_script import run_traitement

app = Flask(__name__)

@app.route('/run_pipeline', methods=['GET'])
def run_pipeline():
    try:
        # 1. Encodage des clients
        encodage_result = run_encodage()
        
        # 2. Si encodage fait, lancer traitement
        if encodage_result != "Aucun client à encoder":
            traitement_result = run_traitement()
            return jsonify({
                "status": "success",
                "encodage": encodage_result,
                "traitement": traitement_result
            })
        else:
            return jsonify({
                "status": "success",
                "encodage": encodage_result,
                "traitement": "Non exécuté, pas de nouveaux clients encodés"
            })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
>>>>>>> a69ddf9 (Remplacement complet du projet par la nouvelle version)
