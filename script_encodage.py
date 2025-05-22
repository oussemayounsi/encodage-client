import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json


def colnum_to_excel(col_num):
    letters = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def run_encodage():
    # Auth Google Sheets
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not credentials_json:
        raise Exception(
            "La variable d'environnement 'GOOGLE_CREDENTIALS_JSON' n'est pas définie."
        )
    credentials_dict = json.loads(credentials_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        credentials_dict, scope)
    client = gspread.authorize(creds)

    # Feuille source
    spreadsheet = client.open_by_url(
        "https://docs.google.com/spreadsheets/d/1rgUGd1PdOWI0rvKz1tTDZp3qbNvq8du_-7IGAhQinak/edit?usp=sharing"
    )
    ws_raw = spreadsheet.worksheet("Data client brut")
    df = pd.DataFrame(ws_raw.get_all_records())
    df.columns = df.columns.astype(str).str.strip().str.lower()

    if "encoded" not in df.columns:
        df["encoded"] = ""
    else:
        df["encoded"] = df["encoded"].fillna("").astype(str)

    df_to_encode = df[df["encoded"].str.lower() != "oui"].copy()
    if df_to_encode.empty:
        return "✅ Aucun nouveau client à encoder."

    # Feuilles de référence
    secteur_ref = pd.DataFrame(
        spreadsheet.worksheet("secteur_reference").get_all_records())
    profession_ref = pd.DataFrame(
        spreadsheet.worksheet("profession_reference").get_all_records())
    secteur_ref.columns = secteur_ref.columns.astype(
        str).str.strip().str.lower()
    profession_ref.columns = profession_ref.columns.astype(
        str).str.strip().str.lower()

    if "secteur_d_activite" not in secteur_ref.columns or "code_secteur" not in secteur_ref.columns:
        raise Exception(
            "La feuille 'secteur_reference' doit contenir 'secteur_d_activite' et 'code_secteur'"
        )
    if "profession" not in profession_ref.columns or "code_profession" not in profession_ref.columns:
        raise Exception(
            "La feuille 'profession_reference' doit contenir 'profession' et 'code_profession'"
        )

    secteur_dict = dict(
        zip(secteur_ref["secteur_d_activite"].str.lower(),
            secteur_ref["code_secteur"]))
    profession_dict = dict(
        zip(profession_ref["profession"].str.lower(),
            profession_ref["code_profession"]))

    # Encodages catégoriels
    mapping_fields = {
        "decision_systeme": {
            "refus système": 0,
            "accord système": 1,
            "demande etude": 2
        },
        "type_du_client": {
            "entreprise": 0,
            "particulier": 1
        },
        "nationalite": {
            "etranger": 0,
            "tunisie": 1
        },
        "pays_de_residence": {
            "etranger": 0,
            "tunisie": 1
        },
        "regime_matrimonial": {
            "indéfini": 0,
            "separation de biens": 1,
            "communaute des biens": 2
        },
        "statut_marital": {
            "celibataire": 1,
            "marie": 2,
            "divorce": 3,
            "veuf": 4
        },
        "logement": {
            "non": 0,
            "oui": 1
        },
        "prive_public": {
            "prive": 0,
            "etatique": 1
        },
        "genre": {
            "femme": 0,
            "homme": 1
        }
    }

    for col, mapping in mapping_fields.items():
        if col in df_to_encode.columns:
            df_to_encode[col] = df_to_encode[col].fillna("").astype(
                str).str.strip().str.lower().map(mapping).fillna(0).astype(int)
        else:
            df_to_encode[col] = 0

    # Statut professionnel en colonnes binaires
    if "statut_professionel" in df_to_encode.columns:
        df_to_encode["statut_professionel"] = df_to_encode[
            "statut_professionel"].fillna("").astype(
                str).str.strip().str.lower()
        df_to_encode["retraite"] = (
            df_to_encode["statut_professionel"] == "retraite").astype(int)
        df_to_encode["cdi"] = df_to_encode["statut_professionel"].isin(
            ["cdi", "cdi non titulaire", "cdi titulaire"]).astype(int)
        df_to_encode["titulaire"] = (
            df_to_encode["statut_professionel"] == "cdi titulaire").astype(int)
        df_to_encode["cdd"] = (
            df_to_encode["statut_professionel"] == "cdd").astype(int)
        df_to_encode["stagiaire"] = (
            df_to_encode["statut_professionel"] == "stagiaire").astype(int)
        df_to_encode["liberale"] = df_to_encode["statut_professionel"].isin(
            ["independant", "independant   conventionne",
             "artisan"]).astype(int)
        df_to_encode.drop(columns=["statut_professionel"], inplace=True)
    else:
        for c in [
                "retraite", "cdi", "titulaire", "cdd", "stagiaire", "liberale"
        ]:
            df_to_encode[c] = 0

    if "secteur_d_activite" in df_to_encode.columns:
        df_to_encode["secteur_d_activite"] = df_to_encode[
            "secteur_d_activite"].fillna("").astype(str).str.lower().map(
                secteur_dict).fillna(0).astype(int)
    else:
        df_to_encode["secteur_d_activite"] = 0

    if "profession" in df_to_encode.columns:
        df_to_encode["profession"] = df_to_encode["profession"].fillna(
            "").astype(str).str.lower().map(profession_dict).fillna(0).astype(
                int)
    else:
        df_to_encode["profession"] = 0

    salaire = df_to_encode.get("salaire", pd.Series(
        [0] * len(df_to_encode))).fillna(0).astype(float)
    autre_revenu = df_to_encode.get(
        "totalautrerevenu",
        pd.Series([0] * len(df_to_encode))).fillna(0).astype(float)
    df_to_encode["revenu"] = salaire + autre_revenu
    df_to_encode.drop(columns=["salaire", "totalautrerevenu"],
                      errors="ignore",
                      inplace=True)

    if "cli" not in df_to_encode.columns:
        raise Exception(
            "Colonne 'cli' obligatoire absente dans les données à encoder.")
    df_to_encode["cli"] = df_to_encode["cli"].astype(str).str.strip()

    if "encoded" in df_to_encode.columns:
        df_to_encode.drop(columns=["encoded"], inplace=True)

    # Feuille cible
    encoded_ws_name = "Data client encoded"
    try:
        encoded_ws = spreadsheet.worksheet(encoded_ws_name)
        existing_values = encoded_ws.get_all_values()
        if existing_values:
            existing_df = pd.DataFrame(existing_values[1:],
                                       columns=existing_values[0])
        else:
            existing_df = pd.DataFrame(columns=df_to_encode.columns)
    except gspread.exceptions.WorksheetNotFound:
        encoded_ws = spreadsheet.add_worksheet(title=encoded_ws_name,
                                               rows="1000",
                                               cols=str(
                                                   len(df_to_encode.columns)))
        encoded_ws.update("A1", [df_to_encode.columns.tolist()])
        existing_df = pd.DataFrame(columns=df_to_encode.columns)

    if "cli" not in existing_df.columns:
        existing_df["cli"] = ""

    existing_cli_set = set(existing_df["cli"].astype(str).str.strip())
    df_to_append = df_to_encode[~df_to_encode["cli"].astype(str).str.strip().
                                isin(existing_cli_set)]

    if df_to_append.empty:
        return "✅ Tous les clients encodés sont déjà présents."

    # Écriture sans toucher à l’en-tête
    start_row = len(existing_values) + 1
    start_col_letter = "A"
    end_col_letter = colnum_to_excel(df_to_append.shape[1])
    cell_range = f"{start_col_letter}{start_row}:{end_col_letter}{start_row + len(df_to_append) - 1}"
    encoded_ws.update(values=df_to_append.values.tolist(),
                      range_name=cell_range)

    # Mise à jour dans la feuille d'origine
    cli_list = df_to_append["cli"].astype(str).tolist()
    cli_col = df["cli"].astype(str).tolist()
    encoded_col_index = df.columns.get_loc("encoded") + 1
    for row_index, cli_value in enumerate(cli_col, start=2):
        if cli_value in cli_list:
            ws_raw.update_cell(row_index, encoded_col_index, "oui")

    return f"✅ {len(df_to_append)} clients encodés et ajoutés dans '{encoded_ws_name}'."


if __name__ == "__main__":
    msg = run_encodage()
    print(msg)
