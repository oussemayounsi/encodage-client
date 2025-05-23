from flask import Flask
from script_encodage import run_encodage
from traitement_script import run_traitement

app = Flask(__name__)

def pipeline():
    try:
        encodage_result = run_encodage()
        print(f"Résultat de l'encodage : {encodage_result}")

        # Vérifie si l'encodage a traité au moins un client
        if "client" in encodage_result.lower() and "encodé" in encodage_result.lower():
            traitement_result = run_traitement()
            print(f"Résultat du traitement : {traitement_result}")
            return f"{encodage_result}\n{traitement_result}", 200

        return encodage_result, 200

    except Exception as e:
        error_msg = f"❌ Erreur dans pipeline : {str(e)}"
        print(error_msg)
        return error_msg, 500

@app.route("/", methods=["GET", "POST"])
@app.route("/run", methods=["GET"])
def run_pipeline():
    return pipeline()

if __name__ == "__main__":
    # Obligatoire pour Render.com (port 5000 accessible publiquement)
    app.run(host="0.0.0.0", port=5000, debug=True)