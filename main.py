from flask import Flask
from encodage_client import run_encodage
from traitement_script import run_traitement

app = Flask(__name__)

def pipeline():
    try:
        encodage_result = run_encodage()
        if "client(s) encodé" in encodage_result:
            traitement_result = run_traitement()
            return f"{encodage_result}\n{traitement_result}", 200
        return encodage_result, 200
    except Exception as e:
        return f"❌ Erreur : {str(e)}", 500

@app.route("/")
@app.route("/run")
def run_pipeline():
    return pipeline()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
