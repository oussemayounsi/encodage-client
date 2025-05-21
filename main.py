from flask import Flask, jsonify
from script_encodage import run_encodage
from traitement_script import run_traitement
import os

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"message": "API est en ligne"})

@app.route('/run_pipeline', methods=['GET'])
def run_pipeline():
    try:
        encodage_result = run_encodage()
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
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port)
