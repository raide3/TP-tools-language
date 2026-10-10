import os

import requests
from flask import Flask, jsonify

app = Flask(__name__)

# Dans Docker : CLIENTS_URL=http://clients:8080
CLIENTS_URL = os.environ.get("CLIENTS_URL", "http://localhost:8080")

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80},
]


@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)


@app.route("/commandes/<int:commande_id>", methods=["GET"])
def get_commande(commande_id):
    commande = next((c for c in commandes if c["id"] == commande_id), None)
    if commande is None:
        return jsonify({"erreur": "Commande introuvable"}), 404

    try:
        reponse = requests.get(
            f"{CLIENTS_URL}/clients/{commande['client_id']}", timeout=5
        )
    except requests.exceptions.RequestException:
        return jsonify({"erreur": "Microservice clients indisponible"}), 503

    if reponse.status_code == 404:
        return jsonify({"erreur": "Client de la commande introuvable"}), 404
    if reponse.status_code != 200:
        return jsonify({"erreur": "Erreur du microservice clients"}), 502

    return jsonify({
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": reponse.json(),
    })



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8081))
    app.run(host="0.0.0.0", port=port)