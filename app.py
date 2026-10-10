import os

from flask import Flask, jsonify

app = Flask(__name__)

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"},
]

# Méthode GET pour retourner tous les clients
@app.route("/clients", methods=["GET"])
def get_clients():
    return jsonify(clients)


# Méthode GET pour retourner un client spécifique avec son ID
@app.route("/clients/<int:client_id>", methods=["GET"])
def get_client(client_id):
    for client in clients:
        if client["id"] == client_id:
            return jsonify(client)
    return jsonify({"erreur": "Client introuvable"}), 404


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)