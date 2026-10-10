import json
import os
import time
from datetime import date
import threading
from kafka import KafkaConsumer, KafkaProducer

import requests
from flask import Flask, jsonify, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

app = Flask(__name__)

CLIENTS_URL = os.environ.get("CLIENTS_URL", "http://clients:8080")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "commandes-creees"
PORT = int(os.environ.get("PORT", 8080))
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_RESULTATS = "resultats-prediction"
GROUP_RESULTATS = "commandes-resultats-group"
CHAMPS_PREDICTION = ["client_id", "age", "anciennete", "nombre_commandes",
                     "montant_total", "montant_moyen", "jours_depuis_derniere_commande"]
resultats = [] 

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80},
]


def creer_producteur():
    """Kafka peut mettre quelques secondes à démarrer : on réessaie."""
    for tentative in range(1, 21):
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[commandes] Connecté à Kafka ({BOOTSTRAP})", flush=True)
            return p
        except NoBrokersAvailable:
            print(f"[commandes] Kafka indisponible, tentative {tentative}/20...", flush=True)
            time.sleep(3)
    raise RuntimeError("Impossible de se connecter à Kafka")


producer = creer_producteur()


@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)


@app.route("/commandes/<int:commande_id>", methods=["GET"])
def get_commande(commande_id):
    commande = next((c for c in commandes if c["id"] == commande_id), None)
    if commande is None:
        return jsonify({"erreur": "Commande introuvable"}), 404
    try:
        r = requests.get(f"{CLIENTS_URL}/clients/{commande['client_id']}", timeout=3)
    except requests.exceptions.RequestException:
        return jsonify({"erreur": "Service clients indisponible"}), 503
    if r.status_code == 404:
        return jsonify({"erreur": "Client associé introuvable"}), 404
    if r.status_code != 200:
        return jsonify({"erreur": "Erreur du service clients"}), 502
    return jsonify({
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": r.json(),
    })


@app.route("/commandes", methods=["POST"])
def creer_commande():
    data = request.get_json(silent=True) or {}
    for champ in ("client_id", "produit", "montant"):
        if champ not in data:
            return jsonify({"erreur": f"Champ manquant : {champ}"}), 400

    nouvel_id = max(c["id"] for c in commandes) + 1
    commande = {
        "id": nouvel_id,
        "client_id": data["client_id"],
        "produit": data["produit"],
        "montant": data["montant"],
    }
    commandes.append(commande)

    # Structure de l'événement publié dans Kafka
    evenement = {
        "commande_id": nouvel_id,
        "client_id": data["client_id"],
        "produit": data["produit"],
        "montant": data["montant"],
        "date": data.get("date", date.today().isoformat()),  # date optionnelle, pratique pour tester le CA par mois
    }
    producer.send(TOPIC, value=evenement)
    producer.flush()   # on attend la confirmation du broker
    print(f"[commandes] Événement publié : {evenement}", flush=True)
    return jsonify(evenement), 201


@app.route("/predictions", methods=["POST"])
def demander_prediction():
    data = request.get_json(silent=True) or {}
    manquants = [c for c in CHAMPS_PREDICTION if c not in data]
    if manquants:
        return jsonify({"erreur": f"Champs manquants : {manquants}"}), 400

    evenement = {c: data[c] for c in CHAMPS_PREDICTION}
    # clé = client_id : un même client va toujours sur la même partition
    producer.send(TOPIC_DEMANDES, key=str(evenement["client_id"]).encode("utf-8"), value=evenement)
    producer.flush()
    # 202 Accepted : la réponse arrivera plus tard, de façon asynchrone
    return jsonify({"statut": "demande envoyée", "evenement": evenement}), 202


@app.route("/predictions/resultats", methods=["GET"])
def get_resultats():
    return jsonify(resultats[-20:])


def consommer_resultats():
    for tentative in range(1, 21):
        try:
            consumer = KafkaConsumer(
                TOPIC_RESULTATS,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_RESULTATS,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            break
        except NoBrokersAvailable:
            print(f"[commandes] Kafka indisponible (consumer), tentative {tentative}/20...", flush=True)
            time.sleep(3)
    else:
        return

    print(f"[commandes] En attente des résultats de prédiction ({TOPIC_RESULTATS})", flush=True)
    for message in consumer:
        r = message.value
        resultats.append(r)
        del resultats[:-100]
        print(f"[commandes] Résultat reçu | client={r['client_id']} | "
              f"prediction={r['prediction']} | probabilite={r['probabilite_commande']:.2f} "
              f"| calculé par {r.get('instance', '?')}", flush=True)
        
if __name__ == "__main__":
    threading.Thread(target=consommer_resultats, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)