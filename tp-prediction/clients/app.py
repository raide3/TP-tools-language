import json
import os
import threading
import time
from collections import defaultdict

from flask import Flask, jsonify
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

app = Flask(__name__)

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "commandes-creees"
GROUP_ID = os.environ.get("GROUP_ID", "clients-group")

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"},
]

# client_id -> nombre de commandes reçues (en mémoire)
compteurs = defaultdict(int)


@app.route("/clients", methods=["GET"])
def get_clients():
    return jsonify(clients)


@app.route("/clients/<int:client_id>", methods=["GET"])
def get_client(client_id):
    for c in clients:
        if c["id"] == client_id:
            return jsonify(c)
    return jsonify({"erreur": "Client introuvable"}), 404


@app.route("/stats", methods=["GET"])
def get_stats():
    return jsonify(compteurs)


def creer_consommateur():
    for tentative in range(1, 21):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",   # nouveau groupe : lire depuis le début
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
        except NoBrokersAvailable:
            print(f"[clients] Kafka indisponible, tentative {tentative}/20...", flush=True)
            time.sleep(3)
    raise RuntimeError("Impossible de se connecter à Kafka")


def consommer():
    consumer = creer_consommateur()
    print(f"[clients] Consommateur prêt (groupe={GROUP_ID}, topic={TOPIC})", flush=True)
    for message in consumer:
        e = message.value
        client = next((c for c in clients if c["id"] == e["client_id"]), None)
        nom = client["nom"] if client else f"client inconnu (id={e['client_id']})"
        compteurs[e["client_id"]] += 1
        print(
            f"[clients] partition={message.partition} offset={message.offset} | "
            f"client={nom} | produit={e['produit']} | montant={e['montant']}",
            flush=True,
        )
        print(f"[clients] Total commandes par client : {dict(compteurs)}", flush=True)


if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)