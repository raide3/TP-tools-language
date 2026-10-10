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
GROUP_ID = os.environ.get("GROUP_ID", "statistiques-group")

nb_par_produit = defaultdict(int)     # "PC" -> 3
ca_par_mois = defaultdict(float)      # "2026-10" -> 2500.0


@app.route("/stats/produits/<produit>", methods=["GET"])
def stats_produit(produit):
    return jsonify({"produit": produit, "nombre_commandes": nb_par_produit.get(produit, 0)})


@app.route("/stats/ca/<mois>", methods=["GET"])   # ex : /stats/ca/2026-10
def stats_ca(mois):
    return jsonify({"mois": mois, "chiffre_affaires": ca_par_mois.get(mois, 0)})


@app.route("/stats", methods=["GET"])
def stats_toutes():
    return jsonify({"par_produit": nb_par_produit, "ca_par_mois": ca_par_mois})


def creer_consommateur():
    for tentative in range(1, 21):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
        except NoBrokersAvailable:
            print(f"[stats] Kafka indisponible, tentative {tentative}/20...", flush=True)
            time.sleep(3)
    raise RuntimeError("Impossible de se connecter à Kafka")


def consommer():
    consumer = creer_consommateur()
    print(f"[stats] Consommateur prêt (groupe={GROUP_ID})", flush=True)
    for message in consumer:
        e = message.value
        mois = e["date"][:7]                      # "2026-10-10" -> "2026-10"
        nb_par_produit[e["produit"]] += 1
        ca_par_mois[mois] += e["montant"]
        print(
            f"[stats] {e['produit']} : {nb_par_produit[e['produit']]} commande(s) | "
            f"CA {mois} : {ca_par_mois[mois]}",
            flush=True,
        )


if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)