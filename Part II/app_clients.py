import json
import os
import threading
import warnings
from collections import defaultdict

from flask import Flask, jsonify
from kafka import KafkaConsumer

warnings.filterwarnings("ignore", category=DeprecationWarning)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "commandes-creees"
GROUP_ID = "clients-group"          # identifiant du groupe de consommateurs

# Remplace par la liste de clients de ton TP1
CLIENTS = {1: "Alice", 2: "Bob", 3: "Chloé"}

nb_commandes = defaultdict(int)     # client_id -> nombre total de commandes
verrou = threading.Lock()


def consommer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",   # premier lancement : lit depuis le début du topic
        enable_auto_commit=True,        # Kafka mémorise où en est ce groupe
    )
    print(f"[clients] Abonné au topic '{TOPIC}' (groupe '{GROUP_ID}')")

    for message in consumer:
        try:
            evt = json.loads(message.value.decode("utf-8"))
        except json.JSONDecodeError:
            print(f"[clients] message illisible ignoré (offset {message.offset})")
            continue

        if evt.get("commande_id") == 0:         # message de test du 1.3
            continue

        client_id = evt["client_id"]
        with verrou:
            nb_commandes[client_id] += 1
            total = dict(nb_commandes)

        nom = CLIENTS.get(client_id, "client inconnu")
        print(f"[clients] Commande #{evt['commande_id']} | client : {nom} (id {client_id}) "
              f"| produit : {evt['produit']} | montant : {evt['montant']} €")
        print(f"[clients] Total de commandes par client : {total}")


app = Flask(__name__)


@app.route("/clients/commandes", methods=["GET"])
def commandes_par_client():
    with verrou:
        return jsonify({
            str(cid): {"nom": CLIENTS.get(cid, "client inconnu"), "nb_commandes": n}
            for cid, n in nb_commandes.items()
        })


if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host="0.0.0.0", port=5001, debug=False)