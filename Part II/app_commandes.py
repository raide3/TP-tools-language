import json
import os
import threading
import warnings
from datetime import datetime

from flask import Flask, jsonify, request
from kafka import KafkaProducer
from kafka.errors import KafkaError

warnings.filterwarnings("ignore", category=DeprecationWarning)

# --- Connexion au broker Kafka -------------------------------------------
# localhost:9092 en local, kafka:19092 si le service tourne dans Docker (réseau kafka-net)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# --- Topic utilisé -------------------------------------------------------
TOPIC = "commandes-creees"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",    # on attend la confirmation du broker
    retries=3,
)

app = Flask(__name__)

commandes = []              # "base de données" en mémoire, comme au TP1
_compteur = 0
_verrou = threading.Lock()


def prochain_id():
    global _compteur
    with _verrou:
        _compteur += 1
        return _compteur


# --- Envoi du message depuis l'API REST ----------------------------------
# CREATION D'UNE COMMANDE
@app.route("/commandes", methods=["POST"])
def creer_commande():
    data = request.get_json(silent=True) or {}

    manquants = [c for c in ("client_id", "produit", "montant") if c not in data]
    if manquants:
        return jsonify({"erreur": f"champs manquants : {manquants}"}), 400

    # --- Structure du message JSON envoyé --------------------------------
    evenement = {
        "commande_id": prochain_id(),
        "client_id": data["client_id"],
        "produit": data["produit"],
        "montant": data["montant"],
        #"date_commande": datetime.now().isoformat(timespec="seconds"),
        "date_commande": data.get("date_commande", datetime.now().isoformat(timespec="seconds")),
    }
    commandes.append(evenement)

    # --- Publication de la commande --------------------------------------
    try:
        # clé = client_id : les commandes d'un même client restent ordonnées (même partition)
        metadata = producer.send(TOPIC, key=evenement["client_id"], value=evenement).get(timeout=10)
    except KafkaError as e:
        return jsonify({"erreur": "Kafka indisponible", "detail": str(e)}), 503

    print(f"[commandes] événement publié -> {evenement} "
          f"(partition {metadata.partition}, offset {metadata.offset})")
    return jsonify({"commande": evenement,
                    "kafka": {"topic": TOPIC, "partition": metadata.partition,
                              "offset": metadata.offset}}), 201


@app.route("/commandes", methods=["GET"])
def lister_commandes():
    return jsonify(commandes)


if __name__ == "__main__":
    # use_reloader=False : évite que le mode debug crée deux producteurs
    app.run(host="0.0.0.0", port=5002, debug=False)