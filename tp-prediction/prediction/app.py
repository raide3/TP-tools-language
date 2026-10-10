import json
import os
import signal
import socket
import sys
import time

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN = "demandes-prediction"
TOPIC_OUT = "resultats-prediction"
GROUP_ID = "prediction-group"          # même groupe pour TOUTES les instances
INSTANCE = socket.gethostname()        # id du conteneur, pour voir qui fait quoi

FEATURES = ["age", "anciennete", "nombre_commandes",
            "montant_total", "montant_moyen", "jours_depuis_derniere_commande"]

# Le modèle est chargé UNE fois au démarrage (pas d'entraînement ici)
model = joblib.load("modele_commande.pkl")


def attendre_kafka(creer, nom):
    for tentative in range(1, 21):
        try:
            return creer()
        except NoBrokersAvailable:
            print(f"[{INSTANCE}] Kafka indisponible ({nom}), tentative {tentative}/20...", flush=True)
            time.sleep(3)
    raise RuntimeError("Impossible de se connecter à Kafka")


def main():
    consumer = attendre_kafka(lambda: KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,      # commit manuel : voir plus bas
    ), "consumer")
    producer = attendre_kafka(lambda: KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    ), "producer")

    # docker stop envoie SIGTERM : on quitte proprement (fermeture du consumer
    # = le groupe est prévenu et réaffecte les partitions immédiatement)
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

    print(f"[{INSTANCE}] Prêt (groupe={GROUP_ID}, topic={TOPIC_IN})", flush=True)
    try:
        for message in consumer:
            try:
                data = json.loads(message.value.decode("utf-8"))

                # Entrée du modèle : DataFrame avec les mêmes colonnes qu'à l'entraînement
                X = pd.DataFrame([{f: data[f] for f in FEATURES}])
                prediction = model.predict(X)[0]
                probabilite = model.predict_proba(X)[0][1]

                print(f"[{INSTANCE}] partition={message.partition} | client={data['client_id']} | "
                      f"prediction={prediction} | probabilite={probabilite:.2f}", flush=True)

                resultat = {
                    "client_id": data["client_id"],
                    "prediction": int(prediction),
                    "probabilite_commande": float(probabilite),
                    "instance": INSTANCE,
                }
                producer.send(TOPIC_OUT, key=str(data["client_id"]), value=resultat)
                producer.flush()
            except (KeyError, ValueError, TypeError) as e:
                # Message invalide : on l'ignore, sans faire planter le service
                print(f"[{INSTANCE}] Message invalide ignoré : {e}", flush=True)

            # Commit APRÈS traitement : si l'instance meurt avant, le message
            # sera relu par une autre (au moins une fois, doublons possibles)
            consumer.commit()
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()