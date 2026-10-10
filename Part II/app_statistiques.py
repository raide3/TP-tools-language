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
GROUP_ID = "statistiques-group"     # groupe différent de clients-group

nb_par_produit = defaultdict(int)       # "pc" -> nombre de commandes
ca_par_mois = defaultdict(float)        # "2026-10" -> chiffre d'affaires
verrou = threading.Lock()


def consommer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",   # premier lancement : relit tout l'historique
        enable_auto_commit=True,
    )
    print(f"[statistiques] Abonné au topic '{TOPIC}' (groupe '{GROUP_ID}')")

    for message in consumer:
        try:
            evt = json.loads(message.value.decode("utf-8"))
        except json.JSONDecodeError:
            continue

        if evt.get("commande_id") == 0:         # message de test du 1.3
            continue

        produit = str(evt["produit"]).strip().lower()
        mois = str(evt.get("date_commande", ""))[:7]     # "2026-10-10T14:16:17" -> "2026-10"

        with verrou:
            nb_par_produit[produit] += 1
            if mois:
                ca_par_mois[mois] += float(evt["montant"])
            nb = nb_par_produit[produit]
            ca = ca_par_mois.get(mois, 0.0)

        print(f"[statistiques] Commande #{evt['commande_id']} | produit : {produit} "
              f"(total {nb}) | mois {mois or '?'} : CA = {ca:.2f} €")


app = Flask(__name__)


@app.route("/statistiques/produits", methods=["GET"])
def tous_les_produits():
    with verrou:
        return jsonify(dict(nb_par_produit))


@app.route("/statistiques/produits/<produit>", methods=["GET"])
def nb_commandes_produit(produit):
    with verrou:
        return jsonify({"produit": produit, "nb_commandes": nb_par_produit.get(produit.lower(), 0)})


@app.route("/statistiques/ca", methods=["GET"])
def tout_le_ca():
    with verrou:
        return jsonify({m: round(v, 2) for m, v in ca_par_mois.items()})


@app.route("/statistiques/ca/<mois>", methods=["GET"])
def ca_mois(mois):
    # format du mois : AAAA-MM, par exemple 2026-10
    with verrou:
        return jsonify({"mois": mois, "chiffre_affaires": round(ca_par_mois.get(mois, 0.0), 2)})


if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host="0.0.0.0", port=5003, debug=False)