import json
import sys
import warnings

from kafka import ConsumerRebalanceListener, KafkaConsumer

warnings.filterwarnings("ignore", category=DeprecationWarning)

BOOTSTRAP = "localhost:9092"
TOPIC = "commandes-creees"

if len(sys.argv) != 3:
    print("Usage : python consumer_groupe.py <group_id> <nom_du_consommateur>")
    raise SystemExit(1)
group_id, nom = sys.argv[1], sys.argv[2]


class Listener(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        pass

    def on_partitions_assigned(self, assigned):
        print(f"[{nom}] partitions assignées : {sorted(p.partition for p in assigned)}")


consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    group_id=group_id,
    auto_offset_reset="latest",     # on ne veut observer que les nouveaux messages
    enable_auto_commit=True,
)
consumer.subscribe([TOPIC], listener=Listener())
print(f"[{nom}] démarré, groupe '{group_id}'. En attente de messages...")

for message in consumer:
    try:
        evt = json.loads(message.value.decode("utf-8"))
    except json.JSONDecodeError:
        continue
    print(f"[{nom} | {group_id}] partition {message.partition}, offset {message.offset} "
          f"-> client {evt['client_id']}, {evt['produit']}, {evt['montant']} €")

