import json
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

BOOTSTRAP = "localhost:9092"
TOPIC = "commandes-creees"

# 1. Connexion au broker
try:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="check-kafka")
except KafkaError as e:
    print("Impossible de joindre Kafka sur", BOOTSTRAP, "->", e)
    print("Vérifie : docker ps (conteneur 'kafka' en Up) et docker logs kafka")
    raise SystemExit(1)
print("Connexion au broker : OK")

# 2. Création du topic
try:
    admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
    print(f"Topic '{TOPIC}' créé")
except TopicAlreadyExistsError:
    print(f"Topic '{TOPIC}' existe déjà")
print("Topics disponibles :", [t for t in admin.list_topics() if not t.startswith("__")])
admin.close()

# 3. Test aller-retour : on envoie un événement puis on le relit
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
producer.send(TOPIC, {"commande_id": 0, "client_id": 0, "produit": "TEST", "montant": 0})
producer.flush()
producer.close()

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)
for msg in consumer:
    print("Message relu :", msg.value)
consumer.close()