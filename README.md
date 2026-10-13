# TP Micro-services : API REST (Partie I) et Kafka (Partie II)

Ce document explique pas à pas comment construire, lancer et tester le projet.
Il est organisé en deux parties, qui correspondent aux deux TP :

- **Partie I** : deux micro-services Flask (`clients`, `commandes`) dockerisés et communiquant par API REST.
- **Partie II** : communication asynchrone via **Apache Kafka** (producteur `commandes`, consommateurs `clients` et `statistiques`).

---

## 0. Prérequis

| Outil | Utilité |
|---|---|
| Docker Desktop (ou Docker Engine + Compose) | Construire et exécuter les conteneurs |
| Python 3.12 | Lancer les micro-services de la Partie II |
| PowerShell | Les commandes `Invoke-RestMethod` sont écrites pour PowerShell (équivalent `curl` possible) |

Vérifier l'installation :

```bash
docker --version
docker compose version
python --version
```

### Arborescence attendue

```
tp-microservices/
├── clients/
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
├── commandes/
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
├── docker-compose.yml        # infrastructure Kafka (Partie II)
├── app_clients.py            # consommateur Kafka "clients"   (Partie II)
├── app_commandes.py          # producteur Kafka + API REST    (Partie II, port 5002)
├── app_statistiques.py       # consommateur Kafka statistiques (Partie II, port 5003)
└── consumer_groupe.py        # test des groupes de consommateurs
```

Tout d'abord lancer tous les fichiers 


> Toutes les commandes ci-dessous sont à lancer **depuis la racine du projet** (`tp-microservices/`).


# PARTIE I : Projet API REST

########## Tout d'abord lancer tous les fichiers python et docker de la Partie I ##########

## 1. Construire et lancer le micro-service Clients (port 8080)

```bash
# Construire l'image
docker build -t microservice-clients ./clients

# (Optionnel) supprimer un ancien conteneur du même nom
docker rm clients

# Lancer le conteneur
docker run -p 8080:8080 --name clients microservice-clients
```

## 2. Construire et lancer le micro-service Commandes (port 8081)

Dans un **second terminal** :

```bash
# Construire l'image
docker build -t microservice-commandes ./commandes

# (Optionnel) supprimer un ancien conteneur du même nom
docker rm commandes

# Lancer le conteneur
docker run -p 8081:8081 --name commandes microservice-commandes
```

> **Remarque sur le réseau Docker** : le micro-service Commandes doit joindre Clients par REST.
> Si la configuration utilise un réseau Docker dédié, le créer **avant** de lancer les conteneurs
> et ajouter `--network` aux deux commandes `docker run` :
>
> ```bash
> docker network create reseau-tp
> docker run -p 8080:8080 --network reseau-tp --name clients microservice-clients
> docker run -p 8081:8081 --network reseau-tp --name commandes microservice-commandes
> ```
>
> Commandes appelle alors Clients via son nom de conteneur (`http://clients:8080`).

## 3. Vérifier que les conteneurs tournent

```bash
docker ps
```

On doit voir les conteneurs `clients` (port 8080) et `commandes` (port 8081) avec le statut `Up`.

## 4. Tester les API

```bash
curl http://localhost:8080/clients          # tous les clients
curl http://localhost:8080/clients/1        # le client n°1
curl http://localhost:8081/commandes        # toutes les commandes
curl http://localhost:8081/commandes/1      # commande n°1 + infos du client (appel REST vers Clients)
```

## 5. Tester la gestion des erreurs

Ouvrir dans un navigateur (ou avec `curl`) :

- http://localhost:8080/clients/100 → **404** (client inexistant)
- http://localhost:8081/commandes/100 → **404** (commande inexistante)

Pour tester l'indisponibilité de Clients : arrêter le conteneur puis rappeler Commandes.

```bash
docker stop clients
curl http://localhost:8081/commandes/1     # doit retourner une erreur gérée proprement
docker start clients
```

---

# PARTIE II : Projet Kafka

## 1. Démarrer Kafka avec Docker

```bash
# Démarrer l'infrastructure Kafka (docker-compose.yml)
docker compose up -d

# Vérifier l'état des conteneurs
docker ps -a
```

Si le conteneur `kafka` existe déjà mais est arrêté :

```bash
docker start kafka
docker ps
```

Le conteneur `kafka` doit apparaître avec le statut `Up`.

## 2. Installer les dépendances Python

```bash
pip install flask requests kafka-python
```

> Adapter selon le contenu des fichiers `requirements.txt` du projet.

## 3. Lancer les micro-services Python

Ouvrir **un terminal par service** :

```bash
python app_clients.py        # consommateur : affiche les commandes reçues et compte par client
python app_commandes.py      # producteur  : API REST sur http://localhost:5002
python app_statistiques.py   # consommateur : API REST sur http://localhost:5003
python consumer_groupe.py <group_id> <nom>   # utilisé à l'étape 8 uniquement
```

## 4. Tester Kafka (commandes Docker)

```bash
# Derniers logs du broker
docker logs kafka --tail 10

# Lister les topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Détails du topic commandes-creees
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic commandes-creees

# Lire tous les messages du topic depuis le début
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic commandes-creees --from-beginning --bootstrap-server localhost:9092

# Lister les groupes de consommateurs
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

## 5. Créer des commandes (producteur)

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:5002/commandes -ContentType "application/json" -Body '{"client_id":3,"produit":"PC","montant":900,"date_commande":"2026-10-05T16:45:00"}'

Invoke-RestMethod -Method Post -Uri http://localhost:5002/commandes -ContentType "application/json" -Body '{"client_id":2,"produit":"Casque","montant":150,"date_commande":"2026-10-05T16:45:00"}'
```

Dans le terminal de `app_clients.py`, on observe pour chaque commande le client, le produit, le montant, puis le nombre total de commandes par client.

## 6. Arrêter un micro-service et observer le lag (question 1.6)

1. Arrêter `app_clients.py` (Ctrl+C dans son terminal).
2. Créer plusieurs commandes (voir étape 5).
3. Observer le **lag** du groupe `clients-group` :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group clients-group
```

   La colonne `LAG` est supérieure à 0 : les messages sont conservés dans Kafka.
4. Relancer `app_clients.py` puis refaire la même commande : le `LAG` retombe à 0 et les messages manqués sont consommés → **aucun événement n'est perdu**.

## 7. Deuxième consommateur : statistiques (question 1.7)

```bash
# Lag de chaque groupe
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group statistiques-group
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group clients-group
```

Consulter les statistiques :

```powershell
Invoke-RestMethod http://localhost:5003/statistiques/ca                # chiffre d'affaires total
Invoke-RestMethod http://localhost:5003/statistiques/ca/2026-10        # chiffre d'affaires du mois
Invoke-RestMethod http://localhost:5003/statistiques/produits          # commandes par produit
Invoke-RestMethod http://localhost:5003/statistiques/produits/pc       # commandes pour le produit "pc"
```

## 8. Groupes de consommateurs (question 8)

**a) Deux consommateurs dans le même groupe** (les messages sont répartis entre eux) :

```bash
python consumer_groupe.py groupe-A A1
python consumer_groupe.py groupe-A A2
```

**b) Deux consommateurs dans des groupes différents** (chacun reçoit tous les messages) :

```bash
python consumer_groupe.py clients-test C1
python consumer_groupe.py statistiques-test S1
```

Puis créer des commandes (étape 5) et comparer les messages reçus.

Pour afficher les offsets des messages et lister les groupes :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic commandes-creees --from-beginning --property print.offset=true

docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

**Conclusion attendue** :
- **Même groupe** : chaque message est traité par **un seul** consommateur (répartition de la charge).
- **Groupes différents** : chaque groupe reçoit **tous** les messages (diffusion, chaque service traite l'événement indépendamment).

---

## Résumé des ports

| Service | Port |
|---|---|
| Clients (REST, Docker) | 8080 |
| Commandes (REST, Docker) | 8081 |
| Commandes (producteur Kafka) | 5002 |
| Statistiques (REST) | 5003 |
| Kafka broker | 9092 |

## Nettoyage

```bash
docker stop clients commandes
docker rm clients commandes
docker compose down
docker network rm reseau-tp    # si un réseau a été créé
```
