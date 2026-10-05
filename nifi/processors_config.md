# Configuration et Paramétrage Apache NiFi (Level 3)

Ce document décrit en détail l'architecture, la configuration des processeurs et la stratégie de gestion de la contre-pression (*back-pressure*) pour le pipeline d'ingestion temps réel du groupe WABA.

---

## 1. Topologie du Flow NiFi

Le flux d'ingestion suit la séquence suivante :
1. **ListS3** : Scrute en continu le bucket MinIO `raw-landing` pour détecter l'arrivée de nouveaux fichiers CSV.
2. **FetchS3Object** : Télécharge le flux d'octets du fichier S3 correspondant.
3. **ConvertRecord** : Parse les lignes CSV selon le schéma d'en-tête et convertit chaque enregistrement en document JSON unitaire.
4. **UpdateAttribute** : Injecte les métadonnées de traçabilité requises :
   - `ingestion_timestamp` : Horodatage ISO 8601 de prise en charge par le broker d'ingestion.
   - `source_file` : Nom d'origine du fichier (ex: `bank_txn_CI_20261005_01.csv`).
   - `kafka_topic` : Routage dynamique vers `raw-bank-transactions`, `raw-insurance-operations`, `raw-mobile-money-payments`, ou `raw-loan-repayments`.
5. **PublishKafka_2_6** : Publie les événements sérialisés vers le cluster Kafka avec acquittement robuste (`acks=all`).

---

## 2. Configuration Détaillée de la Contre-Pression (Back-Pressure)

Afin d'éviter tout engorgement de la mémoire vive de la JVM NiFi ou la saturation du cluster Kafka en cas de charge crête (ex: pic de fin de mois ou campagne promotionnelle Mobile Money) :

| File d'attente (Queue) | Seuil d'Objets (*Count*) | Seuil de Volume (*Size*) | Politique de délestage / Rétention |
|---|---|---|---|
| **ListS3 -> FetchS3Object** | `10 000` | `1 GB` | Ralentit le polling MinIO lorsque le cluster de traitement est saturé. |
| **FetchS3Object -> ConvertRecord** | `5 000` | `500 MB` | Bloque la récupération d'octets volumineux en mémoire heap. |
| **ConvertRecord -> UpdateAttribute** | `50 000` | `500 MB` | Traitement quasi instantané des attributs en mémoire. |
| **UpdateAttribute -> PublishKafka** | `20 000` | `2 GB` | Protège les courtiers Kafka en cas de ralentissement réseau. |

---

## 3. Stratégie de Résilience et Gestion des Erreurs

- **Queue d'échec (Failure Routing)** : Tout FlowFile ne pouvant être lu ou sérialisé est routé vers un processeur `PutFile` local ou une file DLQ dédiée (`failure-nifi/`).
- **Garantie de livraison At-Least-Once** : Les objets ne sont acquittés sur le bucket S3 qu'après confirmation explicite du broker Kafka via le topic `raw-*`.
