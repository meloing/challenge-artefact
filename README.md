# 🏦 WestAfrica BancAssur Group (WABA Group)
## Plateforme Data Lakehouse & Streaming Lambda (Niveaux 1 à 4)

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Docker Compose](https://img.shields.io/badge/docker--compose-2.20+-2496ED.svg)](https://docs.docker.com/compose/)
[![Kubernetes](https://img.shields.io/badge/kubernetes-1.28+-326CE5.svg)](https://kubernetes.io/)
[![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-blue.svg)](https://iceberg.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C.svg)](https://spark.apache.org/)
[![Trino](https://img.shields.io/badge/Trino-SQL-DD00A1.svg)](https://trino.io/)
[![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE.svg)](https://airflow.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-231F20.svg)](https://kafka.apache.org/)

**Auteur :** Armel Kouadio  
**Projet :** Data Lakehouse Multi-Pays & Multi-Métiers WABA Group  
**Périmètre :** 8 pays d'Afrique de l'Ouest (CI, SN, ML, BF, GN, TG, BJ, GH) | 4 Lignes de métier (Banque, Assurance, Mobile Money, Microfinance)

---

## 📋 Table des Matières

1. [Vue d'Ensemble & Architecture](#-vue-densemble--architecture)
2. [Prérequis Techniques](#-prérequis-techniques)
3. [Démarrage Rapide (Quick Start)](#-démarrage-rapide-quick-start)
4. [Niveau 1 : Ingestion & Data Lakehouse Batch](#-niveau-1--ingestion--data-lakehouse-batch)
5. [Niveau 2 : Orchestration Airflow & Médaillon](#-niveau-2--orchestration-airflow--architecture-médaillon)
6. [Niveau 3 : Pipeline Hybride Lambda (Streaming NiFi & Kafka)](#-niveau-3--pipeline-hybride-lambda-streaming-nifi--kafka)
7. [Niveau 4 : Déploiement Kubernetes, Gouvernance & Observabilité](#-niveau-4--déploiement-kubernetes-gouvernance--observabilité)
8. [Validation des Données & Tests](#-validation-des-données--tests)
9. [Fichiers et Livrables du Projet](#-fichiers-et-livrables-du-projet)

---

## 🏛️ Vue d'Ensemble & Architecture

La plateforme implémente le **Pattern Lambda** centré sur un **Object Storage S3 (MinIO)** et le format ouvert de table **Apache Iceberg**, garantissant une haute performance analytique, l'idempotence des chargements et le respect des réglementations financières ouest-africaines (**BCEAO** et **CIMA**).

```
                              [ Générateur Streamlit ]
                                         │
                                         ▼ (CSV)
                              ┌─────────────────────┐
                              │ MinIO : raw-landing │
                              └──────────┬──────────┘
                      ┌──────────────────┴──────────────────┐
                      ▼ (Micro-batch / S3 Events)           ▼ (Scheduled Sensors)
             [ Apache NiFi ]                       [ Apache Airflow ]
                      │                                     │
                      ▼                                     ▼
             [ Apache Kafka ]                      [ PySpark Batch ]
                      │                                     │
                      ▼                                     ▼
        [ Spark Structured Streaming ]             [ Iceberg Bronze / Silver ]
                      │                                     │
                      ▼                                     ▼
     [ Kafka Silver & Gold Alerts ]                [ Iceberg Gold KPIs ]
                      │                                     │
                      └──────────────────┬──────────────────┘
                                         ▼
                            [ Trino SQL Query Engine ]
                                         │
                                         ▼
                            [ Dashboards Apache Superset ]
```

### Règles Clés et Contraintes Respectées :
- **Intégrité Référentielle Absolue :** 100% des transactions émises se réfèrent à des comptes (`account_id`), clients (`customer_id`) et agences (`branch_id`) existants. **Zéro clé orpheline**.
- **Gestion Stricte des Devises :** Franc CFA (**XOF**) pour la zone UEMOA (CI, SN, ML, BF, GN, TG, BJ) avec parité fixe $1\text{ EUR} = 655,957\text{ XOF}$, et Cedi (**GHS**) pour le Ghana.
- **Protection des Données (PII) :** Masquage et pseudonymisation systématiques des numéros de compte et coordonnées clients.

---

## ⚙️ Prérequis Techniques

Pour exécuter le projet en local ou sur un cluster de test :

| Outil | Version Minimale | Rôle |
|---|---|---|
| **Python** | `3.10+` (testé sous `3.12.4`) | Générateur, scripts PySpark et tests |
| **Docker & Docker Compose** | `Docker 24.0+`, `Compose v2.20+` | Déploiement conteneurisé Niveaux 1-3 |
| **Kubernetes (Optionnel)** | `v1.28+` (Minikube / Kind / K3s) | Déploiement Production Niveau 4 |
| **RAM Recommandée** | `8 Go` (Niveaux 1-2) / `16 Go` (Niveau 3-4) | Exécution des conteneurs |

---

## 🚀 Démarrage Rapide (Quick Start)

### 1. Cloner le Dépôt et Configurer l'Environnement
```bash
git clone https://github.com/votre-compte/waba-data-lakehouse.git
cd waba-data-lakehouse

# Copier le fichier d'exemple des variables d'environnement
cp README.env.example .env
```

### 2. Exécuter la Suite de Tests Automatisés
La suite de tests valide immédiatement l'intégrité référentielle, les formules de calcul des KPIs et les règles de conformité :
```bash
make test
# Ou directement en ligne de commande :
python3 -m unittest discover tests -v
```
*Résultat attendu : 20 tests exécutés avec succès (`OK`).*

---

## 📦 Niveau 1 : Ingestion & Data Lakehouse Batch

### 1. Démarrer la Stack Docker des Niveaux 1 & 2
Lance en une seule commande MinIO, Apache Iceberg REST Catalog, Apache Spark, Trino, l'application Streamlit et Apache Airflow :
```bash
make up-level1-2
# Ou :
docker compose -f docker-compose.level1-2.yml up -d
```

### 2. Accéder aux Interfaces Utilisateur
- **MinIO Console :** [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin123`)
- **Générateur Streamlit :** [http://localhost:8501](http://localhost:8501)
- **Trino Coordinator :** [http://localhost:8080](http://localhost:8080)
- **Spark Master UI :** [http://localhost:8081](http://localhost:8081)
- **Airflow Webserver :** [http://localhost:8085](http://localhost:8085) (`admin` / `admin`)

### 3. Génération des Données de Test (Streamlit ou CLI)
Vous pouvez générer les données soit via l'interface Streamlit interactive, soit en ligne de commande sans interface :
```bash
# Génération en mode CLI (Référentiels + Transactions pour les 8 pays)
make run-gen
```

Les fichiers générés sont automatiquement déposés dans le bucket `raw-landing/` selon l'arborescence :
```
raw-landing/
├── references/
│   ├── customers.csv
│   ├── accounts.csv
│   ├── branches.csv
│   └── products.csv
├── CI/BANK/bank_txn_CI_20261005_01.csv
├── CI/INSURANCE/insurance_ops_CI_20261005_01.csv
├── CI/MOBILE_MONEY/mobile_money_CI_20261005_01.csv
└── GH/BANK/bank_txn_GH_20261005_01.csv
```

### 4. Exécuter l'Ingestion PySpark vers la Couche Bronze
```bash
docker exec -it waba-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/lakehouse/spark_jobs/bronze_ingestion.py
```

### 5. Premières Requêtes Analytiques Trino SQL
Connectez-vous à Trino via le client CLI ou votre outil SQL favori (DBeaver, DataGrip) sur `localhost:8080` (catalogue: `iceberg`) :
```sql
-- Vérifier les soldes totaux par pays et devise dans Iceberg
SELECT country_code, currency, COUNT(*) AS nb_comptes, ROUND(SUM(balance), 2) AS solde_total
FROM iceberg.bronze.accounts
GROUP BY country_code, currency;

-- Vérifier les volumes de transactions par statut
SELECT country_code, transaction_status, COUNT(*) AS count_txns, ROUND(SUM(amount), 2) AS montant_total
FROM iceberg.bronze.bank_transactions
GROUP BY country_code, transaction_status;
```

---

## 🔄 Niveau 2 : Orchestration Airflow & Architecture Médaillon

### Les 4 DAGs Airflow Implémentés
1. `dag_ingest_raw` : Détecte les nouveaux fichiers sur `raw-landing` toutes les 15 minutes et lance l'ingestion Bronze.
2. `dag_bronze_to_silver` : Nettoie, déduplique, enrichit avec les référentiels, convertit les montants en EUR et masque les données PII.
3. `dag_silver_to_gold` : Calcule les 7 tables de KPIs métier et de conformité.
4. `dag_regulatory_report` : Planifié quotidiennement à **J+1 (00h30 UTC)** pour générer les déclarations prudentielles **BCEAO** et **CIMA** avec surveillance SLA (alerte si non-finalisé à 06h00 UTC).

### Déclencher manuellement le pipeline complet :
```bash
# Déclencher le DAG d'ingestion brute depuis la ligne de commande Airflow
docker exec -it waba-airflow-webserver airflow dags trigger dag_ingest_raw -c '{"country_code": "ALL"}'
```

### Consultation des Ratios Réglementaires dans Trino :
```sql
-- Surveillance NPL BCEAO (< 5%)
SELECT country_code, loan_type, ROUND(npl_ratio * 100, 2) AS npl_pct, risk_status
FROM iceberg.gold.npl_ratio_by_country;

-- Surveillance Loss Ratio Assurance CIMA (< 70%)
SELECT product_line, country_code, ROUND(loss_ratio * 100, 2) AS s_p_pct, is_cima_alert
FROM iceberg.gold.loss_ratio_by_product;
```

---

## ⚡ Niveau 3 : Pipeline Hybride Lambda (Streaming NiFi & Kafka)

### 1. Démarrer la Stack Complète (incluant Kafka, NiFi & Observabilité)
```bash
make up-all
```
- **Apache NiFi :** [http://localhost:8090/nifi](http://localhost:8090/nifi)
- **Prometheus :** [http://localhost:9090](http://localhost:9090)
- **Grafana :** [http://localhost:3000](http://localhost:3000) (`admin` / `admin`)

### 2. Initialiser les 10 Topics Kafka
```bash
python3 streaming/kafka_topics_init.py localhost:9092
```

### 3. Lancer les Jobs Spark Structured Streaming
```bash
# Job 1 : Raw -> Silver Streaming (Validation JSON, DLQ, Watermark 10min, Double Sink)
python3 streaming/raw_to_silver_streaming.py

# Job 2 : Silver -> Gold Streaming (Détection Fraude Vélocité & Seuils AML)
python3 streaming/silver_to_gold_streaming.py
```

### 4. Exécuter la Requête Trino Lambda Unifiée (Batch + Streaming)
Exécutez la requête canonique combinant historique Iceberg et temps réel Kafka :
```sql
-- Fichier: streaming/lambda_unified_query.sql
SELECT
  COALESCE(b.country_code, s.country_code) AS country,
  COALESCE(b.txn_date, CAST(s.event_time AS DATE)) AS date,
  COALESCE(b.total_amount_eur, 0) + COALESCE(s.streaming_amount_eur, 0) AS total_eur
FROM iceberg.gold.daily_transaction_volume b
FULL OUTER JOIN (
    SELECT country_code, CAST(from_iso8601_timestamp(timestamp) AS DATE) AS event_time, SUM(amount_eur) AS streaming_amount_eur
    FROM kafka.default."silver-bank-transactions"
    GROUP BY country_code, CAST(from_iso8601_timestamp(timestamp) AS DATE)
) s
  ON b.country_code = s.country_code AND b.txn_date = s.event_time
ORDER BY 1, 2;
```

---

## ☸️ Niveau 4 : Déploiement Kubernetes, Gouvernance & Observabilité

### 1. Déploiement sur Cluster Kubernetes (Minikube / Production)
Les manifestes sont cloisonnés en 5 namespaces distincts :
```bash
# 1. Créer les 5 namespaces
kubectl apply -f k8s/namespaces.yaml

# 2. Déployer les configurations et secrets
kubectl apply -f k8s/secrets-configmaps.yaml

# 3. Déployer les composants par domaine
kubectl apply -f k8s/minio/minio-k8s.yaml
kubectl apply -f k8s/kafka/kafka-k8s.yaml
kubectl apply -f k8s/nifi/nifi-k8s.yaml
kubectl apply -f k8s/spark-operator/spark-operator-apps.yaml
kubectl apply -f k8s/airflow/airflow-k8s.yaml
kubectl apply -f k8s/trino/trino-k8s.yaml
kubectl apply -f k8s/superset/superset-k8s.yaml
kubectl apply -f k8s/ingress-routes.yaml
```

### 2. Les 3 Dashboards Apache Superset
Situés dans `superset/dashboards_definitions/` :
1. **Performance Commerciale Groupe :** Revenus consolidés par pays/métier, évolution mensuelle ARPC, carte d'Afrique de l'Ouest, Top 10 produits.
2. **Risque & Conformité Réglementaire :** Jauge NPL par pays (vert $<3\%$, orange $3-5\%$, rouge $>5\%$), Loss Ratio assurance (seuil $70\%$), alertes AML 30 jours, tableau de bord des sinistres vs SLA.
3. **Mobile Money & Transferts :** Heatmap horaire des flux, Top 5 couloirs UEMOA (CI $\rightarrow$ SN, etc.), taux d'échec par opérateur télécom.

### 3. Gouvernance & Sécurité
- **Keycloak SSO (`security_governance/keycloak_realm_waba.json`) :** 4 profils RBAC configurés (`group_admin`, `country_analyst`, `compliance_officer`, `viewer`).
- **OpenMetadata (`security_governance/openmetadata/`) :** Lignage automatisé `raw -> bronze -> silver -> gold` et marquage PII sur les champs identifiants.

### 4. Observabilité & Alerting (Prometheus & Grafana)
Les 3 règles d'alerte critiques sont configurées dans `observability/prometheus/alert_rules.yml` :
- **Alerte 1 :** Job Spark Streaming de fraude inactif $> 5$ minutes.
- **Alerte 2 :** Consumer Lag Kafka AML $> 5\,000$ messages en attente.
- **Alerte 3 :** Échec du DAG réglementaire `dag_regulatory_report` à J+1 06h00 UTC.

---

## 🧪 Validation des Données & Tests

Le projet est livré avec une suite de tests unitaires et d'intégration validant tous les aspects critiques du système :
```bash
python3 -m unittest discover tests -v
```

Tests inclus :
- `tests/test_generator_referential.py` : Absence totale de clés orphelines sur l'ensemble des pays.
- `tests/test_currency_conversion.py` : Validation de la parité XOF/EUR (655.957) et GHS/EUR.
- `tests/test_kpis_calculation.py` : Exactitude mathématique des ratios NPL, Loss Ratio, ARPU.
- `tests/test_streaming_rules.py` : Déclenchement précis des alertes AML ($1\,000\,000\text{ XOF}$ / $5\,000\text{ GHS}$) et masquage PII.

---

## 📁 Fichiers et Livrables du Projet

- `data_generator/` : Moteur de génération synthétique et application Streamlit.
- `lakehouse/` : Jobs PySpark d'ingestion Bronze, transformation Silver, calcul des KPIs Gold et reporting réglementaire.
- `airflow/` : Les 4 DAGs d'orchestration avec plugins d'alerting et retries exponentiels.
- `streaming/` : Pipeline Spark Structured Streaming (Raw $\rightarrow$ Silver avec DLQ, Silver $\rightarrow$ Gold avec détection fraude/AML), initialisation des topics Kafka, requête Trino Lambda unifiée.
- `nifi/` : Template de flow NiFi pour la conversion S3 $\rightarrow$ Kafka et configuration du back-pressure.
- `trino/` : Fichiers de configuration des catalogues Iceberg et Kafka, suite de requêtes SQL de test.
- `superset/` : Configuration SQLAlchemy/SSO et définitions des 3 dashboards décisionnels.
- `security_governance/` : Définition du Realm Keycloak et cartographie OpenMetadata / PII.
- `observability/` : Configuration Prometheus, règles d'alerte, dashboard Grafana et agent Promtail/Loki.
- `k8s/` : Manifestes Kubernetes organisés par namespace.
- `write_up_technique.md` : Document d'architecture et de justification technique détaillé de 5 pages.
- `README.env.example` : Modèle documenté des variables d'environnement.
- `Makefile` : Raccourcis d'automatisation des commandes.
