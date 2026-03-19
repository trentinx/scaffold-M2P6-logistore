# Veille Technologique et Conceptuelle - LogiStore

## 1. Flux EDI (Electronic Document Interface)

### Définition
Un flux EDI est un échange de documents structurés entre systèmes informatiques, utilisant un format standardisé pour permettre une interopérabilité automatisée. Les EDI diffèrent des API et des RPC en ce qu'ils opèrent en **mode asynchrone et batch**, alors que les API sont généralement synchrones et les RPC basées sur des appels de fonction directes.

### Distinction avec API et RPC

**API (Application Programming Interface)** :
- Communication **synchrone** (requête → attente réponse immédiate)
- Basée sur les protocoles HTTP/REST/GraphQL
- Requête-réponse en temps quasi-réel
- Exemple : `GET /api/products` retourne une réponse en millisecondes
- **Cas d'usage** : Applications interactives, services web

**RPC (Remote Procedure Call)** :
- Communication **synchrone** (appel de fonction distante)
- L'appelant attend l'exécution et le retour du distant
- Protocoles : JSON-RPC, XML-RPC, gRPC
- Exemple : `getUser(123)` exécute la fonction sur le serveur distant
- **Cas d'usage** : Microservices tightly-coupled, architectures legacy

**EDI (Electronic Document Interface)** :
- Communication **asynchrone** (envoyer et oublier)
- Basée sur l'échange de fichiers/messages structurés
- Pas d'attente immédiate de réponse
- Exemple : Envoyer un fichier CSV, traitement en batch plus tard
- **Cas d'usage** : Intégration B2B, pipelines de données, haute latence acceptable

### Outils associés
- **EDIFACT** : Standard international pour les EDI
- **X12** : Standard nord-américain
- **CSV/JSON/XML** : Formats simplifiés (dans LogiStore, les CSV sont des EDI minimalistes)
- **Apache Kafka** : Pour structurer les flux EDI en streaming

### Rôle dans le projet LogiStore
Dans LogiStore, les fichiers CSV (`catalogue_small.csv`, `movements_small.csv`) représentent des **flux EDI simplifiés**. Ils sont ingérés par les DAGs Airflow (dag1_ingest_catalogue, dag2_ingest_movements) et constituent le point d'entrée du pipeline de données. Contrairement aux APIs qui nécessitent une connexion persistante, ces fichiers permettent un traitement par batch régulier.

### Liens documentation officielle
- [EDIFACT Standards - UN/CEFACT](https://www.edibasics.com/edi-resources/document-standards/edifact/)
- [Introduction to EDI](https://www.edibasics.com/what-is-edi/)
- [EDI Explained - Connexions Software](https://www.1edisource.com/blog/edi-connection-101-everything-you-need-to-know/)

---

## 2. Contrats d'Interface (Data Contracts)

### Définition
Un contrat d'interface est un **accord formel entre producteur et consommateur de données** qui définit la structure, le type, le schéma et les règles de validation des données échangées. Il formalise l'expectation mutuelle, permet le versionning et automatise la vérification de conformité.

### Outils associés
- **Pydantic** : Validation de schémas en Python (HTTP3 moderne, production-ready)
- **Apache AVRO** : Schémas binaires avec évolution compatible
- **Confluent Schema Registry** : Gestion centralisée des schémas (lourd, nécessite Kafka)
- **Great Expectations** : Framework complet pour les données (tests, docs)
- **YAML/JSON Schemas** : Approches légères et versionables

### Solution recommandée pour LogiStore
**Pydantic** est la solution optimale pour un contexte Python sans infrastructure lourde :
- Léger et sans dépendances externes
- Validation déclarative et claire
- Extensible et facile à versionner
- Files [`contracts/catalogue_contract.py`](contracts/catalogue_contract.py) et [`contracts/movement_contract.py`](contracts/movement_contract.py) implémentent déjà cette approche

### Rôle dans le projet LogiStore
Les contrats assurent que :
- Les fichiers CSV ingérés respectent le schéma attendu
- Les données rejetées sont tracées (raison du rejet documentée)
- Les tests ([`tests/test_contracts.py`](tests/test_contracts.py)) valident la conformité
- La versionning permet d'évoluer sans casser les consommateurs

### Liens documentation officielle
- [Pydantic Documentation](https://docs.pydantic.dev/latest/)
- [Apache AVRO Documentation](https://avro.apache.org/docs/)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [JSON Schema Specification](https://json-schema.org/)

---

## 3. Patron Dead Letter Queue (DLQ) et Rejeu

### Définition
Un **Dead Letter Queue** est un mécanisme de gestion des défaillances dans les systèmes de messagerie. Les messages qui ne peuvent pas être traités sont routed vers une file de rejet, où ils peuvent être :
- Loggés et monitorés
- Rejoués une fois les conditions réunies
- Analysés pour correction

Ce patron garantit la **resilience et l'observabilité**.

### Concepts clés associés

**Résilience** : Capacité d'un système à continuer de fonctionner face à des défaillances sans perte de données. Avec DLQ :
- Les erreurs n'interrompent pas le pipeline
- Les données en erreur sont sauvegardées (pas perdues)
- Le système peut se rétablir automatiquement après correction

**Observabilité** : Capacité à comprendre l'état interne d'un système et diagnostiquer les problèmes en temps réel. Avec DLQ :
- Chaque rejet est loggé avec contexte (raison, timestamp, données)
- Vision complète des défaillances
- Traçabilité des rejeux et récoveries

### Outils associés
- **Kafka** : Topic DLQ natif avec Consumer Groups
- **RabbitMQ** : Dead Letter Exchange (DLX)
- **AWS SQS** : Dead Letter Queue intégré
- **Bull/BullMQ** : Pour queues en mémoire/Redis

### Transposition au pipeline CSV (LogiStore)
Au lieu de files de messagerie, LogiStore utilise un **pipeline de fichiers** :
- Fichiers en erreur : stockés dans [`data/rejected/`](data/rejected/)
- Logs détaillés : [`data/rejected/reports/`](data/rejected/reports/) avec raison du rejet
- DAG de rejeu : [`airflow/dags/dag4_replay_rejected.py`](airflow/dags/dag4_replay_rejected.py) retraite les fichiers rejetés
- Tests : [`tests/test_replay.py`](tests/test_replay.py) valide le mécanisme

### Rôle dans le projet LogiStore
- **dag1, dag2** : Ingestion avec validation (rejet si violation de contrat)
- **Stockage rejeté** : Fichiers non conformes isolés pour analyse
- **dag4** : Rejeu automatisé après correction des sources
- **Observabilité** : Chaque rejet tracé avec métadonnées pour debug

### Liens documentation officielle
- [Kafka Dead Letter Topic Pattern](https://kafka.apache.org/documentation/#design_deathLetter)
- [RabbitMQ Dead Letter Exchanges](https://www.rabbitmq.com/dlx.html)
- [AWS SQS Dead Letter Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)

---

## 4. Orchestration de Workflows avec Airflow

### Définition
**Apache Airflow** est un orchestrateur de workflows basé sur des DAGs (Directed Acyclic Graphs). Chaque nœud représente une tâche, et les dépendances assurent un ordre d'exécution correct. C'est un **orchestrateur Python-natif** avec une interface web, des logs centralisés et une gestion d'état robuste.

### Concepts clés
- **DAG** (Directed Acyclic Graph) : Ensemble de tâches avec dépendances ordonnées
- **Task** : Unité atomique de travail (Python operator, Bash, SQL, etc.)
- **Sensor** : Tâche qui attend une condition (fichier, base de données, signal)
- **Dataset API** (Airflow 2.4+) : Coordination entre DAGs sans couplage fort

### Couplage Fort vs Découplage

**Couplage fort** : Lorsqu'un DAG A dépend directement d'un DAG B (il connaît son ID exact, sa structure, ses outputs).
- DAG A doit être modifié si DAG B change de nom, d'ID ou de structure
- Difficulté à ajouter de nouveaux consommateurs
- Scalabilité réduite (N producteurs × M consommateurs = N×M dépendances)
- Risque : une défaillance en cascade se propage

**Découplage (Loose Coupling)** : Les DAGs communiquent via des abstractions (datasets, événements) plutôt que des références directes.
- Producteur et consommateur ignorent mutuellement leur existence
- Chacun peut évoluer indépendamment
- Simple d'ajouter de nouveaux consommateurs (déclarer le dataset suffit)
- Résilience : défaillance isolée d'un producteur

**Ce que le découplage apporte** :
- ✅ **Flexibilité** : Évolution indépendante des DAGs
- ✅ **Scalabilité** : Relations non explosives avec N producteurs et M consommateurs
- ✅ **Maintenabilité** : Modifications localisées, pas de cascades
- ✅ **Résilience** : Défaillances isolées
- ✅ **Testabilité** : Tests indépendants du DAG

### Dataset API : Alternative au couplage fort
**Avant (TriggerDagRunOperator)** :
```python
TriggerDagRunOperator(trigger_dag_id="dag2")  # Couplage dur
```

**Après (Dataset API)** :
```python
from airflow.datasets import Dataset

my_dataset = Dataset("s3://bucket/catalogue.parquet")

# DAG 1 produit le dataset
with DAG("dag1") as dag1:
    produce = PythonOperator(outlets=[my_dataset])

# DAG 2 consomme le dataset (déclenché auto)
with DAG("dag2") as dag2:
    task = PythonOperator(dag_in=[my_dataset])
```

**Avantages** :
- Découplage logique
- Scalabilité (N producteurs, M consommateurs)
- Sémantique claire (qui dépend de quoi ?)

### Rôle dans le projet LogiStore
- **dag1** : Ingère catalogue CSV → produit dataset `catalogue.parquet`
- **dag2** : Ingère mouvements CSV → produit dataset `movements.parquet`
- **dag3** : Déclenché par dag1 + dag2 (Dataset API) → analytiques
- **dag4** : Rejeu des fichiers rejetés, déclenché manuellement ou par signal

### Comparaison avec autres mécanismes

| Mécanisme | Couplage | Scalabilité | Flexibilité |
|-----------|----------|-------------|------------|
| TriggerDagRunOperator | Fort | Faible (1→1) | Limitée |
| ExternalTaskSensor | Moyen | Moyen | Moyenne |
| Dataset API | Faible | Excellente | Excellente |
| Webhooks | Très faible | Très bonne | Très grande |

### Liens documentation officielle
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
  - [Airflow Dataset API (2.4+)](https://airflow.apache.org/docs/apache-airflow/stable/datasets.html)
- [DAG Dependencies Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#dag-dependencies)

---

## 5. Stockage Columnar et Format Parquet

### Définition
Le **stockage columnar** organise les données par **colonne plutôt que par ligne**. Contrairement au CSV (stockage ligne), Parquet compresse chaque colonne individuellement, ce qui permet :
- Compression optimale (données homogènes)
- Lectures analytiques rapides (projection pushdown)
- Économies d'I/O significatives

### Parquet vs CSV (stockage ligne)
```
CSV (ligne)           Parquet (columnar)
Row 1: [A1, B1, C1]   Col A: [A1, A2, A3, ...]
Row 2: [A2, B2, C2]   Col B: [B1, B2, B3, ...]
Row 3: [A3, B3, C3]   Col C: [C1, C2, C3, ...]
```

### Optimisations clés

**1. Projection Pushdown** : Lecture sélective des colonnes
```sql
SELECT price, quantity FROM sales;  -- Charge SEULEMENT price + quantity
-- Sans pushdown : charge toutes les colonnes
```

**2. Predicate Pushdown** : Filtrage au niveau du stockage
```sql
SELECT * FROM sales WHERE date > '2024-01-01';
-- Avec pushdown : les groupes de lignes hors plage ne sont pas lus
```

### Parquet vs PostgreSQL vs DuckDB

| Aspect | CSV | PostgreSQL | Parquet | DuckDB |
|--------|-----|-----------|---------|---------|
| Compression | Faible | Moyenne | Excellente | Excellente |
| Projection Pushdown | Non | Oui | Oui | Oui |
| Predicate Pushdown | Non | Oui | Oui | Oui |
| Partitionnement | Manuel | Natif | Natif | Natif |
| Temps requête analytique | Lent | Moyen | Rapide | Très rapide |
| Coût infrastructure | Nul | Coûteux | Nul (fichier) | Nul (fichier) |

**DuckDB pour analytiques** :
- Requêtes SQL directes sur Parquet (sans ETL)
- Moteur OLAP optimisé
- 0 dépendance infrastructure (single-file)
- 10-100x plus rapide que PostgreSQL pour les scans

### Moteur OLAP

**OLAP (OnLine Analytical Processing)** : Système optimisé pour les **requêtes analytiques** sur de grands volumes de données, contrairement aux bases OLTP (transactionnelles).

**OLAP vs OLTP** :

| Dimension | OLTP | OLAP |
|-----------|------|------|
| **Cas d'usage** | Transactions (INSERT/UPDATE petit volume) | Analytiques (SELECT sur gros volume) |
| **Structure** | Ligne (row-oriented) | Colonne (columnar) |
| **Exemple requête** | `SELECT userId FROM users WHERE id = 123` | `SELECT SUM(sales) FROM transactions WHERE date > 2024` |
| **Latence attendue** | ms (millisecondes) | s (secondes) acceptable |
| **Optimisations** | Index B-Tree, transactions ACID | Compression columnar, pushdown, partitionnement |
| **Moteur** | PostgreSQL, MySQL, Oracle | DuckDB, Apache Spark, Snowflake, BigQuery |

**DuckDB = Moteur OLAP léger** :
- Optimisé pour les **scans sur colonnes** (projection pushdown)
- Compression et paramétrisation des données
- Parallélisation vectorisée (SIMD)
- Pas de serveur : exécution in-process ou single-file
- Idéal pour analytiques sur Parquet (0 infrastructure)

### Rôle dans le projet LogiStore
- **`data/curated/`** : Sorties en Parquet (catalogue, mouvements, analytiques)
- **dag3** : Génère analytiques en Parquet pour requêtes rapides
- **Scripts analytiques** : Utilisent DuckDB pour explorer les données
- **Scalabilité** : Parquet partitionné par date/catégorie

### Liens documentation officielle
- [Parquet Format Specification](https://parquet.apache.org/docs/overview/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [PostgreSQL vs Column Stores](https://www.postgresql.org/docs/current/sql-syntax.html)
- [Apache Arrow (Parquet Foundation)](https://arrow.apache.org/)

---

## 6. Scalabilité d'un Pipeline de Données

### Définition
**La scalabilité** est la capacité d'un système à traiter des volumes croissants de données sans dégradation proportionnelle de performance. Elle repose sur trois axes :
- **Scalabilité verticale** : Plus de ressources sur une même machine (CPU, RAM)
- **Scalabilité horizontale** : Distribution sur plusieurs machines
- **Scalabilité algorithmique** : Amélioration de l'algorithme lui-même

### Stratégies classiques pour grands volumes

#### 1. **Traitement par Chunks (Batch Processing)**
```python
# Sans chunks : charge 1 milliard de lignes en mémoire ❌
df = pd.read_csv("huge_file.csv")

# Avec chunks : traite par lots ✅
for chunk in pd.read_csv("huge_file.csv", chunksize=100_000):
    process(chunk)
```
**Avantage** : Mémoire constante, simple à implémenter

#### 2. **Parallélisme (Multi-threading / Multi-processing)**
```python
from multiprocessing import Pool

# 8 processus parallèles
with Pool(8) as pool:
    results = pool.map(process_chunk, chunks)
```
**Attention** : GIL en Python → multiprocessing préféré

#### 3. **Partitionnement (Horizontal Scaling)**
```
Data/
├── 2024-01-01/
│   ├── part-1.parquet
│   ├── part-2.parquet
├── 2024-01-02/
│   ├── part-1.parquet
```
**Avantage** : Traitement parallèle par partition, pruning efficace

### Scalabilité Verticale vs Horizontale

| Dimension | Verticale | Horizontale |
|-----------|-----------|------------|
| Coût initial | Modéré | Élevé |
| Complexité | Faible | Élevée |
| Limite | Plafond matériel | Théorique (∞) |
| Latence réseau | Nulle | Significative |
| Exemple | 1 serveur 256GB | 10 serveurs 25GB |

### Coûts Éco-conception : Double Stockage SQL + Parquet

**Problème** : Stocker les mêmes données en PostgreSQL ET en Parquet double l'empreinte carbone et les coûts.

**Solutions** :

1. **Parquet principal + PostgreSQL pour logs/metadata** ✅
   - Données analytiques : Parquet (optimisé, zéro infra)
   - Métadonnées : PostgreSQL (petit volume)

2. **Approche "Cold/Hot"**
   - **Hot data** (derniers jours) : PostgreSQL (requêtes fréquentes)
   - **Cold data** (historique) : Parquet/S3 (requêtes rares)

3. **DuckDB comme cache OLAP** ✅
   - Lire depuis Parquet via DuckDB
   - Zéro duplication
   - Requêtes analytiques ultra-rapides
   - Obsolète : PostgreSQL pour ça → DuckDB

### Rôle dans le projet LogiStore

**Croissance projetée** :
- J0 : 1K catalogue, 10K mouvements (CSV)
- J30 : 100K catalogue, 1M mouvements (Parquet partitionné)
- J365 : 1M+ lignes/jour (multi-nœuds envisageable)

**Stratégie LogiStore** :
1. **Phase 1** (Actuelle) : Chunks + Parquet, DuckDB pour requêtes
2. **Phase 2** : Partitionnement par date
3. **Phase 3** (si volume) : Spark/Dask pour parallélisme distribué

**DAG3 (Analytics)** :
- Partitionne output par `date_traitee`
- Compile uniquement les nouvelles partitions
- DuckDB requête sans PostgreSQL supplémentaire

### Liens documentation officielle
- [Airflow Scaling Patterns](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html)
- [Parquet Partitioning Best Practices](https://parquet.apache.org/docs/file-format/metadata/)
- [DuckDB Parallel Execution](https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html)
- [Designing Data-Intensive Applications (Book)](https://unidel.edu.ng/focelibrary/books/Designing%20Data-Intensive%20Applications%20The%20Big%20Ideas%20Behind%20Reliable,%20Scalable,%20and%20Maintainable%20Systems%20by%20Martin%20Kleppmann%20(z-lib.org).pdf) - Chapitre 4-6
