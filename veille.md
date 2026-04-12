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

---

## 7. dbt (Data Build Tool)

### Définition
**dbt** est un framework de **transformation de données** qui permet de définir, tester et documentarer les pipelines ELT en SQL. Contrairement à un ETL traditionnel (Extract-Transform-Load), dbt assume que les données sont déjà chargées dans l'entrepôt et se concentre sur la partie **Transform** (T).

### Concepts clés

**ELT vs ETL** :
- **ETL** : Extract (données brutes) → Transform (nettoyage/transformation) → Load (base cible). Transformation côté client.
- **ELT** : Extract (données brutes) → Load (base cible) → Transform (côté entrepôt). Plus efficace et scalable.

**dbt apporte** :
- Templating SQL : Variables, macros, jinja2
- Tests automatisés : Assertions sur les données
- Documentation versionnable : Commentaires YAML → portail docs auto
- Lineage détaillé : Traçabilité complète source → output
- Modeling par couches :
  - `staging` : Nettoyage et normalisation
  - `intermediate` : Jointures et agrégations
  - `marts` : Tables finales pour BI/analytiques

### Outils associés
- **dbt Cloud** : Orchestration managée et interface web
- **dbt CLI** : Exécution locale
- **Orchestrateurs** : Airflow (intégration via `dbt_runner`), Prefect, Dagster
- **Entrepôts cibles** : PostgreSQL, Snowflake, BigQuery, DuckDB

### Rôle dans le projet LogiStore
LogiStore utilise a priori dbt avec PostgreSQL pour la **couche de transformation** :

- **[dbt_project/](dbt_project/)** : Modèles de transformation
- **Staging** : Nettoyage des données ingérées (catalogue, mouvements)
- **Marts** : Tables finales pour analytiques, dashboards
- **Tests** : Assertions équivalentes au contrat d'interface
- **Intégration Airflow** : dag3 peut exécuter `dbt run` ou déléguer à dbt Cloud

**Architecture** :
```
DAG1: CSV → Parquet/PostgreSQL (raw)
DAG2: CSV → Parquet/PostgreSQL (raw)
dbt:  raw → staging → marts (validated, documented)
DAG3: marts → analytiques finales
```

### Comparaison avec Pure SQL + Tests

| Aspect | SQL pur | dbt |
|--------|---------|-----|
| Réutilisabilité | Faible (requêtes isolées) | Excellente (macros, refs) |
| Documentation | Manuel | Auto-générée |
| Tests | Pas de pattern | Assertions intégrées |
| Versioning | Git uniquement | Git + dbt state |
| Lineage | À reconstruire | Automatic DAG |
| Collaboration | Difficile | Facile (dbt Cloud) |

### Liens documentation officielle
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [dbt + PostgreSQL](https://docs.getdbt.com/docs/supported-data-platforms/postgres)
- [dbt + Airflow Integration](https://docs.getdbt.com/docs/deploy/airflow)

---

## 8. PostgreSQL (Stockage Persistant)

### Définition
**PostgreSQL** est un système de gestion de bases de données relationnelles (SGBDR) open-source, robuste et production-ready. Contrairement aux fichiers Parquet (immobiles), PostgreSQL offre :
- Transactions ACID : Cohérence garantie
- Schéma rigide : Validation des données
- Indexation avancée : Requêtes rapides
- Concurrence : Lectures/écritures simultanées sans conflits

### Rôle PostgreSQL dans une architecture moderne

**PostgreSQL pour OLTP** (transactions) :
- Nouveaux enregistrements en direct (INSERT/UPDATE)
- Transactions garanties ACID
- Cas d'usage : Métadonnées de pipeline, logs, state d'Airflow

**PostgreSQL vs Parquet/DuckDB** :

| Dimension | PostgreSQL | Parquet/DuckDB |
|-----------|-----------|-----------------|
| **Type** | OLTP (transactions) | OLAP (analytiques) |
| **Cas d'usage** | Metadonnées, logs, state | Données analytiques |
| **Écriture** | Efficace (inserts rapides) | Inefficace (refonte fichier) |
| **Lecture analytique** | Lente (row-oriented) | Rapide (columnar) |
| **Infrastructure** | Serveur requis | Fichier local |
| **Scalabilité** | Verticale/horizontale (complexe) | Fichier distribué |
| **Coût** | Serveur + maintenance | Zéro (stockage) |

### Architecture recommandée pour LogiStore

```
Input CSV
  ↓
[Airflow DAGs]
  ↓
┌─────────────────────┐
│ PostgreSQL (OLTP)   │  ← Métadonnées ingestion, logs
│ - run_history       │  ← Qui a ingéré quoi, quand
│ - rejected_logs     │  ← Détail des rejets
│ - dataset_versions  │  ← Tracking versions dbt
└─────────────────────┘
  ↓
[dbt transformation]  ← SQL models en PostgreSQL  
  ↓
┌─────────────────────────────┐
│ PostgreSQL (staging/marts)  │  ← Petit volume, fréquent
│ - dim_catalogue             │  
│ - dim_movements             │  
│ - fct_inventory_analytics   │  ← Petite table (dims)
└─────────────────────────────┘
  ↓
[Dashboard/BI]
  ↓
Parquet/Data Lake  ← Archive froide analytiques (optionnel)
```

### Outils associés
- **psycopg2/psycopg3** : Driver Python natif
- **SQLAlchemy** : ORM Python
- **pgAdmin** : Interface d'administration
- **Backup** : `pg_dump`, WAL archiving
- **Replication** : Streaming replication pour HA

### Rôle dans le projet LogiStore
- **Métadonnées** : Logs d'exécution des DAGs, rejection details
- **dbt models** : Staging et marts déployés en PostgreSQL
- **État Airflow** : Base de données Airflow elle-même (PostgreSQL backend)
- **Archive rapide** : Petites tables analytiques finales

### Concepts clés : Schéma et Typage

**Schéma strict vs "schemaless"** :

```sql
-- Schéma PostgreSQL (strict) : Rejection = erreur
CREATE TABLE catalogue (
  id INTEGER PRIMARY KEY,
  sku VARCHAR(50) NOT NULL,
  price NUMERIC(10,2) NOT NULL  -- Rejet si chaîne passée
);

-- CSV (schemaless) : Pas de contrôle, erreurs à la requête
id,sku,price
1,SKU-001,29.99
2,invalid-sku,"not-a-number"  ← Bug trouvé à la requête, pas à l'insertion
```

PostgreSQL détecte les erreurs au moment de l'INSERT, Parquet les découvre à la requête.

### Liens documentation officielle
- [PostgreSQL Official Documentation](https://www.postgresql.org/docs/)
- [PostgreSQL Performance Optimization](https://wiki.postgresql.org/wiki/Performance_Optimization)
- [psycopg2 Documentation](https://www.psycopg.org/psycopg2/docs/)
- [PostgreSQL for OLTP/OLAP Comparison](https://wiki.postgresql.org/wiki/Why_PostgreSQL_is_suitable_for_OLAP)

---

## 9. Docker et Docker Compose

### Définition
**Docker** est un orchestrateur de conteneurs qui isole une application avec ses dépendances dans une image portable. **Docker Compose** permet d'orchestrer **plusieurs conteneurs liés** avec un fichier YAML unique, simplifiant la définition de services multi-conteneurs (Airflow + PostgreSQL + Redis, etc.).

### Concepts clés

**Image vs Conteneur** :
- **Image** : Modèle immuable (blueprint), défini par un `Dockerfile`
- **Conteneur** : Instance exécutée d'une image, éphémère par défaut

**Dockerfile** :
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]
```

**Docker Compose (docker-compose.yml)** : Orchestre plusieurs conteneurs
```yaml
services:
  airflow:
    image: my-airflow:latest
    environment:
      AIRFLOW_HOME: /opt/airflow
    ports:
      - "8080:8080"
    depends_on:
      - postgres

  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
```

### Avantages Container vs VM

| Aspect | VM | Container (Docker) |
|--------|----|--------------------|
| **Taille** | 100s MB - GB | 10s - 100s MB |
| **Démarrage** | Minutes | Secondes |
| **Isolation** | Complète (kernel OS) | Légère (kernel partagé) |
| **Portabilité** | OS-spécifique | 100% portable |
| **Surcharge** | Importante | Minimale |
| **Cas d'usage** | Legacy, très isolé | Microservices, CI/CD |

### Orchestration multi-conteneurs avec Compose

**LogiStore compose** :
```yaml
# Exemple (fichier réel : docker-compose.yml)
services:
  airflow-webserver:
    build: .
    ports: ["8080:8080"]
    depends_on: ["postgres", "redis"]
  
  airflow-scheduler:
    build: .
    depends_on: ["postgres", "redis"]
  
  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: airflow_db
    volumes:
      - ./data/postgres:/var/lib/postgresql/data
  
  redis:
    image: redis:7-alpine  ← Cache/queue pour Airflow
```

**Workflow** :
```bash
docker-compose up               # Lance tous les services
docker-compose logs -f airflow  # Logs en temps réel
docker-compose down             # Arrête tout
```

### Outils associés
- **Docker Desktop** : IHM locale (Mac, Windows)
- **Docker Registry (DockerHub)** : Stockage images public
- **Docker Hub** : Repository d'images pré-bâties
- **Kubernetes** : Orchestration à l'échelle (remplace Docker Compose en prod)
- **Docker BuildKit** : Builder optimisé avec cache

### Rôle dans le projet LogiStore
- **Développement** : Environment reproductible (Airflow, PostgreSQL, Redis localement)
- **CI/CD** : Tests en container (~= environnement prod)
- **Déploiement** : Single `docker-compose up` pour stack complète
- **Isolation** : Zéro dépendance système (hormis Docker)

**[docker-compose.yml](docker-compose.yml)** :
- Services : Airflow webserver, scheduler, PostgreSQL
- Volumes : Persistance données et DAGs
- Networks : Communication inter-conteneurs
- Health checks : Redémarrage auto en cas d'erreur

### Networks et Volumes

**Volumes** (persistance) :
```yaml
volumes:
  postgres_data:  ← Named volume (auto-managed)
  airflow_logs: /opt/airflow/logs  ← Bind mount (local path)
```

**Networks** (communication) :
```yaml
networks:
  airflow-network:
    driver: bridge
# Services peuvent communiquer via nom (ex: postgres:5432)
```

### Liens documentation officielle
- [Docker Official Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [Dockerfile Reference](https://docs.docker.com/engine/reference/builder/)

---

## 10. Testing (Unité, Intégration, Contrats)

### Définition
**Testing** est l'ensemble des pratiques pour valider qu'un système fonctionne comme prévu. Trois niveaux coexistent :
- **Tests Unitaires** : Fonctionnalité isolée (fonction seule)
- **Tests d'Intégration** : Interaction entre composants (DAG + DB)
- **Tests de Contrats** : Validation schéma données (Data Contracts)

### Pyramide des Tests

```
       ╱╲          (Lent, coûteux, rare)
      ╱E2E╲        Tests End-to-End (UI complète)
     ╱──────╲      
    ╱ Intég ╲      Tests d'intégration (DAGs + DB)
   ╱──────────╲    
  ╱  Unitaire ╲    Tests unitaires (fonctions)
 ╱─────────────╲   (Rapide, moins cher, fréquent)
```

### Types de tests pour pipelines de données

#### 1. **Tests Unitaires** (Isolés)
```python
# tests/test_contracts.py
from contracts.catalogue_contract import CatalogueContract

def test_valid_catalogue_entry():
    entry = {"id": 1, "sku": "SKU-001", "price": 29.99}
    contract = CatalogueContract(**entry)
    assert contract.id == 1
    assert contract.sku == "SKU-001"

def test_invalid_price_rejected():
    entry = {"id": 1, "sku": "SKU-001", "price": "invalid"}
    with pytest.raises(ValidationError):
        CatalogueContract(**entry)
```

#### 2. **Tests Contrats (Data Contracts)** — Validation Schéma
```python
# tests/test_contracts.py
def test_catalogue_schema():
    """Valide que les données CSV respectent le contrat"""
    df = pd.read_csv("data/inbox/catalogue/catalogue_small.csv")
    for _, row in df.iterrows():
        CatalogueContract(**row.to_dict())  # Rejet si schéma violé
```

#### 3. **Tests d'Intégration** (Composants liés)
```python
# tests/test_rejection_pipeline.py
def test_dag_ingestion_with_invalid_data(tmp_path):
    """Teste que dag1 rejette les fichiers invalides"""
    invalid_csv = tmp_path / "bad_catalogue.csv"
    invalid_csv.write_text("id,sku,price\n1,SKU-001,invalid")
    
    # Lance dag1
    result = dag1.test()  # Exécution de test du DAG
    
    # Assertions
    assert rejected_folder.exists()  # Fichier doit être rejeté
    assert os.path.exists("data/rejected/reports/...")  # Log créé
```

#### 4. **Tests de Replay** (Récupération après erreur)
```python
# tests/test_replay.py
def test_replay_rejected_files():
    """Teste que dag4 retraite les fichiers rejetés"""
    # Setup : fichier rejeté + corrigé dans rejected/
    
    # Execute dag4_replay
    result = dag4_replay_rejected.test()
    
    # Verify : fichier déplacé vers processed
    assert not rejected_file.exists()
```

### Outils associés
- **pytest** : Framework de test Python (simple, puissant)
- **unittest** : Framework std Python (verbeux)
- **Great Expectations** : Validation données en prod
- **dbt tests** : Assertions sur models SQL
- **Airflow TestClient** : Test des DAGs isolé
- **pytest-cov** : Coverage report
- **hypothesis** : Property-based testing (edge cases auto-générés)

### Coverage et CI/CD

**Coverage** (couverture de code) :
```bash
pytest --cov=src --cov-report=html tests/
# Génère rapport: quelles lignes sont exécutées par les tests
```

**CI/CD Pipeline** :
```yaml
# .github/workflows/test.yml (GitHub Actions)
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
      - run: pip install -r requirements.txt
      - run: pytest tests/ --cov=src
      - run: dbt test -s my_project  ← Tests dbt models
```

### Rôle dans le projet LogiStore

**Structure tests** :
```
tests/
├── __init__.py
├── test_contracts.py        ← Validation schemas (unitaire + contrats)
├── test_rejection_pipeline.py ← Intégration (DAGs + DB)
├── test_replay.py           ← Rejeu après erreur
└── data/
    ├── catalogue_test.csv   ← Données de test
    └── movements_test.csv
```

**Stratégie** :
1. **Tests contrats** : À chaque ingestion (dag1, dag2) → rejette schéma invalide
2. **Tests rejection** : Vérifie que dag1/dag2 rejetent correctement
3. **Tests replay** : dag4 retraite correctement les rejets
4. **CI/CD** : Chaque PR lance `pytest` + `dbt test`

### Bonnes pratiques

| Pratique | Bénéfice |
|----------|----------|
| **Arrange-Act-Assert** | Clarté du test (setup, action, vérification) |
| **Test une chose** | Failover facile, isolation |
| **Noms explicites** | `test_invalid_price_raises_validation_error` (vs `test_1`) |
| **Données de test** | Datasets petits et déterministes (dans `tests/data/`) |
| **Mocks** | Isoler les dépendances (DB, API) |
| **Coverage >= 80%** | Fiabilité sans couverture excessive |

### Liens documentation officielle
- [pytest Documentation](https://docs.pytest.org/)
- [Airflow Testing Guide](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing)
- [dbt Testing](https://docs.getdbt.com/docs/building-a-dbt-project/tests)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Python unittest Documentation](https://docs.python.org/3/library/unittest.html)
