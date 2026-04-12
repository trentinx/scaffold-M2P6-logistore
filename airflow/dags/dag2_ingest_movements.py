"""
DAG 2 — Ingestion du flux MOUVEMENT

Ce DAG est déclenché automatiquement par le Dataset Airflow
publié par DAG 1 (catalogue_snapshot.parquet).

Ce DAG :
1. Charge le fichier mouvements depuis data/inbox/movements/
2. Valide le schéma Pydantic (MovementRecordV1)
3. Vérifie l'existence du SKU dans la table products (PostgreSQL)
   - SKU connu → insertion dans movements
   - SKU inconnu → insertion dans rejected_movements + rapport de rejet
4. Exporte les mouvements valides en Parquet
5. Génère un rapport JSON de rejet si nécessaire

TODO étudiant : implémenter les fonctions marquées TODO ci-dessous.
"""
from __future__ import annotations
import pandas as pd
import numpy as np
import json
import os


from psycopg2.extras import execute_values
import psycopg2
from datetime import datetime
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag, task

from contracts.movement_contract import MovementRecordV1

CATALOGUE_DATASET = Dataset("file:///opt/airflow/data/curated/catalogue_snapshot.parquet")
MOVEMENTS_DATASET = Dataset("file:///opt/airflow/data/curated/movements_history.parquet")

DATA_INBOX = Path("/opt/airflow/data/inbox/movements")
DATA_CURATED = Path("/opt/airflow/data/curated")
DATA_REJECTED = Path("/opt/airflow/data/rejected")

POSTGRES_DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}

@dag(
    dag_id="ingest_movements",
    schedule=[CATALOGUE_DATASET],  # Déclenché par DAG 1
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "movements", "ingestion"],
    doc_md="""## DAG 2 — Ingestion Mouvements\n\nIngère les mouvements de stock, rejette ceux sur SKU inconnus.""",
)
def ingest_movements():

    @task
    def load_movements_file() -> str | None:
        """Retourne le chemin du dernier fichier mouvements disponible."""
        files = sorted(DATA_INBOX.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            print("Aucun fichier mouvements trouvé.")
            return None
        return str(files[0])

    @task
    def validate_schema(filepath: str | None) -> dict:
        """
        1. Charger le fichier CSV
        2. Valider chaque ligne avec MovementRecordV1
        3. Retourner {valid_rows: [...], invalid_rows: [...], filepath: filepath}
        """
        if not filepath:
            return {"valid_rows": [], "invalid_rows": [], "filepath": None}
        
        df = pd.read_csv(filepath, dtype={"schema_version": str})\
           .replace({np.nan: None})
        records = df.to_dict(orient="records")
        valid_rows = []
        invalid_rows = []
        for record in records:
            try:
                MovementRecordV1(**record)
                valid_rows.append(record)
            except Exception:
                invalid_rows.append(record)
        return {"valid_rows": valid_rows, "invalid_rows": invalid_rows, "filepath": filepath}

    @task
    def check_sku_and_route(validation_result: dict) -> dict:
        """
        1. Pour chaque ligne valide (schema OK), vérifier que le SKU existe dans products
        2. Lignes avec SKU connu → accepted_rows
        3. Lignes avec SKU inconnu → rejected_rows avec rejection_reason='unknown_sku'
        4. Insérer rejected_rows dans la table rejected_movements
        5. Retourner {accepted: [...], rejected_count: N, total: M}

        C'est le coeur du projet : gérer l'asynchronisme entre les deux flux.
        """

        sku_list = list(((record["sku"],)for record in validation_result["valid_rows"]))
        request = """
                  SELECT
                    sku_table.sku,
                    CASE
                        when products.sku IS NOT NULL THEN 1
                        else 0
                    END AS sku_exists
                  FROM (VALUES %s) AS sku_table(sku)
                  LEFT JOIN products ON products.sku = sku_table.sku
                  """
        try:
            with psycopg2.connect(**POSTGRES_DSN) as conn:
                with conn.cursor() as cur:
                    results = execute_values(cur, request, sku_list, fetch=True )
                    sku_existence = {row[0]: row[1] for row in results}

                    accepted = []
                    rejected = []
                    for record in validation_result["valid_rows"]:
                        if sku_existence.get(record["sku"], 0) == 1:
                            accepted.append(record)
                        else:
                            rejected.append(record)
                    rejected_count = len(rejected)
                    total = len(validation_result["valid_rows"])

                    if rejected_count > 0:
                        records = [
                            (
                                record["movement_id"],
                                record["sku"],
                                record["movement_type"],
                                record["quantity"],
                                record["reason"],
                                record["occurred_at"],
                                "unknown_sku",
                            )
                            for record in rejected
                        ]
                        insert_query = """
                            INSERT INTO rejected_movements (movement_id, sku, movement_type, quantity, reason, occurred_at, rejection_reason)
                            VALUES %s
                        """
                        execute_values(cur, insert_query,records)
                        conn.commit()
            return {"accepted": accepted, "rejected_count": rejected_count, "total": total}
        except Exception as e:
            print(f"Erreur lors de la vérification des SKUs : {e}")
            return {"accepted": [], "rejected_count": 0, "total": 0}        
                
                


    @task(outlets=[MOVEMENTS_DATASET])
    def persist_valid_movements(routing_result: dict) -> dict:
        """
        TODO étudiant :
        1. Insérer les mouvements acceptés dans la table movements (PostgreSQL)
        2. Append dans data/curated/movements_history.parquet
        3. Générer le rapport de rejet JSON si rejected_count > 0
        4. Le décorateur outlets=[MOVEMENTS_DATASET] déclenche DAG 3
        """
        # Insérer les mouvements acceptés dans PostgreSQL
        accepted = routing_result.get("accepted")
        if len(accepted)>0:
            try:
                with psycopg2.connect(**POSTGRES_DSN) as conn:
                    with conn.cursor() as cur:
                        records = [
                            (
                                record["movement_id"],
                                record["sku"],
                                record["movement_type"],
                                record["quantity"],
                                record["reason"],
                                record["occurred_at"],
                            )
                            for record in accepted
                        ]
                        insert_query = """
                            INSERT INTO movements (movement_id, sku, movement_type, quantity, reason, occurred_at)
                            VALUES %s
                        """
                        execute_values(cur, insert_query,records)
                        conn.commit()
            except Exception as e:
                print(f"Erreur lors de l'insertion des mouvements acceptés : {e}")
            
            # Exporter les mouvements acceptés en Parquet en update le dataset existant si déjà présent
            DATA_CURATED.mkdir(parents=True, exist_ok=True)
            output_path = DATA_CURATED / "movements_history.parquet"
            df_accepted = pd.DataFrame(accepted)
            if output_path.exists():
                df_existing = pd.read_parquet(output_path)
                df_combined = pd.concat([df_existing, df_accepted], ignore_index=True)
                df_combined.to_parquet(output_path, index=False)
            else:
                df_accepted.to_parquet(output_path, index=False)

        # Générer le rapport de rejet JSON si nécessaire
        rejected_count = routing_result.get("rejected_count", 0)
        total = routing_result.get("total", 0)
        if rejected_count > 0:
            report = {
                "timestamp": datetime.now().isoformat(),
                "total_records": total,
                "rejected_records": rejected_count,
                "rejection_rate": rejected_count / total if total > 0 else 0,
            }
            report_path = DATA_REJECTED / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            DATA_REJECTED.mkdir(parents=True, exist_ok=True)
            with open(report_path, "w") as f:
                json.dump(report, f, indent=4, ensure_ascii=False)
            print(f"Rapport de rejet généré : {report_path}")
            

    filepath = load_movements_file()
    validation = validate_schema(filepath)
    routing = check_sku_and_route(validation)
    persist_valid_movements(routing)


ingest_movements()
