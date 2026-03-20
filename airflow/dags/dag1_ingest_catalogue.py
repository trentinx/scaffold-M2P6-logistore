"""
DAG 1 — Ingestion du flux CATALOGUE

Ce DAG :
1. Détecte un nouveau fichier catalogue dans data/inbox/catalogue/
2. Valide chaque ligne avec le contrat CatalogueRecordV1 (ou V2)
3. Insère les produits valides dans PostgreSQL (UPSERT)
4. Exporte le catalogue complet en Parquet (data/curated/catalogue_snapshot.parquet)
5. Publie le Dataset Airflow pour déclencher automatiquement DAG 2

TODO étudiant : implémenter les fonctions marquées TODO ci-dessous.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag, task

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from contracts.catalogue_contract import get_catalogue_contract
from contracts.catalogue_schema import get_catalogue_storage_columns
from scripts.db_schema_manager import ensure_products_columns

# Dataset Airflow partagé avec DAG 2
CATALOGUE_DATASET = Dataset("file:///opt/airflow/data/curated/catalogue_snapshot.parquet")

DATA_INBOX = Path("/opt/airflow/data/inbox/catalogue")
DATA_CURATED = Path("/opt/airflow/data/curated")
DATA_REJECTED = Path("/opt/airflow/data/rejected/catalogue")
CATALOGUE_DB_COLUMNS = get_catalogue_storage_columns()

POSTGRES_DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}


@dag(
    dag_id="ingest_catalogue",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "catalogue", "ingestion"],
    doc_md="""## DAG 1 — Ingestion Catalogue\n\nIngère les fichiers CSV catalogue et publie le Dataset pour DAG 2.""",
)
def ingest_catalogue():

    @task
    def detect_new_catalogue_file() -> str | None:
        """Retourne le chemin du dernier fichier catalogue disponible, ou None."""
        files = sorted(DATA_INBOX.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            print("Aucun fichier catalogue trouvé.")
            return None
        path = str(files[0])
        print(f"Fichier détecté : {path}")
        return path

    @task
    def validate_and_upsert_catalogue(filepath: str | None) -> dict:
        """
        TODO étudiant :
        1. Charger le fichier CSV filepath avec pandas
        2. Pour chaque ligne, déterminer la version du contrat (schema_version)
           et valider avec get_catalogue_contract(version)
        3. Séparer les lignes valides des lignes invalides
        4. Insérer/mettre à jour (UPSERT) les lignes valides dans PostgreSQL
        5. Sauvegarder les lignes invalides dans data/rejected/catalogue/
        6. Retourner un dict de stats : {valid: N, rejected: M}
        """
        if not filepath:
            return {"valid": 0, "rejected": 0, "skipped": True}

        df = pd.read_csv(filepath)
        valid_payloads = []
        rejected_rows = []

        for record in df.to_dict("records"):
            normalized = {
                column: None if pd.isna(value)
                else str(value) if column == "schema_version"
                else value
                for column, value in record.items()
            }
            try:
                version = str(normalized.get("schema_version"))
                contract_cls = get_catalogue_contract(version)
                payload = contract_cls(**normalized).model_dump(mode="python")
                valid_payloads.append(payload)
            except Exception as exc:
                rejected_rows.append({**normalized, "rejection_reason": str(exc)})

        if valid_payloads:
            columns = CATALOGUE_DB_COLUMNS
            values = [
                tuple(payload.get(column) for column in columns)
                for payload in valid_payloads
            ]
            set_clause = ", ".join(
                f"{column} = EXCLUDED.{column}"
                for column in columns
                if column != "sku"
            )
            insert_columns = ", ".join(columns)
            upsert_query = f"""
                INSERT INTO products ({insert_columns})
                VALUES %s
                ON CONFLICT (sku) DO UPDATE SET
                    {set_clause}
            """
            with psycopg2.connect(**POSTGRES_DSN) as conn:
                ensure_products_columns(conn)
                with conn.cursor() as cur:
                    execute_values(cur, upsert_query, values)
                conn.commit()

        if rejected_rows:
            DATA_REJECTED.mkdir(parents=True, exist_ok=True)
            rejected_path = DATA_REJECTED / f"{Path(filepath).stem}_rejected_{datetime.utcnow():%Y%m%d%H%M%S}.csv"
            pd.DataFrame(rejected_rows).to_csv(rejected_path, index=False)

        return {"valid": len(valid_payloads), "rejected": len(rejected_rows), "skipped": False}

    @task(outlets=[CATALOGUE_DATASET])
    def export_catalogue_to_parquet(stats: dict) -> None:
        """
        TODO étudiant :
        1. Lire tous les produits depuis PostgreSQL
        2. Exporter en Parquet dans data/curated/catalogue_snapshot.parquet
        3. Le décorateur outlets=[CATALOGUE_DATASET] déclenche automatiquement DAG 2
        """
        DATA_CURATED.mkdir(parents=True, exist_ok=True)
        snapshot_path = DATA_CURATED / "catalogue_snapshot.parquet"
        print(f"Stats ingestion : {stats}")

        with psycopg2.connect(**POSTGRES_DSN) as conn:
            ensure_products_columns(conn)
            df = pd.read_sql_query("SELECT * FROM products ORDER BY sku", conn)

        if df.empty:
            # Créer un fichier Parquet vide avec les colonnes connues
            df = pd.DataFrame(columns=CATALOGUE_DB_COLUMNS)
        df.to_parquet(snapshot_path, index=False)
        print(f"Catalogue exporté vers {snapshot_path}")

    # Chaînage des tâches
    filepath = detect_new_catalogue_file()
    stats = validate_and_upsert_catalogue(filepath)
    export_catalogue_to_parquet(stats)


ingest_catalogue()
