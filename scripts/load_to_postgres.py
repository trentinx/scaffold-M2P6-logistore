"""
Initialisation et chargement de la base PostgreSQL LogiStore.

Usage :
    python scripts/load_to_postgres.py --init         # Créer les tables
    python scripts/load_to_postgres.py --load-catalogue data/inbox/catalogue/catalogue_small.csv
    python scripts/load_to_postgres.py --load-movements data/inbox/movements/movements_small.csv
"""
import argparse
import os
from urllib.parse import urlparse

import numpy as np
import pandas as pd

# Import optionnel pour les opérations DB, pas nécessaire pour la validation des flux
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ModuleNotFoundError:  # pragma: no cover - optional dependency for DB ops
    psycopg2 = None
    execute_values = None
    print("⚠️  psycopg2 n'est pas installé. Les opérations de base de données ne fonctionneront pas. Installez avec 'uv sync --extra db'.")


def _parse_airflow_db_conn() -> dict[str, str | int | None]:
    """Extract DB settings from AIRFLOW__DATABASE__SQL_ALCHEMY_CONN if available."""
    conn_url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not conn_url:
        return {}
    try:
        parsed = urlparse(conn_url)
    except ValueError:
        return {}
    return {
        "host": parsed.hostname,
        "port": parsed.port,
        "dbname": parsed.path.lstrip("/") if parsed.path else None,
        "user": parsed.username,
        "password": parsed.password,
    }


def _first_defined(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


_airflow_defaults = _parse_airflow_db_conn()

from scripts.db_schema_manager import (
    CREATE_MOVEMENTS_TABLE as CREATE_MOVEMENTS,
    CREATE_PRODUCTS_TABLE as CREATE_PRODUCTS,
    CREATE_REJECTED_MOVEMENTS_TABLE as CREATE_REJECTED_MOVEMENTS,
)


DSN = {
    "host": _first_defined(os.getenv("POSTGRES_HOST"), os.getenv("PGHOST"), _airflow_defaults.get("host"), "localhost"),
    "port": int(_first_defined(os.getenv("POSTGRES_PORT"), os.getenv("PGPORT"), _airflow_defaults.get("port"), 5432)),
    "dbname": _first_defined(os.getenv("POSTGRES_DB"), os.getenv("PGDATABASE"), _airflow_defaults.get("dbname"), "logistore"),
    "user": _first_defined(os.getenv("POSTGRES_USER"), os.getenv("PGUSER"), _airflow_defaults.get("user"), "logistore"),
    "password": _first_defined(os.getenv("POSTGRES_PASSWORD"), os.getenv("PGPASSWORD"), _airflow_defaults.get("password"), "logistore"),
}



def get_conn():
    return psycopg2.connect(**DSN)


def init_db():
    """Crée les tables si elles n'existent pas."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_PRODUCTS)
            cur.execute(CREATE_MOVEMENTS)
            cur.execute(CREATE_REJECTED_MOVEMENTS)
        conn.commit()
    print("✅ Tables créées (ou déjà existantes) : products, movements, rejected_movements")


def validate_flow(filepath, model_class):
    df = pd.read_csv(filepath, dtype={"schema_version": str})\
           .replace({np.nan: None})
    records = df.to_dict(orient="records")
    valid_records = []
    rejected_records = []
    for record in records:
        try:
            model_class(**record)
            valid_records.append(record)
        except Exception as e:
            rejected_records.append({**record, "rejection_reason": str(e)})
    return pd.DataFrame(valid_records), pd.DataFrame(rejected_records)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true", help="Créer les tables")
    parser.add_argument("--load-catalogue", metavar="FILE", help="Charger un CSV catalogue")
    parser.add_argument("--load-movements", metavar="FILE", help="Charger un CSV mouvements")
    args = parser.parse_args()

    if args.init:
        init_db()

    if args.load_catalogue:
        # TODO étudiant : implémenter le chargement du catalogue avec validation Pydantic
        print(f"TODO : charger le catalogue depuis {args.load_catalogue}")

    if args.load_movements:
        # TODO étudiant : implémenter le chargement des mouvements avec gestion des rejets
        print(f"TODO : charger les mouvements depuis {args.load_movements}")


if __name__ == "__main__":
    main()
