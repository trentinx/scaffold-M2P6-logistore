"""
Initialisation et chargement de la base PostgreSQL LogiStore.

Usage :
    python scripts/load_to_postgres.py --init         # Créer les tables
    python scripts/load_to_postgres.py --load-catalogue data/inbox/catalogue/catalogue_small.csv
    python scripts/load_to_postgres.py --load-movements data/inbox/movements/movements_small.csv
"""
import argparse
import os

import pandas as pd
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ModuleNotFoundError:  # pragma: no cover - optional dependency for DB ops
    psycopg2 = None
    execute_values = None

DSN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}

CREATE_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    sku         VARCHAR(20)  PRIMARY KEY,
    label       VARCHAR(200) NOT NULL,
    category    VARCHAR(20)  NOT NULL,
    unit        VARCHAR(10)  NOT NULL,
    min_stock   INTEGER      NOT NULL DEFAULT 0,
    supplier_id VARCHAR(50),
    published_at TIMESTAMP   NOT NULL,
    inserted_at  TIMESTAMP   DEFAULT NOW()
);
"""

CREATE_MOVEMENTS = """
CREATE TABLE IF NOT EXISTS movements (
    movement_id   VARCHAR(36)  PRIMARY KEY,
    sku           VARCHAR(20)  NOT NULL REFERENCES products(sku),
    movement_type VARCHAR(10)  NOT NULL,
    quantity      INTEGER      NOT NULL,
    reason        TEXT,
    occurred_at   TIMESTAMP    NOT NULL,
    inserted_at   TIMESTAMP    DEFAULT NOW()
);
"""

CREATE_REJECTED_MOVEMENTS = """
CREATE TABLE IF NOT EXISTS rejected_movements (
    id              SERIAL       PRIMARY KEY,
    movement_id     VARCHAR(36),
    sku             VARCHAR(20)  NOT NULL,
    movement_type   VARCHAR(10),
    quantity        INTEGER,
    reason          TEXT,
    occurred_at     TIMESTAMP,
    rejection_reason TEXT        NOT NULL,
    rejected_at     TIMESTAMP    DEFAULT NOW(),
    status          VARCHAR(20)  DEFAULT 'PENDING'  -- PENDING | REPLAYED | ABANDONED
);
"""


def get_conn():
    if psycopg2 is None:  # pragma: no cover - fail fast when feature is used
        raise ImportError(
            "psycopg2 is required for database access. "
            "Install it (e.g. `uv pip install psycopg2-binary`) "
            "before calling database helpers."
        )
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
    """elle lit un CSV ligne par ligne, valide
    chaque ligne contre le contrat, et retourne deux DataFrames (valides /
    rejetés) avec les raisons de rejet.

    Arguments :
        - filepath : chemin vers le fichier CSV à valider
        - model_class : classe Pydantic à utiliser pour la validation
"""
    df = pd.read_csv(filepath)
    valid_rows = []
    rejected_rows = []
    for _, row in df.iterrows():
        row_dict = {
            column: None if pd.isna(value)
            else str(value) if column == "schema_version"
            else value
            for column, value in row.items()
        }
        try:
            model_class(**row_dict)
            valid_rows.append(row_dict)
        except Exception as e:
            rejected_rows.append({**row_dict, "rejection_reason": str(e)})

    return pd.DataFrame(valid_rows), pd.DataFrame(rejected_rows)



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
