"""
DAG 4 — Rejeu des mouvements rejetés

Déclenché MANUELLEMENT par un opérateur, après :
1. Vérification que de nouveaux produits ont été intégrés dans le catalogue.
2. Identification des mouvements rejetés dont le SKU est maintenant connu.

Ce DAG :
1. Lit la table rejected_movements (status='PENDING') depuis PostgreSQL
2. Vérifie quels SKUs sont désormais présents dans products
3. Reinsère ces mouvements dans la table movements
4. Met à jour leur statut en 'REPLAYED'
5. Génère un rapport de rejeu

TODO étudiant : implémenter les fonctions marquées TODO ci-dessous.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

import os
import psycopg2

POSTGRES_DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}

@dag(
    dag_id="replay_rejected_movements",
    schedule=None,  # Déclenché uniquement manuellement
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "replay", "rejected"],
    doc_md="""## DAG 4 — Rejeu des mouvements rejetés

Déclencher ce DAG manuellement après avoir intégré de nouveaux produits
dans le catalogue. Il tentera de réintégrer les mouvements en attente
dont le SKU est désormais connu.""",
)
def replay_rejected_movements():

    @task
    def fetch_pending_rejected() -> list[dict]:
        with psycopg2.connect(**POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, movement_id, sku, quantity, movement_type, reason, occurred_at
                    FROM rejected_movements
                    WHERE status = 'PENDING'
                    ORDER BY id
                    """
                )
                rows = cur.fetchall()

        columns = ["id", "movement_id", "sku", "quantity", "movement_type", "reason", "occurred_at"]
        return [dict(zip(columns, row)) for row in rows]

    @task
    def filter_now_known_skus(rejected: list[dict]) -> dict:
        if not rejected:
            return {"replayable": [], "still_pending": [], "counts": {"total": 0, "replayable": 0, "still_pending": 0}}

        skus = list({r["sku"] for r in rejected})

        with psycopg2.connect(**POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sku
                    FROM products
                    WHERE sku = ANY(%s)
                    """,
                    (skus,),
                )
                known_skus = {row[0] for row in cur.fetchall()}

        replayable = [r for r in rejected if r["sku"] in known_skus]
        still_pending = [r for r in rejected if r["sku"] not in known_skus]

        return {
        "replayable": replayable,
        "still_pending": still_pending,
        "counts": {
            "total": len(rejected),
            "replayable": len(replayable),
            "still_pending": len(still_pending),
            },
        }

    @task
    def replay_movements(result: dict) -> dict:
        replayable = result.get("replayable", [])
        still_pending = result.get("still_pending", [])

        if not replayable:
            return {"replayed": 0, "still_pending": len(still_pending)}

        with psycopg2.connect(**POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                insert_query = """
                    INSERT INTO movements (movement_id, sku, movement_type, quantity, reason, occurred_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (movement_id) DO NOTHING
                """
                movement_payloads = [
                    (
                        r["movement_id"],
                        r["sku"],
                        r["movement_type"],
                        r["quantity"],
                        r["reason"],
                        r["occurred_at"],
                    )
                    for r in replayable
                ]
                cur.executemany(insert_query, movement_payloads)

                ids = [r["id"] for r in replayable]
                cur.execute(
                    """
                    UPDATE rejected_movements
                    SET status = 'REPLAYED', rejected_at = NOW()
                    WHERE id = ANY(%s)
                    """,
                    (ids,),
                )
            conn.commit()

        # 3. Rapport
        report = {
            "replayed": len(replayable),
            "still_pending": len(still_pending),
        }

        print("Rapport de rejeu :", report)

        return report
    pending = fetch_pending_rejected()
    filtered = filter_now_known_skus(pending)
    replay_movements(filtered)


replay_rejected_movements()
