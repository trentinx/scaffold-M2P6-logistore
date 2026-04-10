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
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
        hook = PostgresHook(postgres_conn_id="postgres_default")

        records = hook.get_records("""
        SELECT id, sku, quantity, movement_type, created_at
        FROM rejected_movements
        WHERE status = 'PENDING'
        """)

        columns = ["id", "sku", "quantity", "movement_type", "created_at"]

        return [dict(zip(columns, row)) for row in records]

    @task
    def filter_now_known_skus(rejected: list[dict]) -> dict:
        if not rejected:
            return {"replayable": [], "still_pending": [], "counts": {}}

        hook = PostgresHook(postgres_conn_id="postgres_default")

        skus = list({r["sku"] for r in rejected})

        format_skus = ",".join(f"'{sku}'" for sku in skus)

        query = f"""
        SELECT sku FROM products
        WHERE sku IN ({format_skus})
        """

        known = hook.get_records(query)
        known_skus = {row[0] for row in known}

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
        hook = PostgresHook(postgres_conn_id="postgres_default")

        replayable = result.get("replayable", [])
        still_pending = result.get("still_pending", [])

        if not replayable:
            return {"replayed": 0, "still_pending": len(still_pending)}

        conn = hook.get_conn()
        cursor = conn.cursor()

        # 1. Insert dans movements
        for r in replayable:
            cursor.execute("""
                INSERT INTO movements (sku, quantity, movement_type, created_at)
                VALUES (%s, %s, %s, %s)
                """, (r["sku"], r["quantity"], r["movement_type"], r["created_at"]))

        # 2. Update status
        ids = [r["id"] for r in replayable]

        cursor.execute("""
            UPDATE rejected_movements
            SET status = 'REPLAYED'
            WHERE id = ANY(%s)
            """, (ids,))

        conn.commit()
        cursor.close()

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
