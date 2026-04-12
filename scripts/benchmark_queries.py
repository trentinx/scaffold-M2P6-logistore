"""
Benchmark comparatif SQL (PostgreSQL) vs Parquet (DuckDB).

Usage :
    python scripts/benchmark_queries.py --palier small
    python scripts/benchmark_queries.py --palier all --output data/reports/benchmark.md
    python scripts/benchmark_queries.py --palier small --ci   # mode CI : sorties réduites
"""
from __future__ import annotations

import argparse
import os
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd
from psycopg2 import sql

from load_to_postgres import get_conn


PALIERS = ("small", "medium", "large")
ROW_ORDER = [
    "Stock courant — PostgreSQL",
    "Stock courant — DuckDB/Parquet",
    "Agrégation mensuelle — PostgreSQL",
    "Agrégation mensuelle — DuckDB/Parquet",
    "Alerte rupture (jointure) — PostgreSQL",
    "Alerte rupture (jointure) — DuckDB/Parquet",
    "Taille sur disque (mouvements)",
]


@dataclass
class DatasetPaths:
    palier: str
    movements_csv: Path
    catalogue_csv: Path
    movements_parquet: Path
    catalogue_parquet: Path


QUERIES_PARQUET = {
    "stock_by_sku": """
        SELECT sku, SUM(quantity) AS current_stock
        FROM read_parquet('{movements_file}')
        GROUP BY sku
        ORDER BY current_stock ASC
    """,
    "movements_by_month": """
        SELECT strftime(occurred_at, '%Y-%m') AS month,
               movement_type, COUNT(*) AS nb, SUM(quantity) AS total_qty
        FROM read_parquet('{movements_file}')
        GROUP BY 1, 2
        ORDER BY 1
    """,
    "low_stock_alert": """
        SELECT m.sku, SUM(m.quantity) AS stock, p.min_stock
        FROM read_parquet('{movements_file}') AS m
        JOIN read_parquet('{catalogue_file}') AS p ON m.sku = p.sku
        GROUP BY m.sku, p.min_stock
        HAVING SUM(m.quantity) < p.min_stock
        ORDER BY stock ASC
    """,
}

QUERIES_POSTGRES = {
    "stock_by_sku": """
        SELECT sku, SUM(quantity) AS current_stock
        FROM {movements_table}
        GROUP BY sku
        ORDER BY current_stock ASC
    """,
    "movements_by_month": """
        SELECT TO_CHAR(occurred_at, 'YYYY-MM') AS month,
               movement_type, COUNT(*) AS nb, SUM(quantity) AS total_qty
        FROM {movements_table}
        GROUP BY 1, 2
        ORDER BY 1
    """,
    "low_stock_alert": """
        SELECT m.sku, SUM(m.quantity) AS stock, p.min_stock
        FROM {movements_table} AS m
        JOIN {products_table} AS p ON m.sku = p.sku
        GROUP BY m.sku, p.min_stock
        HAVING SUM(m.quantity) < p.min_stock
        ORDER BY stock ASC
    """,
}

QUERY_LABELS = {
    "stock_by_sku": "Stock courant",
    "movements_by_month": "Agrégation mensuelle",
    "low_stock_alert": "Alerte rupture (jointure)",
}

ENGINE_LABELS = {
    "postgres": "PostgreSQL",
    "parquet": "DuckDB/Parquet",
}

MOVEMENTS_TABLE_SQL = """
CREATE TABLE {table} (
    schema_version TEXT,
    movement_id TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    movement_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    reason TEXT,
    occurred_at TIMESTAMP NOT NULL
);
"""

PRODUCTS_TABLE_SQL = """
CREATE TABLE {table} (
    schema_version TEXT,
    sku TEXT PRIMARY KEY,
    label TEXT,
    category TEXT,
    unit TEXT,
    min_stock INTEGER,
    published_at TIMESTAMP
);
"""


def ensure_dataset(palier: str, ci_mode: bool = False) -> DatasetPaths | None:
    """Validate/generate the CSV + Parquet files for a given palier."""
    movements_csv = Path(f"data/inbox/movements/movements_{palier}.csv")
    catalogue_csv = Path(f"data/inbox/catalogue/catalogue_{palier}.csv")

    missing = [str(path) for path in (movements_csv, catalogue_csv) if not path.exists()]
    if missing:
        msg = textwrap.dedent(
            f"""
            ⚠️  Données manquantes pour le palier '{palier}' :
                - {os.linesep.join(missing)}
            Utilisez 'python scripts/generate_flows.py --palier {palier}' pour les créer.
            """
        ).strip()
        if ci_mode:
            print(msg)
            return None
        raise FileNotFoundError(msg)

    movements_parquet = Path(f"data/curated/movements_{palier}.parquet")
    catalogue_parquet = Path(f"data/curated/catalogue_{palier}.parquet")
    movements_parquet.parent.mkdir(parents=True, exist_ok=True)

    _maybe_convert_csv_to_parquet(
        movements_csv,
        movements_parquet,
        datetime_columns=["occurred_at"],
    )
    _maybe_convert_csv_to_parquet(
        catalogue_csv,
        catalogue_parquet,
        datetime_columns=["published_at"],
    )

    return DatasetPaths(
        palier=palier,
        movements_csv=movements_csv,
        catalogue_csv=catalogue_csv,
        movements_parquet=movements_parquet,
        catalogue_parquet=catalogue_parquet,
    )


def _maybe_convert_csv_to_parquet(
    csv_path: Path,
    parquet_path: Path,
    datetime_columns: List[str] | None = None,
) -> None:
    """Convert CSV to Parquet if needed (missing or stale)."""
    if parquet_path.exists() and parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
        return

    print(f"↻ Conversion CSV→Parquet : {csv_path.name} → {parquet_path.name}")
    df = pd.read_csv(csv_path)
    if datetime_columns:
        for column in datetime_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], format="ISO8601", errors="coerce")
    df.to_parquet(parquet_path, index=False)


def prepare_postgres_tables(dataset: DatasetPaths, ci_mode: bool = False) -> Dict[str, str] | None:
    """Load the CSV data into dedicated PostgreSQL benchmark tables."""
    movements_table = f"benchmark_movements_{dataset.palier}"
    products_table = f"benchmark_products_{dataset.palier}"

    try:
        with get_conn() as conn:
            _load_csv_into_table(conn, movements_table, MOVEMENTS_TABLE_SQL, dataset.movements_csv)
            _load_csv_into_table(conn, products_table, PRODUCTS_TABLE_SQL, dataset.catalogue_csv)
        return {"movements": movements_table, "products": products_table}
    except Exception as exc:  # pragma: no cover - integration path
        if ci_mode:
            print(f"⚠️  Impossible de préparer PostgreSQL ({exc}). Bench SQL ignoré.")
            return None
        raise


def _load_csv_into_table(conn, table_name: str, ddl_template: str, csv_path: Path) -> None:
    """Drop + recreate a benchmark table, then bulk load from CSV."""
    with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as handle:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
        cur.execute(sql.SQL(ddl_template).format(table=sql.Identifier(table_name)))
        cur.copy_expert(
            sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(sql.Identifier(table_name)),
            handle,
        )
    conn.commit()


def run_parquet_benchmark(dataset: DatasetPaths, ci_mode: bool = False):
    """Exécute les requêtes DuckDB sur des fichiers Parquet et mesure le temps."""
    if not dataset.movements_parquet.exists():
        print(f"⚠️  Fichier Parquet introuvable : {dataset.movements_parquet}")
        return {}

    conn = duckdb.connect()
    results = {}
    try:
        for name, query in QUERIES_PARQUET.items():
            q = query.format(
                movements_file=dataset.movements_parquet.as_posix(),
                catalogue_file=dataset.catalogue_parquet.as_posix(),
            )
            start = time.perf_counter()
            df = conn.execute(q).df()
            elapsed = (time.perf_counter() - start) * 1000
            results[name] = {"engine": "parquet", "rows": len(df), "ms": round(elapsed, 2)}
            if not ci_mode:
                print(f"  [{name}] DuckDB/Parquet : {elapsed:.2f} ms ({len(df)} lignes)")
    finally:
        conn.close()
    return results


def run_postgres_benchmark(
    table_names: Dict[str, str], ci_mode: bool = False
) -> Dict[str, Dict[str, float]]:
    """Exécute les requêtes PostgreSQL et mesure le temps."""
    results: Dict[str, Dict[str, float]] = {}
    with get_conn() as conn:
        for name, query in QUERIES_POSTGRES.items():
            formatted = query.format(
                movements_table=sql.Identifier(table_names["movements"]).as_string(conn),
                products_table=sql.Identifier(table_names["products"]).as_string(conn),
            )
            start = time.perf_counter()
            with conn.cursor() as cur:
                cur.execute(formatted)
                rows = cur.fetchall()
            elapsed = (time.perf_counter() - start) * 1000
            results[name] = {"engine": "postgres", "rows": len(rows), "ms": round(elapsed, 2)}
            if not ci_mode:
                print(f"  [{name}] PostgreSQL : {elapsed:.2f} ms ({len(rows)} lignes)")
    return results


def get_disk_usage_info(dataset: DatasetPaths, pg_table: str | None) -> Dict[str, float | None]:
    """Retourne la taille disque côté Parquet et PostgreSQL (en MB)."""
    parquet_mb = (
        dataset.movements_parquet.stat().st_size / (1024 * 1024)
        if dataset.movements_parquet.exists()
        else 0.0
    )
    postgres_mb = None
    postgres_pretty = None
    if pg_table:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT bytes, pg_size_pretty(bytes) AS pretty
                FROM (SELECT pg_relation_size(%s) AS bytes) t
                """,
                (f"public.{pg_table}",),
            )
            pg_bytes, pg_pretty = cur.fetchone()
        postgres_mb = round(pg_bytes / (1024 * 1024), 2)
        postgres_pretty = pg_pretty
    return {
        "parquet_mb": round(parquet_mb, 2),
        "postgres_mb": postgres_mb,
        "postgres_pretty": postgres_pretty,
    }


def merge_results(
    summary: Dict[str, Dict[str, str]],
    palier: str,
    results: Dict[str, Dict[str, float]],
) -> None:
    """Ajoute les mesures (ms) au tableau récapitulatif."""
    for query_name, payload in results.items():
        label = QUERY_LABELS.get(query_name, query_name)
        engine = ENGINE_LABELS.get(payload["engine"], payload["engine"])
        row_key = f"{label} — {engine}"
        summary.setdefault(row_key, {})[palier] = f"{payload['ms']:.2f} ms"


def record_disk_sizes(
    summary: Dict[str, Dict[str, str]],
    palier: str,
    disk_info: Dict[str, float | None] | None,
) -> None:
    if not disk_info:
        return
    parquet_part = f"{disk_info['parquet_mb']:.2f} MB (Parquet)"
    if disk_info.get("postgres_mb") is None:
        postgres_part = "N/A (PostgreSQL)"
    else:
        pretty = disk_info.get("postgres_pretty") or f"{disk_info['postgres_mb']:.2f} MB"
        postgres_part = f"{pretty} (PostgreSQL)"
    summary.setdefault("Taille sur disque (mouvements)", {})[
        palier
    ] = f"{parquet_part} / {postgres_part}"


def build_markdown_table(summary: Dict[str, Dict[str, str]], palier_order: List[str]) -> str:
    headers = ["Requête"] + [palier.capitalize() for palier in palier_order]
    header_line = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join(["---"] * len(headers)) + " |"

    rows: List[str] = [header_line, separator]
    for row_key in ROW_ORDER:
        values = [row_key]
        row_data = summary.get(row_key, {})
        for palier in palier_order:
            values.append(row_data.get(palier, "—"))
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--palier",
        choices=[*PALIERS, "all"],
        default="small",
        help="Palier à analyser (ou 'all' pour enchaîner les trois).",
    )
    parser.add_argument("--ci", action="store_true", help="Mode CI : skip si fichiers absents")
    parser.add_argument("--output", help="Enregistrer le tableau final (Markdown).")
    args = parser.parse_args()

    palier_list = list(PALIERS) if args.palier == "all" else [args.palier]
    summary: Dict[str, Dict[str, str]] = {}
    processed: List[str] = []

    for palier in palier_list:
        print(f"\n📊 Benchmark — palier '{palier}'")
        print("-" * 50)

        dataset = ensure_dataset(palier, ci_mode=args.ci)
        if dataset is None:
            continue

        parquet_results = run_parquet_benchmark(dataset, ci_mode=args.ci)
        if parquet_results:
            merge_results(summary, palier, parquet_results)

        postgres_results: Dict[str, Dict[str, float]] = {}
        table_names = prepare_postgres_tables(dataset, ci_mode=args.ci)
        disk_info = None
        if table_names:
            postgres_results = run_postgres_benchmark(table_names, ci_mode=args.ci)
            if postgres_results:
                merge_results(summary, palier, postgres_results)
            disk_info = get_disk_usage_info(dataset, table_names["movements"])
        else:
            disk_info = get_disk_usage_info(dataset, None)

        record_disk_sizes(summary, palier, disk_info)

        processed.append(palier)
        if parquet_results or postgres_results:
            print("\n✅ Benchmarks terminés pour ce palier.")
        elif args.ci:
            print("ℹ️  Mode CI : prérequis manquants, palier ignoré.")

    if not processed:
        print("Aucun palier n'a pu être benchmarké.")
        return

    markdown = build_markdown_table(summary, processed)
    print("\n📋 Tableau récapitulatif\n")
    print(markdown)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown + "\n", encoding="utf-8")
        print(f"\n📝 Tableau enregistré dans {output_path}")


if __name__ == "__main__":
    main()
