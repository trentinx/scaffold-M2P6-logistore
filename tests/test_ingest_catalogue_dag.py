import sys
from contextvars import ContextVar
from types import ModuleType, SimpleNamespace

import pandas as pd

from airflow.dags import dag1_ingest_catalogue
from airflow.dags.dag1_ingest_catalogue import ingest_catalogue

# Stub the minimal Airflow APIs (datasets/decorators) needed to import the DAG
datasets_stub = ModuleType("airflow.datasets")


class _Dataset:
    def __init__(self, uri: str):
        self.uri = uri


datasets_stub.Dataset = _Dataset


decorators_stub = ModuleType("airflow.decorators")
_current_registry: ContextVar[dict[str, SimpleNamespace]] = ContextVar(
    "_current_registry", default=None
)


class _FakeTask(SimpleNamespace):
    def __init__(self, func):
        super().__init__(python_callable=func, __name__=func.__name__)

    def __call__(self, *_args, **_kwargs):
        return self


def _task(func=None, **_kwargs):
    def wrap(fn):
        task = _FakeTask(fn)
        registry = _current_registry.get()
        if registry is not None:
            registry[task.__name__] = task
        return task

    if func is None:
        return wrap
    return wrap(func)


def _dag(*_dargs, **_dkwargs):
    def decorator(fn):
        def dag_wrapper(*args, **kwargs):
            registry: dict[str, _FakeTask] = {}
            token = _current_registry.set(registry)
            try:
                fn(*args, **kwargs)
            finally:
                _current_registry.reset(token)
            return SimpleNamespace(task_dict=registry)

        return dag_wrapper

    return decorator


decorators_stub.dag = _dag
decorators_stub.task = _task

sys.modules.setdefault("airflow.datasets", datasets_stub)
sys.modules.setdefault("airflow.decorators", decorators_stub)

# Minimal psycopg2 stub so the DAG module can be imported without the real dependency
psycopg2_stub = ModuleType("psycopg2")
psycopg2_extras_stub = ModuleType("psycopg2.extras")
psycopg2_sql_stub = ModuleType("psycopg2.sql")


class _SQLFragment(str):
    def format(self, *args, **kwargs):
        return self


psycopg2_sql_stub.SQL = lambda s="": _SQLFragment(s)
psycopg2_sql_stub.Identifier = lambda s: _SQLFragment(s)


def _missing_connect(*_args, **_kwargs):
    raise RuntimeError("psycopg2 connect called outside of tests")


psycopg2_stub.connect = _missing_connect
psycopg2_stub.sql = psycopg2_sql_stub
psycopg2_extras_stub.execute_values = lambda *_args, **_kwargs: None

sys.modules.setdefault("psycopg2", psycopg2_stub)
sys.modules.setdefault("psycopg2.extras", psycopg2_extras_stub)
sys.modules.setdefault("psycopg2.sql", psycopg2_sql_stub)




def test_validate_and_upsert_catalogue_stats(tmp_path, monkeypatch):
    """Sanity check: valid rows are upserted, invalid rows are rejected."""
    csv_path = tmp_path / "catalogue_sample.csv"
    pd.DataFrame(
        [
            {
                "schema_version": "2.0",
                "sku": "SKU-20001",
                "label": "Lampe frontale",
                "category": "ELEC",
                "unit": "PCS",
                "min_stock": 1,
                "published_at": "2024-03-01T10:00:00",
                "supplier_id": "SUP-123",
            },
            {
                "schema_version": "2.0",
                "sku": "BADSKU",
                "label": "Produit invalide",
                "category": "ELEC",
                "unit": "PCS",
                "min_stock": -1,
                "published_at": "2024-03-01T10:00:00",
                "supplier_id": "",
            },
        ]
    ).to_csv(csv_path, index=False)

    executed_batches = []

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyConn:
        def __init__(self):
            self.committed = False

        def cursor(self):
            return DummyCursor()

        def commit(self):
            self.committed = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_connect(**_kwargs):
        return DummyConn()

    def fake_execute_values(cursor, query, values):
        executed_batches.append({"query": query, "values": values})

    ensured = []

    def fake_ensure(conn):
        ensured.append(True)

    monkeypatch.setattr(dag1_ingest_catalogue, "psycopg2", SimpleNamespace(connect=fake_connect))
    monkeypatch.setattr(dag1_ingest_catalogue, "execute_values", fake_execute_values)
    monkeypatch.setattr(dag1_ingest_catalogue, "DATA_REJECTED", tmp_path)
    monkeypatch.setattr(dag1_ingest_catalogue, "ensure_products_columns", fake_ensure)

    dag = ingest_catalogue()
    task = dag.task_dict["validate_and_upsert_catalogue"]

    stats = task.python_callable(str(csv_path))

    assert stats == {"valid": 1, "rejected": 1, "skipped": False}
    assert executed_batches and len(executed_batches[0]["values"]) == 1
    inserted_row = executed_batches[0]["values"][0]
    assert inserted_row[0] == "SKU-20001"
    # supplier_id must be part of the UPSERT columns (retro-compatibility)
    assert "supplier_id" in executed_batches[0]["query"]
    assert ensured

    rejected_files = list(tmp_path.glob("*_rejected_*.csv"))
    assert len(rejected_files) == 1
