"""
Catalogue schema metadata and Postgres mapping utilities.

These helpers introspect the Pydantic catalogue contracts to determine
the superset of fields that must be stored in PostgreSQL, along with the
corresponding SQL types. By keeping this logic centralized we can add new
contract versions (with additional columns) without touching the ingestion
code or database DDL manually.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Dict, List, Tuple, get_args, get_origin, Union

from contracts.catalogue_contract import CATALOGUE_CONTRACT_VERSIONS

EXCLUDED_FIELDS = {"schema_version"}

PY_TYPE_TO_PG = {
    str: "TEXT",
    int: "INTEGER",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
    datetime: "TIMESTAMP WITH TIME ZONE",
    date: "DATE",
}


@dataclass(frozen=True)
class FieldMetadata:
    name: str
    sql_type: str


def _unwrap_optional(annotation):
    origin = get_origin(annotation)
    if origin is Union:
        args = [arg for arg in get_args(annotation) if arg is not type(None)]  # noqa: E721
        if len(args) == 1:
            return args[0]
    return annotation


def _resolve_annotation(annotation):
    if annotation is None:
        return str
    base = _unwrap_optional(annotation)
    origin = get_origin(base)
    if origin is not None:
        base = origin
    return base


def _infer_pg_type(annotation) -> str:
    base = _resolve_annotation(annotation)
    if isinstance(base, type) and issubclass(base, Enum):
        return "TEXT"
    for py_type, sql_type in PY_TYPE_TO_PG.items():
        try:
            if isinstance(base, type) and issubclass(base, py_type):
                return sql_type
        except TypeError:
            continue
    if base is datetime:
        return PY_TYPE_TO_PG[datetime]
    if base is date:
        return PY_TYPE_TO_PG[date]
    if base in (str,):
        return "TEXT"
    if base in (int,):
        return "INTEGER"
    if base in (float,):
        return "DOUBLE PRECISION"
    if base in (bool,):
        return "BOOLEAN"
    if base is bytes:
        return "BYTEA"
    # Fallback to TEXT for unknown types
    return "TEXT"


def _iter_models_in_version_order():
    for version in sorted(CATALOGUE_CONTRACT_VERSIONS.keys()):
        yield version, CATALOGUE_CONTRACT_VERSIONS[version]


def get_catalogue_field_metadata() -> List[FieldMetadata]:
    """Return ordered metadata (first occurrence wins) for catalogue storage."""
    seen = set()
    ordered: List[FieldMetadata] = []
    for _, model_cls in _iter_models_in_version_order():
        for name, field in model_cls.model_fields.items():
            if name in EXCLUDED_FIELDS or name in seen:
                continue
            ordered.append(FieldMetadata(name=name, sql_type=_infer_pg_type(field.annotation)))
            seen.add(name)
    return ordered


def get_catalogue_storage_columns() -> List[str]:
    return [meta.name for meta in get_catalogue_field_metadata()]


def get_catalogue_column_types() -> Dict[str, str]:
    return {meta.name: meta.sql_type for meta in get_catalogue_field_metadata()}
