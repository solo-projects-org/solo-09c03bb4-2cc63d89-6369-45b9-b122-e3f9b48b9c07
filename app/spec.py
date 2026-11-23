"""Rule specification parsing, validation and SQL coercion helpers.

A rule specification is deliberately explicit and versioned.  It is validated
when it is published and again, from the pinned copy, when a batch is
processed, so an invalid or edited rule file can never change a running batch.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
from decimal import Decimal
from typing import Any, Callable, Mapping

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
ALLOWED_TYPES = {"string", "integer", "number", "date", "timestamp", "boolean"}
ALLOWED_SCOPES = {"dataset", "partition"}
SPEC_KEYS = {"partition_field", "primary_key", "unique_scope", "allow_extra_columns", "fields"}
FIELD_KEYS = {
    "name",
    "type",
    "required",
    "max_length",
    "min",
    "max",
    "allowed_values",
    "regex",
    "references",
}


class SpecError(ValueError):
    """Raised when a rule specification is malformed."""


def _expect(condition: bool, message: str) -> None:
    if not condition:
        raise SpecError(message)


def _validate_name(value: Any, what: str) -> str:
    _expect(isinstance(value, str), f"{what} must be a string")
    value = value.strip()
    _expect(bool(NAME_RE.match(value)), f"{what} {value!r} must match {NAME_RE.pattern}")
    return value


def _validate_regex(pattern: str) -> str:
    try:
        re.compile(pattern)
    except re.error as exc:  # pragma: no cover - Python/RE2 differ slightly
        raise SpecError(f"invalid regex {pattern!r}: {exc}") from exc
    # DuckDB uses RE2.  Exercise it so unsupported constructs are caught at
    # publication rather than halfway through a batch.
    import duckdb

    try:
        duckdb.execute("SELECT regexp_full_match('', ?)", [pattern])
    except Exception as exc:
        raise SpecError(f"regex {pattern!r} is not supported: {exc}") from exc
    return pattern


def normalize_spec(
    raw: Mapping[str, Any],
    reference_lookup: Callable[[str], Mapping[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """Validate and normalize a rule spec.

    ``reference_lookup(name)`` must return the latest reference metadata
    (``{"key_field": ..., "columns": [...]}``) or ``None``.  It is optional so
    a spec can be syntactically checked in isolation.
    """

    _expect(isinstance(raw, Mapping), "rule spec must be an object")
    unknown = set(raw) - SPEC_KEYS
    _expect(not unknown, f"unknown rule spec keys: {', '.join(sorted(unknown))}")

    partition_field = _validate_name(raw.get("partition_field"), "partition_field")
    primary_key = raw.get("primary_key")
    if isinstance(primary_key, str):
        primary_key = [primary_key]
    _expect(
        isinstance(primary_key, list) and primary_key,
        "primary_key must be a non-empty list of field names",
    )
    pk = [_validate_name(item, "primary_key entry") for item in primary_key]
    _expect(len(pk) == len(set(pk)), "primary_key entries must be unique")

    unique_scope = raw.get("unique_scope", "dataset")
    _expect(unique_scope in ALLOWED_SCOPES, "unique_scope must be 'dataset' or 'partition'")
    allow_extra = raw.get("allow_extra_columns", False)
    _expect(isinstance(allow_extra, bool), "allow_extra_columns must be a boolean")

    fields_raw = raw.get("fields")
    _expect(isinstance(fields_raw, list) and fields_raw, "fields must be a non-empty list")

    fields: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(fields_raw):
        _expect(isinstance(item, Mapping), f"fields[{index}] must be an object")
        unknown = set(item) - FIELD_KEYS
        _expect(
            not unknown,
            f"fields[{index}] has unknown keys: {', '.join(sorted(unknown))}",
        )
        name = _validate_name(item.get("name"), f"fields[{index}].name")
        _expect(name not in seen, f"duplicate field name {name!r}")
        seen.add(name)
        kind = item.get("type")
        _expect(kind in ALLOWED_TYPES, f"field {name!r} has unknown type {kind!r}")
        required = item.get("required", False)
        _expect(isinstance(required, bool), f"field {name!r} required must be a boolean")

        field: dict[str, Any] = {"name": name, "type": kind, "required": required}

        max_length = item.get("max_length")
        if max_length is not None:
            _expect(kind == "string", f"field {name!r}: max_length only applies to string")
            _expect(
                isinstance(max_length, int) and not isinstance(max_length, bool) and max_length > 0,
                f"field {name!r}: max_length must be a positive integer",
            )
            field["max_length"] = max_length

        min_value = item.get("min")
        max_value = item.get("max")
        if min_value is not None or max_value is not None:
            _expect(kind in {"integer", "number"}, f"field {name!r}: min/max only apply to numbers")
            for label, value in (("min", min_value), ("max", max_value)):
                if value is not None:
                    _expect(
                        isinstance(value, (int, float)) and not isinstance(value, bool),
                        f"field {name!r}: {label} must be numeric",
                    )
            if min_value is not None and max_value is not None:
                _expect(float(min_value) <= float(max_value), f"field {name!r}: min > max")
            if min_value is not None:
                field["min"] = min_value
            if max_value is not None:
                field["max"] = max_value

        allowed = item.get("allowed_values")
        if allowed is not None:
            _expect(
                isinstance(allowed, list) and allowed,
                f"field {name!r}: allowed_values must be a non-empty list",
            )
            normalized_allowed: list[str] = []
            for value in allowed:
                _expect(
                    isinstance(value, (str, int, float, bool)),
                    f"field {name!r}: allowed_values must be scalar",
                )
                normalized_allowed.append(str(value))
            _expect(
                len(normalized_allowed) == len(set(normalized_allowed)),
                f"field {name!r}: allowed_values contain duplicates",
            )
            field["allowed_values"] = normalized_allowed

        pattern = item.get("regex")
        if pattern is not None:
            _expect(kind == "string", f"field {name!r}: regex only applies to string")
            _expect(isinstance(pattern, str) and pattern, f"field {name!r}: invalid regex")
            field["regex"] = _validate_regex(pattern)

        reference = item.get("references")
        if reference is not None:
            _expect(isinstance(reference, Mapping), f"field {name!r}: references must be an object")
            unknown = set(reference) - {"table", "field"}
            _expect(
                not unknown,
                f"field {name!r}: references has unknown keys: {', '.join(sorted(unknown))}",
            )
            table = _validate_name(reference.get("table"), f"field {name!r} reference table")
            ref_field = _validate_name(
                reference.get("field"), f"field {name!r} reference field"
            )
            if reference_lookup is not None:
                ref_meta = reference_lookup(table)
                _expect(
                    ref_meta is not None,
                    f"field {name!r} references unknown reference table {table!r}",
                )
                columns = [col["name"] if isinstance(col, Mapping) else str(col) for col in ref_meta.get("columns", [])]
                _expect(
                    ref_field in columns,
                    f"field {name!r} references {table}.{ref_field}, but that column does not exist",
                )
                _expect(
                    ref_field == ref_meta.get("key_field"),
                    f"field {name!r} must reference the key column "
                    f"{table}.{ref_meta.get('key_field')}",
                )
            field["references"] = {"table": table, "field": ref_field}

        fields.append(field)

    by_name = {field["name"]: field for field in fields}
    _expect(partition_field in by_name, "partition_field must be declared in fields")
    _expect(
        by_name[partition_field]["type"] == "date",
        "partition_field must have type 'date'",
    )
    _expect(
        by_name[partition_field]["required"],
        "partition_field must be required",
    )
    for key_field in pk:
        _expect(key_field in by_name, f"primary_key field {key_field!r} is not declared in fields")
        _expect(
            by_name[key_field]["required"],
            f"primary_key field {key_field!r} must be required",
        )

    return {
        "partition_field": partition_field,
        "primary_key": pk,
        "unique_scope": unique_scope,
        "allow_extra_columns": allow_extra,
        "fields": fields,
    }


def parse_spec_json(text: str) -> dict[str, Any]:
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SpecError(f"invalid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise SpecError("rule spec must be a JSON object")
    return raw


def field_names(spec: Mapping[str, Any]) -> list[str]:
    return [field["name"] for field in spec["fields"]]


def field_by_name(spec: Mapping[str, Any], name: str) -> dict[str, Any]:
    for field in spec["fields"]:
        if field["name"] == name:
            return field
    raise KeyError(name)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def source_expr(name: str, field: Mapping[str, Any]) -> str:
    """Expression that returns the field's validated value in its target type."""

    ident = quote_ident(name)
    text = f"CAST({ident} AS VARCHAR)"
    kind = field["type"]
    if kind == "string":
        return f"CAST({ident} AS VARCHAR)"
    if kind == "integer":
        return (
            f"COALESCE(TRY_CAST({text} AS BIGINT), "
            f"CAST(TRY_CAST({text} AS DOUBLE) AS BIGINT))"
        )
    if kind == "number":
        return f"CAST(TRY_CAST({text} AS DOUBLE) AS DOUBLE)"
    if kind == "date":
        return f"CAST(TRY_CAST({text} AS DATE) AS DATE)"
    if kind == "timestamp":
        return f"CAST(TRY_CAST({text} AS TIMESTAMP) AS TIMESTAMP)"
    if kind == "boolean":
        return f"CAST(TRY_CAST({text} AS BOOLEAN) AS BOOLEAN)"
    raise AssertionError(f"unreachable type {kind}")


def invalid_type_expr(name: str, field: Mapping[str, Any]) -> str:
    """Boolean SQL expression: true when a non-null source value is not valid."""

    ident = quote_ident(name)
    text = f"CAST({ident} AS VARCHAR)"
    kind = field["type"]
    if kind == "string":
        return f"{ident} IS NOT NULL AND TRY_CAST({ident} AS VARCHAR) IS NULL"
    if kind == "integer":
        as_double = f"TRY_CAST({text} AS DOUBLE)"
        return (
            f"{ident} IS NOT NULL AND "
            f"(TRY_CAST({text} AS BIGINT) IS NULL AND "
            f"({as_double} IS NULL OR NOT isfinite({as_double}) OR {as_double} <> TRUNC({as_double})))"
        )
    if kind == "number":
        as_double = f"TRY_CAST({text} AS DOUBLE)"
        return f"{ident} IS NOT NULL AND ({as_double} IS NULL OR NOT isfinite({as_double}))"
    if kind == "date":
        return f"{ident} IS NOT NULL AND TRY_CAST({text} AS DATE) IS NULL"
    if kind == "timestamp":
        return f"{ident} IS NOT NULL AND TRY_CAST({text} AS TIMESTAMP) IS NULL"
    if kind == "boolean":
        return f"{ident} IS NOT NULL AND TRY_CAST({text} AS BOOLEAN) IS NULL"
    raise AssertionError(f"unreachable type {kind}")


def type_sql(kind: str) -> str:
    return {
        "string": "VARCHAR",
        "integer": "BIGINT",
        "number": "DOUBLE",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "boolean": "BOOLEAN",
    }[kind]


# ---------------------------------------------------------------------------
# Python-side coercion
# ---------------------------------------------------------------------------
# Parquet files may contain native typed values (for example a real DATE or an
# INT64 column) rather than text.  Every input value is normalized here first,
# so a native value cannot be silently re-interpreted by a permissive cast
# (for example a DATE becoming its epoch-day integer).  Text values are parsed
# explicitly for non-string fields, which is convenient for CSV-to-parquet
# pipelines while still rejecting ambiguous values.

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BOOL_TEXT = {"true": True, "false": False, "1": True, "0": False}


def _parse_date_text(value: str) -> dt.date:
    text = value.strip()
    if not _ISO_DATE.match(text):
        raise ValueError("expected an ISO date (YYYY-MM-DD)")
    return dt.date.fromisoformat(text)


def _parse_timestamp_text(value: str) -> dt.datetime:
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("expected an ISO timestamp") from exc
    return parsed


def normalize_value(field: Mapping[str, Any], value: Any) -> Any:
    """Convert a parquet value to the canonical Python representation."""

    if value is None:
        return None
    kind = field["type"]
    if kind == "string":
        if isinstance(value, str):
            return value
        if isinstance(value, (bytes, bytearray)):
            raise ValueError("binary data is not a string")
        raise ValueError(f"{type(value).__name__} is not a string")
    if kind == "integer":
        if isinstance(value, bool):
            raise ValueError("boolean is not an integer")
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if not math.isfinite(value) or not value.is_integer():
                raise ValueError("expected an integer")
            return int(value)
        if isinstance(value, str):
            text = value.strip()
            try:
                return int(text)
            except ValueError:
                try:
                    as_float = float(text)
                except ValueError:
                    raise ValueError("expected an integer") from None
                if not math.isfinite(as_float) or not as_float.is_integer():
                    raise ValueError("expected an integer")
                return int(as_float)
        raise ValueError(f"{type(value).__name__} is not an integer")
    if kind == "number":
        if isinstance(value, bool):
            raise ValueError("boolean is not a number")
        if isinstance(value, Decimal):
            value = float(value)
        if isinstance(value, (int, float)):
            value = float(value)
            if not math.isfinite(value):
                raise ValueError("number must be finite")
            return value
        if isinstance(value, str):
            try:
                value = float(value.strip())
            except ValueError:
                raise ValueError("expected a number") from None
            if not math.isfinite(value):
                raise ValueError("number must be finite")
            return value
        raise ValueError(f"{type(value).__name__} is not a number")
    if kind == "date":
        if isinstance(value, dt.datetime):
            raise ValueError("expected a date, got a timestamp")
        if isinstance(value, dt.date):
            return value
        if isinstance(value, str):
            return _parse_date_text(value)
        raise ValueError(f"{type(value).__name__} is not a date")
    if kind == "timestamp":
        if isinstance(value, dt.datetime):
            return value
        if isinstance(value, str):
            return _parse_timestamp_text(value)
        raise ValueError(f"{type(value).__name__} is not a timestamp")
    if kind == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in _BOOL_TEXT:
            return _BOOL_TEXT[value.strip().lower()]
        raise ValueError(f"{type(value).__name__} is not a boolean")
    raise AssertionError(f"unreachable type {kind}")


def normalize_row(
    spec: Mapping[str, Any],
    row: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return the typed row and its structural (type/nullability) problems."""

    typed: dict[str, Any] = {}
    problems: list[dict[str, Any]] = []
    for field in spec["fields"]:
        name = field["name"]
        value = row.get(name)
        if value is None:
            if field["required"]:
                problems.append(
                    {
                        "code": "required_missing",
                        "field": name,
                        "message": f"{name} is required",
                    }
                )
            typed[name] = None
            continue
        try:
            typed[name] = normalize_value(field, value)
        except ValueError as exc:
            problems.append(
                {
                    "code": "invalid_type",
                    "field": name,
                    "message": f"{name} has invalid value: {exc}",
                }
            )
            typed[name] = None
    return typed, problems
