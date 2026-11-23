from __future__ import annotations

import datetime as dt

import pytest

from app.spec import SpecError, normalize_row, normalize_spec, normalize_value


BASE = {
    "partition_field": "day",
    "primary_key": ["id"],
    "fields": [
        {"name": "id", "type": "string", "required": True},
        {"name": "day", "type": "date", "required": True},
    ],
}


def test_minimal_spec_normalizes_defaults():
    spec = normalize_spec(BASE)
    assert spec["unique_scope"] == "dataset"
    assert spec["allow_extra_columns"] is False
    assert spec["partition_field"] == "day"


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda spec: spec.update(partition_field="missing"), "partition_field must be declared"),
        (lambda spec: spec["fields"][1].update(type="string"), "partition_field must have type 'date'"),
        (lambda spec: spec["fields"][1].update(required=False), "partition_field must be required"),
        (lambda spec: spec.update(primary_key=["missing"]), "primary_key field 'missing'"),
        (lambda spec: spec.update(primary_key=[]), "primary_key must be a non-empty list"),
        (lambda spec: spec["fields"][0].update(required=False), "must be required"),
        (lambda spec: spec["fields"][0].update(type="guid"), "unknown type"),
        (lambda spec: spec["fields"].append({"name": "x", "type": "string", "typo": 1}), "unknown keys"),
        (lambda spec: spec["fields"].append({"name": "x", "type": "date", "regex": "x"}), "regex only applies"),
        (lambda spec: spec["fields"].append({"name": "x", "type": "integer", "min": 2, "max": 1}), "min > max"),
        (lambda spec: spec.update(unique_scope="global"), "unique_scope"),
        (lambda spec: spec.update(unknown_key=True), "unknown rule spec keys"),
    ],
)
def test_invalid_specs_are_rejected(mutate, message):
    import copy

    spec = copy.deepcopy(BASE)
    mutate(spec)
    with pytest.raises(SpecError, match=message):
        normalize_spec(spec)


def test_reference_must_exist_and_match_key():
    spec = {
        **BASE,
        "fields": BASE["fields"]
        + [
            {
                "name": "customer",
                "type": "string",
                "references": {"table": "customers", "field": "customer_id"},
            }
        ],
    }

    def lookup(name):
        if name == "customers":
            return {"key_field": "customer_id", "columns": [{"name": "customer_id"}]}
        return None

    assert normalize_spec(spec, lookup)["fields"][2]["references"]["table"] == "customers"

    with pytest.raises(SpecError, match="unknown reference table"):
        normalize_spec(spec, lambda _name: None)

    def wrong_key(_name):
        return {"key_field": "other_id", "columns": [{"name": "customer_id"}]}

    with pytest.raises(SpecError, match="must reference the key column"):
        normalize_spec(spec, wrong_key)


def test_normalize_value_never_reinterprets_native_types():
    with pytest.raises(ValueError):
        normalize_value({"type": "string"}, 5)
    with pytest.raises(ValueError):
        normalize_value({"type": "date"}, dt.datetime(2026, 1, 1, 12))
    with pytest.raises(ValueError):
        normalize_value({"type": "integer"}, 1.5)
    with pytest.raises(ValueError):
        normalize_value({"type": "number"}, float("nan"))
    with pytest.raises(ValueError):
        normalize_value({"type": "date"}, 20000)  # epoch days are not dates

    assert normalize_value({"type": "integer"}, "42") == 42
    assert normalize_value({"type": "number"}, "2.5") == 2.5
    assert normalize_value({"type": "date"}, "2026-02-28") == dt.date(2026, 2, 28)
    assert normalize_value({"type": "boolean"}, "TRUE") is True
    with pytest.raises(ValueError):
        normalize_value({"type": "date"}, "2026-02-30")


def test_normalize_row_reports_missing_and_invalid():
    spec = normalize_spec(BASE)
    typed, problems = normalize_row(spec, {"id": 5, "day": None})
    assert typed == {"id": None, "day": None}
    codes = {problem["code"] for problem in problems}
    assert codes == {"invalid_type", "required_missing"}
