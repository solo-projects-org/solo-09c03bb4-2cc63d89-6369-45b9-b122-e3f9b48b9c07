from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.settings import Settings


@pytest.fixture()
def env(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("GSB_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GSB_RULES_FILE", str(Path(__file__).parent.parent / "config" / "rules.yaml"))
    settings = Settings.from_env()
    app = create_app(settings)
    with TestClient(app) as client:
        yield client, app, settings


def write_parquet(path: Path, rows: dict) -> Path:
    pq.write_table(pa.table(rows), path)
    return path


def upload_reference(client: TestClient, path: Path, name="customers", key="customer_id"):
    with path.open("rb") as handle:
        response = client.post(
            "/api/references",
            files={"file": (path.name, handle, "application/octet-stream")},
            data={"name": name, "key_field": key},
        )
    assert response.status_code == 201, response.text
    return response.json()


def upload_batch(client: TestClient, dataset: str, path: Path, process=True):
    with path.open("rb") as handle:
        response = client.post(
            f"/api/datasets/{dataset}/batches",
            files={"files": (path.name, handle, "application/octet-stream")},
            data={"process": "true" if process else "false"},
        )
    return response


def order_rows(ids, dates, customers, amounts, currencies, statuses):
    return {
        "order_id": ids,
        "order_date": [dt.date.fromisoformat(value) for value in dates],
        "customer_id": customers,
        "amount": amounts,
        "currency": currencies,
        "status": statuses,
    }


@pytest.fixture()
def customers(env, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "customers.parquet",
        {"customer_id": ["C1", "C2"], "customer_name": ["One", "Two"]},
    )
    upload_reference(client, path)
    return path


def test_bootstrap_dataset_and_ui(env):
    client, _app, _settings = env
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/").status_code == 200
    datasets = client.get("/api/datasets").json()["datasets"]
    assert [item["name"] for item in datasets] == ["daily_orders"]
    assert datasets[0]["current_rules_version"] == 1


def test_valid_batch_publishes_daily_partitions(env, customers, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "good.parquet",
        order_rows(
            ["O1", "O2"],
            ["2026-01-01", "2026-01-02"],
            ["C1", "C2"],
            [10.0, 20.5],
            ["EUR", "USD"],
            ["paid", "pending"],
        ),
    )
    response = upload_batch(client, "daily_orders", path)
    assert response.status_code == 200, response.text
    batch = response.json()["batch"]
    assert batch["status"] == "published"
    assert batch["row_count"] == 2
    assert batch["valid_count"] == 2
    assert batch["quarantine_count"] == 0

    partitions = client.get("/api/datasets/daily_orders/partitions").json()["partitions"]
    assert {row["date_key"] for row in partitions} == {"2026-01-01", "2026-01-02"}
    preview = client.get(
        "/api/datasets/daily_orders/preview?date=2026-01-02"
    ).json()
    assert preview["rows"][0]["order_id"] == "O2"
    assert preview["rows"][0]["amount"] == 20.5


def test_invalid_rows_are_quarantined_with_reasons(env, customers, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "mixed.parquet",
        order_rows(
            ["O1", "O2", "O3", "O4", "O5"],
            ["2026-01-01"] * 5,
            ["C1", "C404", "C1", "C2", "C2"],
            [10.0, 1.0, -5.0, 3.0, 4.0],
            ["USD", "USD", "usd", "USD", "USD"],
            ["paid", "paid", "paid", "unknown", "paid"],
        ),
    )
    response = upload_batch(client, "daily_orders", path)
    assert response.status_code == 200, response.text
    batch = response.json()["batch"]
    assert batch["status"] == "published"
    assert batch["valid_count"] == 2
    assert batch["quarantine_count"] == 3

    quarantine = client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]
    by_row = {row["row_index"]: row for row in quarantine}
    codes = {row_index: {reason["code"] for reason in row["reasons"]} for row_index, row in by_row.items()}
    assert "reference_missing" in codes[1]
    assert {"below_min", "pattern_mismatch"} <= codes[2]
    assert "not_allowed" in codes[3]
    assert by_row[1]["raw_row"]["order_id"] == "O2"

    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert sorted(row["order_id"] for row in preview["rows"]) == ["O1", "O5"]


def test_duplicate_primary_key_in_batch_is_quarantined(env, customers, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "dupes.parquet",
        order_rows(
            ["O1", "O1", "O2"],
            ["2026-01-01", "2026-01-01", "2026-01-01"],
            ["C1", "C1", "C2"],
            [1.0, 2.0, 3.0],
            ["USD", "USD", "USD"],
            ["paid", "paid", "paid"],
        ),
    )
    response = upload_batch(client, "daily_orders", path)
    assert response.status_code == 200, response.text
    batch = response.json()["batch"]
    assert batch["valid_count"] == 1
    quarantine = client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]
    assert {row["row_index"] for row in quarantine} == {0, 1}
    assert all(
        any(reason["code"] == "duplicate_key" for reason in row["reasons"])
        for row in quarantine
    )


def test_all_rows_invalid_keeps_dataset_empty(env, customers, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "bad.parquet",
        order_rows(
            ["O1"],
            ["2026-01-01"],
            ["C999"],
            [1.0],
            ["USD"],
            ["paid"],
        ),
    )
    response = upload_batch(client, "daily_orders", path)
    assert response.status_code == 202, response.text
    batch = response.json()["batch"]
    assert batch["status"] == "failed"
    assert batch["quarantine_count"] == 1
    assert batch["run_error"]["code"] == "all_rows_invalid"
    assert client.get("/api/datasets/daily_orders/partitions").json()["partitions"] == []


def test_rules_version_is_pinned_at_acceptance(env, customers, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "pinned.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [10.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", path, process=False)
    assert response.status_code == 202, response.text
    batch = response.json()["batch"]
    assert batch["rules_version"] == 1
    assert batch["reference_versions"]["customers"]["version"] == 1

    # Publish a stricter rule version after acceptance.
    detail = client.get("/api/datasets/daily_orders").json()
    spec = json.loads(json.dumps(detail["current_spec"]))
    for field in spec["fields"]:
        if field["name"] == "amount":
            field["max"] = 5
    response = client.post(
        "/api/datasets/daily_orders/rules",
        json={"spec": spec, "note": "lower amount max", "expected_version": 1},
    )
    assert response.status_code == 201, response.text
    assert response.json()["version"] == 2

    # The accepted batch still validates against v1 and publishes the 10.0 row.
    response = client.post(f"/api/batches/{batch['id']}/run")
    assert response.status_code == 200, response.text
    assert response.json()["batch"]["status"] == "published"
    assert response.json()["batch"]["rules_version"] == 1

    # A batch accepted after the change is pinned to v2 and quarantines it.
    path2 = write_parquet(
        tmp_path / "after-change.parquet",
        order_rows(["O2"], ["2026-01-02"], ["C1"], [10.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", path2)
    assert response.status_code == 202, response.text
    batch2 = response.json()["batch"]
    assert batch2["rules_version"] == 2
    assert batch2["quarantine_count"] == 1
    quarantine = client.get(f"/api/batches/{batch2['id']}/quarantine").json()["rows"]
    assert quarantine[0]["reasons"][0]["code"] == "above_max"


def test_stale_batch_cannot_overwrite_newer(env, customers, tmp_path):
    client, _app, _settings = env
    old_source = write_parquet(
        tmp_path / "old.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    # Accept but do not process: it owns acceptance sequence 1.
    response = upload_batch(client, "daily_orders", old_source, process=False)
    assert response.status_code == 202, response.text
    old_batch = response.json()["batch"]

    new_source = write_parquet(
        tmp_path / "new.parquet",
        order_rows(["O2"], ["2026-01-01"], ["C2"], [2.0], ["EUR"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", new_source)
    assert response.status_code == 200, response.text
    new_batch = response.json()["batch"]
    assert new_batch["status"] == "published"

    response = client.post(f"/api/batches/{old_batch['id']}/run")
    assert response.status_code == 409, response.text
    assert response.json()["error"]["code"] == "stale_batch"

    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert [row["order_id"] for row in preview["rows"]] == ["O2"]


def test_failed_batch_rerun_replaces_only_its_own_result(env, customers, tmp_path):
    client, _app, _settings = env
    # First batch publishes O1.
    first = write_parquet(
        tmp_path / "first.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", first)
    assert response.status_code == 200, response.text
    first_batch = response.json()["batch"]

    # Second batch fails because its only row is invalid.
    second = write_parquet(
        tmp_path / "second.parquet",
        order_rows(["O2"], ["2026-01-01"], ["C9"], [2.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", second)
    assert response.status_code == 202, response.text
    second_batch = response.json()["batch"]
    assert second_batch["status"] == "failed"
    assert second_batch["attempt"] == 1

    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert [row["order_id"] for row in preview["rows"]] == ["O1"]

    # Fix the source file in place and re-run only the failed batch.
    fixed = write_parquet(
        tmp_path / "second-fixed.parquet",
        order_rows(["O2"], ["2026-01-01"], ["C2"], [2.0], ["USD"], ["paid"]),
    )
    with fixed.open("rb") as handle:
        response = client.post(
            f"/api/batches/{second_batch['id']}/files",
            files={"files": (fixed.name, handle, "application/octet-stream")},
            data={"process": "true"},
        )
    assert response.status_code == 200, response.text
    fixed_batch = response.json()["batch"]
    assert fixed_batch["status"] == "published"
    assert fixed_batch["attempt"] == 2
    assert fixed_batch["id"] == second_batch["id"]

    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert sorted(row["order_id"] for row in preview["rows"]) == ["O1", "O2"]
    assert first_batch["id"] != second_batch["id"]

    # A re-run of an already published batch is rejected unless explicitly
    # allowed; when allowed it replaces only that batch's rows.
    response = client.post(f"/api/batches/{second_batch['id']}/run")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "already_published"

    response = client.post(
        f"/api/batches/{second_batch['id']}/run?replace_published=true"
    )
    assert response.status_code == 200, response.text
    assert response.json()["batch"]["attempt"] == 3
    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert sorted(row["order_id"] for row in preview["rows"]) == ["O1", "O2"]


def test_rerun_after_failure_replaces_quarantine_attempt(env, customers, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "bad.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C9"], [1.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", path, process=False)
    batch = response.json()["batch"]
    response = client.post(f"/api/batches/{batch['id']}/run")
    assert response.status_code == 202
    assert len(client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]) == 1

    # Re-run without changing anything: same failure, attempt 2, no duplicate
    # quarantine rows.
    response = client.post(f"/api/batches/{batch['id']}/run")
    assert response.status_code == 202
    detail = client.get(f"/api/batches/{batch['id']}").json()
    assert detail["attempt"] == 2
    rows = client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]
    assert len(rows) == 1


def test_cross_batch_key_conflict_is_quarantined(env, customers, tmp_path):
    client, _app, _settings = env
    first = write_parquet(
        tmp_path / "first.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    second = write_parquet(
        tmp_path / "second.parquet",
        order_rows(["O1", "O2"], ["2026-01-01", "2026-01-01"], ["C1", "C2"], [2.0, 3.0], ["USD", "USD"], ["paid", "paid"]),
    )
    assert upload_batch(client, "daily_orders", first).status_code == 200
    response = upload_batch(client, "daily_orders", second)
    assert response.status_code == 200, response.text
    batch = response.json()["batch"]
    assert batch["valid_count"] == 1
    quarantine = client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]
    assert quarantine[0]["reasons"][0]["code"] == "key_conflict_trusted"
    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert sorted(row["order_id"] for row in preview["rows"]) == ["O1", "O2"]


def test_schema_mismatch_fails_without_writing(env, customers, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "missing.parquet",
        {"order_id": ["O1"], "order_date": [dt.date(2026, 1, 1)]},
    )
    response = upload_batch(client, "daily_orders", path)
    assert response.status_code == 202, response.text
    batch = response.json()["batch"]
    assert batch["status"] == "failed"
    assert batch["run_error"]["code"] == "schema_mismatch"
    assert client.get("/api/datasets/daily_orders/partitions").json()["partitions"] == []


def test_non_parquet_upload_is_rejected(env, tmp_path):
    client, _app, _settings = env
    path = tmp_path / "notes.txt"
    path.write_text("not parquet")
    response = upload_batch(client, "daily_orders", path)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_upload"


def test_invalid_rule_version_is_rejected(env):
    client, _app, _settings = env
    detail = client.get("/api/datasets/daily_orders").json()
    spec = json.loads(json.dumps(detail["current_spec"]))
    spec["primary_key"] = ["does_not_exist"]
    response = client.post(
        "/api/datasets/daily_orders/rules",
        json={"spec": spec, "expected_version": 1},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_spec"
    assert client.get("/api/datasets/daily_orders").json()["current_rules_version"] == 1


def test_rule_version_optimistic_concurrency(env, customers):
    client, _app, _settings = env
    detail = client.get("/api/datasets/daily_orders").json()
    spec = detail["current_spec"]
    response = client.post(
        "/api/datasets/daily_orders/rules",
        json={"spec": spec, "expected_version": 99},
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "version_conflict"


def test_reference_version_is_pinned(env, tmp_path):
    client, _app, _settings = env
    ref1 = write_parquet(
        tmp_path / "customers-v1.parquet", {"customer_id": ["C1"], "customer_name": ["One"]}
    )
    upload_reference(client, ref1)
    source = write_parquet(
        tmp_path / "accepted.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C2"], [1.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", source, process=False)
    batch = response.json()["batch"]
    assert batch["reference_versions"]["customers"]["version"] == 1

    # A newer reference includes C2.
    ref2 = write_parquet(
        tmp_path / "customers-v2.parquet",
        {"customer_id": ["C1", "C2"], "customer_name": ["One", "Two"]},
    )
    upload_reference(client, ref2)
    response = client.post(f"/api/batches/{batch['id']}/run")
    assert response.status_code == 202, response.text
    detail = response.json()["batch"]
    assert detail["status"] == "failed"
    quarantine = client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]
    assert quarantine[0]["reasons"][0]["code"] == "reference_missing"
    assert "v1" in quarantine[0]["reasons"][0]["message"]


def test_trusted_data_survives_write_failure(env, customers, tmp_path, monkeypatch):
    client, app, _settings = env
    first = write_parquet(
        tmp_path / "first.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", first)
    assert response.status_code == 200, response.text

    second = write_parquet(
        tmp_path / "second.parquet",
        order_rows(["O2"], ["2026-01-02"], ["C2"], [2.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", second, process=False)
    batch = response.json()["batch"]

    pipeline = app.state.pipeline
    original_commit = pipeline._commit

    def broken_commit(*args, **kwargs):
        raise RuntimeError("simulated disk failure during commit")

    monkeypatch.setattr(pipeline, "_commit", broken_commit)
    response = client.post(f"/api/batches/{batch['id']}/run")
    assert response.status_code == 500, response.text
    detail = client.get(f"/api/batches/{batch['id']}").json()
    assert detail["status"] == "failed"
    assert "simulated disk failure" in detail["error_message"]

    monkeypatch.setattr(pipeline, "_commit", original_commit)
    # First batch's trusted partition is still present.
    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert [row["order_id"] for row in preview["rows"]] == ["O1"]
    partitions = client.get("/api/datasets/daily_orders/partitions").json()["partitions"]
    assert [row["date_key"] for row in partitions] == ["2026-01-01"]

    response = client.post(f"/api/batches/{batch['id']}/run")
    assert response.status_code == 200, response.text
    partitions = {
        row["date_key"] for row in
        client.get("/api/datasets/daily_orders/partitions").json()["partitions"]
    }
    assert partitions == {"2026-01-01", "2026-01-02"}


def test_invalid_pinned_rules_cannot_be_used(env, customers, tmp_path):
    """A batch keeps validating against its pinned copy even if a newer rule
    version exists; an invalid spec can never be pinned."""
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "x.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", path, process=False)
    batch = response.json()["batch"]
    detail = client.get(f"/api/batches/{batch['id']}").json()
    assert detail["rules_spec"]["fields"]
    assert detail["rules_version"] == 1


def test_rerun_failure_of_published_batch_preserves_previous_result(
    env, customers, tmp_path, monkeypatch
):
    """If a published batch is re-run and validation now fails, its previously
    published partition must remain readable and untouched."""
    client, app, _settings = env
    path = write_parquet(
        tmp_path / "published.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    batch = upload_batch(client, "daily_orders", path).json()["batch"]
    assert batch["status"] == "published"

    pipeline = app.state.pipeline
    original = pipeline._add_field_issues

    def failing(_spec):
        from app.pipeline import PipelineError

        raise PipelineError("simulated validation crash", code="processing_error")

    monkeypatch.setattr(pipeline, "_add_field_issues", failing)
    response = client.post(
        f"/api/batches/{batch['id']}/run?replace_published=true"
    )
    assert response.status_code == 500
    detail = client.get(f"/api/batches/{batch['id']}").json()
    assert detail["status"] == "failed"
    assert detail["previous_result_preserved"] is True

    monkeypatch.setattr(pipeline, "_add_field_issues", original)
    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert [row["order_id"] for row in preview["rows"]] == ["O1"]
    files = app.state.db.fetchall(
        "SELECT file_path FROM partition_files WHERE batch_id = ?", [batch["id"]]
    )
    assert len(files) == 1
    from pathlib import Path

    assert Path(files[0][0]).exists()


def test_replacing_files_of_unknown_batch_is_404(env, customers, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "x.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    with path.open("rb") as handle:
        response = client.post(
            "/api/batches/nope/files",
            files={"files": (path.name, handle, "application/octet-stream")},
            data={"process": "false"},
        )
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "unknown_batch"


def test_interrupted_batch_is_recovered_on_startup(env, customers, tmp_path):
    """A crash mid-processing leaves status=running; startup makes it retryable
    without touching trusted data."""
    client, app, settings = env
    path = write_parquet(
        tmp_path / "queued.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    batch = upload_batch(client, "daily_orders", path, process=False).json()["batch"]
    app.state.db.execute(
        "UPDATE batches SET status = 'running', attempt = 1 WHERE id = ?", [batch["id"]]
    )

    # Simulate a process restart.
    from app.main import create_app

    with TestClient(create_app(settings)) as restarted:
        detail = restarted.get(f"/api/batches/{batch['id']}").json()
        assert detail["status"] == "failed"
        assert detail["errors"]["code"] == "interrupted"
        response = restarted.post(f"/api/batches/{batch['id']}/run")
        assert response.status_code == 200, response.text
        assert response.json()["batch"]["status"] == "published"
        assert response.json()["batch"]["attempt"] == 2


def test_newer_failed_batch_does_not_block_older_batch(env, customers, tmp_path):
    """Staleness is defined by published data ownership.  A newer batch that
    failed owns no trusted files, so the older pending batch may publish."""
    client, _app, _settings = env
    old = write_parquet(
        tmp_path / "old.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    bad = write_parquet(
        tmp_path / "bad.parquet",
        order_rows(["O2"], ["2026-01-01"], ["C404"], [1.0], ["USD"], ["paid"]),
    )
    old_batch = upload_batch(client, "daily_orders", old, process=False).json()["batch"]
    bad_batch = upload_batch(client, "daily_orders", bad).json()["batch"]
    assert bad_batch["status"] == "failed"

    response = client.post(f"/api/batches/{old_batch['id']}/run")
    assert response.status_code == 200, response.text
    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert [row["order_id"] for row in preview["rows"]] == ["O1"]


def test_concurrent_run_is_rejected(env, customers, tmp_path):
    client, app, _settings = env
    path = write_parquet(
        tmp_path / "x.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    batch = upload_batch(client, "daily_orders", path, process=False).json()["batch"]
    app.state.db.execute(
        "UPDATE batches SET status = 'running', attempt = 1 WHERE id = ?", [batch["id"]]
    )
    response = client.post(f"/api/batches/{batch['id']}/run")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "batch_busy"


def test_commit_metadata_failure_keeps_old_files_readable(env, customers, tmp_path, monkeypatch):
    """If the metadata transaction fails after new files are placed, the old
    trusted files must remain and the new unreferenced files must be removed."""
    client, app, _settings = env
    first = write_parquet(
        tmp_path / "first.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    batch = upload_batch(client, "daily_orders", first).json()["batch"]
    assert batch["status"] == "published"
    old_files = app.state.db.fetchall(
        "SELECT file_path FROM partition_files WHERE batch_id = ?", [batch["id"]]
    )

    pipeline = app.state.pipeline
    real_conn = pipeline.db.conn

    class FlakyConn:
        def execute(self, sql, params=None):
            # Fail only the transaction's final publication update.
            if "UPDATE batches SET status" in sql and "published" in str(params):
                raise RuntimeError("simulated metadata failure")
            return real_conn.execute(sql, params)

        def __getattr__(self, item):
            return getattr(real_conn, item)

    monkeypatch.setattr(pipeline.db, "conn", FlakyConn())
    response = client.post(f"/api/batches/{batch['id']}/run?replace_published=true")
    assert response.status_code == 500
    monkeypatch.setattr(pipeline.db, "conn", real_conn)

    detail = client.get(f"/api/batches/{batch['id']}").json()
    assert detail["status"] == "failed"
    assert detail["previous_result_preserved"] is True
    from pathlib import Path

    assert [row[0] for row in old_files if Path(row[0]).exists()] == [
        row[0] for row in old_files
    ]
    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert preview["rows"][0]["amount"] == 1.0
    # No unreferenced files left in the partition directory.
    partition_dir = Path(old_files[0][0]).parent
    assert sorted(partition_dir.iterdir()) == [Path(old_files[0][0])]
