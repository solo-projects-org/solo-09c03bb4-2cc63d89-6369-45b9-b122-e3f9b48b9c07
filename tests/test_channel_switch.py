"""Tests for the one-time (channel, order_id) composite-key switch.

Scenarios covered:

* the switch publishes a new immutable rule version carrying the channel field
  and is idempotent across repeated calls and process restarts;
* legacy batches accepted before the switch keep validating with their pinned
  single-key rules whenever they are published, and are classified direct;
* after the switch, accepted parquet files require a channel column with only
  direct/partner values;
* same order id can coexist across channels (even across days/batches), while
  same-channel duplicates are isolated inside a batch and against trusted data;
* a failed switch leaves acceptance, deduplication and preview on legacy
  semantics;
* trusted preview exposes the channel of every row, including migrated ones.
"""

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


def order_rows(ids, dates, customers, amounts, currencies, statuses, channels=None):
    rows = {
        "order_id": ids,
        "order_date": [dt.date.fromisoformat(value) for value in dates],
        "customer_id": customers,
        "amount": amounts,
        "currency": currencies,
        "status": statuses,
    }
    if channels is not None:
        rows["channel"] = channels
    return rows


@pytest.fixture()
def customers(env, tmp_path):
    client, _app, _settings = env
    path = write_parquet(
        tmp_path / "customers.parquet",
        {"customer_id": ["C1", "C2"], "customer_name": ["One", "Two"]},
    )
    upload_reference(client, path)
    return path


def switch(client: TestClient, note="partner onboarding"):
    return client.post(
        "/api/datasets/daily_orders/channel-switch", json={"note": note}
    )


def test_switch_state_is_absent_by_default(env, customers):
    client, _app, _settings = env
    assert client.get("/api/datasets/daily_orders/channel-switch").json() == {"switch": None}
    detail = client.get("/api/datasets/daily_orders").json()
    assert detail["channel_key_enabled"] is False
    listing = client.get("/api/datasets").json()["datasets"]
    assert listing[0]["channel_key_enabled"] is False
    switch(client)
    listing = client.get("/api/datasets").json()["datasets"]
    assert listing[0]["channel_key_enabled"] is True


def test_switch_publishes_channel_rule_and_migrates_trusted_files(env, customers, tmp_path):
    client, app, settings = env
    legacy = write_parquet(
        tmp_path / "legacy.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", legacy)
    assert response.status_code == 200, response.text

    response = switch(client)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["already_effective"] is False
    assert result["migrated_files"] == 1
    new_version = result["rules_version"]
    assert new_version == 2

    spec = result["spec"]
    assert spec["primary_key"] == ["channel", "order_id"]
    assert spec["unique_scope"] == "dataset"
    channel_field = next(field for field in spec["fields"] if field["name"] == "channel")
    assert channel_field["required"] is True
    assert channel_field["allowed_values"] == ["direct", "partner"]
    order_id_field = next(field for field in spec["fields"] if field["name"] == "order_id")
    assert order_id_field["type"] == "string"

    detail = client.get("/api/datasets/daily_orders").json()
    assert detail["current_rules_version"] == new_version
    assert detail["channel_key_enabled"] is True

    # Every migrated trusted row now reports a channel.
    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert "channel" in preview["columns"]
    assert preview["columns"][0] == "channel"
    assert preview["rows"][0]["channel"] == "direct"
    assert preview["rows"][0]["order_id"] == "O1"

    # Rule versions remain immutable and the new version is a normal rule row.
    versions = client.get("/api/datasets/daily_orders/rules").json()["versions"]
    assert [version["version"] for version in versions] == [1, 2]
    assert versions[0]["spec"]["primary_key"] == ["order_id"]


def test_switch_is_idempotent_and_returns_effective_version(env, customers):
    client, _app, _settings = env
    first = switch(client).json()
    second = switch(client).json()
    assert second["already_effective"] is True
    assert second["rules_version"] == first["rules_version"]
    assert second["switched_at"] == first["switched_at"]

    versions = client.get("/api/datasets/daily_orders/rules").json()["versions"]
    assert len(versions) == 2
    status = client.get("/api/datasets/daily_orders/channel-switch").json()
    assert status["switch"]["rules_version"] == first["rules_version"]


def test_switch_survives_restart(env, customers):
    client, _app, settings = env
    result = switch(client).json()

    with TestClient(create_app(settings)) as restarted:
        status = restarted.get(
            "/api/datasets/daily_orders/channel-switch"
        ).json()["switch"]
        assert status is not None
        assert status["rules_version"] == result["rules_version"]
        again = restarted.post(
            "/api/datasets/daily_orders/channel-switch", json={}
        ).json()
        assert again["already_effective"] is True
        assert again["rules_version"] == result["rules_version"]


def test_new_batches_require_channel_column_after_switch(env, customers, tmp_path):
    client, _app, _settings = env
    switch(client)
    no_channel = write_parquet(
        tmp_path / "no-channel.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", no_channel)
    assert response.status_code == 202, response.text
    batch = response.json()["batch"]
    assert batch["status"] == "failed"
    assert batch["run_error"]["code"] == "schema_mismatch"
    assert batch["run_error"]["details"]["missing_columns"] == ["channel"]
    assert client.get("/api/datasets/daily_orders/partitions").json()["partitions"] == []


def test_channel_value_is_restricted_to_direct_or_partner(env, customers, tmp_path):
    client, _app, _settings = env
    switch(client)
    path = write_parquet(
        tmp_path / "bad-channel.parquet",
        order_rows(
            ["O1", "O2"],
            ["2026-01-01", "2026-01-01"],
            ["C1", "C2"],
            [1.0, 2.0],
            ["USD", "EUR"],
            ["paid", "paid"],
            channels=["direct", "marketplace"],
        ),
    )
    response = upload_batch(client, "daily_orders", path)
    assert response.status_code == 200, response.text
    batch = response.json()["batch"]
    assert batch["valid_count"] == 1
    quarantine = client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]
    assert len(quarantine) == 1
    assert quarantine[0]["row_index"] == 1
    assert any(reason["code"] == "not_allowed" and reason["field"] == "channel"
               for reason in quarantine[0]["reasons"])

    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert [(row["channel"], row["order_id"]) for row in preview["rows"]] == [("direct", "O1")]


def test_same_id_coexists_across_channels_but_is_isolated_within_channel(
    env, customers, tmp_path
):
    client, _app, _settings = env
    # Direct rows trusted before the switch.
    direct = write_parquet(
        tmp_path / "direct.parquet",
        order_rows(["O1", "O2"], ["2026-01-01", "2026-01-02"],
                   ["C1", "C2"], [1.0, 2.0], ["USD", "EUR"], ["paid", "paid"]),
    )
    assert upload_batch(client, "daily_orders", direct).status_code == 200
    switch(client)

    # Partner channel reuses the same order ids, even on different days: both
    # rows must publish because the key is (channel, order_id).
    partner = write_parquet(
        tmp_path / "partner.parquet",
        order_rows(
            ["O1", "O2"],
            ["2026-01-03", "2026-01-04"],
            ["C1", "C2"],
            [3.0, 4.0],
            ["USD", "EUR"],
            ["paid", "paid"],
            channels=["partner", "partner"],
        ),
    )
    response = upload_batch(client, "daily_orders", partner)
    assert response.status_code == 200, response.text
    assert response.json()["batch"]["valid_count"] == 2

    # A direct twin of an already trusted direct id conflicts, even across days.
    direct_conflict = write_parquet(
        tmp_path / "direct-conflict.parquet",
        order_rows(
            ["O1"], ["2026-01-09"], ["C1"], [5.0], ["USD"], ["paid"],
            channels=["direct"],
        ),
    )
    response = upload_batch(client, "daily_orders", direct_conflict)
    assert response.status_code == 202, response.text
    batch = response.json()["batch"]
    assert batch["valid_count"] == 0
    quarantine = client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]
    assert quarantine[0]["reasons"][0]["code"] == "key_conflict_trusted"

    # In-batch duplicates are scoped by the composite key.
    in_batch = write_parquet(
        tmp_path / "in-batch.parquet",
        order_rows(
            ["O9", "O9", "O9"],
            ["2026-01-05"] * 3,
            ["C1", "C1", "C2"],
            [1.0, 2.0, 3.0],
            ["USD", "USD", "EUR"],
            ["paid", "paid", "paid"],
            channels=["partner", "partner", "direct"],
        ),
    )
    response = upload_batch(client, "daily_orders", in_batch)
    assert response.status_code == 200, response.text
    batch = response.json()["batch"]
    assert batch["valid_count"] == 1
    quarantine = client.get(f"/api/batches/{batch['id']}/quarantine").json()["rows"]
    assert {row["row_index"] for row in quarantine} == {0, 1}
    assert all(
        any(reason["code"] == "duplicate_key" for reason in row["reasons"])
        for row in quarantine
    )

    rows = client.get("/api/datasets/daily_orders/preview?limit=100").json()["rows"]
    keys = sorted((row["channel"], row["order_id"]) for row in rows)
    assert ("direct", "O1") in keys
    assert ("partner", "O1") in keys
    assert ("partner", "O2") in keys
    assert ("direct", "O9") in keys


def test_legacy_batch_accepted_before_switch_keeps_old_rules_and_publishes_direct(
    env, customers, tmp_path
):
    client, _app, _settings = env
    # Accepted under legacy semantics, processed only after the switch.
    legacy_source = write_parquet(
        tmp_path / "legacy.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [10.0], ["USD"], ["paid"]),
    )
    accepted = upload_batch(client, "daily_orders", legacy_source, process=False)
    assert accepted.status_code == 202
    legacy_batch = accepted.json()["batch"]
    assert legacy_batch["channel_mode"] == "legacy"
    assert legacy_batch["rules_version"] == 1

    # Tighten amount max in a new rule version, then switch to channel keys.
    detail = client.get("/api/datasets/daily_orders").json()
    spec = json.loads(json.dumps(detail["current_spec"]))
    for field in spec["fields"]:
        if field["name"] == "amount":
            field["max"] = 5
    tightened = client.post(
        "/api/datasets/daily_orders/rules",
        json={"spec": spec, "expected_version": 1},
    )
    assert tightened.status_code == 201, tightened.text
    switch(client)

    # The legacy batch still validates with its pinned v1 rules (amount 10 is
    # allowed) and needs no channel column in its source file.
    response = client.post(f"/api/batches/{legacy_batch['id']}/run")
    assert response.status_code == 200, response.text
    published = response.json()["batch"]
    assert published["status"] == "published"
    assert published["rules_version"] == 1
    assert published["channel_mode"] == "legacy"

    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert preview["rows"][0]["channel"] == "direct"
    assert preview["rows"][0]["order_id"] == "O1"


def test_legacy_batch_keeps_single_key_dedup_after_switch(env, customers, tmp_path):
    client, _app, _settings = env
    first = write_parquet(
        tmp_path / "first.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    assert upload_batch(client, "daily_orders", first).status_code == 200
    pending_source = write_parquet(
        tmp_path / "pending.parquet",
        order_rows(["O1"], ["2026-01-08"], ["C1"], [2.0], ["EUR"], ["paid"]),
    )
    pending = upload_batch(
        client, "daily_orders", pending_source, process=False
    ).json()["batch"]
    switch(client)

    # Legacy semantics: order_id alone is the key, so this conflicts with the
    # already trusted O1 even though every trusted row is now tagged direct.
    response = client.post(f"/api/batches/{pending['id']}/run")
    assert response.status_code == 202, response.text
    detail = response.json()["batch"]
    assert detail["status"] == "failed"
    quarantine = client.get(f"/api/batches/{pending['id']}/quarantine").json()["rows"]
    assert quarantine[0]["reasons"][0]["code"] == "key_conflict_trusted"


def test_failed_switch_keeps_legacy_semantics(env, customers, tmp_path, monkeypatch):
    client, app, settings = env
    legacy = write_parquet(
        tmp_path / "legacy.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    assert upload_batch(client, "daily_orders", legacy).status_code == 200
    trusted_files = app.state.db.fetchall(
        "SELECT file_path FROM partition_files"
    )
    original_bytes = {
        path: Path(path).read_bytes() for (path,) in trusted_files
    }

    pipeline = app.state.pipeline

    def broken_migration(_dataset):
        raise RuntimeError("simulated migration failure")

    monkeypatch.setattr(pipeline, "_migrate_trusted_files_to_channel", broken_migration)
    response = switch(client)
    assert response.status_code == 500, response.text
    monkeypatch.undo()

    # No switch record, no new rule version, files byte-identical.
    assert client.get("/api/datasets/daily_orders/channel-switch").json() == {"switch": None}
    detail = client.get("/api/datasets/daily_orders").json()
    assert detail["current_rules_version"] == 1
    assert detail["channel_key_enabled"] is False
    for (path,) in trusted_files:
        assert Path(path).read_bytes() == original_bytes[path]
        assert not Path(path + ".pre-channel.bak").exists()
        assert "channel" not in pq.read_schema(path).names

    # Acceptance, dedup and preview all still work on the legacy single key.
    another = write_parquet(
        tmp_path / "another.parquet",
        order_rows(["O2"], ["2026-01-02"], ["C2"], [2.0], ["EUR"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", another)
    assert response.status_code == 200, response.text
    assert response.json()["batch"]["channel_mode"] == "legacy"

    conflict = write_parquet(
        tmp_path / "conflict.parquet",
        order_rows(["O2"], ["2026-01-03"], ["C2"], [3.0], ["EUR"], ["paid"]),
    )
    response = upload_batch(client, "daily_orders", conflict)
    assert response.json()["batch"]["quarantine_count"] == 1

    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert "channel" not in preview["columns"]

    # A later retry of the switch succeeds.
    assert switch(client).status_code == 200


def test_switch_metadata_failure_restores_backups_and_keeps_legacy(
    env, customers, tmp_path, monkeypatch
):
    """A failure at the metadata commit point restores the backed-up trusted
    files and leaves every surface (acceptance, dedup, preview) on legacy
    semantics; the switch can then be retried successfully."""
    client, app, _settings = env
    legacy = write_parquet(
        tmp_path / "legacy.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    assert upload_batch(client, "daily_orders", legacy).status_code == 200
    trusted = app.state.db.fetchone("SELECT file_path FROM partition_files")[0]
    original_bytes = Path(trusted).read_bytes()

    real_conn = app.state.pipeline.db.conn

    class FlakyConn:
        def execute(self, sql, params=None):
            if "INSERT INTO channel_key_switches" in sql:
                raise RuntimeError("simulated switch metadata failure")
            return real_conn.execute(sql, params)

        def __getattr__(self, item):
            return getattr(real_conn, item)

    monkeypatch.setattr(app.state.pipeline.db, "conn", FlakyConn())
    # The shared client re-raises by default; flip only its transport flag so
    # the application's 500 handler responds without re-running the lifespan.
    transport = client._transport
    monkeypatch.setattr(transport, "raise_server_exceptions", False)
    response = client.post(
        "/api/datasets/daily_orders/channel-switch", json={"note": "x"}
    )
    monkeypatch.undo()
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "switch_failed"

    assert client.get("/api/datasets/daily_orders/channel-switch").json() == {"switch": None}
    detail = client.get("/api/datasets/daily_orders").json()
    assert detail["current_rules_version"] == 1
    assert detail["channel_key_enabled"] is False
    assert Path(trusted).read_bytes() == original_bytes
    assert not Path(trusted + ".pre-channel.bak").exists()
    assert "channel" not in pq.read_schema(trusted).names

    preview = client.get("/api/datasets/daily_orders/preview?date=2026-01-01").json()
    assert "channel" not in preview["columns"]

    retry = switch(client)
    assert retry.status_code == 200
    assert retry.json()["migrated_files"] == 1
    assert client.get("/api/datasets/daily_orders/channel-switch").json()["switch"] is not None


def test_switch_on_unknown_dataset_is_404(env, customers):
    client, _app, _settings = env
    response = client.post("/api/datasets/nope/channel-switch", json={})
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "unknown_dataset"


def test_preview_lists_channel_for_every_row_after_switch(env, customers, tmp_path):
    client, _app, _settings = env
    legacy = write_parquet(
        tmp_path / "legacy.parquet",
        order_rows(["O1"], ["2026-01-01"], ["C1"], [1.0], ["USD"], ["paid"]),
    )
    upload_batch(client, "daily_orders", legacy)
    switch(client)
    partner = write_parquet(
        tmp_path / "partner.parquet",
        order_rows(
            ["O1"], ["2026-01-01"], ["C1"], [9.0], ["USD"], ["paid"],
            channels=["partner"],
        ),
    )
    upload_batch(client, "daily_orders", partner)

    preview = client.get(
        "/api/datasets/daily_orders/preview?date=2026-01-01&limit=50"
    ).json()
    assert "channel" in preview["columns"]
    assert sorted(row["channel"] for row in preview["rows"]) == ["direct", "partner"]
    assert {row["order_id"] for row in preview["rows"]} == {"O1"}
