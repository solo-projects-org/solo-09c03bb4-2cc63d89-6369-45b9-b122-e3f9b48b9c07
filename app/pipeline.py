"""Batch validation and trusted-dataset publication.

Lifecycle of a batch::

    accepted -> running -> published
                       \\-> failed  (retryable; trusted data untouched)

Invariants implemented here:

* At acceptance time a batch stores an immutable copy of the rules version and
  of the exact reference-table versions that were latest.  Later changes to
  rules or references cannot alter an already accepted batch.
* Field, primary-key and cross-table reference checks run before anything is
  written.  Failing rows are stored in the quarantine together with their
  reasons; only rows without issues are written to the trusted dataset.
* Each trusted daily partition is a set of immutable parquet files owned by a
  batch.  Re-running a failed batch replaces only that batch's own files for
  that batch id; other batches' files are left untouched.
* A batch whose acceptance sequence is lower than the sequence of an already
  published owner of a partition it targets is rejected as stale.  The check is
  repeated immediately before commit.
* Validation and writing are staged.  If anything fails the previous trusted
  partition files and metadata remain in place.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
import shutil
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .db import Database
from .spec import (
    NAME_RE,
    field_names,
    normalize_row,
    normalize_spec,
    quote_ident,
)

log = logging.getLogger("gsb.pipeline")

BATCH_ACCEPTED = "accepted"
BATCH_RUNNING = "running"
BATCH_FAILED = "failed"
BATCH_PUBLISHED = "published"

BATCH_STATUSES = (BATCH_ACCEPTED, BATCH_RUNNING, BATCH_FAILED, BATCH_PUBLISHED)

ARROW_TYPES = {
    "string": pa.string(),
    "integer": pa.int64(),
    "number": pa.float64(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "boolean": pa.bool_(),
}


class PipelineError(Exception):
    """A user-facing processing failure."""

    def __init__(self, message: str, *, code: str = "processing_error", details: Any = None):
        super().__init__(message)
        self.code = code
        self.details = details


class StaleBatchError(PipelineError):
    def __init__(self, message: str):
        super().__init__(message, code="stale_batch")


@dataclass
class RunResult:
    batch_id: str
    attempt: int
    status: str
    row_count: int
    valid_count: int
    quarantine_count: int
    partitions: list[str]
    error: str | None = None


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _loads(value: Any, default: Any = None) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value if value is not None else default


class Pipeline:
    def __init__(self, db: Database, settings):
        self.db = db
        self.settings = settings
        self._run_lock = threading.Lock()

    # ==================================================================
    # References
    # ==================================================================
    def publish_reference(
        self,
        name: str,
        key_field: str,
        uploaded: Path,
        original_filename: str,
        note: str | None,
    ) -> dict[str, Any]:
        if not NAME_RE.match(name or ""):
            raise PipelineError(
                "reference name must match [A-Za-z_][A-Za-z0-9_]*", code="invalid_reference"
            )
        if not NAME_RE.match(key_field or ""):
            raise PipelineError("key_field is invalid", code="invalid_reference")
        try:
            table = pq.read_table(uploaded)
        except Exception as exc:
            raise PipelineError(
                f"reference file is not readable parquet: {exc}", code="invalid_reference"
            ) from exc
        if table.num_rows == 0:
            raise PipelineError("reference file has no rows", code="invalid_reference")
        columns = table.column_names
        if key_field not in columns:
            raise PipelineError(
                f"key column {key_field!r} not found in columns {columns}",
                code="invalid_reference",
            )
        keys = table.column(key_field).to_pylist()
        if any(key is None for key in keys):
            raise PipelineError("reference key column contains nulls", code="invalid_reference")
        if len(set(keys)) != len(keys):
            raise PipelineError("reference key column is not unique", code="invalid_reference")
        col_meta = [
            {"name": column, "type": str(table.schema.field(column).type)} for column in columns
        ]

        with self.db.lock:
            row = self.db.conn.execute(
                "SELECT COALESCE(MAX(version), 0) FROM reference_versions WHERE name = ?", [name]
            ).fetchone()
            version = int(row[0]) + 1
            version_dir = self.settings.references_dir / name / f"v{version:06d}"
            version_dir.mkdir(parents=True, exist_ok=False)
            target = version_dir / "data.parquet"
            pq.write_table(table, target)
            try:
                self.db.conn.execute(
                    """
                    INSERT INTO reference_versions
                        (name, version, key_field, columns, file_path, original_filename,
                         row_count, note, created_at)
                    VALUES (?, ?, ?, CAST(? AS JSON), ?, ?, ?, ?, ?)
                    """,
                    [
                        name,
                        version,
                        key_field,
                        _dumps(col_meta),
                        str(target),
                        original_filename,
                        table.num_rows,
                        note,
                        _utcnow(),
                    ],
                )
            except Exception:
                shutil.rmtree(version_dir, ignore_errors=True)
                raise
        return {
            "name": name,
            "version": version,
            "key_field": key_field,
            "columns": col_meta,
            "row_count": table.num_rows,
        }

    def latest_references(self) -> dict[str, dict[str, Any]]:
        rows = self.db.query_dicts(
            """
            SELECT r.name, r.version, r.key_field, r.columns, r.file_path, r.row_count, r.created_at
            FROM reference_versions r
            JOIN (
                SELECT name, MAX(version) AS version FROM reference_versions GROUP BY name
            ) latest ON latest.name = r.name AND latest.version = r.version
            ORDER BY r.name
            """
        )
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            row["columns"] = _loads(row["columns"], [])
            row["created_at"] = str(row["created_at"])
            result[row["name"]] = row
        return result

    def list_reference_versions(self, name: str | None = None) -> list[dict[str, Any]]:
        if name:
            rows = self.db.query_dicts(
                "SELECT * FROM reference_versions WHERE name = ? ORDER BY version DESC", [name]
            )
        else:
            rows = self.db.query_dicts(
                "SELECT * FROM reference_versions ORDER BY name, version DESC"
            )
        for row in rows:
            row["columns"] = _loads(row["columns"], [])
            row["created_at"] = str(row["created_at"])
        return rows

    # ==================================================================
    # Datasets and immutable rule versions
    # ==================================================================
    def get_dataset(self, name: str) -> dict[str, Any] | None:
        rows = self.db.query_dicts("SELECT * FROM datasets WHERE name = ?", [name])
        if not rows:
            return None
        rows[0]["created_at"] = str(rows[0]["created_at"])
        rows[0]["updated_at"] = str(rows[0]["updated_at"])
        return rows[0]

    def list_datasets(self) -> list[dict[str, Any]]:
        rows = self.db.query_dicts(
            """
            SELECT d.*,
                   (SELECT COUNT(*) FROM batches b WHERE b.dataset = d.name) AS batch_count,
                   (SELECT COUNT(*) FROM batches b WHERE b.dataset = d.name AND b.status = 'published')
                       AS published_batches
            FROM datasets d ORDER BY d.name
            """
        )
        for row in rows:
            row["created_at"] = str(row["created_at"])
            row["updated_at"] = str(row["updated_at"])
        return rows

    def create_dataset(
        self,
        name: str,
        description: str | None,
        raw_spec: Mapping[str, Any],
        *,
        verify_references: bool = True,
    ) -> dict[str, Any]:
        if not NAME_RE.match(name or ""):
            raise PipelineError(
                "dataset name must match [A-Za-z_][A-Za-z0-9_]*", code="invalid_spec"
            )
        refs = self.latest_references() if verify_references else {}
        spec = normalize_spec(
            raw_spec, (lambda ref_name: refs.get(ref_name)) if verify_references else None
        )
        with self.db.transaction() as conn:
            if conn.execute("SELECT 1 FROM datasets WHERE name = ?", [name]).fetchone():
                raise PipelineError(f"dataset {name!r} already exists", code="dataset_exists")
            now = _utcnow()
            conn.execute(
                "INSERT INTO datasets (name, description, current_rules_version, created_at, updated_at)"
                " VALUES (?, ?, 1, ?, ?)",
                [name, description, now, now],
            )
            conn.execute(
                "INSERT INTO rule_versions (dataset, version, spec, note, created_at)"
                " VALUES (?, 1, CAST(? AS JSON), ?, ?)",
                [name, _dumps(spec), "initial", now],
            )
        return {"name": name, "version": 1, "spec": spec}

    def add_rule_version(
        self,
        dataset: str,
        raw_spec: Mapping[str, Any],
        note: str | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        refs = self.latest_references()
        spec = normalize_spec(raw_spec, lambda ref_name: refs.get(ref_name))
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT current_rules_version FROM datasets WHERE name = ?", [dataset]
            ).fetchone()
            if row is None:
                raise PipelineError(f"unknown dataset {dataset!r}", code="unknown_dataset")
            current = int(row[0])
            if expected_version is not None and expected_version != current:
                raise PipelineError(
                    f"rules changed concurrently: expected version {expected_version}, "
                    f"current version is {current}",
                    code="version_conflict",
                )
            version = current + 1
            now = _utcnow()
            conn.execute(
                "INSERT INTO rule_versions (dataset, version, spec, note, created_at)"
                " VALUES (?, ?, CAST(? AS JSON), ?, ?)",
                [dataset, version, _dumps(spec), note, now],
            )
            conn.execute(
                "UPDATE datasets SET current_rules_version = ?, updated_at = ? WHERE name = ?",
                [version, now, dataset],
            )
        return {"name": dataset, "version": version, "spec": spec}

    def get_rule_version(self, dataset: str, version: int) -> dict[str, Any] | None:
        rows = self.db.query_dicts(
            "SELECT version, spec, note, created_at FROM rule_versions"
            " WHERE dataset = ? AND version = ?",
            [dataset, version],
        )
        if not rows:
            return None
        rows[0]["spec"] = _loads(rows[0]["spec"], {})
        rows[0]["created_at"] = str(rows[0]["created_at"])
        return rows[0]

    def list_rule_versions(self, dataset: str) -> list[dict[str, Any]]:
        rows = self.db.query_dicts(
            "SELECT version, spec, note, created_at FROM rule_versions"
            " WHERE dataset = ? ORDER BY version",
            [dataset],
        )
        for row in rows:
            row["spec"] = _loads(row["spec"], {})
            row["created_at"] = str(row["created_at"])
        return rows

    # ==================================================================
    # Batch intake
    # ==================================================================
    def accept_batch(
        self,
        dataset: str,
        files: list[tuple[str, Path]],
        *,
        expected_rules_version: int | None = None,
    ) -> dict[str, Any]:
        if not files:
            raise PipelineError("at least one parquet file is required", code="invalid_upload")

        ds = self.get_dataset(dataset)
        if ds is None:
            raise PipelineError(f"unknown dataset {dataset!r}", code="unknown_dataset")
        current_version = int(ds["current_rules_version"])
        if expected_rules_version is not None and expected_rules_version != current_version:
            raise PipelineError(
                f"current rules version is {current_version}, upload expected "
                f"{expected_rules_version}",
                code="version_conflict",
            )

        rule = self.get_rule_version(dataset, current_version)
        assert rule is not None
        spec = normalize_spec(rule["spec"], None)

        # Quick structural check at intake so a non-parquet upload is rejected
        # immediately.  Full schema/business validation happens on process.
        for original_name, path in files:
            try:
                pq.read_schema(path)
            except Exception as exc:
                raise PipelineError(
                    f"{original_name} is not a readable parquet file: {exc}",
                    code="invalid_upload",
                ) from exc

        # Pin the exact reference versions used by this rule version now.
        pinned: dict[str, Any] = {}
        latest = self.latest_references()
        for field in spec["fields"]:
            reference = field.get("references")
            if not reference:
                continue
            meta = latest.get(reference["table"])
            if meta is None:
                raise PipelineError(
                    f"reference table {reference['table']!r} has no published version",
                    code="missing_reference",
                )
            pinned[reference["table"]] = {
                "version": meta["version"],
                "file_path": meta["file_path"],
                "key_field": meta["key_field"],
                "row_count": meta["row_count"],
            }

        batch_id = uuid.uuid4().hex
        batch_dir = self.settings.uploads_dir / batch_id
        batch_dir.mkdir(parents=True, exist_ok=False)
        stored: list[dict[str, Any]] = []
        try:
            for index, (original_name, source_path) in enumerate(files):
                safe = (
                    re.sub(r"[^A-Za-z0-9._-]+", "_", Path(original_name).name)
                    or "upload.parquet"
                )
                target = batch_dir / f"{index:03d}_{safe}"
                shutil.move(str(source_path), str(target))
                stored.append(
                    {
                        "original_name": original_name,
                        "stored_name": target.name,
                        "size": target.stat().st_size,
                    }
                )
            with self.db.transaction() as conn:
                seq = int(conn.execute("SELECT nextval('batch_acceptance_seq')").fetchone()[0])
                conn.execute(
                    """
                    INSERT INTO batches
                        (id, dataset, acceptance_seq, rules_version, rules_spec,
                         reference_versions, status, attempt, source_files, accepted_at)
                    VALUES (?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, 0,
                            CAST(? AS JSON), ?)
                    """,
                    [
                        batch_id,
                        dataset,
                        seq,
                        current_version,
                        _dumps(spec),
                        _dumps(pinned),
                        BATCH_ACCEPTED,
                        _dumps(stored),
                        _utcnow(),
                    ],
                )
        except BaseException:
            shutil.rmtree(batch_dir, ignore_errors=True)
            raise
        return self.get_batch(batch_id)  # type: ignore[return-value]

    def replace_batch_files(
        self, batch_id: str, files: list[tuple[str, Path]]
    ) -> dict[str, Any]:
        """Replace source files of a failed batch (fix-and-retry workflow).

        Rules and reference versions stay pinned exactly as accepted.
        """

        batch = self.get_batch(batch_id)
        if batch is None:
            raise PipelineError(f"unknown batch {batch_id!r}", code="unknown_batch")
        if batch["status"] == BATCH_RUNNING:
            raise PipelineError(
                "cannot replace source files while the batch is running", code="invalid_state"
            )
        if batch["status"] == BATCH_PUBLISHED:
            raise PipelineError(
                "source files of a published batch cannot be replaced; re-run the batch "
                "against its original sources, or create a new batch for corrected data",
                code="invalid_state",
            )
        if not files:
            raise PipelineError("at least one parquet file is required", code="invalid_upload")
        for original_name, path in files:
            try:
                pq.read_schema(path)
            except Exception as exc:
                raise PipelineError(
                    f"{original_name} is not a readable parquet file: {exc}",
                    code="invalid_upload",
                ) from exc

        batch_dir = self.settings.uploads_dir / batch_id
        replacement_dir = self.settings.uploads_dir / f"{batch_id}.replacement"
        if replacement_dir.exists():
            shutil.rmtree(replacement_dir)
        replacement_dir.mkdir(parents=True)
        stored: list[dict[str, Any]] = []
        try:
            for index, (original_name, source_path) in enumerate(files):
                safe = (
                    re.sub(r"[^A-Za-z0-9._-]+", "_", Path(original_name).name)
                    or "upload.parquet"
                )
                target = replacement_dir / f"{index:03d}_{safe}"
                shutil.move(str(source_path), str(target))
                stored.append(
                    {
                        "original_name": original_name,
                        "stored_name": target.name,
                        "size": target.stat().st_size,
                    }
                )
            backup = self.settings.uploads_dir / f"{batch_id}.replaced"
            if backup.exists():
                shutil.rmtree(backup)
            if batch_dir.exists():
                batch_dir.rename(backup)
            try:
                replacement_dir.rename(batch_dir)
                with self.db.transaction() as conn:
                    conn.execute(
                        "UPDATE batches SET source_files = CAST(? AS JSON),"
                        " error_message = NULL, errors = NULL WHERE id = ?",
                        [_dumps(stored), batch_id],
                    )
            except BaseException:
                # Restore the original files so a failed replacement cannot
                # leave the batch pointing at half-swapped sources.
                if batch_dir.exists():
                    shutil.rmtree(batch_dir)
                if backup.exists():
                    backup.rename(batch_dir)
                raise
            shutil.rmtree(backup, ignore_errors=True)
        except BaseException:
            shutil.rmtree(replacement_dir, ignore_errors=True)
            raise
        return self.get_batch(batch_id)  # type: ignore[return-value]

    # ==================================================================
    # Reads
    # ==================================================================
    def _public_batch(self, row: dict[str, Any]) -> dict[str, Any]:
        for key, default in (
            ("rules_spec", {}),
            ("reference_versions", {}),
            ("source_files", []),
            ("errors", None),
        ):
            row[key] = _loads(row.get(key), default)
        for key in ("accepted_at", "processing_at", "finished_at", "last_success_at"):
            if row.get(key) is not None:
                row[key] = str(row[key])
        # A failed re-run of a previously published batch keeps the old
        # trusted files in place; make that explicit for operators.
        row["previous_result_preserved"] = (
            row.get("status") == BATCH_FAILED and int(row.get("trusted_files") or 0) > 0
        )
        return row

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        rows = self.db.query_dicts(
            "SELECT b.*, (SELECT COUNT(*) FROM partition_files p WHERE p.batch_id = b.id)"
            " AS trusted_files FROM batches b WHERE b.id = ?",
            [batch_id],
        )
        return self._public_batch(rows[0]) if rows else None

    def list_batches(
        self, dataset: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        if dataset:
            rows = self.db.query_dicts(
                "SELECT b.*, (SELECT COUNT(*) FROM partition_files p WHERE p.batch_id = b.id)"
                " AS trusted_files FROM batches b WHERE b.dataset = ?"
                " ORDER BY b.acceptance_seq DESC LIMIT ?",
                [dataset, limit],
            )
        else:
            rows = self.db.query_dicts(
                "SELECT b.*, (SELECT COUNT(*) FROM partition_files p WHERE p.batch_id = b.id)"
                " AS trusted_files FROM batches b ORDER BY b.acceptance_seq DESC LIMIT ?",
                [limit],
            )
        return [self._public_batch(row) for row in rows]

    def list_quarantine(
        self, batch_id: str, attempt: int | None = None
    ) -> list[dict[str, Any]]:
        if attempt is None:
            batch = self.get_batch(batch_id)
            if batch is None:
                return []
            attempt = int(batch["attempt"])
        rows = self.db.query_dicts(
            "SELECT row_index, source_file, raw_row, reasons, created_at FROM quarantine"
            " WHERE batch_id = ? AND attempt = ? ORDER BY row_index",
            [batch_id, attempt],
        )
        for row in rows:
            row["raw_row"] = _loads(row["raw_row"], {})
            row["reasons"] = _loads(row["reasons"], [])
            row["source_file"] = (
                Path(row["source_file"]).name if row.get("source_file") else None
            )
            row["created_at"] = str(row["created_at"])
        return rows

    def partition_summary(self, dataset: str) -> list[dict[str, Any]]:
        rows = self.db.query_dicts(
            """
            SELECT date_key::VARCHAR AS date_key, COUNT(DISTINCT batch_id) AS batches,
                   COUNT(*) AS files, SUM(row_count) AS row_count
            FROM partition_files WHERE dataset = ?
            GROUP BY date_key ORDER BY date_key DESC
            """,
            [dataset],
        )
        return [dict(row) for row in rows]

    def trusted_preview(
        self, dataset: str, date_key: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        if date_key:
            paths = [
                row[0]
                for row in self.db.fetchall(
                    "SELECT file_path FROM partition_files WHERE dataset = ? AND date_key = ?"
                    " ORDER BY acceptance_seq, batch_id",
                    [dataset, dt.date.fromisoformat(date_key)],
                )
            ]
        else:
            paths = [
                row[0]
                for row in self.db.fetchall(
                    "SELECT file_path FROM partition_files WHERE dataset = ?"
                    " ORDER BY acceptance_seq, batch_id",
                    [dataset],
                )
            ]
        paths = [path for path in paths if Path(path).exists()]
        if not paths:
            return {"columns": [], "rows": [], "files": 0}
        limit = max(1, min(int(limit), 1000))
        with self.db.lock:
            cur = self.db.conn.execute(
                "SELECT * FROM read_parquet(?, union_by_name=true, hive_partitioning=false) LIMIT ?",
                [paths, limit],
            )
            columns = [description[0] for description in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        for row in rows:
            for key, value in row.items():
                if isinstance(value, (dt.date, dt.datetime)):
                    row[key] = value.isoformat()
        return {"columns": columns, "rows": rows, "files": len(paths)}

    # ==================================================================
    # Processing
    # ==================================================================
    def run_batch(self, batch_id: str, *, replace_published: bool = False) -> RunResult:
        with self._run_lock:
            batch = self.get_batch(batch_id)
            if batch is None:
                raise PipelineError(f"unknown batch {batch_id!r}", code="unknown_batch")
            if batch["status"] == BATCH_PUBLISHED and not replace_published:
                raise PipelineError(
                    "batch is already published; use replace_published to re-run it "
                    "(only this batch's result is replaced) or create a new batch",
                    code="already_published",
                )

            with self.db.transaction() as conn:
                row = conn.execute(
                    "SELECT status, attempt FROM batches WHERE id = ?", [batch_id]
                ).fetchone()
                status, previous_attempt = row[0], int(row[1])
                if status == BATCH_RUNNING:
                    raise PipelineError(
                        "batch is already being processed", code="batch_busy"
                    )
                if status == BATCH_PUBLISHED and not replace_published:
                    raise PipelineError(
                        "batch is already published; use replace_published to re-run it "
                        "(only this batch's result is replaced) or create a new batch",
                        code="already_published",
                    )
                attempt = previous_attempt + 1
                conn.execute(
                    "UPDATE batches SET status = ?, attempt = ?, processing_at = ?,"
                    " error_message = NULL, errors = NULL WHERE id = ?",
                    [BATCH_RUNNING, attempt, _utcnow(), batch_id],
                )

            try:
                return self._process(batch_id, attempt)
            except BaseException as exc:
                code = getattr(exc, "code", "processing_error")
                details = getattr(exc, "details", None)
                message = str(exc) or exc.__class__.__name__
                try:
                    self.db.execute(
                        "UPDATE batches SET status = ?, error_message = ?,"
                        " errors = CAST(? AS JSON), finished_at = ? WHERE id = ?",
                        [
                            BATCH_FAILED,
                            message,
                            _dumps({"code": code, "message": message, "details": details}),
                            _utcnow(),
                            batch_id,
                        ],
                    )
                except Exception:  # pragma: no cover - DB failure while recording
                    log.exception("could not record failure of batch %s", batch_id)
                log.exception("batch %s failed", batch_id)
                if isinstance(exc, PipelineError):
                    raise
                raise PipelineError(message, code=code, details=details) from exc

    def _process(self, batch_id: str, attempt: int) -> RunResult:
        batch = self.get_batch(batch_id)
        assert batch is not None
        dataset = batch["dataset"]
        spec = normalize_spec(batch["rules_spec"], None)
        pinned_refs: Mapping[str, Any] = batch["reference_versions"]
        source_files: list[dict[str, Any]] = batch["source_files"]
        paths = [
            str(self.settings.uploads_dir / batch_id / entry["stored_name"])
            for entry in source_files
        ]
        missing = [path for path in paths if not Path(path).exists()]
        if missing:
            raise PipelineError(
                f"source files are missing: {[Path(p).name for p in missing]}",
                code="source_missing",
            )

        con = self.db.conn
        with self.db.lock:
            self._drop_temp_tables()
            try:
                row_count = self._load_input(spec, paths, int(batch["rules_version"]))
                if row_count == 0:
                    raise PipelineError("batch contains no rows", code="empty_batch")
                self._add_field_issues(spec)
                self._add_duplicate_issues(spec)
                self._add_reference_issues(spec, pinned_refs)

                valid_rows = self._valid_row_count()
                if valid_rows == 0:
                    self._write_quarantine(batch_id, attempt)
                    self._record_counts(batch_id, row_count, 0, row_count)
                    raise PipelineError(
                        "no valid rows: every row was quarantined",
                        code="all_rows_invalid",
                        details={"quarantine_count": row_count},
                    )

                self._add_cross_batch_key_issues(spec, batch)
                valid_rows = self._valid_row_count()
                partitions = self._valid_partitions(spec)
                if valid_rows == 0 or not partitions:
                    self._write_quarantine(batch_id, attempt)
                    self._record_counts(batch_id, row_count, 0, row_count)
                    raise PipelineError(
                        "no valid rows remain after cross-batch key checks",
                        code="all_rows_invalid",
                        details={"quarantine_count": row_count},
                    )

                self._assert_not_stale(dataset, int(batch["acceptance_seq"]), partitions)

                quarantine_count = row_count - valid_rows
                staging = (
                    self.settings.staging_dir / batch_id / f"attempt-{attempt}"
                )
                if staging.exists():
                    shutil.rmtree(staging)
                staging.mkdir(parents=True)
                written: dict[str, tuple[Path, int]] = {}
                try:
                    for index, date_key in enumerate(partitions):
                        target = staging / f"part-{index:06d}.parquet"
                        count = self._write_partition(spec, date_key, target)
                        if count:
                            written[date_key] = (target, count)
                    self._write_quarantine(batch_id, attempt)
                    self._record_counts(
                        batch_id, row_count, valid_rows, quarantine_count
                    )
                    self._commit(spec, batch, attempt, written)
                finally:
                    shutil.rmtree(staging, ignore_errors=True)

                return RunResult(
                    batch_id=batch_id,
                    attempt=attempt,
                    status=BATCH_PUBLISHED,
                    row_count=row_count,
                    valid_count=valid_rows,
                    quarantine_count=quarantine_count,
                    partitions=sorted(written),
                )
            finally:
                self._drop_temp_tables()

    # ------------------------------------------------------------------
    # Input loading
    # ------------------------------------------------------------------
    def _drop_temp_tables(self) -> None:
        for name in ("gsb_input", "gsb_typed", "gsb_raw", "gsb_issues"):
            self.db.conn.execute(f"DROP TABLE IF EXISTS {name}")

    def _load_input(
        self, spec: Mapping[str, Any], paths: list[str], rules_version: int
    ) -> int:
        declared = field_names(spec)
        declared_set = set(declared)
        allow_extra = bool(spec.get("allow_extra_columns", False))
        required = {field["name"] for field in spec["fields"] if field["required"]}

        # Validate schemas before writing any row.
        all_columns: list[str] = []
        for path in paths:
            schema = pq.read_schema(path)
            columns = schema.names
            missing_required = sorted(required - set(columns))
            if missing_required:
                raise PipelineError(
                    "input is missing required columns: " + ", ".join(missing_required),
                    code="schema_mismatch",
                    details={"missing_columns": missing_required},
                )
            extras = sorted(column for column in columns if column not in declared_set)
            if extras and not allow_extra:
                raise PipelineError(
                    f"input contains columns not declared by rules version {rules_version}: "
                    + ", ".join(extras),
                    code="schema_mismatch",
                    details={"unexpected_columns": extras},
                )
            for column in columns:
                if column not in all_columns:
                    all_columns.append(column)

        typed_sql = ", ".join(
            f"{quote_ident(field['name'])} {_duck_type(field['type'])}"
            for field in spec["fields"]
        )
        con = self.db.conn
        with self.db.lock:
            con.execute(
                "CREATE TEMP TABLE gsb_typed (_gsb_row BIGINT, _gsb_file VARCHAR, "
                + typed_sql
                + ")"
            )
            con.execute(
                "CREATE TEMP TABLE gsb_raw (_gsb_row BIGINT, _gsb_file VARCHAR, raw_row VARCHAR)"
            )
            con.execute(
                "CREATE TEMP TABLE gsb_issues "
                "(row_index BIGINT, code VARCHAR, field VARCHAR, message VARCHAR)"
            )

        row_index = 0
        for path in paths:
            parquet = pq.ParquetFile(path)
            all_source_columns = list(parquet.schema_arrow.names)
            for record_batch in parquet.iter_batches(batch_size=65536):
                table = pa.Table.from_batches([record_batch])
                arrays_by_name = {
                    name: table.column(name).to_pylist() for name in all_source_columns
                }
                count = table.num_rows
                typed_data: dict[str, list[Any]] = {
                    field["name"]: [] for field in spec["fields"]
                }
                issue_rows: list[tuple[int, str, str | None, str]] = []
                raw_json: list[str] = []
                for offset in range(count):
                    row = {
                        name: arrays_by_name[name][offset]
                        for name in all_source_columns
                    }
                    typed, problems = normalize_row(spec, row)
                    for field in spec["fields"]:
                        typed_data[field["name"]].append(typed[field["name"]])
                    for problem in problems:
                        issue_rows.append(
                            (
                                row_index + offset,
                                problem["code"],
                                problem.get("field"),
                                problem.get("message") or problem["code"],
                            )
                        )
                    raw_json.append(_dumps(row))
                self._insert_batch(
                    spec, typed_data, raw_json, path, row_index, count
                )
                if issue_rows:
                    self._insert_issues(issue_rows)
                row_index += count
        return row_index

    def _insert_batch(
        self,
        spec: Mapping[str, Any],
        typed_data: Mapping[str, list[Any]],
        raw_json: list[str],
        source_file: str,
        start_index: int,
        count: int,
    ) -> None:
        typed_schema = pa.schema(
            [pa.field("_gsb_row", pa.int64()), pa.field("_gsb_file", pa.string())]
            + [
                pa.field(field["name"], ARROW_TYPES[field["type"]])
                for field in spec["fields"]
            ]
        )
        arrays = [
            pa.array(range(start_index, start_index + count), type=pa.int64()),
            pa.array([source_file] * count, type=pa.string()),
        ]
        for field in spec["fields"]:
            arrays.append(
                pa.array(typed_data[field["name"]], type=ARROW_TYPES[field["type"]])
            )
        typed_table = pa.Table.from_arrays(arrays, schema=typed_schema)
        raw_table = pa.table(
            {
                "_gsb_row": pa.array(range(start_index, start_index + count), pa.int64()),
                "_gsb_file": pa.array([source_file] * count, pa.string()),
                "raw_row": pa.array(raw_json, pa.string()),
            }
        )
        con = self.db.conn
        with self.db.lock:
            con.register("_gsb_typed_batch", typed_table)
            con.register("_gsb_raw_batch", raw_table)
            try:
                con.execute("INSERT INTO gsb_typed SELECT * FROM _gsb_typed_batch")
                con.execute("INSERT INTO gsb_raw SELECT * FROM _gsb_raw_batch")
            finally:
                con.unregister("_gsb_typed_batch")
                con.unregister("_gsb_raw_batch")

    def _insert_issues(self, rows: list[tuple[int, str, str | None, str]]) -> None:
        issues = pa.table(
            {
                "row_index": pa.array([row[0] for row in rows], pa.int64()),
                "code": pa.array([row[1] for row in rows], pa.string()),
                "field": pa.array([row[2] for row in rows], pa.string()),
                "message": pa.array([row[3] for row in rows], pa.string()),
            }
        )
        con = self.db.conn
        with self.db.lock:
            con.register("_gsb_issues_batch", issues)
            try:
                con.execute("INSERT INTO gsb_issues SELECT * FROM _gsb_issues_batch")
            finally:
                con.unregister("_gsb_issues_batch")

    # ------------------------------------------------------------------
    # Checks
    # ------------------------------------------------------------------
    def _add_field_issues(self, spec: Mapping[str, Any]) -> None:
        checks: list[tuple[str, str, list[Any]]] = []
        for field in spec["fields"]:
            name = field["name"]
            ident = quote_ident(name)
            # Only check rows not already failing a structural check; this
            # avoids noisy duplicate reasons for the same bad value.
            # Field names are validated identifiers, so embedding is safe and
            # keeps the parameter order intuitive.
            base = (
                "FROM gsb_typed t WHERE NOT EXISTS ("
                "SELECT 1 FROM gsb_issues i WHERE i.row_index = t._gsb_row"
                f" AND i.field = '{name}') "
            )
            if field["required"]:
                checks.append(
                    (
                        "required_missing",
                        f"SELECT t._gsb_row, ?, ?, ? {base} AND t.{ident} IS NULL",
                        ["required_missing", name, f"{name} is required"],
                    )
                )
            if field["type"] == "string" and field.get("max_length"):
                limit = int(field["max_length"])
                checks.append(
                    (
                        "too_long",
                        f"SELECT t._gsb_row, ?, ?, ? {base} AND t.{ident} IS NOT NULL"
                        f" AND LENGTH(t.{ident}) > {limit}",
                        ["too_long", name, f"{name} exceeds max_length {limit}"],
                    )
                )
            if field["type"] == "string" and field.get("regex"):
                # DuckDB anchors the pattern at both ends, so the rule author can
                # write `^[A-Z]{3}$` or `[A-Z]{3}` with identical meaning.
                checks.append(
                    (
                        "pattern_mismatch",
                        f"SELECT t._gsb_row, ?, ?, ? {base} AND t.{ident} IS NOT NULL"
                        f" AND NOT regexp_matches(t.{ident}, ?)",
                        ["pattern_mismatch", name, f"{name} does not match required pattern", field["regex"]],
                    )
                )
            if field["type"] in {"integer", "number"}:
                if field.get("min") is not None:
                    checks.append(
                        (
                            "below_min",
                            f"SELECT t._gsb_row, ?, ?, ? {base} AND t.{ident} IS NOT NULL"
                            f" AND t.{ident} < ?",
                            ["below_min", name, f"{name} is below minimum {field['min']}", float(field["min"])],
                        )
                    )
                if field.get("max") is not None:
                    checks.append(
                        (
                            "above_max",
                            f"SELECT t._gsb_row, ?, ?, ? {base} AND t.{ident} IS NOT NULL"
                            f" AND t.{ident} > ?",
                            ["above_max", name, f"{name} is above maximum {field['max']}", float(field["max"])],
                        )
                    )
            allowed = field.get("allowed_values")
            if allowed:
                placeholders = ", ".join(["?"] * len(allowed))
                checks.append(
                    (
                        "not_allowed",
                        f"SELECT t._gsb_row, ?, ?, ? {base} AND t.{ident} IS NOT NULL"
                        f" AND CAST(t.{ident} AS VARCHAR) NOT IN ({placeholders})",
                        ["not_allowed", name, f"{name} value is not in allowed_values", *allowed],
                    )
                )
        with self.db.lock:
            for _code, sql, params in checks:
                self.db.conn.execute(
                    "INSERT INTO gsb_issues SELECT * FROM (" + sql + ")", params
                )

    def _add_reference_issues(
        self, spec: Mapping[str, Any], pinned_refs: Mapping[str, Any]
    ) -> None:
        for field in spec["fields"]:
            reference = field.get("references")
            if not reference:
                continue
            name = field["name"]
            table = reference["table"]
            meta = pinned_refs.get(table)
            if not meta:
                raise PipelineError(
                    f"rules reference {table!r} but no version was pinned",
                    code="missing_reference",
                )
            path = Path(meta["file_path"])
            if not path.exists():
                raise PipelineError(
                    f"pinned reference file for {table} v{meta['version']} is missing",
                    code="missing_reference",
                )
            key_ident = quote_ident(meta["key_field"])
            self.db.execute(
                f"""
                INSERT INTO gsb_issues
                SELECT t._gsb_row, 'reference_missing', ?, ?
                FROM gsb_typed t
                WHERE t._gsb_row NOT IN (SELECT row_index FROM gsb_issues)
                  AND t.{quote_ident(name)} IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM read_parquet(?) r
                      WHERE CAST(r.{key_ident} AS VARCHAR) = CAST(t.{quote_ident(name)} AS VARCHAR)
                  )
                """,
                [name, f"{name} is not present in reference {table} v{meta['version']}", str(path)],
            )

    def _add_duplicate_issues(self, spec: Mapping[str, Any]) -> None:
        pk = spec["primary_key"]
        partition_ident = quote_ident(spec["partition_field"])
        scope = (
            [f"t.{partition_ident}", *(f"t.{quote_ident(name)}" for name in pk)]
            if spec["unique_scope"] == "partition"
            else [f"t.{quote_ident(name)}" for name in pk]
        )
        scope_sql = ", ".join(scope)
        key_fields = ", ".join(pk)
        self.db.execute(
            f"""
            INSERT INTO gsb_issues
            SELECT _gsb_row, 'duplicate_key', ?, ?
            FROM (
                SELECT t._gsb_row,
                       COUNT(*) OVER (PARTITION BY {scope_sql}) AS _key_count
                FROM gsb_typed t
                WHERE t._gsb_row NOT IN (SELECT row_index FROM gsb_issues)
                  AND {' AND '.join(f't.{quote_ident(n)} IS NOT NULL' for n in pk)}
            )
            WHERE _key_count > 1
            """,
            [pk[0], f"duplicate primary key ({key_fields}) within this batch"],
        )

    def _add_cross_batch_key_issues(
        self, spec: Mapping[str, Any], batch: Mapping[str, Any]
    ) -> None:
        pk = spec["primary_key"]
        dataset = batch["dataset"]
        existing = self._trusted_paths(dataset, exclude_batch=batch["id"])
        if not existing:
            return
        join = " AND ".join(
            f"x.{quote_ident(name)} = t.{quote_ident(name)}" for name in pk
        )
        partition_ident = quote_ident(spec["partition_field"])
        partition_join = (
            f" AND CAST(x.{partition_ident} AS VARCHAR) = CAST(t.{partition_ident} AS VARCHAR)"
            if spec["unique_scope"] == "partition"
            else ""
        )
        self.db.execute(
            f"""
            INSERT INTO gsb_issues
            SELECT t._gsb_row, 'key_conflict_trusted', ?, ?
            FROM gsb_typed t
            WHERE t._gsb_row NOT IN (SELECT row_index FROM gsb_issues)
              AND EXISTS (
                  SELECT 1 FROM read_parquet(?, union_by_name=true, hive_partitioning=false) x
                  WHERE {join}{partition_join}
              )
            """,
            [
                pk[0],
                "primary key already exists in trusted data from another batch",
                existing,
            ],
        )

    def _valid_row_count(self) -> int:
        return int(
            self.db.fetchone(
                "SELECT COUNT(*) FROM gsb_typed"
                " WHERE _gsb_row NOT IN (SELECT row_index FROM gsb_issues)"
            )[0]
        )

    def _valid_partitions(self, spec: Mapping[str, Any]) -> list[str]:
        partition_ident = quote_ident(spec["partition_field"])
        rows = self.db.fetchall(
            f"SELECT DISTINCT CAST({partition_ident} AS VARCHAR) FROM gsb_typed"
            " WHERE _gsb_row NOT IN (SELECT row_index FROM gsb_issues)"
            f" AND {partition_ident} IS NOT NULL ORDER BY 1"
        )
        return [row[0] for row in rows]

    def _trusted_paths(
        self, dataset: str, *, exclude_batch: str | None = None
    ) -> list[str]:
        sql = "SELECT file_path FROM partition_files WHERE dataset = ?"
        params: list[Any] = [dataset]
        if exclude_batch:
            sql += " AND batch_id <> ?"
            params.append(exclude_batch)
        return [row[0] for row in self.db.fetchall(sql, params) if Path(row[0]).exists()]

    def _assert_not_stale(
        self, dataset: str, acceptance_seq: int, partitions: Iterable[str]
    ) -> None:
        for date_key in partitions:
            newest = self.db.fetchone(
                "SELECT MAX(acceptance_seq) FROM partition_files"
                " WHERE dataset = ? AND date_key = ?",
                [dataset, dt.date.fromisoformat(date_key)],
            )
            if newest and newest[0] is not None and int(newest[0]) > acceptance_seq:
                raise StaleBatchError(
                    f"stale batch: partition {date_key} was already published by a "
                    f"newer batch (acceptance sequence {int(newest[0])} > "
                    f"{acceptance_seq}); newer data was not overwritten"
                )

    # ------------------------------------------------------------------
    # Quarantine and partitions
    # ------------------------------------------------------------------
    def _write_quarantine(self, batch_id: str, attempt: int) -> None:
        now = _utcnow()
        with self.db.lock:
            self.db.conn.execute(
                "DELETE FROM quarantine WHERE batch_id = ? AND attempt = ?",
                [batch_id, attempt],
            )
            self.db.conn.execute(
                """
                INSERT INTO quarantine
                    (batch_id, attempt, row_index, source_file, raw_row, reasons, created_at)
                SELECT ?, ?, r._gsb_row, r._gsb_file, CAST(r.raw_row AS JSON),
                       to_json(list({'code': i.code, 'field': i.field, 'message': i.message}
                                     ORDER BY i.code, i.field, i.message)),
                       ?
                FROM gsb_raw r
                JOIN (
                    SELECT DISTINCT row_index, code, field, message FROM gsb_issues
                ) i ON i.row_index = r._gsb_row
                GROUP BY r._gsb_row, r._gsb_file, r.raw_row
                """,
                [batch_id, attempt, now],
            )

    def _record_counts(
        self, batch_id: str, row_count: int, valid: int, quarantined: int
    ) -> None:
        self.db.execute(
            "UPDATE batches SET row_count = ?, valid_count = ?, quarantine_count = ?"
            " WHERE id = ?",
            [row_count, valid, quarantined, batch_id],
        )

    def _write_partition(
        self, spec: Mapping[str, Any], date_key: str, target: Path
    ) -> int:
        target.parent.mkdir(parents=True, exist_ok=True)
        partition_ident = quote_ident(spec["partition_field"])
        columns = ", ".join(quote_ident(field["name"]) for field in spec["fields"])
        with self.db.lock:
            self.db.conn.execute(
                f"""
                COPY (
                    SELECT {columns} FROM gsb_typed
                    WHERE {partition_ident} = ?
                      AND _gsb_row NOT IN (SELECT row_index FROM gsb_issues)
                    ORDER BY _gsb_row
                ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                [dt.date.fromisoformat(date_key), str(target)],
            )
            if not target.exists():
                return 0
            count = int(pq.read_metadata(target).num_rows)
            if count == 0:
                target.unlink()
            return count

    def _commit(
        self,
        spec: Mapping[str, Any],
        batch: Mapping[str, Any],
        attempt: int,
        written: Mapping[str, tuple[Path, int]],
    ) -> None:
        """Publish staged partition files, then swap this batch's metadata.

        Ordering is chosen so readers always see a consistent dataset:

        1. Move new files into the trusted tree under unique names.  Until the
           metadata transaction commits they are unreferenced and invisible.
        2. In one transaction, remove this batch's old ``partition_files`` rows,
           insert the new rows and mark the batch published.  This is the
           commit point.  If it fails, the new files are removed and metadata
           still points at the untouched old files.
        3. Only after the commit are this batch's previous files deleted.  A
           crash between 2 and 3 leaves harmless orphan files, never missing
           trusted data.
        """

        dataset = batch["dataset"]
        batch_id = batch["id"]
        seq = int(batch["acceptance_seq"])
        trusted_root = self.settings.trusted_dir / dataset
        old_paths = [
            row[0]
            for row in self.db.fetchall(
                "SELECT file_path FROM partition_files WHERE dataset = ? AND batch_id = ?",
                [dataset, batch_id],
            )
        ]

        with self.db.lock:
            # Repeat the staleness check immediately before the commit point.
            self._assert_not_stale(dataset, seq, list(written.keys()))
            placed: list[Path] = []
            new_rows: list[tuple[Any, ...]] = []
            for index, (date_key, (staged, count)) in enumerate(written.items()):
                target_dir = trusted_root / f"date={date_key}"
                target_dir.mkdir(parents=True, exist_ok=True)
                target = target_dir / f"batch-{batch_id}-a{attempt}-{index:03d}.parquet"
                shutil.move(str(staged), str(target))
                placed.append(target)
                new_rows.append(
                    (
                        dataset,
                        dt.date.fromisoformat(date_key),
                        batch_id,
                        seq,
                        attempt,
                        str(target),
                        count,
                        _utcnow(),
                    )
                )

            try:
                self.db.conn.execute("BEGIN TRANSACTION")
                try:
                    self.db.conn.execute(
                        "DELETE FROM partition_files WHERE dataset = ? AND batch_id = ?",
                        [dataset, batch_id],
                    )
                    for row in new_rows:
                        self.db.conn.execute(
                            "INSERT INTO partition_files"
                            " (dataset, date_key, batch_id, acceptance_seq, attempt,"
                            "  file_path, row_count, committed_at)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            list(row),
                        )
                    self.db.conn.execute(
                        "UPDATE batches SET status = ?, finished_at = ?, last_success_at = ?,"
                        " error_message = NULL, errors = NULL WHERE id = ?",
                        [BATCH_PUBLISHED, _utcnow(), _utcnow(), batch_id],
                    )
                    self.db.conn.execute("COMMIT")
                except BaseException:
                    try:
                        self.db.conn.execute("ROLLBACK")
                    except Exception:  # pragma: no cover
                        pass
                    raise
            except BaseException:
                # The transaction did not commit: remove unreferenced new files.
                for target in placed:
                    try:
                        target.unlink()
                    except FileNotFoundError:
                        pass
                for date_key in written:
                    directory = trusted_root / f"date={date_key}"
                    try:
                        if directory.exists() and not any(directory.iterdir()):
                            directory.rmdir()
                    except OSError:  # pragma: no cover
                        pass
                raise

            # Commit succeeded.  Old files are no longer referenced; removing
            # them is best effort and must never fail the publication.
            for old_path in old_paths:
                if old_path in {row[5] for row in new_rows}:
                    continue
                try:
                    Path(old_path).unlink(missing_ok=True)
                except OSError:  # pragma: no cover
                    log.warning("could not remove superseded file %s", old_path)
            for date_key in written:
                directory = trusted_root / f"date={date_key}"
                try:
                    if directory.exists() and not any(directory.iterdir()):
                        directory.rmdir()
                except OSError:  # pragma: no cover
                    pass


def _duck_type(kind: str) -> str:
    return {
        "string": "VARCHAR",
        "integer": "BIGINT",
        "number": "DOUBLE",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "boolean": "BOOLEAN",
    }[kind]
