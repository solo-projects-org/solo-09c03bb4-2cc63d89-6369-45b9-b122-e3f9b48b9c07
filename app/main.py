from __future__ import annotations

import json
import logging
import shutil
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import yaml
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from .db import Database
from .pipeline import Pipeline, PipelineError
from .settings import Settings
from .spec import SpecError

log = logging.getLogger("gsb.api")

MAX_SPEC_BYTES = 200_000

STATIC_DIR = Path(__file__).parent / "static"


def _error_status(code: str) -> int:
    if code in {"unknown_dataset", "unknown_batch", "missing_reference"}:
        return 404
    if code in {
        "version_conflict",
        "stale_batch",
        "already_published",
        "batch_busy",
        "invalid_state",
        "all_rows_invalid",
    }:
        return 409
    if code in {
        "invalid_spec",
        "invalid_upload",
        "invalid_reference",
        "schema_mismatch",
        "dataset_exists",
        "empty_batch",
        "source_missing",
    }:
        return 400
    return 500


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        settings.ensure_dirs()
        db = Database(settings.meta_db_path)
        pipeline = Pipeline(db, settings)
        app.state.db = db
        app.state.pipeline = pipeline
        app.state.settings = settings
        # A process restart during validation leaves a batch in "running".
        # No trusted file was committed (commit happens after validation), so
        # mark it failed and make it retryable instead of leaving it stuck.
        db.execute(
            "UPDATE batches SET status = 'failed', finished_at = ?,"
            " error_message = COALESCE(error_message, 'processing was interrupted by a server restart;"
            " re-run the batch'), errors = COALESCE(errors, CAST(? AS JSON))"
            " WHERE status = 'running'",
            [
                __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                json.dumps(
                    {
                        "code": "interrupted",
                        "message": "processing was interrupted by a server restart; re-run the batch",
                    }
                ),
            ],
        )
        _bootstrap_rules(pipeline, settings.rules_file)
        try:
            yield
        finally:
            db.close()

    app = FastAPI(title="Trusted Data Gateway", version="1.0.0", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.exception_handler(PipelineError)
    async def pipeline_error_handler(_request: Request, exc: PipelineError):
        return JSONResponse(
            status_code=_error_status(exc.code),
            content={
                "error": {
                    "code": exc.code,
                    "message": str(exc),
                    "details": exc.details,
                }
            },
        )

    @app.exception_handler(SpecError)
    async def spec_error_handler(_request: Request, exc: SpecError):
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "invalid_spec", "message": str(exc), "details": None}},
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: Request, exc: Exception):
        log.exception("unhandled error")
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "unexpected server error",
                    "details": None,
                }
            },
        )

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------
    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    # ------------------------------------------------------------------
    # Datasets
    # ------------------------------------------------------------------
    @app.get("/api/summary")
    async def summary(request: Request):
        db: Database = request.app.state.db
        counts = db.fetchone(
            """
            SELECT
              (SELECT COUNT(*) FROM datasets),
              (SELECT COUNT(*) FROM batches),
              (SELECT COUNT(*) FROM batches WHERE status = 'published'),
              (SELECT COUNT(*) FROM batches WHERE status = 'failed'),
              (SELECT COUNT(*) FROM quarantine),
              (SELECT COUNT(*) FROM partition_files),
              (SELECT COUNT(*) FROM reference_versions)
            """
        )
        return {
            "datasets": counts[0],
            "batches": counts[1],
            "published_batches": counts[2],
            "failed_batches": counts[3],
            "quarantined_rows": counts[4],
            "trusted_files": counts[5],
            "reference_versions": counts[6],
        }

    @app.get("/api/datasets")
    async def list_datasets(request: Request):
        pipeline: Pipeline = request.app.state.pipeline
        return {"datasets": pipeline.list_datasets()}

    @app.post("/api/datasets", status_code=201)
    async def create_dataset(request: Request):
        pipeline: Pipeline = request.app.state.pipeline
        body = await _json_body(request)
        spec = body.get("spec")
        if isinstance(spec, str):
            try:
                spec = json.loads(spec)
            except json.JSONDecodeError:
                try:
                    spec = yaml.safe_load(spec)
                except yaml.YAMLError as exc:
                    raise PipelineError(f"rule spec is not valid JSON/YAML: {exc}", code="invalid_spec")
        if not isinstance(spec, dict):
            raise PipelineError("spec must be a JSON/YAML object", code="invalid_spec")
        result = pipeline.create_dataset(
            str(body.get("name") or "").strip(),
            (body.get("description") or "").strip() or None,
            spec,
        )
        return result

    @app.get("/api/datasets/{dataset}")
    async def get_dataset(dataset: str, request: Request):
        pipeline: Pipeline = request.app.state.pipeline
        item = pipeline.get_dataset(dataset)
        if item is None:
            raise PipelineError(f"unknown dataset {dataset!r}", code="unknown_dataset")
        rule = pipeline.get_rule_version(dataset, item["current_rules_version"])
        item["current_spec"] = rule["spec"] if rule else None
        item["partitions"] = pipeline.partition_summary(dataset)
        return item

    @app.get("/api/datasets/{dataset}/rules")
    async def list_rules(dataset: str, request: Request):
        pipeline: Pipeline = request.app.state.pipeline
        if pipeline.get_dataset(dataset) is None:
            raise PipelineError(f"unknown dataset {dataset!r}", code="unknown_dataset")
        return {"versions": pipeline.list_rule_versions(dataset)}

    @app.post("/api/datasets/{dataset}/rules", status_code=201)
    async def add_rule_version(dataset: str, request: Request):
        pipeline: Pipeline = request.app.state.pipeline
        body = await _json_body(request)
        spec = body.get("spec")
        if isinstance(spec, str):
            spec = _parse_spec_text(spec)
        if not isinstance(spec, dict):
            raise PipelineError("spec must be a JSON object", code="invalid_spec")
        expected = body.get("expected_version")
        if expected is not None:
            try:
                expected = int(expected)
            except (TypeError, ValueError):
                raise PipelineError("expected_version must be an integer", code="invalid_spec")
        return pipeline.add_rule_version(
            dataset, spec, body.get("note"), expected_version=expected
        )

    @app.get("/api/datasets/{dataset}/partitions")
    async def list_partitions(dataset: str, request: Request):
        pipeline: Pipeline = request.app.state.pipeline
        if pipeline.get_dataset(dataset) is None:
            raise PipelineError(f"unknown dataset {dataset!r}", code="unknown_dataset")
        return {"partitions": pipeline.partition_summary(dataset)}

    @app.get("/api/datasets/{dataset}/preview")
    async def preview(dataset: str, request: Request, date: str | None = None, limit: int = 50):
        pipeline: Pipeline = request.app.state.pipeline
        if pipeline.get_dataset(dataset) is None:
            raise PipelineError(f"unknown dataset {dataset!r}", code="unknown_dataset")
        if date:
            from datetime import date as date_type

            try:
                date_type.fromisoformat(date)
            except ValueError:
                raise PipelineError("date must be YYYY-MM-DD", code="invalid_spec")
        return pipeline.trusted_preview(dataset, date, limit)

    # ------------------------------------------------------------------
    # References
    # ------------------------------------------------------------------
    @app.get("/api/references")
    async def list_references(request: Request):
        pipeline: Pipeline = request.app.state.pipeline
        return {
            "latest": list(pipeline.latest_references().values()),
            "versions": pipeline.list_reference_versions(),
        }

    @app.post("/api/references", status_code=201)
    async def publish_reference(
        request: Request,
        name: str = Form(...),
        key_field: str = Form(...),
        note: str | None = Form(None),
        file: UploadFile = File(...),
    ):
        pipeline: Pipeline = request.app.state.pipeline
        settings: Settings = request.app.state.settings
        with _upload_workspace(settings) as workspace:
            stored = await _save_upload(file, workspace, settings, label="reference")
            try:
                return pipeline.publish_reference(
                    name.strip(), key_field.strip(), stored, file.filename or "reference.parquet", note
                )
            finally:
                stored.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Batches
    # ------------------------------------------------------------------
    @app.get("/api/batches")
    async def list_batches(request: Request, dataset: str | None = None, limit: int = 100):
        pipeline: Pipeline = request.app.state.pipeline
        return {"batches": pipeline.list_batches(dataset, limit=max(1, min(limit, 500)))}

    @app.post("/api/datasets/{dataset}/batches")
    async def upload_batch(
        request: Request,
        dataset: str,
        files: list[UploadFile] = File(...),
        expected_rules_version: int | None = Form(None),
        process: bool = Form(True),
    ):
        pipeline: Pipeline = request.app.state.pipeline
        settings: Settings = request.app.state.settings
        if not files:
            raise PipelineError("at least one parquet file is required", code="invalid_upload")
        if len(files) > settings.max_files:
            raise PipelineError(
                f"at most {settings.max_files} files can be uploaded at once",
                code="invalid_upload",
            )
        with _upload_workspace(settings) as workspace:
            stored: list[tuple[str, Path]] = []
            total = 0
            try:
                for index, upload in enumerate(files):
                    path = await _save_upload(
                        upload, workspace, settings, label=f"batch-{index}"
                    )
                    total += path.stat().st_size
                    if total > settings.max_upload_bytes:
                        raise PipelineError(
                            "upload exceeds the total size limit", code="invalid_upload"
                        )
                    stored.append((upload.filename or f"file-{index}.parquet", path))
                batch = pipeline.accept_batch(
                    dataset,
                    stored,
                    expected_rules_version=expected_rules_version,
                )
            except BaseException:
                for _name, path in stored:
                    path.unlink(missing_ok=True)
                raise
        if process:
            try:
                result = await run_in_threadpool(pipeline.run_batch, batch["id"])
                batch = pipeline.get_batch(batch["id"]) or batch
                batch["run"] = result.__dict__
            except PipelineError as exc:
                if exc.code in {"batch_busy"}:
                    raise
                # Acceptance succeeded; report the processing failure together
                # with the batch so the UI can show quarantine details.
                batch = pipeline.get_batch(batch["id"]) or batch
                batch["run_error"] = {"code": exc.code, "message": str(exc), "details": exc.details}
                return JSONResponse(status_code=202, content={"batch": batch})
        elif batch["status"] == "accepted":
            return JSONResponse(status_code=202, content={"batch": batch})
        return {"batch": batch}

    @app.get("/api/batches/{batch_id}")
    async def get_batch(batch_id: str, request: Request):
        pipeline: Pipeline = request.app.state.pipeline
        batch = pipeline.get_batch(batch_id)
        if batch is None:
            raise PipelineError(f"unknown batch {batch_id!r}", code="unknown_batch")
        return batch

    @app.get("/api/batches/{batch_id}/quarantine")
    async def get_quarantine(
        batch_id: str, request: Request, attempt: int | None = None
    ):
        pipeline: Pipeline = request.app.state.pipeline
        if pipeline.get_batch(batch_id) is None:
            raise PipelineError(f"unknown batch {batch_id!r}", code="unknown_batch")
        return {"rows": pipeline.list_quarantine(batch_id, attempt)}

    @app.post("/api/batches/{batch_id}/run")
    async def run_batch(request: Request, batch_id: str, replace_published: bool = False):
        pipeline: Pipeline = request.app.state.pipeline
        try:
            result = await run_in_threadpool(
            pipeline.run_batch, batch_id, replace_published=replace_published
        )
        except PipelineError as exc:
            # A batch-level validation failure is a normal outcome, not an API
            # error: return the batch record (and its quarantine) so the UI can
            # show the reasons.  Conflicts and missing objects stay errors.
            if exc.code == "all_rows_invalid":
                batch = pipeline.get_batch(batch_id)
                return JSONResponse(
                    status_code=202,
                    content={
                        "batch": batch,
                        "run_error": {
                            "code": exc.code,
                            "message": str(exc),
                            "details": exc.details,
                        },
                    },
                )
            raise
        batch = pipeline.get_batch(batch_id)
        return {"batch": batch, "run": result.__dict__}

    @app.post("/api/batches/{batch_id}/files")
    async def replace_batch_files(
        request: Request,
        batch_id: str,
        files: list[UploadFile] = File(...),
        process: bool = Form(True),
    ):
        pipeline: Pipeline = request.app.state.pipeline
        settings: Settings = request.app.state.settings
        if not files:
            raise PipelineError("at least one parquet file is required", code="invalid_upload")
        if len(files) > settings.max_files:
            raise PipelineError(
                f"at most {settings.max_files} files can be uploaded at once",
                code="invalid_upload",
            )
        with _upload_workspace(settings) as workspace:
            stored: list[tuple[str, Path]] = []
            try:
                for index, upload in enumerate(files):
                    path = await _save_upload(
                        upload, workspace, settings, label=f"replacement-{index}"
                    )
                    stored.append((upload.filename or f"file-{index}.parquet", path))
                batch = pipeline.replace_batch_files(batch_id, stored)
            except BaseException:
                for _name, path in stored:
                    path.unlink(missing_ok=True)
                raise
        if process:
            try:
                await run_in_threadpool(pipeline.run_batch, batch_id)
            except PipelineError as exc:
                batch = pipeline.get_batch(batch_id) or batch
                batch["run_error"] = {"code": exc.code, "message": str(exc), "details": exc.details}
                return JSONResponse(status_code=202, content={"batch": batch})
            batch = pipeline.get_batch(batch_id) or batch
        else:
            return JSONResponse(status_code=202, content={"batch": batch})
        return {"batch": batch}

    return app


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _bootstrap_rules(pipeline: Pipeline, rules_file: Path) -> None:
    """Create baseline datasets from the YAML file if they do not exist.

    Existing datasets are never overwritten: their rule versions are immutable
    and managed through the API.
    """

    if not rules_file.exists():
        log.warning("rules file %s not found; skipping bootstrap", rules_file)
        return
    try:
        document = yaml.safe_load(rules_file.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        log.error("could not parse %s: %s", rules_file, exc)
        return
    entries = document.get("datasets") or []
    for entry in entries:
        name = entry.get("name")
        if not name or pipeline.get_dataset(name) is not None:
            continue
        try:
            pipeline.create_dataset(
                name,
                entry.get("description"),
                entry.get("spec") or {},
                verify_references=False,
            )
        except (PipelineError, SpecError) as exc:
            log.error("could not bootstrap dataset %s: %s", name, exc)


def _parse_spec_text(text: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        try:
            value = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise PipelineError(f"rule spec is not valid JSON/YAML: {exc}", code="invalid_spec")
    if not isinstance(value, dict):
        raise PipelineError("rule spec must be an object", code="invalid_spec")
    return value


async def _json_body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if len(raw) > MAX_SPEC_BYTES:
        raise PipelineError("request body is too large", code="invalid_spec")
    if not raw:
        return {}
    try:
        body = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PipelineError(f"request body must be JSON: {exc}", code="invalid_spec")
    if not isinstance(body, dict):
        raise PipelineError("request body must be a JSON object", code="invalid_spec")
    return body


class _upload_workspace:
    """Temporary directory for streamed uploads, outside final storage."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.path: Path | None = None

    def __enter__(self) -> Path:
        base = self.settings.uploads_dir / "_incoming"
        base.mkdir(parents=True, exist_ok=True)
        self.path = Path(tempfile.mkdtemp(prefix=f"{uuid.uuid4().hex}-", dir=base))
        return self.path

    def __exit__(self, *_exc) -> None:
        if self.path is not None:
            shutil.rmtree(self.path, ignore_errors=True)


async def _save_upload(
    upload: UploadFile, directory: Path, settings: Settings, *, label: str
) -> Path:
    suffix = Path(upload.filename or "").suffix.lower()
    if suffix != ".parquet":
        raise PipelineError(
            f"{upload.filename or label}: only .parquet files are accepted", code="invalid_upload"
        )
    target = directory / f"{label}-{uuid.uuid4().hex}.parquet"
    size = 0
    with target.open("wb") as handle:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > settings.max_upload_bytes:
                handle.close()
                target.unlink(missing_ok=True)
                raise PipelineError(
                    f"{upload.filename or label} exceeds the per-file size limit",
                    code="invalid_upload",
                )
            handle.write(chunk)
    if size == 0:
        target.unlink(missing_ok=True)
        raise PipelineError(f"{upload.filename or label} is empty", code="invalid_upload")
    return target


app = create_app()
