from __future__ import annotations

import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

import duckdb

SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS batch_acceptance_seq START 1;

CREATE TABLE IF NOT EXISTS datasets (
    name VARCHAR PRIMARY KEY,
    description VARCHAR,
    current_rules_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS rule_versions (
    dataset VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    spec JSON NOT NULL,
    note VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (dataset, version)
);

CREATE TABLE IF NOT EXISTS reference_versions (
    name VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    key_field VARCHAR NOT NULL,
    columns JSON NOT NULL,
    file_path VARCHAR NOT NULL,
    original_filename VARCHAR,
    row_count BIGINT NOT NULL,
    note VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (name, version)
);

CREATE TABLE IF NOT EXISTS batches (
    id VARCHAR PRIMARY KEY,
    dataset VARCHAR NOT NULL,
    acceptance_seq BIGINT NOT NULL,
    rules_version INTEGER NOT NULL,
    rules_spec JSON NOT NULL,
    reference_versions JSON NOT NULL,
    status VARCHAR NOT NULL,
    attempt INTEGER NOT NULL,
    source_files JSON NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    valid_count BIGINT NOT NULL DEFAULT 0,
    quarantine_count BIGINT NOT NULL DEFAULT 0,
    error_message VARCHAR,
    errors JSON,
    accepted_at TIMESTAMPTZ NOT NULL,
    processing_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ
);

-- Records the one-time switch from a single-column string key (order_id) to
-- the composite (channel, order_id) key.  A row exists exactly when the
-- dataset operates under channel semantics; it is never deleted, so a restart
-- keeps the last effective semantics.
CREATE TABLE IF NOT EXISTS channel_key_switches (
    dataset VARCHAR PRIMARY KEY,
    rules_version INTEGER NOT NULL,
    switched_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    batch_id VARCHAR NOT NULL,
    attempt INTEGER NOT NULL,
    row_index BIGINT NOT NULL,
    source_file VARCHAR,
    raw_row JSON,
    reasons JSON,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS partition_files (
    dataset VARCHAR NOT NULL,
    date_key DATE NOT NULL,
    batch_id VARCHAR NOT NULL,
    acceptance_seq BIGINT NOT NULL,
    attempt INTEGER NOT NULL,
    file_path VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (dataset, date_key, batch_id)
);
"""


class Database:
    """Serialized access to a single DuckDB metadata connection.

    DuckDB connections are not intended for concurrent write use.  A single
    process-wide connection guarded by an RLock keeps metadata and temp-table
    state deterministic.  The service runs with one uvicorn worker.
    """

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        self._closed = False
        self.conn = duckdb.connect(str(path))
        self.conn.execute("SET TimeZone='UTC'")
        with self.lock:
            self.conn.execute(SCHEMA_SQL)
            self._migrate()

    def execute(self, sql: str, params: Iterable[Any] | None = None):
        with self.lock:
            return self.conn.execute(sql, list(params) if params is not None else [])

    def fetchall(self, sql: str, params: Iterable[Any] | None = None):
        return self.execute(sql, params).fetchall()

    def fetchone(self, sql: str, params: Iterable[Any] | None = None):
        rows = self.fetchall(sql, params)
        return rows[0] if rows else None

    def query_dicts(self, sql: str, params: Iterable[Any] | None = None) -> list[dict]:
        with self.lock:
            cur = self.conn.execute(sql, list(params) if params is not None else [])
            columns = [d[0] for d in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    @contextmanager
    def transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        with self.lock:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except BaseException:
                try:
                    self.conn.execute("ROLLBACK")
                finally:
                    raise

    def _migrate(self) -> None:
        """Lightweight additive migrations for metadata created by older code."""
        columns = {
            row[1]
            for row in self.conn.execute(
                "SELECT table_name, column_name FROM information_schema.columns"
                " WHERE table_name = 'batches'"
            ).fetchall()
        }
        if "channel_mode" not in columns:
            # Batches accepted before the channel switch keep legacy semantics.
            self.conn.execute(
                "ALTER TABLE batches ADD COLUMN channel_mode VARCHAR NOT NULL"
                " DEFAULT 'legacy'"
            )

    def close(self) -> None:
        with self.lock:
            if not self._closed:
                self.conn.close()
                self._closed = True
