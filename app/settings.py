from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MAX_UPLOAD_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_FILES = 50


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    rules_file: Path
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES
    max_files: int = DEFAULT_MAX_FILES

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_dir=Path(os.environ.get("GSB_DATA_DIR", "./data")).resolve(),
            rules_file=Path(
                os.environ.get("GSB_RULES_FILE", "./config/rules.yaml")
            ).resolve(),
            max_upload_bytes=int(
                os.environ.get("GSB_MAX_UPLOAD_BYTES", DEFAULT_MAX_UPLOAD_BYTES)
            ),
            max_files=int(os.environ.get("GSB_MAX_FILES", DEFAULT_MAX_FILES)),
        )

    def ensure_dirs(self) -> None:
        for name in (
            "uploads",
            "references",
            "trusted_staging",
            "trusted",
        ):
            (self.data_dir / name).mkdir(parents=True, exist_ok=True)

    @property
    def meta_db_path(self) -> Path:
        return self.data_dir / "meta.duckdb"

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def references_dir(self) -> Path:
        return self.data_dir / "references"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "trusted_staging"

    @property
    def trusted_dir(self) -> Path:
        return self.data_dir / "trusted"
