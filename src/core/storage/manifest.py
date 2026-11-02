from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .base import StorageBackend, PathLike


def persist_artifact_manifest(
    storage: StorageBackend,
    artifact_path: PathLike,
    object_key: str,
    *,
    stage: str,
    year: int,
    month: int,
    source_key: str | None = None,
) -> str:
    """Compute streaming integrity metadata and persist a JSON sidecar.

    The manifest is stored at metadata/{object_key}.json. Repeated writes for
    the same artifact key replace the previous manifest, making retries
    idempotent at the storage-key level.
    """
    path = Path(artifact_path)
    digest = hashlib.sha256()
    size_bytes = 0

    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
            size_bytes += len(chunk)

    payload = {
        "schema_version": 1,
        "stage": stage,
        "period": {"year": year, "month": month},
        "object_key": object_key,
        "source_key": source_key,
        "size_bytes": size_bytes,
        "checksum": {
            "algorithm": "sha256",
            "value": digest.hexdigest(),
        },
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_key = f"metadata/{object_key}.json"

    with tempfile.TemporaryDirectory(prefix="cnpj-manifest-") as workspace:
        manifest_path = Path(workspace) / "manifest.json"
        manifest_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        storage.upload_file(manifest_path, manifest_key)

    return manifest_key
