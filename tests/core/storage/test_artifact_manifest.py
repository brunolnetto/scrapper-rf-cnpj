import json
import hashlib
from pathlib import Path

from src.core.storage import LocalStorageBackend, persist_artifact_manifest


def test_manifest_records_size_checksum_and_object_provenance(tmp_path: Path):
    storage = LocalStorageBackend(tmp_path / "objects")
    artifact = tmp_path / "empresa.parquet"
    payload = b"PAR1-cnpj-artifact"
    artifact.write_bytes(payload)

    manifest_key = persist_artifact_manifest(
        storage,
        artifact,
        "processed/parquet/2026/11/empresa.parquet",
        stage="processed/parquet",
        year=2026,
        month=11,
        source_key="raw/extracted/2026/11/Empresas/Empresas.csv",
    )

    assert manifest_key == "metadata/processed/parquet/2026/11/empresa.parquet.json"
    manifest = json.loads((storage.root / manifest_key).read_text(encoding="utf-8"))
    assert manifest["size_bytes"] == len(payload)
    assert manifest["checksum"] == {
        "algorithm": "sha256",
        "value": hashlib.sha256(payload).hexdigest(),
    }
    assert manifest["period"] == {"year": 2026, "month": 11}
    assert manifest["source_key"] == "raw/extracted/2026/11/Empresas/Empresas.csv"
