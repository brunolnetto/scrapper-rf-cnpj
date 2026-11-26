"""Temp-file cleanup in conversion is best-effort but no longer silent (issue #61)."""

from src.core.services.conversion import utils
from src.core.services.conversion.utils import remove_quietly


def test_removes_nested_tree_and_files(tmp_path):
    tree = tmp_path / ".parts_x" / "part_0000_chunks"
    tree.mkdir(parents=True)
    (tree / "chunk.parquet").write_bytes(b"x")
    loose = tmp_path / "part.parquet"
    loose.write_bytes(b"x")

    remove_quietly(tmp_path / ".parts_x")
    remove_quietly(loose)

    assert not (tmp_path / ".parts_x").exists()
    assert not loose.exists()


def test_missing_path_is_fine(tmp_path):
    remove_quietly(tmp_path / "never-created.parquet")


def test_failure_is_logged_not_raised(tmp_path, monkeypatch):
    target = tmp_path / "busy.parquet"
    target.write_bytes(b"x")
    warnings = []
    monkeypatch.setattr(utils.logger, "warning", warnings.append)
    monkeypatch.setattr(type(target), "unlink", lambda *_a, **_k: (_ for _ in ()).throw(PermissionError("locked")))

    remove_quietly(target)

    assert warnings and "busy.parquet" in warnings[0] and "locked" in warnings[0]
