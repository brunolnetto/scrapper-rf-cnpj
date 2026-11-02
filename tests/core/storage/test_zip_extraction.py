from pathlib import Path
from zipfile import ZipFile

import pytest

from src.utils.zip import extract_zip_file


def test_extracts_nested_files_and_returns_local_paths(tmp_path: Path):
    archive = tmp_path / "source.zip"
    with ZipFile(archive, "w") as zip_file:
        zip_file.writestr("nested/data.csv", "cnpj;name\n1;example\n")

    output = tmp_path / "workspace"
    extracted = extract_zip_file(archive, output)

    assert len(extracted) == 1
    assert extracted[0] == output / "nested" / "data.csv"
    assert extracted[0].read_text() == "cnpj;name\n1;example\n"


@pytest.mark.parametrize("member", ["../outside.csv", "/tmp/outside.csv", r"..\outside.csv", "C:/outside.csv"])
def test_rejects_unsafe_member_paths(tmp_path: Path, member: str):
    archive = tmp_path / "unsafe.zip"
    with ZipFile(archive, "w") as zip_file:
        zip_file.writestr(member, "do not write")

    with pytest.raises(ValueError):
        extract_zip_file(archive, tmp_path / "workspace")

    assert not (tmp_path / "outside.csv").exists()
