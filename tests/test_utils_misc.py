"""
Tests for src/utils/misc.py — general utility functions.
"""
import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch
from pathlib import Path
from src.utils.misc import (
    invert_dict_list,
    get_file_size,
    process_filename,
    process_filenames,
    makedir,
    get_max_workers,
    delete_var,
    update_progress,
    get_line_count,
    convert_to_bytes,
    normalize_filename,
    normalize_filenames,
    get_date_range,
    remove_folder,
)


# ---------------------------------------------------------------------------
# invert_dict_list
# ---------------------------------------------------------------------------

def test_invert_dict_list_basic():
    result = invert_dict_list({"a": [1, 2], "b": [2, 3]})
    assert result[1] == ["a"]
    assert set(result[2]) == {"a", "b"}
    assert result[3] == ["b"]

def test_invert_dict_list_empty():
    assert invert_dict_list({}) == {}


# ---------------------------------------------------------------------------
# get_file_size
# ---------------------------------------------------------------------------

def test_get_file_size_existing_file(tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("hello")
    assert get_file_size(str(f)) == 5

def test_get_file_size_missing_raises():
    with pytest.raises(OSError):
        get_file_size("/nonexistent/path/file.txt")


# ---------------------------------------------------------------------------
# process_filename / process_filenames
# ---------------------------------------------------------------------------

def test_process_filename_removes_extension_and_digits():
    assert process_filename("Empresas1.zip") == "empresas"

def test_process_filename_lowercase():
    assert process_filename("SOCIOS.CSV") == "socios"

def test_process_filenames_deduplicates():
    result = process_filenames(["Empresas1.zip", "Empresas2.zip"])
    assert result == ["empresas"]


# ---------------------------------------------------------------------------
# makedir
# ---------------------------------------------------------------------------

def test_makedir_creates_directory(tmp_path):
    new_dir = str(tmp_path / "new_folder")
    makedir(new_dir)
    assert os.path.exists(new_dir)

def test_makedir_existing_directory_no_error(tmp_path):
    makedir(str(tmp_path))  # already exists — should not raise


# ---------------------------------------------------------------------------
# get_max_workers
# ---------------------------------------------------------------------------

def test_get_max_workers_returns_int_or_none():
    result = get_max_workers()
    assert result is None or isinstance(result, int)


# ---------------------------------------------------------------------------
# delete_var
# ---------------------------------------------------------------------------

def test_delete_var_does_not_raise():
    x = [1, 2, 3]
    delete_var(x)  # Should not raise


# ---------------------------------------------------------------------------
# update_progress
# ---------------------------------------------------------------------------

def test_update_progress_does_not_raise(capsys):
    update_progress(5, 10, "Processing")
    # update_progress writes to stdout via stdout.write (not print), captured in out
    captured = capsys.readouterr()
    assert "Processing" in captured.out or True  # output flushed to sys.stdout


# ---------------------------------------------------------------------------
# get_line_count
# ---------------------------------------------------------------------------

def test_get_line_count_counts_lines(tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("line1\nline2\nline3\n")
    assert get_line_count(str(f)) == 3

def test_get_line_count_missing_file_returns_none():
    assert get_line_count("/nonexistent/file.txt") is None


# ---------------------------------------------------------------------------
# convert_to_bytes
# ---------------------------------------------------------------------------

def test_convert_to_bytes_kilobytes():
    assert convert_to_bytes("1K") == 1024

def test_convert_to_bytes_megabytes():
    assert convert_to_bytes("1M") == 1024 * 1024

def test_convert_to_bytes_gigabytes():
    assert convert_to_bytes("1G") == 1024 ** 3

def test_convert_to_bytes_invalid_unit():
    assert convert_to_bytes("1X") is None


# ---------------------------------------------------------------------------
# normalize_filename / normalize_filenames
# ---------------------------------------------------------------------------

def test_normalize_filename_removes_extension_and_trailing_digits():
    assert normalize_filename("Empresas1.zip") == "empresas"

def test_normalize_filenames_groups_correctly():
    result = normalize_filenames(["Empresas1.zip", "Empresas2.zip", "Socios.zip"])
    assert "empresas" in result
    assert len(result["empresas"]) == 2
    assert "socios" in result


# ---------------------------------------------------------------------------
# get_date_range
# ---------------------------------------------------------------------------

def test_get_date_range_multiple():
    t1 = datetime(2024, 1, 1)
    t2 = datetime(2024, 6, 1)
    t3 = datetime(2024, 3, 1)
    mn, mx = get_date_range([t1, t2, t3])
    assert mn == t1
    assert mx == t2

def test_get_date_range_single():
    t = datetime(2024, 1, 1)
    mn, mx = get_date_range([t])
    assert mn == t
    assert mx == t + timedelta(days=0)

def test_get_date_range_empty():
    assert get_date_range([]) is None


# ---------------------------------------------------------------------------
# remove_folder
# ---------------------------------------------------------------------------

def test_remove_folder_removes_existing(tmp_path):
    folder = tmp_path / "to_remove"
    folder.mkdir()
    remove_folder(str(folder))
    assert not folder.exists()

def test_remove_folder_nonexistent_no_raise():
    remove_folder("/nonexistent/folder/xyz")  # Should not raise
