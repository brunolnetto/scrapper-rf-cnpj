import re
from typing import Dict, List, Optional

from ..constants import TABLES_INFO_DICT

# One anchored pattern per table, derived from its label: e.g. "Empresa" matches
# Empresas.zip, Empresas0.zip ... Empresas9.zip (and Empresas_00.zip), while
# "CNAEs" matches Cnaes.zip. Matching is exact on the whole name, so it does
# not depend on the order of TABLES_INFO_DICT.
_ZIP_PATTERNS: Dict[str, "re.Pattern[str]"] = {
    tablename: re.compile(rf"^{re.escape(props['label'])}s?[_-]?\d*\.zip$", re.IGNORECASE)
    for tablename, props in TABLES_INFO_DICT.items()
}


def table_for_zip_filename(filename: str) -> Optional[str]:
    """Return the table a Receita Federal ZIP file belongs to, or None."""
    matches = [table for table, pattern in _ZIP_PATTERNS.items() if pattern.match(filename)]
    if len(matches) > 1:
        raise ValueError(f"ZIP filename {filename!r} matches several tables: {matches}")
    return matches[0] if matches else None


def get_zip_to_tablename(zip_file_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Map each ZIP filename to the (zero or one) table it belongs to."""
    mapping: Dict[str, List[str]] = {}
    for zipped_file in zip_file_dict:
        table = table_for_zip_filename(zipped_file)
        mapping[zipped_file] = [table] if table else []
    return mapping
