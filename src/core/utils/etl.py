from typing import Dict, List

from ..constants import TABLES_INFO_DICT

# Tabelas
tablename_list = list(TABLES_INFO_DICT.keys())
zipname_label_list = [props["label"] for props in TABLES_INFO_DICT.values()]
tablename_tuples = list(zip(tablename_list, zipname_label_list))


def get_zip_to_tablename(zip_file_dict: Dict[str, List[str]]) -> Dict[str, str]:
    """Retrieves the filenames of the extracted files from the Receita Federal."""
    return {
        zipped_file: [
            tablename
            for tablename, prefix in tablename_tuples
            if prefix.lower() in zipped_file.lower()
        ]
        for zipped_file in zip_file_dict.keys()
    }
