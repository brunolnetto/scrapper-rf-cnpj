from typing import List, Dict
from utils.misc import normalize_filenames, get_date_range
from core.schemas import FileInfo, FileGroupInfo
from core.constants import TABLES_INFO_DICT

def file_group_name_to_table_name(file_group_name: str) -> str:
    """
    Maps a file group name to a table name.

    Args:
        file_group_name (str): The name of the file group.

    Returns:
        str: The corresponding table name.
    """
    matching_table = next(
        (table_name for table_name, table_info in TABLES_INFO_DICT.items() 
         if table_info['group'] == file_group_name), 
        None
    )
    if matching_table is None:
        raise ValueError(f"No table found for file group name: {file_group_name}")
    return matching_table

def create_file_groups(files_info: List[FileInfo]) -> List[FileGroupInfo]:
    """
    Creates a list of file groups based on the provided file information.

    Args:
        files_info (List[FileInfo]): A list of FileInfo objects.
    
    Returns:
        List[FileGroupInfo]: A list of FileGroupInfo objects.
    """
    filename_to_update_at: Dict[str, str] = {
        file_info.filename: file_info.updated_at for file_info in files_info
    }
    filename_to_size: Dict[str, int] = {
        file_info.filename: file_info.file_size for file_info in files_info
    }
    filenames: List[str] = [file_info.filename for file_info in files_info]
    normalized_filenames: Dict[str, List[str]] = normalize_filenames(filenames)

    file_groups = [
        {
            "name": normalized_name,
            "elements": original_filenames,
            "size_bytes": sum(filename_to_size[filename] for filename in original_filenames),
            "date_range": get_date_range([filename_to_update_at[filename] for filename in original_filenames]),
            "table_name": file_group_name_to_table_name(normalized_name)
        }
        for normalized_name, original_filenames in normalized_filenames.items()
    ]

    return [
        FileGroupInfo(
            name=group['name'], 
            elements=group['elements'], 
            date_range=group['date_range'],
            table_name=group['table_name'],
            size_bytes=group['size_bytes']
        )
        for group in file_groups
    ]
