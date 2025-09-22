from typing import List

from ...utils.misc import normalize_filenames, get_date_range
from ..schemas import FileInfo, FileGroupInfo
from ..constants import TABLES_INFO_DICT
from ...setup.logging import logger


def file_group_name_to_table_name(file_group_name: str) -> str:
    """
    Maps a file group name to a table name.

    Args:
        file_group_name (str): The name of the file group.

    Returns:
        str: The corresponding table name.
    """
    matching_table = next(
        (
            table_name
            for table_name, table_info in TABLES_INFO_DICT.items()
            if table_info["group"] == file_group_name
        ),
        None,
    )
    if matching_table is None:
        raise ValueError(f"No table found for file group name: {file_group_name}")
    return matching_table


def create_file_groups(files_info: List[FileInfo], year: int, month: int) -> List[FileGroupInfo]:
    """
    Creates a list of file groups based on the provided file information.

    Args:
        files_info (List[FileInfo]): A list of FileInfo objects.
        year (int): The year for the file groups.
        month (int): The month for the file groups.

    Returns:
        List[FileGroupInfo]: A list of FileGroupInfo objects.
    """
    # Normalize filenames and group them
    filenames = [file_info.filename for file_info in files_info]
    normalized_filenames = normalize_filenames(filenames)  # Returns a dict

    # Create file groups
    file_groups = []
    for normalized_name, original_filenames in normalized_filenames.items():
        # Get the FileInfo objects for this group
        group_files = [
            file_info for file_info in files_info 
            if file_info.filename in original_filenames
        ]

        try:
            table_name = file_group_name_to_table_name(normalized_name)
        except ValueError as e:
            logger.error(f"Error mapping file group {normalized_name}: {e}")
            continue

        file_groups.append(
            FileGroupInfo(
                table_name=table_name,
                files=group_files,
                year=year,
                month=month,
            )
        )

    return file_groups
