from zipfile import ZipFile
from typing import Optional


def extract_zip_file(file_path, extracted_files_path, max_files: Optional[int] = None):
    """
    Extracts a zip file to the specified directory.

    Args:
        file_path (str): The path to the zip file.
        extracted_files_path (str): The path to the directory where the files will be extracted.
        max_files (int, optional): Maximum number of files to extract. If None, extract all files.
    """
    with ZipFile(file_path, "r") as zip_ref:
        file_list = zip_ref.namelist()
        
        if max_files is not None and len(file_list) > max_files:
            # Sort files to ensure consistent selection (by name)
            file_list = sorted(file_list)[:max_files]
        
        for file_name in file_list:
            zip_ref.extract(file_name, extracted_files_path)


def list_zip_contents(zip_file_path):
    """
    Lists the filenames and other information about files within a zip archive.

    Args:
        zip_file_path (str): Path to the zip archive.

    Returns:
        list: A list of ZipInfo objects containing information about each file in the zip.
    """
    with ZipFile(zip_file_path, "r") as zip_ref:
        return zip_ref.infolist()
