from sys import stdout
from os import path, cpu_count, stat
from shutil import rmtree
from unicodedata import normalize
from os import makedirs
import re
from datetime import timedelta

from ..setup.logging import logger

def invert_dict_list(dict_: dict):
    """
    Inverts a dictionary where the values are lists of keys.

    Args:
        dict_ (dict): The dictionary to be inverted.

    Returns:
        dict: The inverted dictionary where the keys are the values from the original dictionary
              and the values are the corresponding keys from the original dictionary.
    """
    inverted_dict = dict()
    for key, values_list in dict_.items():
        for value in values_list:
            if value not in inverted_dict:
                inverted_dict[value] = [key]
            else:
                inverted_dict[value].append(key)

    return inverted_dict


def get_file_size(file_path):
    """
    This function retrieves the size of a file in bytes.

    Args:
        file_path (str): The path to the file.

    Returns:
        int: The size of the file in bytes, or None if the file doesn't exist or can't be accessed.

    Raises:
        OSError: If an error occurs while accessing the file.
    """
    try:
        # Use os.stat to get file information in a platform-independent way
        file_stats = stat(file_path)
        return file_stats.st_size

    except OSError as e:
        # Raise OSError for potential file access issues
        raise OSError(f"Error accessing file: {file_path}. Reason: {e}") from e

    except Exception as e:
        # Catch unexpected exceptions and re-raise with more context
        raise Exception(
            f"Unexpected error getting file size for {file_path}: {e}"
        ) from e


def process_filename(filename):
    """
    Processes a filename by removing the extension and numbers, and converting it to lowercase.

    Args:
        filename (str): The filename to process.

    Returns:
        str: The processed filename.
    """
    # Split the filename at the last dot (".") to separate the base name and extension
    base_name, _ = filename.rsplit(".", 1)

    # Remove numbers from the base name using regular expressions
    return re.sub(r"\d+", "", base_name).lower()


def process_filenames(filenames):
    """
    Processes a list of filenames by removing extensions and numbers, and converting them to lowercase.

    Args:
        filenames (list): A list of strings representing filenames.

    Returns:
        list: A new list of strings with processed filenames.
    """
    processed_names = []
    for filename in filenames:
        processed_names.append(process_filename(filename))

    return list(set(processed_names))


def makedir(folder_name: str, is_verbose: bool = False):
    """
    Creates a new directory if it doesn't already exist.

    Args:
        folder_name (str): The name of the folder to create.
        is_verbose (bool, optional): Whether to log verbose information. Defaults to False.
    """
    if not path.exists(folder_name):
        makedirs(folder_name)

        if is_verbose:
            logger.info("Folder: \n" + repr(str(folder_name)))

    else:
        if is_verbose:
            logger.warn(f"Folder {repr(str(folder_name))} already exists!")


def get_max_workers():
    """
    Gets the maximum number of workers based on the number of CPU cores.

    Returns:
        int: The maximum number of workers.
    """
    # Get the number of CPU cores
    num_cores = cpu_count()

    # Adjust the number of workers based on the requirements
    # You should leave some cores free for other tasks
    max_workers = num_cores - 1 if num_cores else None

    return max_workers


def delete_var(var):
    """
    Deletes a variable from memory.

    Args:
        var: The variable to delete.
    """
    try:
        del var
    except:
        pass

def update_progress(index, total, message):
    """
    Updates and displays a progress message.

    Args:
        index (int): The current index.
        total (int): The total number of items.
        message (str): The message to display.
    """
    percent = (index * 100) / total
    curr_perc_pos = f"{index:0{len(str(total))}}/{total}"
    progress = f"{message} {percent:.2f}% {curr_perc_pos}"

    stdout.write(f"\r{progress}")
    stdout.flush()


def get_line_count(filepath):
    """
    Counts the number of lines in a large file efficiently.

    Args:
        filepath (str): Path to the file.

    Returns:
        int: Number of lines in the file (or None on error).
    """
    try:
        # Open the file in read mode with specified encoding
        with open(filepath, "r", encoding="latin-1") as file:
            line_count = sum(1 for _ in file)
        return line_count
    except Exception as e:
        logger.error(f"Error counting lines of file {filepath}: {e}")
        return None


def convert_to_bytes(size_str):
    """
    This function converts a size string (e.g., "22K", "321M") into bytes.

    Args:
        size_str (str): The size string to convert.

    Returns:
        int: The size in bytes, or None if the format is invalid.
    """
    size_value = float(size_str[:-1])  # Extract numerical value
    size_unit = size_str[-1].upper()  # Get the unit (K, M, G)

    unit_multiplier = {"K": 1024, "M": 1024 * 1024, "G": 1024 * 1024 * 1024}

    if size_unit in unit_multiplier:
        return int(size_value * unit_multiplier[size_unit])
    else:
        return None  # Handle invalid units


def normalize_filename(filename):
    """
    This function normalizes a filename by removing the extension and numbers,
    and converting it to lowercase.

    Args:
        filename (str): The filename to normalize.

    Returns:
        str: The normalized filename.
    """

    # Remove extension
    base_name = path.splitext(filename)[0]

    # Remove number (assuming numbers are at the end)
    base_name = re.sub(r"\d+$", "", base_name)

    # Normalize accentuation (assuming NFD normalization)
    base_name = normalize("NFD", base_name).casefold()

    return base_name


def normalize_filenames(filenames):
    """
    This function normalizes a list of filenames and creates a dictionary
    with key as normalized filename and value as original zip filename.

    Args:
        filenames (list): A list of filenames to normalize.

    Returns:
        dict: A dictionary with normalized filenames as keys and original filenames as values.
    """
    normalized_dict = {}
    for filename in filenames:
        base_name = normalize_filename(filename)

        # Create dictionary entry
        if base_name not in normalized_dict:
            normalized_dict[base_name] = [filename]
        else:
            normalized_dict[base_name].append(filename)

    return normalized_dict


def get_date_range(timestamps):
    """
    This function finds the minimum and maximum date in a list of datetime timestamps.
    If there's only one element, it returns the same date and a timedelta of 0 days.

    Args:
        timestamps (list): A list of datetime timestamps.

    Returns:
        tuple: A tuple containing the minimum date and maximum date (or the same date
                and a timedelta of 0 days if there's only one element).
    """
    if not timestamps:
        return None  # Handle empty list case

    if len(timestamps) == 1:
        return timestamps[0], timestamps[0] + timedelta(days=0)
    else:
        return min(timestamps), max(timestamps)


def remove_folder(folder: str):
    try:
        rmtree(folder)
    except Exception as e:
        logger.error(f"Error deleting folder {folder}: {e}")
