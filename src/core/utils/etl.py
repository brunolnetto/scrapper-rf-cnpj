from wget import download 
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import path
from functools import reduce

from database.models import AuditDB
from database.dml import populate_table
from utils.misc import get_file_size, get_max_workers
from utils.zip import extract_zip_file
from core.constants import TABLES_INFO_DICT
from setup.logging import logger 

# Tabelas
tablename_list = list(TABLES_INFO_DICT.keys())
trimmed_tablename_list = [name[:5] for name in TABLES_INFO_DICT.keys()]
tablename_tuples = list(zip(tablename_list, trimmed_tablename_list))

def download_zipfile(audit, url, zip_filename, download_path, has_progress_bar):
    """Downloads a zip file from the given URL to the specified output path."""
    full_path = path.join(download_path, zip_filename)
    file_url = f"{url}/{zip_filename}"

    try:
        logger.info(f"Downloading file {zip_filename}.")
        download(file_url, out=download_path, bar=None if not has_progress_bar else 'default')
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return None
    finally:
        logger.info(f"Finished file download of file {zip_filename}...")
        audit.audi_downloaded_at = datetime.now()
        audit.audi_file_size_bytes = get_file_size(full_path)
        return audit

def extract_zipfile(audit, zip_filename, download_path, extracted_path):
    """Extracts a zip file to the specified directory."""
    full_path = path.join(download_path, zip_filename)

    try:
        logger.info(f"Extracting file {zip_filename}...")
        extract_zip_file(full_path, extracted_path)
    except Exception as e:
        logger.error(f"Error extracting file {zip_filename}: {e}")
    finally:
        logger.info(f"Finished file extraction of file {zip_filename}.")
        audit.audi_processed_at = datetime.now()
        return audit

def download_and_extract_file(audit, url, zip_filename, download_path, extracted_path, has_progress_bar):
    """Downloads and extracts a single file."""
    audit = download_zipfile(audit, url, zip_filename, download_path, has_progress_bar)
    if audit:
        audit = extract_zipfile(audit, zip_filename, download_path, extracted_path)
    return audit

def download_and_extract_files(audit, url, download_path, extracted_path, has_progress_bar):
    """Downloads and extracts multiple files."""
    for zip_filename in audit.audi_filenames:
        audit = download_and_extract_file(audit, url, zip_filename, download_path, extracted_path, has_progress_bar)
    return audit

def process_files_parallel(url, audits, output_path, extracted_path, max_workers):
    """Downloads and extracts files in parallel."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_and_extract_files, audit, url, output_path, extracted_path, False) for audit in audits]
        results = []
        for future in tqdm(as_completed(futures), total=len(audits)):
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"An error occurred on parallelization: {e}")
        return results

def process_files_serial(url, audits, output_path, extracted_path):
    """Downloads and extracts files serially."""
    error_count = 0
    error_basefiles = []
    total_count = len(audits)
    audits_ = sorted(audits, key=lambda x: x.audi_file_size_bytes)

    for index, audit in enumerate(audits_):
        try:
            audit = download_and_extract_files(audit, url, output_path, extracted_path, True)
        except OSError as e:
            logger.error(f"Erro ao baixar ou extrair arquivo da tabela {audit.audi_table_name}: {e}")
            error_count += 1
            error_basefiles.append(audit.audi_table_name)
        finally:
            logger.info(f"({index}/{total_count}) arquivos baixados. {error_count} erros: {error_basefiles}")

def download_and_extract_RF_data(zips_url, layout_url, audits, output_path, extracted_path, is_parallel=True):
    """Downloads and extracts Receita Federal data."""
    max_workers = get_max_workers()
    is_parallel = max_workers > 1 and is_parallel

    if is_parallel:
        audits = process_files_parallel(zips_url, audits, output_path, extracted_path, max_workers)
    else:
        process_files_serial(zips_url, audits, output_path, extracted_path)

    download(layout_url, out=output_path, bar=None)
    logger.info("Layout baixado com sucesso!")
    return audits

def get_RF_data(data_url, layout_url, audits, from_folder, to_folder, is_parallel=True):
    """Retrieves and extracts the data from the Receita Federal."""
    return download_and_extract_RF_data(data_url, layout_url, audits, from_folder, to_folder, is_parallel)

def load_RF_data_on_database(database, source_folder, audit_metadata):
    """Populates the database with data from multiple tables."""
    table_to_filenames = audit_metadata.tablename_to_zipfile_to_files

    # Load data
    for index, (table_name, zipfile_content_dict) in enumerate(table_to_filenames.items()):
        table_files_list = list(zipfile_content_dict.values())

        table_filenames = sum(table_files_list, [])

        # Populate this table
        populate_table(database, table_name, source_folder, table_filenames)
        audit_metadata.audit_list[index].audi_inserted_at = datetime.now()

    zip_filenames = list(reduce(lambda x, y: list(x) + list(y), map(lambda x: x.keys(), table_to_filenames.values())))
    logger.info(f"Carga dos arquivos zip {zip_filenames} finalizado!")
    return audit_metadata

def get_zip_to_tablename(zip_file_dict):
    """Retrieves the filenames of the extracted files from the Receita Federal."""
    zip_to_tablename = {zipped_file: [] for zipped_file in zip_file_dict.keys()}

    for zipfile_filename, unzipped_files in zip_file_dict.items():
        for item in unzipped_files:
            has_label_map = lambda label: item.lower().find(label[1].lower()) > -1
            this_tablename_tuple = list(filter(has_label_map, tablename_tuples))
            if this_tablename_tuple:
                zip_to_tablename[zipfile_filename].append(this_tablename_tuple[0][0])

    return zip_to_tablename
