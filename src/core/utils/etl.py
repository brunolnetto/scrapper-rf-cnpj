import requests
from tqdm import tqdm
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import path
from functools import reduce
from tenacity import retry, stop_after_attempt, wait_exponential

from core.schemas import AuditMetadata
from database.models import AuditDB
from database.dml import populate_table
from utils.misc import get_file_size, get_max_workers
from utils.zip import extract_zip_file
from core.constants import TABLES_INFO_DICT
from setup.logging import logger 

# Tabelas
tablename_list = list(TABLES_INFO_DICT.keys())
expression_list = [TABLES_INFO_DICT[name]['expression'] for name in TABLES_INFO_DICT.keys()]
tablename_tuples = list(zip(tablename_list, expression_list))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_zipfile(
    audit: AuditDB, url: str, zip_filename: str, download_path: str, has_progress_bar: bool
):
    """Downloads a zip file from the given URL to the specified output path."""
    full_path = path.join(download_path, zip_filename)
    file_url = f"{url}/{zip_filename}"
    local_filename = path.join(download_path, zip_filename)

    try:
        logger.info(f"Starting download for file: {zip_filename}")
        
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('Content-Length', 0))  # Get total file size
            
            # Initialize tqdm progress bar if enabled
            progress = tqdm(
                total=total_size, unit='B', unit_scale=True, desc=zip_filename
            ) if has_progress_bar else None
            
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=131072):
                    if chunk:
                        f.write(chunk)
                        if has_progress_bar:
                            progress.update(len(chunk))  # Update the progress bar
            
            # Close the progress bar if enabled
            if has_progress_bar and progress is not None:
                progress.close()

        logger.info(f"Successfully downloaded file: {zip_filename}")
        audit.audi_downloaded_at = datetime.now()
        audit.audi_file_size_bytes = get_file_size(local_filename)

    except Exception as e:
        logger.error(f"Error downloading {zip_filename}: {e}")
        if has_progress_bar and progress is not None:
            progress.close()  # Close the progress bar in case of failure
        raise

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
    try:
        audit = download_zipfile(audit, url, zip_filename, download_path, has_progress_bar)
        if audit:
            audit = extract_zipfile(audit, zip_filename, download_path, extracted_path)
        return audit
    except Exception as e:
        logger.error(f"Error processing file {zip_filename}: {e}")
        return None

def download_and_extract_files(audit, url, download_path, extracted_path, has_progress_bar):
    """Downloads and extracts multiple files."""
    for zip_filename in audit.audi_filenames:
        audit = download_and_extract_file(audit, url, zip_filename, download_path, extracted_path, has_progress_bar)
    return audit

def process_files_parallel(url, audits, output_path, extracted_path, max_workers):
    """Downloads and extracts files in parallel."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                download_and_extract_files, 
                audit, url, output_path, extracted_path, False
            ) for audit in audits
        ]
        results = []
        for future in tqdm(as_completed(futures), total=len(audits), desc="Processing Files", unit="file"):
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Error during parallel execution: {e}")

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

def download_and_extract_RF_data(zips_url, audits, output_path, extracted_path, is_parallel=True):
    """Downloads and extracts Receita Federal data."""
    max_workers = get_max_workers()
    is_parallel = max_workers > 1 and is_parallel

    if is_parallel:
        audits = process_files_parallel(zips_url, audits, output_path, extracted_path, max_workers)
    else:
        process_files_serial(zips_url, audits, output_path, extracted_path)

    return audits

def get_RF_data(data_url, audits, from_folder, to_folder, is_parallel=True):
    """Retrieves and extracts the data from the Receita Federal."""
    return download_and_extract_RF_data(data_url, audits, from_folder, to_folder, is_parallel)

def load_RF_data_on_database(database, source_folder, audit_metadata: AuditMetadata):
    """
    DEPRECATED: Use DataLoadingService and strategy pattern instead.
    Populates the database with data from multiple tables."""
    table_to_filenames = audit_metadata.tablename_to_zipfile_to_files

    # Debug: Log what tables are being processed
    logger.info(f"Processing tables: {list(table_to_filenames.keys())}")
    logger.info(f"Audit records: {[audit.audi_table_name for audit in audit_metadata.audit_list]}")

    # Load data
    for index, (table_name, zipfile_content_dict) in enumerate(table_to_filenames.items()):
        table_files_list = list(zipfile_content_dict.values())

        table_filenames = sum(table_files_list, [])

        # Populate this table
        populate_table(database, table_name, source_folder, table_filenames)
        
        # Update audit metadata - find the correct audit record by table name
        audit_found = False
        for audit in audit_metadata.audit_list:
            if audit.audi_table_name == table_name:
                audit.audi_inserted_at = datetime.now()
                audit_found = True
                logger.info(f"Set audi_inserted_at for table: {table_name}")
                break
        
        if not audit_found:
            logger.warning(f"No audit record found for table: {table_name}")

    # Ensure all audit records have audi_inserted_at set
    processed_tables = set(table_to_filenames.keys())
    for audit in audit_metadata.audit_list:
        if audit.audi_inserted_at is None:
            if audit.audi_table_name not in processed_tables:
                logger.info(f"Setting audi_inserted_at for table '{audit.audi_table_name}' (no files to process)")
            else:
                logger.warning(f"Setting audi_inserted_at for table '{audit.audi_table_name}' (was not set during processing)")
            audit.audi_inserted_at = datetime.now()
    
    # Log final status
    tables_with_inserted_at = [
        audit.audi_table_name 
        for audit in audit_metadata.audit_list if audit.audi_inserted_at is not None
    ]
    logger.info(f"All audit records have audi_inserted_at set: {tables_with_inserted_at}")

    sum_lists_map=lambda x, y: list(x) + list(y)
    filenames_map=map(lambda x: x.keys(), table_to_filenames.values())
    zip_filenames = list(reduce(sum_lists_map, filenames_map))
    logger.info(f"Carga dos arquivos zip {zip_filenames} finalizado!")
    
    # Final summary
    processed_count = len([audit for audit in audit_metadata.audit_list if audit.audi_inserted_at is not None])
    total_count = len(audit_metadata.audit_list)
    logger.info(f"Database loading complete: {processed_count}/{total_count} audit records processed")
    
    return audit_metadata

def get_zip_to_tablename(zip_file_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Retrieves the filenames of the extracted files from the Receita Federal."""
    result = {}
    for zipped_file in zip_file_dict.keys():
        matching_tables = []
        for tablename, expression in tablename_tuples:
            if expression.lower() in zipped_file.lower():
                matching_tables.append(tablename)
        result[zipped_file] = matching_tables
    return result


