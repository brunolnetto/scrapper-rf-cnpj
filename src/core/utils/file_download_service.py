import os
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm
from database.models import AuditDB
from setup.logging import logger
from utils.misc import get_file_size, get_max_workers
from utils.zip import extract_zip_file

class FileDownloadService:
    def __init__(self, max_workers: int = None):
        self.max_workers = max_workers or get_max_workers()

    def download_and_extract(
        self,
        audits: List[AuditDB],
        url: str,
        download_path: str,
        extract_path: str,
        parallel: bool = True
    ) -> List[AuditDB]:
        """
        Download and extract all files for the given audits.
        Preserves retry, progress bar, and parallelization features.
        """
        if parallel and self.max_workers > 1:
            return self._process_files_parallel(url, audits, download_path, extract_path)
        else:
            return self._process_files_serial(url, audits, download_path, extract_path)

    def _process_files_parallel(self, url, audits, download_path, extract_path):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._download_and_extract_files,
                    audit, url, download_path, extract_path, False
                ) for audit in audits
            ]
            results = []
            for future in tqdm(as_completed(futures), total=len(audits), desc="Processing Files", unit="file"):
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(f"Error during parallel execution: {e}")
            return results

    def _process_files_serial(self, url, audits, download_path, extract_path):
        error_count = 0
        error_basefiles = []
        total_count = len(audits)
        audits_ = sorted(audits, key=lambda x: x.audi_file_size_bytes)
        for index, audit in enumerate(audits_):
            try:
                audit = self._download_and_extract_files(audit, url, download_path, extract_path, True)
            except OSError as e:
                logger.error(f"Error downloading or extracting file for table {audit.audi_table_name}: {e}")
                error_count += 1
                error_basefiles.append(audit.audi_table_name)
            finally:
                logger.info(f"({index}/{total_count}) files processed. {error_count} errors: {error_basefiles}")
        return audits_

    def _download_and_extract_files(self, audit, url, download_path, extract_path, has_progress_bar):
        for zip_filename in audit.audi_filenames:
            audit = self._download_and_extract_file(audit, url, zip_filename, download_path, extract_path, has_progress_bar)
        return audit

    def _download_and_extract_file(self, audit, url, zip_filename, download_path, extract_path, has_progress_bar):
        try:
            audit = self._download_zipfile(audit, url, zip_filename, download_path, has_progress_bar)
            if audit:
                audit = self._extract_zipfile(audit, zip_filename, download_path, extract_path)
            return audit
        except Exception as e:
            logger.error(f"Error processing file {zip_filename}: {e}")
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _download_zipfile(self, audit: AuditDB, url: str, zip_filename: str, download_path: str, has_progress_bar: bool):
        full_path = os.path.join(download_path, zip_filename)
        file_url = f"{url}/{zip_filename}"
        local_filename = os.path.join(download_path, zip_filename)
        progress = None
        try:
            logger.info(f"Starting download for file: {zip_filename}")
            import requests
            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('Content-Length', 0))
                if has_progress_bar:
                    progress = tqdm(total=total_size, unit='B', unit_scale=True, desc=zip_filename)
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=131072):
                        if chunk:
                            f.write(chunk)
                            if has_progress_bar and progress:
                                progress.update(len(chunk))
                if has_progress_bar and progress:
                    progress.close()
            logger.info(f"Successfully downloaded file: {zip_filename}")
            audit.audi_downloaded_at = datetime.now()
            audit.audi_file_size_bytes = get_file_size(local_filename)
        except Exception as e:
            logger.error(f"Error downloading {zip_filename}: {e}")
            if has_progress_bar and progress:
                progress.close()
            raise
        return audit

    def _extract_zipfile(self, audit, zip_filename, download_path, extract_path):
        full_path = os.path.join(download_path, zip_filename)
        try:
            logger.info(f"Extracting file {zip_filename}...")
            extract_zip_file(full_path, extract_path)
        except Exception as e:
            logger.error(f"Error extracting file {zip_filename}: {e}")
        finally:
            logger.info(f"Finished file extraction of file {zip_filename}.")
            audit.audi_processed_at = datetime.now()
            return audit