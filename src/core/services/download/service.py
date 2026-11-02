import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

from ....database.models.audit import TableAuditManifest
from ....setup.logging import logger
from ....setup.config import ConfigurationService
from ....utils.misc import get_max_workers
from ....utils.zip import extract_zip_file
from ....core.storage import StorageBackend, create_storage_backend


class FileDownloadService:
    def __init__(self, max_workers: int = None, config: ConfigurationService = None, storage_backend: StorageBackend | None = None):
        self.config = config
        self.storage_backend = storage_backend
        if self.storage_backend is None and config is not None:
            self.storage_backend = create_storage_backend(config.pipeline.storage)
        self.max_workers = max_workers or get_max_workers()
            
        # Apply development mode filtering if enabled
        if config and config.is_development_mode():
            # Reduce workers in development mode for better debugging
            self.max_workers = min(self.max_workers, 2)
            logger.info(f"[DEV-MODE] Limited download workers to {self.max_workers}")

    def _object_key(self, stage: str, filename: str) -> str:
        """Build a deterministic storage key for the configured ingestion period."""
        if self.config is None:
            return f"{stage}/{filename}"
        year = self.config.year or self.config.pipeline.year
        month = self.config.month or self.config.pipeline.month
        if year is None or month is None:
            raise ValueError("Storage-backed download requires an ingestion year and month")
        return f"{stage}/{year:04d}/{month:02d}/{filename}"

    def download_and_extract(
        self,
        audits: List[TableAuditManifest],
        url: str,
        download_path: str,
        extract_path: str,
        parallel: bool = True,
    ) -> List[TableAuditManifest]:
        """
        Download and extract all files for the given audits.
        Preserves retry, progress bar, and parallelization features.
        """
        if parallel and self.max_workers > 1:
            return self._process_files_parallel(
                url, audits, download_path, extract_path
            )
        else:
            return self._process_files_serial(url, audits, download_path, extract_path)

    def _process_files_parallel(self, url, audits, download_path, extract_path):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._download_and_extract_files,
                    audit,
                    url,
                    download_path,
                    extract_path,
                    False,
                )
                for audit in audits
            ]
            results = []
            for future in tqdm(
                as_completed(futures),
                total=len(audits),
                desc="Processing Files",
                unit="file",
            ):
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(f"Error during parallel execution: {e}")
            return results

    def _process_files_serial(self, url, audits, download_path, extract_path):
        error_count = 0
        error_basefiles = []
        total_count = len(audits)
        # Process audits in order (sorting by file size removed since field no longer exists)
        for index, audit in enumerate(audits):
            try:
                audit = self._download_and_extract_files(
                    audit, url, download_path, extract_path, True
                )
            except OSError as e:
                logger.error(
                    f"Error downloading or extracting file for table {audit.entity_name}: {e}"
                )
                error_count += 1
                error_basefiles.append(audit.entity_name)
            finally:
                logger.info(
                    f"({index}/{total_count}) files processed. {error_count} errors: {error_basefiles}"
                )
        return audits

    def _download_and_extract_files(
        self, audit, url, download_path, extract_path, has_progress_bar
    ):
        for zip_filename in audit.source_files:
            audit = self._download_and_extract_file(
                audit, url, zip_filename, download_path, extract_path, has_progress_bar
            )
        return audit

    def _download_and_extract_file(
        self, audit, url, zip_filename, download_path, extract_path, has_progress_bar
    ):
        try:
            audit = self._download_zipfile(
                audit, url, zip_filename, download_path, has_progress_bar
            )
            if audit:
                audit = self._extract_zipfile(
                    audit, zip_filename, download_path, extract_path
                )
            return audit
        except Exception as e:
            logger.error(f"Error processing file {zip_filename}: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _download_zipfile(
        self,
        audit: TableAuditManifest,
        url: str,
        zip_filename: str,
        download_path: str,
        has_progress_bar: bool,
    ):
        file_url = f"{url.rstrip('/')}/{zip_filename}"
        if Path(zip_filename).name != zip_filename:
            raise ValueError(f"Invalid ZIP filename: {zip_filename!r}")
        download_dir = Path(download_path)
        download_dir.mkdir(parents=True, exist_ok=True)
        local_filename = download_dir / zip_filename
        progress = None
        try:
            logger.info(f"Starting download for file: {zip_filename}")
            import requests

            import base64 as _b64
            token = os.getenv("RF_DOWNLOAD_TOKEN")
            _headers = {}
            if token:
                encoded_token = _b64.b64encode(f"{token}:".encode()).decode()
                _headers["Authorization"] = f"Basic {encoded_token}"
            with requests.get(file_url, stream=True, headers=_headers) as r:
                r.raise_for_status()
                total_size = int(r.headers.get("Content-Length", 0))
                if has_progress_bar:
                    progress = tqdm(
                        total=total_size, unit="B", unit_scale=True, desc=zip_filename
                    )
                with open(local_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=131072):
                        if chunk:
                            f.write(chunk)
                            if has_progress_bar and progress:
                                progress.update(len(chunk))
                if has_progress_bar and progress:
                    progress.close()
            if self.storage_backend is not None:
                self.storage_backend.upload_file(
                    local_filename,
                    self._object_key("raw/zip", zip_filename),
                )
            logger.info(f"Successfully downloaded file: {zip_filename}")
            audit.downloaded_at = datetime.now()
            # file_size_bytes removed - file size tracking moved to file manifest level
        except Exception as e:
            logger.error(f"Error downloading {zip_filename}: {e}")
            if has_progress_bar and progress:
                progress.close()
            raise
        return audit

    def _extract_zipfile(self, audit, zip_filename, download_path, extract_path):
        if Path(zip_filename).name != zip_filename:
            raise ValueError(f"Invalid ZIP filename: {zip_filename!r}")

        full_path = Path(download_path) / zip_filename
        extract_dir = Path(extract_path)
        extract_dir.mkdir(parents=True, exist_ok=True)

        try:
            logger.info(f"Extracting file {zip_filename}...")
            max_files = self.config.get_max_files_per_blob() if self.config else None
            if self.config and not self.config.is_development_mode():
                max_files = None

            with TemporaryDirectory(prefix="cnpj-extract-") as workspace:
                workspace_path = Path(workspace)
                extracted_files = extract_zip_file(full_path, workspace_path, max_files)

                for extracted_file in extracted_files:
                    relative_key = extracted_file.relative_to(workspace_path).as_posix()
                    if self.storage_backend is not None:
                        self.storage_backend.upload_file(
                            extracted_file,
                            self._object_key("raw/extracted", f"{Path(zip_filename).stem}/{relative_key}"),
                        )

                    local_target = extract_dir / relative_key
                    local_target.parent.mkdir(parents=True, exist_ok=True)
                    with extracted_file.open("rb") as source, local_target.open("wb") as target:
                        shutil.copyfileobj(source, target, length=1024 * 1024)

            audit.processed_at = datetime.now()
            return audit
        except Exception as e:
            logger.error(f"Error extracting file {zip_filename}: {e}")
            raise
        finally:
            logger.info(f"Finished file extraction of file {zip_filename}.")
