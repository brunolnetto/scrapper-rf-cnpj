from __future__ import annotations

from pathlib import Path

from ....core.storage.factory import create_storage_backend


class FileDownloadService:
    def __init__(self, max_workers: int = None, config: object = None, storage_backend=None):
        self.config = config
        self.max_workers = max_workers
        self.storage_backend = storage_backend or None
        if self.storage_backend is None:
            # Default to local filesystem behavior for backward compatibility.
            self.storage_backend = None

    def _upload_local_file_if_needed(self, source_path: str, destination_key: str):
        if self.storage_backend is None:
            return source_path
        return self.storage_backend.upload_file(source_path, destination_key)

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

    def _download_zipfile(self, audit, url, zip_filename, download_path, has_progress_bar):
        file_url = f"{url.rstrip('/')}/{zip_filename}"
        download_dir = Path(download_path)
        download_dir.mkdir(parents=True, exist_ok=True)
        local_filename = download_dir / zip_filename

        import requests

        try:
            logger.info(f"Starting download for file: {zip_filename}")
            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=131072):
                        if chunk:
                            f.write(chunk)

            if self.storage_backend is not None:
                destination_key = f"{Path(download_path).name}/{zip_filename}"
                self._upload_local_file_if_needed(str(local_filename), destination_key)

            logger.info(f"Successfully downloaded file: {zip_filename}")
            return audit
        except Exception:
            raise

    def _extract_zipfile(self, audit, zip_filename, download_path, extract_path):
        full_path = Path(download_path) / zip_filename
        extract_dir = Path(extract_path)
        extract_dir.mkdir(parents=True, exist_ok=True)

        try:
            logger.info(f"Extracting file {zip_filename}...")
            with ZipFile(full_path, "r") as zip_ref:
                file_list = zip_ref.namelist()
                if self.config and getattr(self.config, "is_development_mode", lambda: False)():
                    max_files = self.config.get_max_files_per_blob()
                    if max_files is not None and len(file_list) > max_files:
                        file_list = sorted(file_list)[:max_files]
                for file_name in file_list:
                    zip_ref.extract(file_name, str(extract_dir))

            if self.storage_backend is not None:
                for extracted_file in extract_dir.iterdir():
                    if extracted_file.name.endswith(".csv") or extracted_file.name.endswith(".txt"):
                        destination_key = f"{Path(extract_path).name}/{extracted_file.name}"
                        self._upload_local_file_if_needed(str(extracted_file), destination_key)

        finally:
            logger.info(f"Finished file extraction of file {zip_filename}.")
            return audit
