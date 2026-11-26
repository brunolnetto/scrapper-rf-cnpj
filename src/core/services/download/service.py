import errno
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
from ....core.storage import (
    StorageBackend,
    create_storage_backend,
    persist_artifact_manifest,
    read_artifact_manifest,
)

CONNECT_TIMEOUT_SECONDS = 30
DEFAULT_READ_TIMEOUT_SECONDS = 300


class IncompleteDownloadError(IOError):
    """The number of bytes written differs from the advertised Content-Length."""


class DownloadError(RuntimeError):
    """Raised when one or more files could not be downloaded or extracted.

    ``failures`` maps each affected table (audit entity) to the error message
    so callers can report exactly what is missing.
    """

    def __init__(self, failures: dict[str, str]):
        self.failures = dict(failures)
        details = "; ".join(f"{name}: {error}" for name, error in self.failures.items())
        super().__init__(
            f"{len(self.failures)} file group(s) failed to download/extract: {details}"
        )


def _move_into_place(source: Path, target: Path) -> None:
    """Rename *source* to *target*, copying only across filesystems."""
    try:
        os.replace(source, target)
    except OSError as e:
        if e.errno != errno.EXDEV:
            raise
        tmp_target = target.with_name(f".{target.name}.partial")
        with source.open("rb") as src, tmp_target.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        os.replace(tmp_target, target)
        source.unlink()


def _audit_label(audit) -> str:
    return str(getattr(audit, "entity_name", None) or getattr(audit, "source_files", None) or audit)


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

    def _record_storage_manifest(self, audit, artifact_path, object_key, stage, source_key=None):
        if self.storage_backend is None or self.config is None:
            return None

        year = self.config.year or self.config.pipeline.year
        month = self.config.month or self.config.pipeline.month
        if year is None or month is None:
            raise ValueError("Artifact manifests require an ingestion year and month")

        manifest_key = persist_artifact_manifest(
            self.storage_backend,
            artifact_path,
            object_key,
            stage=stage,
            year=year,
            month=month,
            source_key=source_key,
        )
        notes = dict(getattr(audit, "notes", None) or {})
        manifests = list(notes.get("storage_manifests", []))
        if manifest_key not in manifests:
            manifests.append(manifest_key)
        notes["storage_manifests"] = manifests
        audit.notes = notes
        return manifest_key

    def _request_timeout(self) -> tuple[int, int]:
        """(connect, read) timeout for HTTP requests.

        The read timeout bounds the wait between received bytes, not the
        whole transfer, so it is safe for multi-GB files.
        """
        download_config = getattr(getattr(self.config, "pipeline", None), "download", None)
        read_timeout = getattr(download_config, "timeout_seconds", None) or DEFAULT_READ_TIMEOUT_SECONDS
        return CONNECT_TIMEOUT_SECONDS, int(read_timeout)

    def _reuse_existing_zip(self, local_filename: Path, object_key: str, expected_size: int) -> str | None:
        """Reuse a previously downloaded archive instead of downloading it again.

        Returns ``"local"`` when the local file already has the expected size,
        ``"storage"`` when it was restored from durable storage (whose manifest
        records the expected size), or ``None`` when it must be downloaded.
        Without a known ``expected_size`` nothing is reused.
        """
        if expected_size <= 0:
            return None

        if local_filename.is_file() and local_filename.stat().st_size == expected_size:
            return "local"

        if self.storage_backend is None or self.config is None:
            return None
        try:
            if not self.storage_backend.exists(object_key):
                return None
            manifest = read_artifact_manifest(self.storage_backend, object_key)
            if not manifest or manifest.get("size_bytes") != expected_size:
                return None
            self.storage_backend.download_file(object_key, local_filename)
        except Exception as e:
            logger.warning(f"Could not restore {object_key} from storage, downloading instead: {e}")
            local_filename.unlink(missing_ok=True)
            return None

        if local_filename.stat().st_size != expected_size:
            local_filename.unlink(missing_ok=True)
            return None
        return "storage"

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
        failures: dict[str, str] = {}
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_audit = {
                executor.submit(
                    self._download_and_extract_files,
                    audit,
                    url,
                    download_path,
                    extract_path,
                    False,
                ): audit
                for audit in audits
            }
            for future in tqdm(
                as_completed(future_to_audit),
                total=len(audits),
                desc="Processing Files",
                unit="file",
            ):
                audit = future_to_audit[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    label = _audit_label(audit)
                    logger.error(f"Error downloading or extracting files for {label}: {e}")
                    failures[label] = str(e)
        if failures:
            raise DownloadError(failures)
        return results

    def _process_files_serial(self, url, audits, download_path, extract_path):
        failures: dict[str, str] = {}
        results = []
        total_count = len(audits)
        for index, audit in enumerate(audits, start=1):
            try:
                results.append(
                    self._download_and_extract_files(
                        audit, url, download_path, extract_path, True
                    )
                )
            except Exception as e:
                label = _audit_label(audit)
                logger.error(f"Error downloading or extracting files for {label}: {e}")
                failures[label] = str(e)
            finally:
                logger.info(
                    f"({index}/{total_count}) files processed. "
                    f"{len(failures)} errors: {list(failures)}"
                )
        if failures:
            raise DownloadError(failures)
        return results

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
            return self._extract_zipfile(
                audit, zip_filename, download_path, extract_path
            )
        except Exception as e:
            raise RuntimeError(f"{zip_filename}: {e}") from e

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
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
            object_key = (
                self._object_key("raw/zip", zip_filename)
                if self.storage_backend is not None and self.config is not None
                else None
            )
            with requests.get(
                file_url, stream=True, headers=_headers, timeout=self._request_timeout()
            ) as r:
                r.raise_for_status()
                total_size = int(r.headers.get("Content-Length", 0))
                # Only the headers have been read so far: when an identical
                # archive is already available the body is never transferred.
                reused = self._reuse_existing_zip(local_filename, object_key, total_size)
                if reused is None:
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

            if reused is not None:
                logger.info(f"Reusing {zip_filename} from {reused} ({total_size} bytes); skipping download")
                if (
                    reused == "local"
                    and self.storage_backend is not None
                    and object_key is not None
                    and not self.storage_backend.exists(object_key)
                ):
                    self.storage_backend.upload_file(local_filename, object_key)
                    self._record_storage_manifest(audit, local_filename, object_key, "raw/zip")
                audit.downloaded_at = datetime.now()
                return audit

            written = local_filename.stat().st_size
            if total_size and written != total_size:
                raise IncompleteDownloadError(
                    f"expected {total_size} bytes, got {written}"
                )
            if self.storage_backend is not None:
                self.storage_backend.upload_file(
                    local_filename,
                    self._object_key("raw/zip", zip_filename),
                )
                self._record_storage_manifest(
                    audit,
                    local_filename,
                    self._object_key("raw/zip", zip_filename),
                    "raw/zip",
                )
            logger.info(f"Successfully downloaded file: {zip_filename}")
            audit.downloaded_at = datetime.now()
            # file_size_bytes removed - file size tracking moved to file manifest level
        except Exception as e:
            logger.error(f"Error downloading {zip_filename}: {e}")
            if has_progress_bar and progress:
                progress.close()
            # Never leave a partial archive behind for a later step to trust.
            local_filename.unlink(missing_ok=True)
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

            # The workspace lives next to the extraction directory (same
            # filesystem) so extracted files are renamed into place instead of
            # copied, keeping peak disk usage at one copy of the CSVs.
            with TemporaryDirectory(prefix=".cnpj-extract-", dir=extract_dir.parent) as workspace:
                workspace_path = Path(workspace)
                extracted_files = extract_zip_file(full_path, workspace_path, max_files)

                for extracted_file in extracted_files:
                    relative_key = extracted_file.relative_to(workspace_path).as_posix()
                    if self.storage_backend is not None:
                        self.storage_backend.upload_file(
                            extracted_file,
                            self._object_key("raw/extracted", f"{Path(zip_filename).stem}/{relative_key}"),
                        )
                        self._record_storage_manifest(
                            audit,
                            extracted_file,
                            self._object_key("raw/extracted", f"{Path(zip_filename).stem}/{relative_key}"),
                            "raw/extracted",
                            source_key=self._object_key("raw/zip", zip_filename),
                        )

                    local_target = extract_dir / relative_key
                    local_target.parent.mkdir(parents=True, exist_ok=True)
                    _move_into_place(extracted_file, local_target)

            audit.processed_at = datetime.now()
            return audit
        except Exception as e:
            logger.error(f"Error extracting file {zip_filename}: {e}")
            raise
        finally:
            logger.info(f"Finished file extraction of file {zip_filename}.")
