"""Unified audit management service for the CNPJ ETL project.

This service centralizes audit creation, validation, and insertion logic for use by ETL and data loading components.
Includes comprehensive manifest tracking, file integrity verification, and batch tracking capabilities.

The implementation is split by responsibility into mixins (table_audit,
file_audit, batch_audit, metrics); ``AuditService`` composes them.
"""

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Dict, Optional, Union

from ....database.engine import Database
from ....database.models.audit import AuditStatus
from ....setup.config import ConfigurationService, get_config
from .accumulator import BatchAccumulator, BatchTimeout
from .batch_audit import BatchAuditMixin
from .file_audit import FileAuditMixin
from .metrics import AuditMetricsMixin
from .repository import AuditRepository
from .table_audit import TableAuditMixin

__all__ = ["AuditService", "BatchAccumulator", "BatchTimeout"]


class AuditService(TableAuditMixin, FileAuditMixin, BatchAuditMixin, AuditMetricsMixin):
    """Unified audit and batch tracking service with comprehensive manifest capabilities and timeout support."""

    def __init__(self, database: Database, config: Optional[ConfigurationService] = None, 
                 timeout_config: Optional[BatchTimeout] = None):
        self.database = database
        self.config = config or get_config()
        self.repository = AuditRepository(database)
        self.timeout_config = timeout_config or BatchTimeout()
        
        # Batch tracking state with enhanced concurrency
        self._active_batches: Dict[str, BatchAccumulator] = {}
        self._active_subbatches: Dict[str, BatchAccumulator] = {}
        self._subbatch_to_batch: Dict[str, str] = {}
        self._metrics_lock = Lock()
        
        # Async support for database operations
        self._executor: Optional[ThreadPoolExecutor] = None
        self._async_enabled = getattr(self.config.pipeline.loading, 'enable_internal_parallelism', True)
        
        if self._async_enabled:
            max_workers = getattr(self.config.pipeline.loading, 'internal_concurrency', 3)
            self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="audit-worker")

    def delete_period_audits(self, year: int, month: int) -> Dict[str, int]:
        """Delete all audit rows for an ingestion period (used by --force-reload)."""
        return self.repository.delete_period_audits(year, month)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

    def shutdown(self):
        """Shutdown the service and cleanup resources."""
        # Cancel all active batches
        with self._metrics_lock:
            for accumulator in list(self._active_batches.values()):
                accumulator.cancel()
            for accumulator in list(self._active_subbatches.values()):
                accumulator.cancel()
        
        # Shutdown thread pool
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

    def _coerce_status(self, status: Union[AuditStatus, str]) -> AuditStatus:
        """Coerce a status value (enum or string) into an AuditStatus enum.

        - If already an AuditStatus, return as-is.
        - If string, try mapping by name (case-insensitive) then by value.
        - On failure, return AuditStatus.PENDING as safe default.
        """
        try:
            if isinstance(status, AuditStatus):
                return status
            if isinstance(status, str):
                try:
                    return AuditStatus[status.strip().upper()]
                except KeyError:
                    try:
                        return AuditStatus(status)
                    except ValueError:
                        return AuditStatus.PENDING
        except (TypeError, AttributeError):
            return AuditStatus.PENDING
        return AuditStatus.PENDING
