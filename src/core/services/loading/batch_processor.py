"""
Batch Processor for loading service.
Handles batch and subbatch processing with audit context management.
"""
from typing import List, Tuple, Optional, Dict, Any, Generator
from datetime import datetime
from contextlib import contextmanager
import uuid
from ....setup.logging import logger
from ....database.models.audit import AuditStatus
from ....setup.config.loader import ConfigLoader


class BatchProcessor:
    """
    Handles batch and subbatch processing with proper audit context management.
    """

    def __init__(
        self, config: Optional[ConfigLoader] = None, audit_service: Optional[Any] = None
    ):
        self.audit_service = audit_service
        self.config = config

    def split_into_subbatches(self, batch_chunk: List[Tuple], subbatch_size: int) -> List[List[Tuple]]:
        """Split batch into subbatches of specified size."""
        return [batch_chunk[i:i + subbatch_size] for i in range(0, len(batch_chunk), subbatch_size)]

    @contextmanager
    def batch_context(
        self,
        table_name: str,
        batch_name: str,
        file_manifest_id: Optional[str] = None,
        table_manifest_id: Optional[str] = None
    ) -> Generator[uuid.UUID, None, None]:
        """Create batch context for audit tracking."""
        if not self.audit_service:
            yield None
            return

        try:
            with self.audit_service.batch_context(
                target_table=table_name,
                batch_name=batch_name,
                file_manifest_id=file_manifest_id,
                table_manifest_id=table_manifest_id
            ) as batch_id:
                yield batch_id
        except Exception as e:
            logger.warning(f"Failed to create batch context: {e}")
            yield None

    @contextmanager
    def subbatch_context(
        self,
        batch_id: Optional[uuid.UUID],
        table_name: str,
        description: str = ""
    ) -> Generator[uuid.UUID, None, None]:
        """Create subbatch context for audit tracking."""
        if not self.audit_service or not batch_id:
            yield None
            return

        try:
            with self.audit_service.subbatch_context(
                batch_id=batch_id,
                table_name=table_name,
                description=description
            ) as subbatch_id:
                yield subbatch_id
        except Exception as e:
            logger.warning(f"Failed to create subbatch context: {e}")
            yield None

    def process_batch_with_context(
        self,
        batch_chunk: List[Tuple],
        loader: Any,
        table_info: Any,
        table_name: str,
        batch_num: int,
        file_manifest_id: Optional[str] = None,
        table_manifest_id: Optional[str] = None,
        recommendations: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[str], int]:
        """
        Process a batch chunk with proper audit context management.
        """
        if not self.audit_service:
            # Fallback without context management
            return self._load_batch_chunk_direct(batch_chunk, loader, table_info)

        # Create batch context
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        batch_name = f"MemoryAware_Batch_{table_name}_chunk{batch_num}_{len(batch_chunk)}rows_{timestamp}"

        print('Teste 2')

        with self.batch_context(
            table_name=table_name,
            batch_name=batch_name,
            file_manifest_id=file_manifest_id,
            table_manifest_id=table_manifest_id
        ) as batch_id:

            logger.info(f"[BatchProcessor] Processing batch {batch_num}: {len(batch_chunk)} rows")

            # Process subbatches
            subbatch_size = recommendations.get('sub_batch_size', 3_000) if recommendations else 3_000
            subbatch_chunks = self.split_into_subbatches(batch_chunk, subbatch_size)

            total_batch_rows = 0

            for subbatch_num, subbatch_chunk in enumerate(subbatch_chunks):
                subbatch_name = f"Subbatch_{subbatch_num+1}of{len(subbatch_chunks)}_rows{len(subbatch_chunk)}"

                with self.subbatch_context(
                    batch_id=batch_id,
                    table_name=table_name,
                    description=subbatch_name
                ) as subbatch_id:

                    success, error, rows = self._load_batch_chunk_direct(subbatch_chunk, loader, table_info)

                    # Record processing event
                    if subbatch_id:
                        status = AuditStatus.COMPLETED if success else AuditStatus.FAILED
                        self.audit_service.collect_file_processing_event(
                            subbatch_id=subbatch_id,
                            status=status,
                            rows=rows or 0,
                            bytes_=0
                        )

                    if not success:
                        return False, error, total_batch_rows

                    total_batch_rows += rows

            return True, None, total_batch_rows

    def _load_batch_chunk_direct(
        self,
        batch_chunk: List[Tuple],
        loader: Any,
        table_info: Any
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load batch chunk directly without temporary files.
        """
        try:
            if not batch_chunk:
                return True, None, 0

            # Check if loader supports direct record loading
            if hasattr(loader, 'load_records_directly'):
                return loader.load_records_directly(
                    table_info=table_info,
                    records=batch_chunk
                )

            # Fallback: use the existing uploader's copy_records_to_table mechanism
            return self._load_records_via_uploader(batch_chunk, loader, table_info)

        except Exception as e:
            logger.error(f"[BatchProcessor] Failed to load batch chunk: {e}")
            return False, str(e), 0

    def _load_records_via_uploader(self, batch_chunk: List[Tuple], loader: Any, table_info: Any) -> Tuple[bool, Optional[str], int]:
        """
        Load records directly via uploader mechanism.
        """
        # This would contain the async uploader logic
        # For now, return a placeholder implementation
        logger.warning("[BatchProcessor] Direct uploader not implemented, using fallback")
        return False, "Direct uploader not implemented", 0