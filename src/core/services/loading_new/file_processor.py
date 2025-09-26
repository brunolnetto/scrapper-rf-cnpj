"""
File Processor for loading service.
Handles file loading, format detection, and processing.
"""
from typing import List, Tuple, Optional, Dict, Any, Union
from pathlib import Path
from ....setup.logging import logger
from .file_loader import create_file_loader


class FileProcessor:
    """
    Handles file loading operations with memory awareness.
    """

    def __init__(self, config: Any, memory_monitor: Optional[Any] = None):
        self.config = config
        self.memory_monitor = memory_monitor

    def load_csv_file(
        self,
        table_info: Any,
        file_path: Path,
        batch_processor: Optional[Any] = None,
        recommendations: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load a single CSV file with memory monitoring and batch processing.
        """
        try:
            # Create memory-aware file loader
            encoding = getattr(self.config.pipeline.data_source, 'encoding', 'utf-8')
            file_loader = create_file_loader(str(file_path), encoding=encoding, memory_monitor=self.memory_monitor)

            logger.info(f"[FileProcessor] FileLoader detected format: {file_loader.get_format()}")

            # Get processing recommendations
            if not recommendations:
                recommendations = file_loader.get_recommended_processing_params()
            logger.debug(f"[FileProcessor] Processing recommendations: {recommendations}")

            # Use batch processing if available
            if batch_processor:
                return batch_processor.process_batch_with_context(
                    file_loader.batch_generator(table_info.columns, chunk_size=recommendations.get('final_chunk_size', recommendations.get('chunk_size', 20_000))),
                    file_loader,
                    table_info,
                    1,  # batch_num
                    recommendations=recommendations
                )
            else:
                # Fallback: process without batch context
                return self._process_file_without_context(file_loader, table_info, recommendations)

        except Exception as e:
            logger.error(f"[FileProcessor] Failed to load file {file_path}: {e}")
            return False, str(e), 0

    def _process_file_without_context(
        self,
        file_loader: Any,
        table_info: Any,
        recommendations: Dict[str, Any]
    ) -> Tuple[bool, Optional[str], int]:
        """
        Process file without batch context management.
        """
        total_processed_rows = 0
        headers = table_info.columns

        # Get recommended chunk size
        chunk_size = recommendations.get('final_chunk_size', recommendations.get('chunk_size', 20_000))

        try:
            # Process file in batches
            for batch_chunk in file_loader.batch_generator(headers, chunk_size=chunk_size):
                # Memory check before each batch
                if self.memory_monitor and self.memory_monitor.should_prevent_processing():
                    error_msg = f"Memory limit exceeded during processing"
                    logger.error(f"[FileProcessor] {error_msg}")
                    return False, error_msg, total_processed_rows

                # Process batch directly
                success, error, rows = self._load_batch_chunk_direct(batch_chunk, loader, table_info)

                if not success:
                    return False, error, total_processed_rows

                total_processed_rows += rows
                logger.debug(f"[FileProcessor] Batch completed: {rows} rows")

            logger.info(f"[FileProcessor] File processing complete: {total_processed_rows} total rows")
            return True, None, total_processed_rows

        except Exception as e:
            logger.error(f"[FileProcessor] File processing failed: {e}")
            return False, str(e), total_processed_rows

    def _load_batch_chunk_direct(
        self,
        batch_chunk: List[Tuple],
        loader: Any,
        table_info: Any
    ) -> Tuple[bool, Optional[str], int]:
        """
        Load batch chunk directly.
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
            logger.error(f"[FileProcessor] Failed to load batch chunk: {e}")
            return False, str(e), 0

    def _load_records_via_uploader(self, batch_chunk: List[Tuple], loader: Any, table_info: Any) -> Tuple[bool, Optional[str], int]:
        """
        Load records directly via uploader mechanism.
        """
        # Placeholder for async uploader logic
        logger.warning("[FileProcessor] Direct uploader not implemented")
        return False, "Direct uploader not implemented", 0

    def get_file_size(self, file_path: Path) -> int:
        """Get file size in bytes."""
        try:
            return file_path.stat().st_size
        except Exception as e:
            logger.warning(f"Could not get size for {file_path}: {e}")
            return 0

    def optimize_processing_order(self, file_paths: List[Path]) -> List[Path]:
        """Optimize processing order by file size (smallest first)."""
        try:
            file_sizes = []
            for file_path in file_paths:
                size = self.get_file_size(file_path)
                file_sizes.append((file_path, size))

            # Sort by size (smallest first)
            file_sizes.sort(key=lambda x: x[1])
            optimized_order = [file_path for file_path, _ in file_sizes]

            logger.info(f"[FileProcessor] Optimized processing order: {[str(p.name) for p in optimized_order]}")
            return optimized_order

        except Exception as e:
            logger.error(f"Failed to optimize processing order: {e}")
            return file_paths