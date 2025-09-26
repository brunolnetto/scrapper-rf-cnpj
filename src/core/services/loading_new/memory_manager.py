"""
Memory Manager for loading service.
Handles memory monitoring, checks, and optimization.
"""
from typing import Dict, Any, Optional, List
from pathlib import Path
from ....setup.logging import logger


class MemoryManager:
    """
    Centralized memory management for data loading operations.
    """

    def __init__(self, memory_monitor: Optional[Any] = None):
        self.memory_monitor = memory_monitor

    def get_status_report(self) -> Optional[Dict[str, Any]]:
        """Get current memory status report."""
        if not self.memory_monitor:
            return None
        try:
            return self.memory_monitor.get_status_report()
        except Exception as e:
            logger.warning(f"Failed to get memory status: {e}")
            return None

    def should_prevent_processing(self) -> bool:
        """Check if processing should be prevented due to memory constraints."""
        if not self.memory_monitor:
            return False
        try:
            return self.memory_monitor.should_prevent_processing()
        except Exception as e:
            logger.warning(f"Memory check failed: {e}")
            return False

    def is_memory_pressure_high(self) -> bool:
        """Check if memory pressure is high."""
        if not self.memory_monitor:
            return False
        try:
            return self.memory_monitor.is_memory_pressure_high()
        except Exception as e:
            logger.warning(f"Memory pressure check failed: {e}")
            return False

    def perform_aggressive_cleanup(self) -> Dict[str, Any]:
        """Perform aggressive memory cleanup."""
        if not self.memory_monitor:
            return {"freed_mb": 0}
        try:
            return self.memory_monitor.perform_aggressive_cleanup()
        except Exception as e:
            logger.warning(f"Memory cleanup failed: {e}")
            return {"freed_mb": 0}

    def estimate_memory_requirements(self, file_paths: List[Path]) -> Dict[str, Any]:
        """Estimate memory requirements for processing files."""
        if not self.memory_monitor:
            return {"estimated_memory_per_chunk_mb": 100}

        total_estimated = 0
        for file_path in file_paths:
            try:
                # Rough estimation based on file size
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                # Estimate ~2x file size for processing overhead
                total_estimated += file_size_mb * 2
            except Exception as e:
                logger.warning(f"Could not estimate memory for {file_path}: {e}")
                total_estimated += 100  # Conservative default

        return {
            "estimated_memory_per_chunk_mb": total_estimated / max(len(file_paths), 1),
            "total_estimated_mb": total_estimated
        }

    def log_memory_status(self, operation: str = "operation"):
        """Log current memory status."""
        status = self.get_status_report()
        if status:
            logger.info(f"[MemoryManager] {operation} - "
                       f"Usage: {status.get('usage_above_baseline_mb', 0):.1f}MB, "
                       f"Budget: {status.get('budget_remaining_mb', 0):.1f}MB, "
                       f"Pressure: {status.get('pressure_level', 0):.2f}")
        else:
            logger.info(f"[MemoryManager] {operation} - No memory monitor available")