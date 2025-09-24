from pathlib import Path
import os
import gc
import time
from typing import Dict, Optional
from dataclasses import dataclass
import threading

import polars as pl

from ....setup.config import MemoryMonitorConfig
from ....setup.logging import logger

import psutil

@dataclass
class MemorySnapshot:
    """Snapshot of memory usage at a point in time."""
    timestamp: float
    process_rss_mb: float
    process_vms_mb: float
    system_available_mb: float
    system_used_percent: float

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.time()

class MemoryMonitor:
    """
    Enhanced memory monitoring with better cleanup between operations.
    """

    def __init__(self, config: MemoryMonitorConfig):
        self.config = config
        self.baseline_snapshot: Optional[MemorySnapshot] = None
        self.process = None
        self.lock = threading.Lock()
        self.last_cleanup_time = 0
        
        try:
            self.process = psutil.Process()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            logger.warning("Cannot monitor process memory - using fallback mode")
        
        # Establish baseline immediately
        self._establish_baseline()

    def _get_current_memory_info(self) -> MemorySnapshot:
        """Get current comprehensive memory information."""
        if self.process:
            try:
                proc_info = self.process.memory_info()
                sys_info = psutil.virtual_memory()
                
                return MemorySnapshot(
                    timestamp=time.time(),
                    process_rss_mb=proc_info.rss / (1024 * 1024),
                    process_vms_mb=proc_info.vms / (1024 * 1024),
                    system_available_mb=sys_info.available / (1024 * 1024),
                    system_used_percent=sys_info.percent
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                pass
        
        # Fallback when psutil is unavailable
        return MemorySnapshot(
            timestamp=time.time(),
            process_rss_mb=0.0,
            process_vms_mb=0.0,
            system_available_mb=float('inf'),
            system_used_percent=0.0
        )

    def _establish_baseline(self) -> None:
        """Establish baseline memory usage before processing starts."""
        with self.lock:
            # Take multiple samples to get stable baseline
            samples = []
            for _ in range(3):
                samples.append(self._get_current_memory_info())
                time.sleep(0.1)
            
            # Use median values for stability
            rss_values = [s.process_rss_mb for s in samples]
            vms_values = [s.process_vms_mb for s in samples]
            
            rss_values.sort()
            vms_values.sort()
            median_idx = len(rss_values) // 2
            
            self.baseline_snapshot = MemorySnapshot(
                timestamp=time.time(),
                process_rss_mb=rss_values[median_idx],
                process_vms_mb=vms_values[median_idx],
                system_available_mb=samples[-1].system_available_mb,
                system_used_percent=samples[-1].system_used_percent
            )
            
        logger.info(f"Memory baseline established: "
                   f"Process={self.baseline_snapshot.process_rss_mb:.1f}MB, "
                   f"System available={self.baseline_snapshot.system_available_mb:.1f}MB")

    def get_memory_usage_above_baseline(self) -> float:
        """Get current memory usage above the established baseline in MB."""
        if not self.baseline_snapshot:
            return 0.0
            
        current = self._get_current_memory_info()
        return max(0.0, current.process_rss_mb - self.baseline_snapshot.process_rss_mb)

    def get_available_memory_budget(self) -> float:
        """Get remaining memory budget in MB considering baseline."""
        usage_above_baseline = self.get_memory_usage_above_baseline()
        return max(0.0, self.config.memory_limit_mb - usage_above_baseline)

    def get_memory_pressure_level(self) -> float:
        """Get memory pressure as a ratio (0.0 = no pressure, 1.0 = at limit)."""
        usage_above_baseline = self.get_memory_usage_above_baseline()
        if self.config.memory_limit_mb <= 0:
            return 0.0
        return min(1.0, usage_above_baseline / self.config.memory_limit_mb)

    def is_memory_pressure_high(self) -> bool:
        """Check if memory pressure is above cleanup threshold."""
        return self.get_memory_pressure_level() >= self.config.cleanup_threshold_ratio

    def is_memory_limit_exceeded(self) -> bool:
        """Check if memory usage exceeds the configured limit above baseline."""
        usage_above_baseline = self.get_memory_usage_above_baseline()
        return usage_above_baseline > self.config.memory_limit_mb

    def should_prevent_processing(self) -> bool:
        """
        Determine if processing should be prevented due to memory constraints.
        This considers both process memory and system-wide memory availability.
        """
        if self.is_memory_limit_exceeded():
            return True
            
        # Also check system memory availability
        current = self._get_current_memory_info()
        if current.system_available_mb < self.config.baseline_buffer_mb:
            logger.warning(f"Low system memory: {current.system_available_mb:.1f}MB available")
            return True
 
        if current.system_used_percent > 90:
            logger.warning(f"High system memory usage: {current.system_used_percent:.1f}%")
            return True
            
        return False

    def perform_aggressive_cleanup(self) -> Dict[str, float]:
        """
        Perform more aggressive cleanup between large files.
        """
        with self.lock:
            now = time.time()
            if now - self.last_cleanup_time < 0.5:  # Reduced rate limit
                return {"skipped": True}
                
            self.last_cleanup_time = now
            
        before_snapshot = self._get_current_memory_info()
        
        # Multi-stage aggressive cleanup
        cleanup_stats = {
            "before_mb": before_snapshot.process_rss_mb,
            "cleanup_type": "aggressive_inter_file"
        }
        
        # Stage 1: Multiple garbage collections
        for i in range(5):
            gc.collect()
            
        # Stage 2: Clear generation-specific collections
        for gen in range(3):
            try:
                gc.collect(gen)
            except:
                pass
                
        # Stage 3: Force Polars cleanup if available
        try:
            # Clear any cached state in Polars
            pl.clear_schema_cache()
        except:
            pass
            
        # Stage 4: OS-level hints
        try:
            os.sync()  # Flush OS buffers
        except:
            pass
        
        # Brief pause for cleanup to take effect
        time.sleep(0.1)
        
        after_snapshot = self._get_current_memory_info()
        cleanup_stats["after_mb"] = after_snapshot.process_rss_mb
        cleanup_stats["freed_mb"] = max(0, before_snapshot.process_rss_mb - after_snapshot.process_rss_mb)
        
        if cleanup_stats["freed_mb"] > 0:
            logger.info(f"Aggressive cleanup freed {cleanup_stats['freed_mb']:.1f}MB")
        
        return cleanup_stats

    def get_status_report(self) -> Dict[str, any]:
        """Get comprehensive memory status report."""
        current = self._get_current_memory_info()
        usage_above_baseline = self.get_memory_usage_above_baseline()
        budget_remaining = self.get_available_memory_budget()
        pressure = self.get_memory_pressure_level()
        
        return {
            "timestamp": current.timestamp,
            "baseline_mb": self.baseline_snapshot.process_rss_mb if self.baseline_snapshot else 0,
            "current_process_mb": current.process_rss_mb,
            "usage_above_baseline_mb": usage_above_baseline,
            "configured_limit_mb": self.config.memory_limit_mb,
            "budget_remaining_mb": budget_remaining,
            "pressure_level": pressure,
            "system_available_mb": current.system_available_mb,
            "system_used_percent": current.system_used_percent,
            "should_cleanup": self.is_memory_pressure_high(),
            "should_block": self.should_prevent_processing()
        }