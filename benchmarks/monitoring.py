"""Real-time monitoring with alerts and dashboards."""

import threading
import time
from typing import Callable, Optional, List, Dict
from dataclasses import dataclass
import logging
import psutil

logger = logging.getLogger(__name__)

@dataclass
class MemoryAlert:
    """Memory usage alert."""
    timestamp: float
    current_gb: float
    threshold_gb: float
    message: str
    severity: str  # "warning" | "critical"

class RealTimeMonitor:
    """Monitor benchmarks with real-time alerts."""
    
    def __init__(
        self,
        warning_threshold: float = 0.7,  # Alert at 70% memory
        critical_threshold: float = 0.9,  # Critical at 90% memory
        check_interval: float = 1.0  # Check every second
    ):
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.check_interval = check_interval
        self.alerts: List[MemoryAlert] = []
        self.monitoring = False
        self.monitor_thread = None
        self.memory_limit_gb = 8.0
        self.on_warning: Optional[Callable] = None
        self.on_critical: Optional[Callable] = None
    
    def start(self, memory_limit_gb: float):
        """Start monitoring."""
        if self.monitoring:
            return
            
        self.memory_limit_gb = memory_limit_gb
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info(f"Started real-time monitoring with {memory_limit_gb}GB limit")
    
    def stop(self):
        """Stop monitoring."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        logger.info("Stopped real-time monitoring")
    
    def _monitor_loop(self):
        """Background monitoring loop."""
        while self.monitoring:
            try:
                # Get current memory usage
                mem = psutil.virtual_memory()
                used_gb = (mem.total - mem.available) / (1024**3)
                usage_ratio = used_gb / self.memory_limit_gb
                
                # Check thresholds
                if usage_ratio >= self.critical_threshold:
                    alert = MemoryAlert(
                        timestamp=time.time(),
                        current_gb=used_gb,
                        threshold_gb=self.memory_limit_gb * self.critical_threshold,
                        message=f"CRITICAL: Memory usage {used_gb:.1f}GB ({usage_ratio:.1%}) exceeds critical threshold",
                        severity="critical"
                    )
                    self.alerts.append(alert)
                    logger.critical(alert.message)
                    
                    if self.on_critical:
                        self.on_critical(alert)
                        
                elif usage_ratio >= self.warning_threshold:
                    alert = MemoryAlert(
                        timestamp=time.time(),
                        current_gb=used_gb,
                        threshold_gb=self.memory_limit_gb * self.warning_threshold,
                        message=f"WARNING: Memory usage {used_gb:.1f}GB ({usage_ratio:.1%}) exceeds warning threshold",
                        severity="warning"
                    )
                    self.alerts.append(alert)
                    logger.warning(alert.message)
                    
                    if self.on_warning:
                        self.on_warning(alert)
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.check_interval)
    
    def get_alert_summary(self) -> Dict[str, int]:
        """Get summary of alerts."""
        return {
            "total_alerts": len(self.alerts),
            "warnings": sum(1 for a in self.alerts if a.severity == "warning"),
            "critical": sum(1 for a in self.alerts if a.severity == "critical")
        }