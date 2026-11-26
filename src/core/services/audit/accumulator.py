"""Batch timeout configuration and thread-safe metric accumulator for batch/subbatch audits."""

import time
from dataclasses import dataclass, field
from threading import Event, Lock, Timer
from typing import Any, Dict, Optional

from ....database.models.audit import AuditStatus
from ....setup.logging import logger


@dataclass
class BatchTimeout:
    """Configuration for batch operation timeouts and watchdogs."""
    operation_timeout_seconds: float = 3600.0  # 1 hour default
    watchdog_interval_seconds: float = 300.0   # 5 minutes check interval
    max_idle_seconds: float = 1800.0           # 30 minutes max idle time
    enable_watchdog: bool = True


@dataclass
class BatchAccumulator:
    """Thread-safe metrics accumulator with timeout and watchdog capabilities."""
    files_completed: int = 0
    files_failed: int = 0
    total_rows: int = 0
    total_bytes: int = 0
    start_time: float = field(default_factory=time.perf_counter)
    last_activity: float = field(default_factory=time.perf_counter)
    timeout_config: BatchTimeout = field(default_factory=BatchTimeout)
    
    # Concurrency control
    _lock: Lock = field(default_factory=Lock)
    _cancelled: Event = field(default_factory=Event)
    _watchdog_timer: Optional[Timer] = field(default=None, init=False)

    def add_file_event(self, status: AuditStatus, rows: int = 0, bytes_: int = 0):
        """Add a file processing event to the accumulator with thread safety.

        Accepts legacy status strings and normalizes them to the
        AuditStatus values before updating metrics.
        """
        with self._lock:
            if self._cancelled.is_set():
                logger.warning("Attempting to add event to cancelled batch accumulator")
                return
                
            # Accept either AuditStatus instances or legacy status strings and normalize
            norm_status = None
            try:
                if isinstance(status, AuditStatus):
                    norm_status = status
                elif isinstance(status, str):
                    # try mapping by name (case-insensitive) then by value
                    try:
                        norm_status = AuditStatus[status.strip().upper()]
                    except KeyError:
                        try:
                            norm_status = AuditStatus(status)
                        except ValueError:
                            norm_status = AuditStatus.PENDING
                else:
                    norm_status = AuditStatus.PENDING
            except (TypeError, AttributeError):
                norm_status = AuditStatus.PENDING

            if norm_status == AuditStatus.COMPLETED:
                self.files_completed += 1
                self.total_rows += int(rows or 0)
                try:
                    self.total_bytes += int(bytes_ or 0)
                except (TypeError, ValueError):
                    logger.debug(f"Ignoring non-numeric byte count {bytes_!r} in batch metrics")
            elif norm_status == AuditStatus.FAILED:
                self.files_failed += 1
            
            # Update last activity timestamp
            self.last_activity = time.perf_counter()
    
    def is_expired(self) -> bool:
        """Check if the batch operation has exceeded timeout limits."""
        current_time = time.perf_counter()
        elapsed = current_time - self.start_time
        idle_time = current_time - self.last_activity
        
        return (elapsed > self.timeout_config.operation_timeout_seconds or 
                idle_time > self.timeout_config.max_idle_seconds)
    
    def cancel(self) -> None:
        """Cancel the batch operation and stop watchdog."""
        with self._lock:
            self._cancelled.set()
            if self._watchdog_timer:
                self._watchdog_timer.cancel()
                self._watchdog_timer = None
    
    def is_cancelled(self) -> bool:
        """Check if the batch has been cancelled."""
        return self._cancelled.is_set()
        
    def start_watchdog(self, callback) -> None:
        """Start watchdog timer for monitoring batch progress."""
        if not self.timeout_config.enable_watchdog:
            return
            
        def watchdog_check():
            if self.is_expired() and not self.is_cancelled():
                logger.warning("Batch watchdog triggered - operation timeout or idle")
                callback(self)
            elif not self.is_cancelled():
                # Schedule next check
                self._watchdog_timer = Timer(self.timeout_config.watchdog_interval_seconds, watchdog_check)
                # Daemon: a forgotten watchdog must never keep the process alive.
                self._watchdog_timer.daemon = True
                self._watchdog_timer.start()
        
        with self._lock:
            if not self._watchdog_timer and not self._cancelled.is_set():
                self._watchdog_timer = Timer(self.timeout_config.watchdog_interval_seconds, watchdog_check)
                # Daemon: a forgotten watchdog must never keep the process alive.
                self._watchdog_timer.daemon = True
                self._watchdog_timer.start()
    
    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """Get thread-safe snapshot of current metrics."""
        with self._lock:
            return {
                'files_completed': self.files_completed,
                'files_failed': self.files_failed,
                'total_rows': self.total_rows,
                'total_bytes': self.total_bytes,
                'elapsed_seconds': time.perf_counter() - self.start_time,
                'idle_seconds': time.perf_counter() - self.last_activity,
                'is_cancelled': self.is_cancelled(),
                'is_expired': self.is_expired()
            }
