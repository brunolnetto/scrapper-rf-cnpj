from dataclasses import dataclass
import time
import threading
import gc
import os
from typing import Optional, Dict, Callable, List, Any

import psutil

# Assume MemoryMonitorConfig is your pydantic model. Example added fields expected:
# - memory_limit_mb (int)
# - cleanup_threshold_ratio (float)
# - baseline_buffer_mb (int)
# - memory_limit_mode: "absolute" | "fraction"  (optional)
# - memory_limit_fraction: float (if using fraction)
# - baseline_samples: int
# - warmup_seconds: float
# - cleanup_rate_limit_seconds: float
# - smoothing_alpha: float

@dataclass
class MemorySnapshot:
    timestamp: Optional[float] = None
    process_rss_mb: float = 0.0
    process_vms_mb: float = 0.0
    system_total_mb: float = 0.0
    system_available_mb: float = 0.0
    system_used_percent: float = 0.0

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


class MemoryMonitor:
    def __init__(
        self,
        config,
        on_cleanup: Optional[Callable[[], None]] = None,
    ):
        self.config = config
        self.on_cleanup = on_cleanup  # callback to clear caches / drop references
        self.lock = threading.Lock()
        self.last_cleanup_time = 0.0
        self.ewma_pressure = 0.0  # smoothed pressure
        self.process = None

        # defaults if not present in config
        self.config.baseline_samples = getattr(self.config, "baseline_samples", 7)
        self.config.warmup_seconds = getattr(self.config, "warmup_seconds", 1.0)
        self.config.cleanup_rate_limit_seconds = getattr(self.config, "cleanup_rate_limit_seconds", 1.0)
        self.config.smoothing_alpha = getattr(self.config, "smoothing_alpha", 0.25)
        self.config.memory_limit_mode = getattr(self.config, "memory_limit_mode", "absolute")
        self.config.memory_limit_fraction = getattr(self.config, "memory_limit_fraction", 0.5)

        try:
            self.process = psutil.Process()
        except Exception:
            self.process = None

        self.baseline_snapshot: Optional[MemorySnapshot] = None
        self._establish_baseline()

    def _get_sys(self) -> Any:
        return psutil.virtual_memory()

    def _get_current_memory_info(self) -> MemorySnapshot:
        if self.process:
            try:
                proc_info = self.process.memory_info()
                sys_info = self._get_sys()
                return MemorySnapshot(
                    timestamp=time.time(),
                    process_rss_mb=proc_info.rss / (1024*1024),
                    process_vms_mb=proc_info.vms / (1024*1024),
                    system_total_mb=sys_info.total / (1024*1024),
                    system_available_mb=sys_info.available / (1024*1024),
                    system_used_percent=sys_info.percent,
                )
            except Exception:
                pass

        sys_info = self._get_sys()
        return MemorySnapshot(
            timestamp=time.time(),
            process_rss_mb=0.0,
            process_vms_mb=0.0,
            system_total_mb=sys_info.total / (1024*1024),
            system_available_mb=sys_info.available / (1024*1024),
            system_used_percent=sys_info.percent,
        )

    def _establish_baseline(self) -> None:
        """Trimmed median baseline over a short warmup window — more robust to noise."""
        with self.lock:
            samples: List[MemorySnapshot] = []
            # small warmup
            if getattr(self.config, "warmup_seconds", 0) > 0:
                time.sleep(self.config.warmup_seconds)

            for _ in range(self.config.baseline_samples):
                samples.append(self._get_current_memory_info())
                time.sleep(0.05)

            # Use median for process RSS/VMS and median available/used
            rss = sorted(s.process_rss_mb for s in samples)
            vms = sorted(s.process_vms_mb for s in samples)
            avail = sorted(s.system_available_mb for s in samples)
            used_pct = sorted(s.system_used_percent for s in samples)
            idx = len(rss) // 2

            self.baseline_snapshot = MemorySnapshot(
                timestamp=time.time(),
                process_rss_mb=rss[idx],
                process_vms_mb=vms[idx],
                system_total_mb=samples[idx].system_total_mb,
                system_available_mb=avail[idx],
                system_used_percent=used_pct[idx],
            )

    def _effective_memory_limit_mb(self, sys_total_mb: float) -> float:
        """Return the effective memory cap (absolute MB) used to compute process_pressure."""
        if self.config.memory_limit_mode == "fraction":
            return max(1.0, sys_total_mb * float(self.config.memory_limit_fraction))
        # absolute semantics: configured is budget above baseline (legacy behavior)
        return float(self.config.memory_limit_mb)

    def _update_ewma(self, pressure: float) -> float:
        alpha = float(getattr(self.config, "smoothing_alpha", 0.25))
        self.ewma_pressure = alpha * pressure + (1 - alpha) * self.ewma_pressure
        return self.ewma_pressure

    def get_memory_usage_above_baseline(self) -> float:
        if not self.baseline_snapshot:
            return 0.0
        cur = self._get_current_memory_info()
        return max(0.0, cur.process_rss_mb - self.baseline_snapshot.process_rss_mb)

    def get_memory_pressure_level(self) -> float:
        """Combine process and system pressures into a single [0..1] metric and smooth it."""
        cur = self._get_current_memory_info()
        usage_above = max(0.0, cur.process_rss_mb - (self.baseline_snapshot.process_rss_mb if self.baseline_snapshot else 0.0))
        effective_limit = self._effective_memory_limit_mb(cur.system_total_mb)

        # process pressure
        process_pressure = 0.0
        if effective_limit > 0:
            process_pressure = min(1.0, usage_above / effective_limit)

        # system pressure: 0 if available comfortably above buffer, 1 if at or below buffer
        buffer_mb = float(self.config.baseline_buffer_mb)
        system_pressure = 0.0
        if cur.system_available_mb <= buffer_mb:
            system_pressure = 1.0
        else:
            # scale between buffer and (buffer + effective_limit) to avoid false positives on small systems
            denom = max(1.0, effective_limit)
            system_pressure = max(0.0, 1.0 - ((cur.system_available_mb - buffer_mb) / denom))
            system_pressure = min(1.0, system_pressure)

        combined = max(process_pressure, system_pressure)
        return self._update_ewma(combined)

    def is_memory_pressure_high(self) -> bool:
        pressure = self.get_memory_pressure_level()
        return pressure >= float(self.config.cleanup_threshold_ratio)

    def is_memory_limit_exceeded(self) -> bool:
        usage = self.get_memory_usage_above_baseline()
        cur = self._get_current_memory_info()
        eff = self._effective_memory_limit_mb(cur.system_total_mb)
        return usage > eff

    def should_prevent_processing(self) -> bool:
        cur = self._get_current_memory_info()
        # absolute safety: if system availability is below buffer, prevent
        if cur.system_available_mb < float(self.config.baseline_buffer_mb):
            return True
        # if smoothed pressure exceeds 0.95, definitely block
        if self.get_memory_pressure_level() >= 0.95:
            return True
        # otherwise defer to is_memory_limit_exceeded
        return self.is_memory_limit_exceeded()

    def perform_aggressive_cleanup(self) -> Dict[str, float]:
        with self.lock:
            now = time.time()
            if now - self.last_cleanup_time < float(self.config.cleanup_rate_limit_seconds):
                return {"skipped": True}
            self.last_cleanup_time = now

        before = self._get_current_memory_info()
        freed = 0.0

        # Stage 0: user-provided cleanup hook (preferred)
        if callable(self.on_cleanup):
            try:
                self.on_cleanup()
            except Exception:
                pass

        # Stage 1: multiple GC passes but measure after each; stop early if no change
        prev_rss = before.process_rss_mb
        for i in range(3):
            gc.collect()
            time.sleep(0.03)
            cur = self._get_current_memory_info()
            if cur.process_rss_mb >= prev_rss - 0.5:  # less than 0.5MB change -> stop
                break
            prev_rss = cur.process_rss_mb

        # Stage 2: optional OS-level trim (glibc) — best-effort
        try:
            libc = None
            if os.name == "posix":
                import ctypes
                libc = ctypes.CDLL("libc.so.6")
                try:
                    libc.malloc_trim(0)
                except Exception:
                    pass
        except Exception:
            pass

        # Stage 3: sync (best-effort)
        try:
            if hasattr(os, "sync"):
                os.sync()
        except Exception:
            pass

        after = self._get_current_memory_info()
        freed = max(0.0, before.process_rss_mb - after.process_rss_mb)
        result = {
            "before_mb": before.process_rss_mb,
            "after_mb": after.process_rss_mb,
            "freed_mb": freed,
            "skipped": False,
        }
        return result

    def get_status_report(self) -> Dict[str, any]:
        cur = self._get_current_memory_info()
        usage_above = self.get_memory_usage_above_baseline()
        pressure = self.get_memory_pressure_level()
        eff_limit = self._effective_memory_limit_mb(cur.system_total_mb)

        # Backwards-compatible budget_remaining_mb:
        budget_remaining_mb = max(0.0, eff_limit - usage_above)

        # Helpful additional fields for callers:
        budget_remaining_percent = (budget_remaining_mb / max(1.0, eff_limit)) if eff_limit > 0 else 0.0
        configured_limit_mb = float(self.config.memory_limit_mb) if hasattr(self.config, "memory_limit_mb") else eff_limit

        return {
            "timestamp": cur.timestamp,
            "baseline_mb": self.baseline_snapshot.process_rss_mb if self.baseline_snapshot else 0.0,
            "current_process_mb": cur.process_rss_mb,
            "usage_above_baseline_mb": usage_above,
            "effective_limit_mb": eff_limit,
            "configured_limit_mb": configured_limit_mb,
            "budget_remaining_mb": budget_remaining_mb,         # <--- legacy key callers expect
            "budget_remaining_percent": budget_remaining_percent,
            "pressure_level": pressure,
            "system_available_mb": cur.system_available_mb,
            "system_total_mb": cur.system_total_mb,
            "system_used_percent": cur.system_used_percent,
            "should_cleanup": pressure >= float(self.config.cleanup_threshold_ratio),
            "should_block": self.should_prevent_processing(),
        }