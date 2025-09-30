from typing import Optional
import psutil

def _get_system_memory_mb() -> Optional[int]:
    try:
        return psutil.virtual_memory().total // (1024 * 1024)
    except Exception:
        return None