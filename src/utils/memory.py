from typing import Optional
import os

def _get_system_memory_mb() -> Optional[int]:
    try:
        if os.path.exists('/proc/meminfo'):
            with open('/proc/meminfo', 'r') as f:
                first = f.readline()
            # Expect format: MemTotal: 16384256 kB
            parts = first.split()
            if len(parts) >= 2 and parts[1].isdigit():
                kb = int(parts[1])
                return kb // 1024
    except Exception:
        pass
    return None