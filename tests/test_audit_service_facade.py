"""AuditService is a facade over per-responsibility mixins (issue #60)."""

from src.core.services.audit import AuditService as PackageAuditService
from src.core.services.audit.accumulator import BatchAccumulator, BatchTimeout
from src.core.services.audit.service import AuditService
from src.core.services.audit.service import BatchAccumulator as LegacyAccumulator
from src.core.services.audit.service import BatchTimeout as LegacyTimeout


def _own_methods(cls):
    return {name for name, value in vars(cls).items() if callable(value)}


def test_mixins_do_not_shadow_each_other():
    mixins = AuditService.__mro__[1:-1]
    seen = {}
    for mixin in (AuditService, *mixins):
        for name in _own_methods(mixin) - {"__init__"}:
            assert name not in seen, f"{name} defined in both {seen[name]} and {mixin.__name__}"
            seen[name] = mixin.__name__


def test_legacy_import_paths_still_work():
    assert PackageAuditService is AuditService
    assert LegacyAccumulator is BatchAccumulator
    assert LegacyTimeout is BatchTimeout
