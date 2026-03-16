"""
Source processor registry.

Import all processors here and expose the ``get_processor_registry`` factory
so the runner can iterate over active processors without hard-coding any
specific source names.

Adding a new source
-------------------
1. Create ``src/batch/services/sources/my_source.py`` with a class
   ``MySourceProcessor(BaseSourceProcessor)``.
2. Import it below and add it to ``_ALL_PROCESSORS``.
3. Add the corresponding ``SourceName`` enum value.
4. Done — the runner picks it up via ``get_processor_registry``.
"""

from __future__ import annotations

from src.batch.config.settings.cm import CMSettings
from src.batch.models.enums import SourceName
from src.batch.services.sources.base import BaseSourceProcessor
from src.batch.services.sources.hashicorp import HashiCorpSourceProcessor
from src.batch.services.sources.other_sources import (
    AkamaiSourceProcessor,
    ApigeeSourceProcessor,
    EvolvenSourceProcessor,
    SSLTrackerSourceProcessor,
)
from src.batch.services.sources.ssg import SSGSourceProcessor

# ---------------------------------------------------------------------------
# All available processors — add new ones here
# ---------------------------------------------------------------------------

_ALL_PROCESSORS: dict[SourceName, type[BaseSourceProcessor]] = {
    SourceName.SSG: SSGSourceProcessor,
    SourceName.HASHICORP: HashiCorpSourceProcessor,
    SourceName.EVOLVEN: EvolvenSourceProcessor,
    SourceName.APIGEE: ApigeeSourceProcessor,
    SourceName.AKAMAI: AkamaiSourceProcessor,
    SourceName.SSL_TRACKER: SSLTrackerSourceProcessor,
}


def get_processor_registry(
    settings: CMSettings,
    source_filter: list[str] | None = None,
) -> dict[SourceName, BaseSourceProcessor]:
    """
    Build and return a dict of instantiated source processors.

    Only processors whose ``SourceName`` is in ``settings.sources.active_sources``
    (and optionally in ``source_filter``) are included.

    Parameters
    ----------
    settings:
        Populated ``CMSettings``.
    source_filter:
        Optional list of source name strings to further restrict which
        processors are returned (used by the ``--source`` CLI flag).

    Returns
    -------
    dict[SourceName, BaseSourceProcessor]
        Ready-to-use processor instances keyed by ``SourceName``.
    """
    active = set(settings.sources.active_sources)

    if source_filter:
        active &= set(source_filter)

    registry: dict[SourceName, BaseSourceProcessor] = {}
    for source_name, processor_cls in _ALL_PROCESSORS.items():
        if source_name.value in active:
            registry[source_name] = processor_cls(settings)

    return registry


__all__ = [
    "BaseSourceProcessor",
    "SSGSourceProcessor",
    "HashiCorpSourceProcessor",
    "EvolvenSourceProcessor",
    "ApigeeSourceProcessor",
    "AkamaiSourceProcessor",
    "SSLTrackerSourceProcessor",
    "get_processor_registry",
]
