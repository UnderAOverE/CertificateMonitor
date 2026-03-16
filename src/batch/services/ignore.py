"""
IgnoreAlerts service.

Loads manually acknowledged certificates from the ``IgnoreAlerts`` MongoDB
collection and exposes a fast lookup structure that source processors can
query without hitting the database repeatedly.

Acknowledgement matching
------------------------
A certificate is considered acknowledged if:
    1. Its ``source`` matches the IgnoreAlerts document's ``source``.
    2. Its uppercased ``serial_number`` matches the IgnoreAlerts document's
       uppercased ``serial_number``.
    3. The IgnoreAlerts document's ``log_datetime`` is within the configured
       lookback window (``ignore_alert_lookback_days``).

The DN is *not* part of the match key because CSI IDs and DNs can change
during renewal — the serial number is the stable identifier.

Usage
-----
    ignore_set = await load_ignore_set(repo, settings)
    is_acked = is_acknowledged(serial_number, source, ignore_set)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import IgnoreAlertDocument

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# A set of (source, serial_number_upper) tuples for O(1) lookup.
IgnoreSet = frozenset[tuple[str, str]]


async def load_ignore_set(
    ignore_repo,  # your existing IgnoreAlerts repository
    settings: CMSettings,
) -> IgnoreSet:
    """
    Load all relevant IgnoreAlerts documents and return them as a frozenset
    of ``(source, serial_number_upper)`` tuples.

    Only documents whose ``log_datetime`` falls within the lookback window
    are included.

    Parameters
    ----------
    ignore_repo:
        An instance of your IgnoreAlerts Motor repository.  Must expose a
        method that accepts a ``log_datetime_gte`` filter and returns
        ``IgnoreAlertDocument`` objects (or plain dicts).
        Placeholder — replace with your actual repository call.
    settings:
        Populated ``CMSettings``.

    Returns
    -------
    IgnoreSet
        Frozen set of ``(source, serial_number_upper)`` tuples.
    """
    lookback_days = settings.thresholds.ignore_alert_lookback_days
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=lookback_days)

    logger.info(
        "Loading IgnoreAlerts records newer than %s (lookback=%dd) …",
        cutoff.isoformat(),
        lookback_days,
    )

    # ── PLACEHOLDER: replace with your actual repository method ──────────────
    # Expected signature (adjust to match your base repository):
    #
    #   docs = await ignore_repo.find_many(
    #       filter_query={"log_datetime": {"$gte": cutoff}},
    #       projection={"source": 1, "serial_number": 1, "_id": 0},
    #   )
    #
    # For now we return an empty set so the pipeline runs without the repo.
    docs: list[IgnoreAlertDocument] = []
    # ── END PLACEHOLDER ───────────────────────────────────────────────────────

    pairs: list[tuple[str, str]] = []
    for doc in docs:
        source = (doc.source or "").strip()
        sn = (doc.serial_number or "").strip().upper()
        if source and sn:
            pairs.append((source, sn))

    ignore_set: IgnoreSet = frozenset(pairs)
    logger.info("IgnoreAlerts: %d active acknowledgements loaded.", len(ignore_set))
    return ignore_set


def is_acknowledged(
    serial_number: str | None,
    source: str | None,
    ignore_set: IgnoreSet,
) -> tuple[bool, str | None]:
    """
    Check whether a certificate has been manually acknowledged.

    Parameters
    ----------
    serial_number:
        Certificate serial number (case-insensitive comparison).
    source:
        Source name (e.g. ``'SSG'``).
    ignore_set:
        The frozenset returned by ``load_ignore_set``.

    Returns
    -------
    tuple[bool, str | None]
        ``(True, None)`` if acknowledged (user info not available in the set),
        ``(False, None)`` if not acknowledged.

        Note: The ``user`` field is not stored in the set for performance
        reasons.  If you need the acknowledging user's name in the email,
        extend this to a dict lookup instead of a set.
    """
    if not serial_number or not source:
        return False, None

    key = (source.strip(), serial_number.strip().upper())
    if key in ignore_set:
        logger.debug("Certificate '%s' (source=%s) is acknowledged.", serial_number, source)
        return True, None

    return False, None
