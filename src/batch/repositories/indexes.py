"""
MongoDB index management for the certificate-monitoring pipeline.

Run this script ONCE after deployment (or whenever the schema changes) to
ensure all required indexes exist.  The script is idempotent — re-running it
on a collection that already has the indexes is safe; MongoDB will skip
creation for indexes that already exist.

Usage
-----
    python -m src.batch.repositories.indexes

The script reads connection details from the same ``.env`` / environment
variables used by the rest of the pipeline (``CM_MONGO__URI``, etc.).

Index rationale
---------------

ConsolidatedData (5 M+ documents)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Primary query index
    Fields: status (eq) → environment (eq) → source name (eq, optional)
            → days_to_expiration (range) → log_date (range) → _id (seek)
    Matches the ``find_eligible_certificates`` query exactly.
    The trailing ``_id`` field supports seek-pagination tie-breaking without
    an extra sort pass.

    Note: ``source_properties.name`` is placed *after* the range fields so
    that queries that omit it (no source filter) still benefit from the index
    on the leading equality fields.

    We intentionally *do not* create a partial index on
    ``status = "Valid"`` because the status value is varied enough that a
    full compound index is more useful across different queries.

Log-date recency index
    A separate index on ``log_date`` alone (descending) helps the staleness
    filter when no other conditions are pushed down.

CertificateAlerts
~~~~~~~~~~~~~~~~~
TTL index on ``log_datetime`` → documents auto-deleted after 24 h.
Lookup index on ``(source, log_datetime)`` for upsert operations.

IgnoreAlerts
~~~~~~~~~~~~
Lookup index on ``(source, serial_number, log_datetime)`` for the
acknowledgement check (matches the query used in ``ignore.py``).
"""

from __future__ import annotations

import asyncio
import logging

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, IndexModel

from src.batch.config.settings.cm import CMSettings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Index definitions
# ---------------------------------------------------------------------------

# ── ConsolidatedData ─────────────────────────────────────────────────────────

CONSOLIDATED_DATA_INDEXES: list[IndexModel] = [
    # Primary seek-pagination index — covers find_eligible_certificates query.
    # Field order is critical for MongoDB compound index selectivity:
    #   equality fields first, range fields after, _id last for seek.
    IndexModel(
        keys=[
            ("status", ASCENDING),
            ("source_properties.environment", ASCENDING),
            ("source_properties.name", ASCENDING),
            ("days_to_expiration", ASCENDING),
            ("log_date", ASCENDING),
            ("_id", ASCENDING),
        ],
        name="idx_eligibility_seek",
        background=True,
        comment=(
            "Covers find_eligible_certificates with source filter. "
            "Also partial-covers the query without source filter (leading fields match)."
        ),
    ),
    # Secondary index without source_properties.name for queries that do
    # not filter by source (broader parquet refresh scenarios).
    IndexModel(
        keys=[
            ("status", ASCENDING),
            ("source_properties.environment", ASCENDING),
            ("days_to_expiration", ASCENDING),
            ("log_date", ASCENDING),
            ("_id", ASCENDING),
        ],
        name="idx_eligibility_no_source_seek",
        background=True,
        comment=(
            "Fallback for find_eligible_certificates without a source filter. "
            "Also used by monitoring / ad-hoc queries."
        ),
    ),
    # Log-date recency index — used for staleness filtering in isolation.
    IndexModel(
        keys=[("log_date", DESCENDING)],
        name="idx_log_date_desc",
        background=True,
    ),
]


# ── CertificateAlerts ────────────────────────────────────────────────────────

CERTIFICATE_ALERTS_INDEXES: list[IndexModel] = [
    # TTL: auto-delete documents 24 hours after log_datetime.
    # expireAfterSeconds = 86400 (24 × 60 × 60).
    IndexModel(
        keys=[("log_datetime", ASCENDING)],
        name="idx_log_datetime_ttl",
        expireAfterSeconds=86_400,
        background=True,
        comment="TTL index — documents expire 24 h after log_datetime.",
    ),
    # Upsert lookup index — per-source key fields are embedded in the filter
    # at upsert time, but a broad (source, log_datetime) index still helps.
    IndexModel(
        keys=[
            ("source", ASCENDING),
            ("log_datetime", ASCENDING),
        ],
        name="idx_source_log_datetime",
        background=True,
    ),
]


# ── IgnoreAlerts ─────────────────────────────────────────────────────────────

IGNORE_ALERTS_INDEXES: list[IndexModel] = [
    # Acknowledgement lookup: (source, serial_number) equality +
    # log_datetime range (lookback window).
    IndexModel(
        keys=[
            ("source", ASCENDING),
            ("serial_number", ASCENDING),
            ("log_datetime", ASCENDING),
        ],
        name="idx_source_sn_log_datetime",
        background=True,
        comment="Supports the (source, serial_number, log_datetime >= cutoff) lookup in ignore.py.",
    ),
]


# ---------------------------------------------------------------------------
# Creation helper
# ---------------------------------------------------------------------------


async def _create_indexes_for_collection(
    db,
    collection_name: str,
    indexes: list[IndexModel],
) -> None:
    """
    Create all ``indexes`` on ``collection_name``.

    Uses ``create_indexes`` (bulk) which is idempotent — existing indexes
    with the same specification are skipped silently.
    """
    collection = db[collection_name]
    try:
        created = await collection.create_indexes(indexes)
        logger.info(
            "Collection '%s': created/confirmed %d index(es): %s",
            collection_name,
            len(created),
            created,
        )
    except Exception as exc:
        logger.error(
            "Failed to create indexes on '%s': %r",
            collection_name,
            exc,
        )
        raise


async def create_all_indexes(settings: CMSettings) -> None:
    """
    Entry point: connect to MongoDB and create all required indexes.

    Parameters
    ----------
    settings:
        Populated ``CMSettings`` instance (reads URI and collection names).
    """
    logger.info("Connecting to MongoDB to create indexes …")
    client: AsyncIOMotorClient = AsyncIOMotorClient(settings.mongo.uri)
    db = client[settings.mongo.database]

    try:
        await _create_indexes_for_collection(
            db,
            settings.mongo.consolidated_data_collection,
            CONSOLIDATED_DATA_INDEXES,
        )
        await _create_indexes_for_collection(
            db,
            settings.mongo.certificate_alerts_collection,
            CERTIFICATE_ALERTS_INDEXES,
        )
        await _create_indexes_for_collection(
            db,
            settings.mongo.ignore_alerts_collection,
            IGNORE_ALERTS_INDEXES,
        )
        logger.info("All indexes created / confirmed successfully.")
    finally:
        client.close()
        logger.info("MongoDB connection closed.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    _settings = CMSettings()
    asyncio.run(create_all_indexes(_settings))
