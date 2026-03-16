"""
Enumerations used across the certificate monitoring pipeline.

Keeping all enums in a single module avoids circular imports and provides
a single source of truth for every categorical value the system understands.
"""

from __future__ import annotations

from enum import StrEnum


# ---------------------------------------------------------------------------
# Source names — must match the values stored in source_properties.name
# ---------------------------------------------------------------------------


class SourceName(StrEnum):
    """
    Canonical names for every certificate source the pipeline processes.

    Values are matched case-sensitively against the 'source_properties.name'
    field in MongoDB, so they must exactly mirror what the ingestion process
    writes.  Add new sources here; the rest of the pipeline picks them up
    automatically through the base-processor registry.
    """

    SSG = "SSG"
    AKAMAI = "AKAMAI"
    HASHICORP = "HashiCorp"
    SSL_TRACKER = "SSL Tracker"
    EVOLVEN = "Evolven"
    APIGEE = "APIGEE"


# ---------------------------------------------------------------------------
# Certificate / alert statuses
# ---------------------------------------------------------------------------


class CertStatus(StrEnum):
    """
    Lifecycle status stored on the MongoDB certificate document.

    Only 'Valid' certificates are loaded into the parquet cache.
    """

    VALID = "Valid"
    EXPIRED = "Expired"
    REVOKED = "Revoked"


class AlertStatus(StrEnum):
    """
    Outcome status assigned to each certificate during alert processing.

    ┌─────────────────┬──────────────────────────────────────────────────────┐
    │ Action Required │ No valid renewal found; human intervention needed.   │
    │ Matched Renewal │ A newer cert with matching source fields was found.  │
    │ Missing Service │ The backing service could not be located (HashiCorp  │
    │                 │ replicas unknown, service decommissioned, etc.).      │
    └─────────────────┴──────────────────────────────────────────────────────┘
    """

    ACTION_REQUIRED = "Action Required"
    MATCHED_RENEWAL = "Matched Renewal"
    MISSING_SERVICE = "Missing Service"


# ---------------------------------------------------------------------------
# Sort / order helpers
# ---------------------------------------------------------------------------


class SortOrder(StrEnum):
    """Direction used when sorting query results."""

    ASCENDING = "ascending"
    DESCENDING = "descending"
