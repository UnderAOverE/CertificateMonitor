"""
Pydantic domain models for the certificate-monitoring pipeline.

Design decisions
----------------
* Every model uses ``model_config = ConfigDict(populate_by_name=True)`` so
  field aliases (matching MongoDB field names) work alongside the Python
  attribute names.
* ``source_details`` on the final alert document uses a Pydantic
  *discriminated union* keyed on ``source_type``.  This gives us a single
  heterogeneous collection in MongoDB while still allowing type-safe
  deserialization in Python.
* All datetime fields carry UTC timezone info.  Mongo stores them as BSON
  Date; Motor returns them as timezone-aware ``datetime`` objects.
* "null" strings from MongoDB are normalized to ``None`` during validation
  via ``@field_validator``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.batch.models.enums import AlertStatus, SourceName


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _null_to_none(value: Any) -> Any:
    """
    Convert the literal string ``'null'`` (written by the ingestion process)
    to Python ``None``.  Applied as a ``@field_validator`` on every optional
    string field that could carry this sentinel.
    """
    if isinstance(value, str) and value.strip().lower() == "null":
        return None
    return value


# ---------------------------------------------------------------------------
# Possible-match model (used inside CertificateModel)
# ---------------------------------------------------------------------------


class PossibleMatchModel(BaseModel):
    """
    A candidate certificate that *might* be the renewal for an expiring cert.

    Shown in Table 2 of the consolidated email (up to 3 per expiring cert).
    """

    model_config = ConfigDict(populate_by_name=True)

    distinguished_name: str = Field(..., description="Full DN of the candidate cert.")
    days_to_expiration: int = Field(..., ge=0)
    expiration_date: datetime
    serial_number: str
    similarity_score: float = Field(..., ge=0.0, le=100.0)
    csi_id: int | None = Field(None, description="CSI application ID; may differ from the alert cert.")
    ssl_cm_status: str | None = Field(None, description="SSL CM status; highlighted if not 'Activated'.")
    source: str | None = Field(None, description="Source name of the candidate.")

    @classmethod
    @field_validator("ssl_cm_status", "distinguished_name", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


# ---------------------------------------------------------------------------
# Certificate model (one per expiring cert, stored inside the final alert doc)
# ---------------------------------------------------------------------------


class CertificateModel(BaseModel):
    """
    Represents a single expiring certificate and the outcome of the renewal
    / possible-match search.

    Fields prefixed with ``renewed_`` are populated only when a same-source
    renewal is found (``attention_required = False``).  ``possible_matches``
    is populated only when no renewal is found.
    """

    model_config = ConfigDict(populate_by_name=True)

    # ── Expiring cert ────────────────────────────────────────────────────────
    distinguished_name: str
    days_to_expiration: int = Field(..., ge=0)
    expiration_date: datetime
    serial_number: str
    csi_id: int | None = None

    # ── Outcome flags ────────────────────────────────────────────────────────
    attention_required: bool = True
    status: AlertStatus = AlertStatus.ACTION_REQUIRED

    # ── Renewal fields (populated when a renewal is found) ──────────────────
    similarity_score: float | None = Field(None, ge=0.0, le=100.0)
    renewed_distinguished_name: str | None = None
    renewed_days_to_expiration: int | None = None
    renewed_expiration_date: datetime | None = None
    renewed_serial_number: str | None = None

    # ── Possible matches (populated when no renewal found) ──────────────────
    possible_matches: list[PossibleMatchModel] = Field(default_factory=list)

    # ── Ignore / acknowledgement ─────────────────────────────────────────────
    acknowledged: bool = False
    acknowledged_by: str | None = None

    @classmethod
    @field_validator("distinguished_name", "renewed_distinguished_name",
                     "serial_number", "renewed_serial_number", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


# ===========================================================================
# Source-detail models (one per source type)
# ===========================================================================


class SSGSourceModel(BaseModel):
    """Service-Security Gateway source details."""

    source_type: Literal[SourceName.SSG] = SourceName.SSG
    model_config = ConfigDict(populate_by_name=True)

    domain: str | None = None
    internal_domain: str | None = None
    service_name: str | None = None
    url: str | None = None
    url_in: str | None = None
    url_out: str | None = None
    instances: list[str] = Field(default_factory=list)

    @classmethod
    @field_validator("domain", "internal_domain", "service_name",
                     "url", "url_in", "url_out", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


class HashiCorpSourceModel(BaseModel):
    """HashiCorp Vault / OpenShift source details."""

    source_type: Literal[SourceName.HASHICORP] = SourceName.HASHICORP
    model_config = ConfigDict(populate_by_name=True)

    cluster: str | None = None
    project: str | None = None        # OpenShift namespace
    service_name: str | None = None   # deployment name
    replicas: dict[str, int | str] = Field(
        default_factory=lambda: {"total": "unknown", "available": "unknown"},
        description="Populated by the OpenShift API call.  Falls back to unknown/unknown.",
    )

    @classmethod
    @field_validator("cluster", "project", "service_name", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


class EvolvenSourceModel(BaseModel):
    """Evolven source details."""

    source_type: Literal[SourceName.EVOLVEN] = SourceName.EVOLVEN
    model_config = ConfigDict(populate_by_name=True)

    host: str | None = None
    path: str | None = None
    instances: list[str] = Field(default_factory=list)

    @classmethod
    @field_validator("host", "path", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


class ApigeeSourceModel(BaseModel):
    """Apigee API gateway source details."""

    source_type: Literal[SourceName.APIGEE] = SourceName.APIGEE
    model_config = ConfigDict(populate_by_name=True)

    domain: str | None = None
    host: str | None = None
    path: str | None = None
    url_in: str | None = None
    url_out: str | None = None

    @classmethod
    @field_validator("domain", "host", "path", "url_in", "url_out", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


class AkamaiSourceModel(BaseModel):
    """Akamai CDN source details."""

    source_type: Literal[SourceName.AKAMAI] = SourceName.AKAMAI
    model_config = ConfigDict(populate_by_name=True)

    certificate_owner: str | None = None
    support_group_email: str | None = None
    san_names: list[str] = Field(default_factory=list)

    @classmethod
    @field_validator("certificate_owner", "support_group_email", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


class SSLTrackerSourceModel(BaseModel):
    """SSL Tracker source details."""

    source_type: Literal[SourceName.SSL_TRACKER] = SourceName.SSL_TRACKER
    model_config = ConfigDict(populate_by_name=True)

    status: str | None = None

    @classmethod
    @field_validator("status", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


# ---------------------------------------------------------------------------
# Discriminated union — used as the type of ``source_details`` on the alert
# ---------------------------------------------------------------------------

SourceDetailsType = Annotated[
    Union[
        SSGSourceModel,
        HashiCorpSourceModel,
        EvolvenSourceModel,
        ApigeeSourceModel,
        AkamaiSourceModel,
        SSLTrackerSourceModel,
    ],
    Field(discriminator="source_type"),
]


# ===========================================================================
# Final alert model — upserted into CertificateAlerts collection
# ===========================================================================


class CertificateAlertDocument(BaseModel):
    """
    Top-level document written to the ``CertificateAlerts`` MongoDB collection.

    One document per logical source-group (e.g. one SSG domain+service
    combination).  TTL index on ``log_datetime`` expires documents after 24 h.

    ``source_details`` is a discriminated union; Pydantic selects the correct
    sub-model based on the ``source_type`` literal field.
    """

    model_config = ConfigDict(populate_by_name=True)

    csi_id: int | None = Field(None, description="CSI application ID from the source record.")
    source: SourceName
    source_details: SourceDetailsType
    certificates: list[CertificateModel] = Field(default_factory=list)
    log_datetime: datetime = Field(description="UTC timestamp of this pipeline run.")


# ===========================================================================
# IgnoreAlerts model — read from the IgnoreAlerts MongoDB collection
# ===========================================================================


class IgnoreAlertDocument(BaseModel):
    """
    Represents a manual acknowledgement stored in the ``IgnoreAlerts``
    collection.

    The pipeline looks up ``(source, serial_number)`` tuples where
    ``log_datetime >= now - ignore_alert_lookback_days`` and marks matching
    certs as acknowledged so they are suppressed from the consolidated email.
    """

    model_config = ConfigDict(populate_by_name=True)

    distinguished_name: str
    log_datetime: datetime
    serial_number: str
    user: str
    source: str

    @classmethod
    @field_validator("distinguished_name", "serial_number", mode="before")
    def _normalise_null(cls, v: Any) -> Any:
        return _null_to_none(v)


# ===========================================================================
# Runtime summary model — written to reports/summary.json each run
# ===========================================================================


class SourceSummary(BaseModel):
    """Per-source counts for the run summary."""

    source: SourceName
    action_required: int = 0
    matched_renewal: int = 0
    missing_service: int = 0
    total_alerts: int = 0


class RunSummary(BaseModel):
    """
    Written to ``reports/summary.json`` at the end of each pipeline run.
    Overwrites the previous run's file.
    """

    run_datetime: datetime
    parquet_cache_path: str
    parquet_row_count: int
    total_action_required: int
    total_matched_renewal: int
    total_missing_service: int
    sources: list[SourceSummary] = Field(default_factory=list)
    settings_snapshot: dict[str, Any] = Field(
        default_factory=dict,
        description="Serialised threshold / source settings used for this run.",
    )
    errors: list[str] = Field(
        default_factory=list,
        description="Non-fatal errors encountered during the run.",
    )
