"""
Pydantic ``BaseSettings`` configuration for the certificate-monitoring pipeline.

All environment variables are prefixed with ``CM_`` (Certificate Monitor).
Nested settings use double-underscore as a separator, e.g.:
    CM_MONGO__URI=mongodb+srv://...
    CM_EMAIL__DEVELOPER_ALERT_EMAIL=dev@example.com
    CM_THRESHOLDS__ALERT_DAYS_MAX=1095

A ``.env`` file at the project root is loaded automatically.

Design notes
------------
* Every tunable threshold lives in ``ThresholdSettings`` so operators can
  adjust behavior without touching code.
* ``SourceSettings.per_source_recipients`` maps each ``SourceName`` value to
  a list of email addresses.  Missing entries mean that source's email is
  skipped (no error).
* ``noise_words`` is the list used during DN cleaning.  Adjust to your
  environment — common candidates are environment suffixes, corporate domain
  fragments, and role prefixes.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.batch.models.enums import SourceName


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------


class MongoSettings(BaseSettings):
    """Connection and collection settings for MongoDB."""

    model_config = SettingsConfigDict(env_prefix="CM_MONGO__", env_file=".env", extra="ignore")

    uri: str = Field(..., description="Full MongoDB connection URI.")
    database: str = Field(..., description="Database name.")

    # Collections
    consolidated_data_collection: str = Field(
        "ConsolidatedData",
        description="Source collection holding all certificate records.",
    )
    certificate_alerts_collection: str = Field(
        "CertificateAlerts",
        description="Destination collection for processed alert documents.",
    )
    ignore_alerts_collection: str = Field(
        "IgnoreAlerts",
        description="Collection holding manually acknowledged certificates.",
    )


# ---------------------------------------------------------------------------
# Email / SMTP
# ---------------------------------------------------------------------------


class EmailSettings(BaseSettings):
    """SMTP and recipient settings."""

    model_config = SettingsConfigDict(env_prefix="CM_EMAIL__", env_file=".env", extra="ignore")

    smtp_host: str = Field("localhost", description="SMTP relay hostname.")
    smtp_port: int = Field(25, description="SMTP port (25=plain, 587=STARTTLS, 465=SSL).")
    smtp_use_tls: bool = Field(False, description="Use STARTTLS when connecting.")
    smtp_username: str | None = Field(None, description="SMTP auth username (if required).")
    smtp_password: str | None = Field(None, description="SMTP auth password (if required).")
    sender_address: str = Field("cert-monitor@example.com", description="From address.")

    # Recipients
    consolidated_recipients: list[str] = Field(
        default_factory=list,
        description="Email addresses that receive the consolidated (action-required) email.",
    )
    developer_alert_email: str = Field(
        "developer@example.com",
        description="Single address to receive pipeline failure notifications.",
    )

    # Per-source recipient lists.
    # Keys are SourceName values (e.g. 'SSG', 'HashiCorp').
    # Each source email is sent only if the source has a non-empty list here.
    per_source_recipients: dict[str, list[str]] = Field(
        default_factory=dict,
        description=(
            "Map of source name → list of email addresses for per-source emails. "
            "Example: {\"SSG\": [\"ssg-team@example.com\"]}"
        ),
    )

    # Subject prefixes
    consolidated_subject_prefix: str = Field(
        "[CERT ALERT] Production Certificate Expiration Report",
        description="Subject prefix for the consolidated email.",
    )
    per_source_subject_prefix: str = Field(
        "[CERT ALERT]",
        description="Subject prefix prepended to each per-source email.",
    )

    # Footer / branding
    contact_email: str = Field(
        "shanereddy@email.com",
        description="Contact email address shown in the footer banner of every email.",
    )
    app_version: str = Field(
        "1.0.0",
        description="Application version string shown in the footer banner.",
    )

    # Table 1 sort order.
    # Accepted values: "days_to_expiration" | "source" | "csi_id"
    # Secondary sort is always days_to_expiration ascending so ties are
    # broken by urgency regardless of the primary sort field.
    table1_sort_by: str = Field(
        "days_to_expiration",
        description=(
            "Primary sort column for Table 1 in the consolidated email. "
            "Accepted: 'days_to_expiration' (default, most urgent first), "
            "'source' (alphabetical by source name, then days asc), "
            "'csi_id' (numeric CSI ascending, then days asc)."
        ),
    )


# ---------------------------------------------------------------------------
# Processing thresholds (all editable via env vars)
# ---------------------------------------------------------------------------


class ThresholdSettings(BaseSettings):
    """
    All numeric thresholds used during certificate filtering, matching, and
    alerting.  Every value here has a sensible default but can be overridden
    via environment variables so operators can tune behavior without code
    changes.
    """

    model_config = SettingsConfigDict(env_prefix="CM_THRESHOLDS__", env_file=".env", extra="ignore")

    # ── Parquet cache ────────────────────────────────────────────────────────
    parquet_cache_max_age_hours: float = Field(
        2.0,
        description="Maximum age of the parquet cache before it is rebuilt.",
    )

    # ── MongoDB query window ─────────────────────────────────────────────────
    alert_days_max: int = Field(
        1095,
        description="Upper bound (exclusive) of days_to_expiration loaded into the cache (≈3 years).",
    )
    alert_days_min: int = Field(
        1,
        description="Lower bound (inclusive) of days_to_expiration loaded into the cache.",
    )
    log_date_staleness_days: int = Field(
        7,
        description="Records with log_date older than this many days are excluded.",
    )

    # ── Alert extraction (from parquet) ──────────────────────────────────────
    alert_days_threshold: int = Field(
        7,
        description="Certificates expiring within this many days are flagged as alerts.",
    )

    # ── Renewal / possible-match search ──────────────────────────────────────
    renewal_min_days: int = Field(
        90,
        description="Minimum days_to_expiration a candidate must have to qualify as a renewal.",
    )
    renewal_score_threshold: float = Field(
        90.0,
        ge=0.0,
        le=100.0,
        description="Minimum rapidfuzz token_sort_ratio score to accept a same-source renewal.",
    )
    possible_match_score_threshold: float = Field(
        90.0,
        ge=0.0,
        le=100.0,
        description="Minimum score to accept a cross-source possible match.",
    )
    max_possible_candidates: int = Field(
        6,
        description="Stop collecting possible matches after this many candidates.",
    )
    max_possible_display: int = Field(
        3,
        description="Number of possible matches shown in Table 2 of the consolidated email.",
    )
    length_ratio_min: float = Field(
        0.6,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum len(shorter)/len(longer) ratio before fuzzy scoring is attempted. "
            "Prevents false positives like 'CRSFSVC' matching '163242crsfsvcprodsecret'."
        ),
    )

    # ── IgnoreAlerts lookup window ───────────────────────────────────────────
    ignore_alert_lookback_days: int = Field(
        7,
        description="Only consider IgnoreAlerts records newer than this many days.",
    )

    # ── Perfect-score early-stop ─────────────────────────────────────────────
    perfect_score_early_stop: float = Field(
        100.0,
        description=(
            "If a candidate reaches this score AND we already have at least "
            "``max_possible_display`` matches, stop the search immediately."
        ),
    )


# ---------------------------------------------------------------------------
# Source / environment settings
# ---------------------------------------------------------------------------


class SourceSettings(BaseSettings):
    """
    Controls which sources are active, which appear in the consolidated email,
    and common environment / noise-word settings.
    """

    model_config = SettingsConfigDict(env_prefix="CM_SOURCES__", env_file=".env", extra="ignore")

    active_sources: list[str] = Field(
        default_factory=lambda: [s.value for s in SourceName],
        description="Sources to process.  Defaults to all known sources.",
    )
    consolidated_sources: list[str] = Field(
        default_factory=lambda: [s.value for s in SourceName],
        description="Sources whose action-required certs appear in the consolidated email.",
    )
    environments: list[str] = Field(
        default_factory=lambda: ["PROD", "prod", "PRODUCTION", "Production", "production"],
        description="Accepted values for source_properties.environment.",
    )
    noise_words: list[str] = Field(
        default_factory=lambda: [
            "corp", "chase", "net", "com", "prod", "dev", "uat", "stg",
            "internal", "ext", "external", "svc", "service", "app",
        ],
        description="Words stripped from DNs before fuzzy comparison.",
    )

    @classmethod
    @field_validator("active_sources", "consolidated_sources", mode="before")
    def _coerce_to_list(cls, v: list[str] | str) -> list[str]:
        """Accept a comma-separated string from env vars."""
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return v


# ---------------------------------------------------------------------------
# Cache / report paths
# ---------------------------------------------------------------------------


class PathSettings(BaseSettings):
    """File-system paths for cache and report artifacts."""

    model_config = SettingsConfigDict(env_prefix="CM_PATHS__", env_file=".env", extra="ignore")

    cache_dir: Path = Field(Path("cache"), description="Directory for parquet cache files.")
    reports_dir: Path = Field(Path("reports"), description="Directory for HTML and JSON reports.")
    parquet_filename: str = Field("cert_candidates.parquet", description="Parquet cache filename.")

    @property
    def parquet_path(self) -> Path:
        return self.cache_dir / self.parquet_filename

    @property
    def summary_json_path(self) -> Path:
        return self.reports_dir / "summary.json"

    @property
    def consolidated_html_path(self) -> Path:
        return self.reports_dir / "consolidated.html"

    def per_source_html_path(self, source: str) -> Path:
        return self.reports_dir / f"{source.lower().replace(' ', '_')}.html"


# ---------------------------------------------------------------------------
# Root settings — single object passed throughout the pipeline
# ---------------------------------------------------------------------------


class CMSettings(BaseSettings):
    """
    Root settings object.  Instantiate once at startup and pass through the
    pipeline via dependency injection.

    Example .env file
    -----------------
    CM_MONGO__URI=mongodb+srv://user:pass@cluster.mongodb.net/
    CM_MONGO__DATABASE=CertMonitor
    CM_EMAIL__CONSOLIDATED_RECIPIENTS=["team@example.com"]
    CM_EMAIL__DEVELOPER_ALERT_EMAIL=dev@example.com
    CM_THRESHOLDS__ALERT_DAYS_THRESHOLD=7
    CM_SOURCES__NOISE_WORDS=["corp","chase","net","com","prod"]
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo: MongoSettings = Field(default_factory=MongoSettings)
    email: EmailSettings = Field(default_factory=EmailSettings)
    thresholds: ThresholdSettings = Field(default_factory=ThresholdSettings)
    sources: SourceSettings = Field(default_factory=SourceSettings)
    paths: PathSettings = Field(default_factory=PathSettings)
