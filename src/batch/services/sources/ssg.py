"""
SSG (Service Security Gateway) source processor.

Key fields for grouping: ssg_domain + microservice_name + internal_ssg_domain
Fields extracted: serial_number, ssg_domain, ssg_url_in, ssg_url_out,
                  internal_ssg_domain, microservice_name (service_name),
                  instance_name (comma-separated list)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import polars as pl

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import (
    CertificateAlertDocument,
    CertificateModel,
    SSGSourceModel,
)
from src.batch.models.enums import AlertStatus, SourceName
from src.batch.services.ignore import IgnoreSet, is_acknowledged
from src.batch.services.sources.base import BaseSourceProcessor
from src.batch.utilities.cm import split_instance_name

logger = logging.getLogger(__name__)


class SSGSourceProcessor(BaseSourceProcessor):
    """Processes SSG certificate alerts."""

    def __init__(self, settings: CMSettings) -> None:
        super().__init__(settings)

    @property
    def source_name(self) -> SourceName:
        return SourceName.SSG

    @property
    def key_fields(self) -> list[str]:
        # These fields must all match for two SSG certs to be in the same group
        return ["sp_ssg_domain", "sp_microservice_name", "sp_internal_ssg_domain"]

    async def process(
        self,
        parquet_df: pl.DataFrame,
        ignore_set: IgnoreSet,
    ) -> list[CertificateAlertDocument]:
        """
        Full SSG processing pipeline.

        Groups alert certs by (ssg_domain, microservice_name, internal_ssg_domain),
        builds source details, runs matching, and assembles alert documents.
        """
        alert_df = self.extract_alert_certs(parquet_df)

        if alert_df.is_empty():
            self._logger.info("SSG: No alert certificates found.")
            return []

        groups = self.group_alert_certs(alert_df)
        documents: list[CertificateAlertDocument] = []

        for group_key, rows in groups.items():
            self._logger.debug(
                "SSG group %s: processing %d cert(s).", group_key, len(rows)
            )

            # ── Build source details from the first row in the group ──────────
            # (All rows in a group share the same key fields by definition)
            representative = rows[0]
            source_details = _build_ssg_source_model(representative)

            # ── Run fuzzy matching ────────────────────────────────────────────
            matching_results = self.run_matching(rows, parquet_df)

            # ── Build certificate models ──────────────────────────────────────
            cert_models: list[CertificateModel] = []
            for row, match_result in zip(rows, matching_results):
                acked, acked_by = is_acknowledged(
                    row.get("serial_number_upper"),
                    self.source_name.value,
                    ignore_set,
                )

                cert = _build_certificate_model(row, match_result, acked, acked_by)
                cert_models.append(cert)

            # ── Assemble alert document ───────────────────────────────────────
            doc = CertificateAlertDocument(
                csi_id=representative.get("csi_application_id"),
                source=self.source_name,
                source_details=source_details,
                certificates=cert_models,
                log_datetime=datetime.now(tz=timezone.utc),
            )
            documents.append(doc)

        self._logger.info(
            "SSG: produced %d alert document(s) from %d group(s).",
            len(documents),
            len(groups),
        )
        return documents


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _build_ssg_source_model(row: dict[str, Any]) -> SSGSourceModel:
    """Build an ``SSGSourceModel`` from a parquet row."""
    return SSGSourceModel(
        domain=row.get("sp_ssg_domain"),
        internal_domain=row.get("sp_internal_ssg_domain"),
        service_name=row.get("sp_microservice_name"),
        url=row.get("sp_ssg_url"),
        url_in=row.get("sp_ssg_url_in"),
        url_out=row.get("sp_ssg_url_out"),
        instances=split_instance_name(row.get("sp_instance_name")),
    )


def _build_certificate_model(
    row: dict[str, Any],
    match_result,
    acknowledged: bool,
    acknowledged_by: str | None,
) -> CertificateModel:
    """Build a ``CertificateModel`` from a row + matching result."""
    has_renewal = match_result.renewal is not None
    renewal = match_result.renewal

    status: AlertStatus
    attention_required: bool

    if acknowledged:
        # Acknowledged certs still show in per-source email but not in
        # consolidated action-required table
        status = AlertStatus.ACTION_REQUIRED
        attention_required = False
    elif has_renewal:
        status = AlertStatus.MATCHED_RENEWAL
        attention_required = False
    else:
        status = AlertStatus.ACTION_REQUIRED
        attention_required = True

    return CertificateModel(
        distinguished_name=row.get("distinguished_name") or "",
        days_to_expiration=int(row.get("days_to_expiration") or 0),
        expiration_date=row["expiration_date"],
        serial_number=(row.get("serial_number_upper") or "").upper(),
        csi_id=row.get("csi_application_id"),
        attention_required=attention_required,
        status=status,
        acknowledged=acknowledged,
        acknowledged_by=acknowledged_by,
        # Renewal fields
        similarity_score=renewal.similarity_score if renewal else None,
        renewed_distinguished_name=renewal.distinguished_name if renewal else None,
        renewed_days_to_expiration=renewal.days_to_expiration if renewal else None,
        renewed_expiration_date=renewal.expiration_date if renewal else None,
        renewed_serial_number=renewal.serial_number if renewal else None,
        # Possible matches (only when no renewal)
        possible_matches=match_result.possible_matches if not has_renewal else [],
    )
