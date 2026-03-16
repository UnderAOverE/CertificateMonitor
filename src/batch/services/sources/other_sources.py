"""
Evolven, Apigee, Akamai and SSL Tracker source processors.

Each processor follows the same pattern as SSGSourceProcessor:
    1. Extract alert certs (inherited from BaseSourceProcessor).
    2. Group by source-specific key fields.
    3. Build source-details model.
    4. Run fuzzy matching.
    5. Apply to ignore check.
    6. Return CertificateAlertDocument list.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import polars as pl

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import (
    AkamaiSourceModel,
    ApigeeSourceModel,
    CertificateAlertDocument,
    CertificateModel,
    EvolvenSourceModel,
    SSLTrackerSourceModel,
)
from src.batch.models.enums import SourceName
from src.batch.services.ignore import IgnoreSet, is_acknowledged
from src.batch.services.sources.base import BaseSourceProcessor
from src.batch.services.sources.ssg import _build_certificate_model
from src.batch.utilities.cm import parse_san_names, split_instance_name

logger = logging.getLogger(__name__)


# ===========================================================================
# Evolven
# ===========================================================================


class EvolvenSourceProcessor(BaseSourceProcessor):
    """Processes Evolven certificate alerts."""

    @property
    def source_name(self) -> SourceName:
        return SourceName.EVOLVEN

    @property
    def key_fields(self) -> list[str]:
        return ["sp_evolven_host", "sp_evolven_path", "sp_instance_name"]

    async def process(
        self,
        parquet_df: pl.DataFrame,
        ignore_set: IgnoreSet,
    ) -> list[CertificateAlertDocument]:
        alert_df = self.extract_alert_certs(parquet_df)
        if alert_df.is_empty():
            return []

        groups = self.group_alert_certs(alert_df)
        documents: list[CertificateAlertDocument] = []

        for _, rows in groups.items():
            representative = rows[0]
            source_details = EvolvenSourceModel(
                host=representative.get("sp_evolven_host"),
                path=representative.get("sp_evolven_path"),
                # instance_name for Evolven can be numeric — normalize to str list
                instances=split_instance_name(
                    str(representative.get("sp_instance_name") or "")
                ),
            )

            matching_results = self.run_matching(rows, parquet_df)
            cert_models = _build_cert_models(rows, matching_results, self.source_name, ignore_set)

            documents.append(CertificateAlertDocument(
                csi_id=representative.get("csi_application_id"),
                source=self.source_name,
                source_details=source_details,
                certificates=cert_models,
                log_datetime=datetime.now(tz=timezone.utc),
            ))

        self._logger.info("Evolven: produced %d alert document(s).", len(documents))
        return documents


# ===========================================================================
# Apigee
# ===========================================================================


class ApigeeSourceProcessor(BaseSourceProcessor):
    """Processes Apigee API gateway certificate alerts."""

    @property
    def source_name(self) -> SourceName:
        return SourceName.APIGEE

    @property
    def key_fields(self) -> list[str]:
        # Apigee field mapping (from requirements):
        #   evolven_host  → apigee host
        #   evolven_path  → apigee path
        #   ssg_domain    → apigee domain
        return ["sp_evolven_host", "sp_evolven_path", "sp_ssg_domain"]

    async def process(
        self,
        parquet_df: pl.DataFrame,
        ignore_set: IgnoreSet,
    ) -> list[CertificateAlertDocument]:
        alert_df = self.extract_alert_certs(parquet_df)
        if alert_df.is_empty():
            return []

        groups = self.group_alert_certs(alert_df)
        documents: list[CertificateAlertDocument] = []

        for _, rows in groups.items():
            representative = rows[0]
            source_details = ApigeeSourceModel(
                domain=representative.get("sp_ssg_domain"),
                host=representative.get("sp_evolven_host"),
                path=representative.get("sp_evolven_path"),
                url_in=representative.get("sp_ssg_url_in"),
                url_out=representative.get("sp_ssg_url_out"),
            )

            matching_results = self.run_matching(rows, parquet_df)
            cert_models = _build_cert_models(rows, matching_results, self.source_name, ignore_set)

            documents.append(CertificateAlertDocument(
                csi_id=representative.get("csi_application_id"),
                source=self.source_name,
                source_details=source_details,
                certificates=cert_models,
                log_datetime=datetime.now(tz=timezone.utc),
            ))

        self._logger.info("Apigee: produced %d alert document(s).", len(documents))
        return documents


# ===========================================================================
# Akamai
# ===========================================================================


class AkamaiSourceProcessor(BaseSourceProcessor):
    """
    Processes Akamai CDN certificate alerts.

    Akamai has no natural grouping key beyond the serial number, so each
    certificate produces its own alert document.
    """

    @property
    def source_name(self) -> SourceName:
        return SourceName.AKAMAI

    @property
    def key_fields(self) -> list[str]:
        return ["sp_serial_number"]

    async def process(
        self,
        parquet_df: pl.DataFrame,
        ignore_set: IgnoreSet,
    ) -> list[CertificateAlertDocument]:
        alert_df = self.extract_alert_certs(parquet_df)
        if alert_df.is_empty():
            return []

        groups = self.group_alert_certs(alert_df)
        documents: list[CertificateAlertDocument] = []

        for _, rows in groups.items():
            representative = rows[0]
            source_details = AkamaiSourceModel(
                certificate_owner=representative.get("sp_certificate_owner"),
                support_group_email=representative.get("sp_support_group_email"),
                san_names=parse_san_names(representative.get("sp_san_names")),
            )

            matching_results = self.run_matching(rows, parquet_df)
            cert_models = _build_cert_models(rows, matching_results, self.source_name, ignore_set)

            documents.append(CertificateAlertDocument(
                csi_id=representative.get("csi_application_id"),
                source=self.source_name,
                source_details=source_details,
                certificates=cert_models,
                log_datetime=datetime.now(tz=timezone.utc),
            ))

        self._logger.info("Akamai: produced %d alert document(s).", len(documents))
        return documents


# ===========================================================================
# SSL Tracker
# ===========================================================================


class SSLTrackerSourceProcessor(BaseSourceProcessor):
    """
    Processes SSL Tracker certificate alerts.

    Like Akamai, SSL Tracker has no natural multi-cert grouping key.
    """

    @property
    def source_name(self) -> SourceName:
        return SourceName.SSL_TRACKER

    @property
    def key_fields(self) -> list[str]:
        return ["sp_serial_number"]

    async def process(
        self,
        parquet_df: pl.DataFrame,
        ignore_set: IgnoreSet,
    ) -> list[CertificateAlertDocument]:
        alert_df = self.extract_alert_certs(parquet_df)
        if alert_df.is_empty():
            return []

        groups = self.group_alert_certs(alert_df)
        documents: list[CertificateAlertDocument] = []

        for _, rows in groups.items():
            representative = rows[0]
            source_details = SSLTrackerSourceModel(
                status=representative.get("sp_ssl_cm_status"),
            )

            matching_results = self.run_matching(rows, parquet_df)
            cert_models = _build_cert_models(rows, matching_results, self.source_name, ignore_set)

            documents.append(CertificateAlertDocument(
                csi_id=representative.get("csi_application_id"),
                source=self.source_name,
                source_details=source_details,
                certificates=cert_models,
                log_datetime=datetime.now(tz=timezone.utc),
            ))

        self._logger.info("SSLTracker: produced %d alert document(s).", len(documents))
        return documents


# ===========================================================================
# Shared helper (reused by all processors above)
# ===========================================================================


def _build_cert_models(
    rows: list[dict[str, Any]],
    matching_results: list,
    source_name: SourceName,
    ignore_set: IgnoreSet,
) -> list[CertificateModel]:
    """
    Build ``CertificateModel`` list from rows + matching results.

    Reuses the ``_build_certificate_model`` helper from the SSG module —
    the logic is identical regardless of source.
    """
    cert_models: list[CertificateModel] = []
    for row, match_result in zip(rows, matching_results):
        acked, acked_by = is_acknowledged(
            row.get("serial_number_upper"),
            source_name.value,
            ignore_set,
        )
        cert_models.append(_build_certificate_model(row, match_result, acked, acked_by))
    return cert_models
