"""
HashiCorp source processor.

Key fields for grouping: openshift_namespace + microservice_name
The microservice_name field follows the pattern:
    ``clustername_projectname_deploymentname``

Replica details are fetched asynchronously from the OpenShift API per unique
(cluster, project, service_name) tuple.  Failures silently set replicas to
``{"total": "unknown", "available": "unknown"}`` and set the cert status to
``MissingService``.

The actual OSE API call is a placeholder — replace the body of
``_fetch_replicas`` with your existing async code.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import polars as pl

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import (
    CertificateAlertDocument,
    CertificateModel,
    HashiCorpSourceModel,
)
from src.batch.models.enums import AlertStatus, SourceName
from src.batch.services.ignore import IgnoreSet, is_acknowledged
from src.batch.services.sources.base import BaseSourceProcessor
from src.batch.services.sources.ssg import _build_certificate_model

logger = logging.getLogger(__name__)

# Replica dict type alias for clarity
ReplicaInfo = dict[str, int | str]
_UNKNOWN_REPLICAS: ReplicaInfo = {"total": "unknown", "available": "unknown"}


class HashiCorpSourceProcessor(BaseSourceProcessor):
    """Processes HashiCorp / OpenShift certificate alerts."""

    def __init__(self, settings: CMSettings) -> None:
        super().__init__(settings)

    @property
    def source_name(self) -> SourceName:
        return SourceName.HASHICORP

    @property
    def key_fields(self) -> list[str]:
        return ["sp_openshift_namespace", "sp_microservice_name"]

    async def process(
        self,
        parquet_df: pl.DataFrame,
        ignore_set: IgnoreSet,
    ) -> list[CertificateAlertDocument]:
        """
        Full HashiCorp processing pipeline.

        1. Extract alert certs.
        2. Parse cluster/project/service from microservice_name
           (pattern: ``clustername_projectname_deploymentname``).
        3. Deduplicate OSE API calls by (cluster, project, service).
        4. Fetch replicas concurrently (one call per unique triple).
        5. Build source details, run matching, assemble documents.
        """
        alert_df = self.extract_alert_certs(parquet_df)

        if alert_df.is_empty():
            self._logger.info("HashiCorp: No alert certificates found.")
            return []

        groups = self.group_alert_certs(alert_df)

        # ── Deduplicate OSE calls ─────────────────────────────────────────────
        # Parse microservice_name → (cluster, project, service)
        unique_triples: set[tuple[str, str, str]] = set()
        for rows in groups.values():
            for row in rows:
                triple = _parse_microservice_name(row.get("sp_microservice_name"))
                unique_triples.add(triple)

        # Fetch replicas concurrently for all unique triples
        replica_map = await _fetch_all_replicas(unique_triples, self._logger)

        documents: list[CertificateAlertDocument] = []

        for group_key, rows in groups.items():
            representative = rows[0]
            triple = _parse_microservice_name(representative.get("sp_microservice_name"))
            replicas = replica_map.get(triple, _UNKNOWN_REPLICAS)

            source_details = _build_hashicorp_source_model(representative, replicas)

            matching_results = self.run_matching(rows, parquet_df)

            cert_models: list[CertificateModel] = []
            for row, match_result in zip(rows, matching_results):
                acked, acked_by = is_acknowledged(
                    row.get("serial_number_upper"),
                    self.source_name.value,
                    ignore_set,
                )
                cert = _build_certificate_model(row, match_result, acked, acked_by)

                # Override status to MissingService when replicas are unknown
                if _replicas_unknown(replicas) and not cert.acknowledged:
                    cert.status = AlertStatus.MISSING_SERVICE
                    cert.attention_required = True

                cert_models.append(cert)

            doc = CertificateAlertDocument(
                csi_id=representative.get("csi_application_id"),
                source=self.source_name,
                source_details=source_details,
                certificates=cert_models,
                log_datetime=datetime.now(tz=timezone.utc),
            )
            documents.append(doc)

        self._logger.info(
            "HashiCorp: produced %d alert document(s).", len(documents)
        )
        return documents


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _parse_microservice_name(
    name: str | None,
) -> tuple[str, str, str]:
    """
    Parse a HashiCorp microservice_name into (cluster, project, deployment).

    Expected pattern: ``clustername_projectname_deploymentname``
    Falls back to ("unknown", "unknown", "unknown") on any parse error.
    """
    if not name or name.strip().lower() == "null":
        return "unknown", "unknown", "unknown"

    parts = name.strip().split("_", maxsplit=2)
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return parts[0], parts[1], "unknown"
    return name.strip(), "unknown", "unknown"


def _replicas_unknown(replicas: ReplicaInfo) -> bool:
    """Return True when both replica counts are the sentinel 'unknown'."""
    return replicas.get("total") == "unknown" and replicas.get("available") == "unknown"


async def _fetch_replicas(
    cluster: str,
    project: str,
    service_name: str,
    logger_: logging.Logger,
) -> ReplicaInfo:
    """
    Fetch replica details for a single (cluster, project, service_name) triple
    from the OpenShift API.

    ── PLACEHOLDER ──────────────────────────────────────────────────────────
    Replace the body of this function with your existing async OSE API code.
    The function must return a dict with at least:
        {"total": <int or "unknown">, "available": <int or "unknown">}

    Failures must be caught internally and return ``_UNKNOWN_REPLICAS`` so
    that processing continues for other certs.
    ─────────────────────────────────────────────────────────────────────────

    Parameters
    ----------
    cluster, project, service_name:
        Parsed from the HashiCorp microservice_name field.
    logger_:
        Logger instance for this processor.

    Returns
    -------
    ReplicaInfo
        Dict with ``total`` and ``available`` keys.
    """
    try:
        # ── INSERT YOUR OSE API CALL HERE ─────────────────────────────────────
        # Example:
        #   response = await ose_client.get_deployment(cluster, project, service_name)
        #   return {"total": response.replicas, "available": response.ready_replicas}
        # ─────────────────────────────────────────────────────────────────────

        # Placeholder return — remove when wiring in real code
        logger_.debug(
            "OSE replica fetch placeholder called for %s/%s/%s",
            cluster, project, service_name,
        )
        return _UNKNOWN_REPLICAS

    except Exception as exc:
        logger_.warning(
            "OSE replica fetch failed for %s/%s/%s: %r — using unknown/unknown.",
            cluster, project, service_name, exc,
        )
        return _UNKNOWN_REPLICAS


async def _fetch_all_replicas(
    triples: set[tuple[str, str, str]],
    logger_: logging.Logger,
) -> dict[tuple[str, str, str], ReplicaInfo]:
    """
    Concurrently fetch replica info for all unique (cluster, project, service)
    triples.  Returns a mapping triple → ReplicaInfo.
    """
    if not triples:
        return {}

    tasks = {
        triple: asyncio.create_task(_fetch_replicas(*triple, logger_))
        for triple in triples
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    replica_map: dict[tuple[str, str, str], ReplicaInfo] = {}
    for triple, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            logger_.warning("Replica gather exception for %s: %r", triple, result)
            replica_map[triple] = _UNKNOWN_REPLICAS
        else:
            replica_map[triple] = result  # type: ignore[assignment]

    return replica_map


def _build_hashicorp_source_model(
    row: dict[str, Any],
    replicas: ReplicaInfo,
) -> HashiCorpSourceModel:
    """Build a ``HashiCorpSourceModel`` from a parquet row + replica info."""
    cluster, project, service = _parse_microservice_name(row.get("sp_microservice_name"))
    return HashiCorpSourceModel(
        cluster=cluster if cluster != "unknown" else None,
        project=project if project != "unknown" else None,
        service_name=service if service != "unknown" else None,
        replicas=replicas,
    )
