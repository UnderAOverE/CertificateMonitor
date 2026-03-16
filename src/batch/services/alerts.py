"""
CertificateAlerts MongoDB upsert service.

Persists ``CertificateAlertDocument`` objects to the ``CertificateAlerts``
collection using per-source upsert filter keys.

Upsert strategy
---------------
Each document is upserted (replace) using a filter built from the
source-specific key fields.  This ensures that re-running the pipeline
on the same day updates existing documents rather than creating duplicates.

The TTL index on ``log_datetime`` automatically expires documents after 24 h,
so there is no need to manually clean up old documents.

Upsert filter keys per source
------------------------------
SSG          → source + ssg_domain + microservice_name + internal_ssg_domain
HashiCorp    → source + openshift_namespace + microservice_name (cluster parsed)
Evolven      → source + evolven_host + evolven_path
Apigee       → source + evolven_host (apigee host) + evolven_path + ssg_domain (apigee domain)
Akamai       → source + serial_number  (per-cert, no group key)
SSL Tracker  → source + serial_number
"""

from __future__ import annotations

import logging
from typing import Any

from src.batch.models.alerts import (
    AkamaiSourceModel,
    ApigeeSourceModel,
    CertificateAlertDocument,
    EvolvenSourceModel,
    HashiCorpSourceModel,
    SSGSourceModel,
    SSLTrackerSourceModel,
)
from src.batch.models.enums import SourceName

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Filter key builders (per source)
# ---------------------------------------------------------------------------


def _build_upsert_filter(doc: CertificateAlertDocument) -> dict[str, Any]:
    """
    Build the MongoDB filter dict used for the upsert operation.

    The filter uniquely identifies the logical "source entity" this document
    represents so that re-running the pipeline updates rather than duplicates.

    Parameters
    ----------
    doc:
        The alert document whose source_details drive the filter construction.

    Returns
    -------
    dict[str, Any]
        MongoDB filter document.
    """
    base: dict[str, Any] = {"source": doc.source.value}
    sd = doc.source_details

    if isinstance(sd, SSGSourceModel):
        base.update({
            "source_details.domain": sd.domain,
            "source_details.service_name": sd.service_name,
            "source_details.internal_domain": sd.internal_domain,
        })

    elif isinstance(sd, HashiCorpSourceModel):
        base.update({
            "source_details.cluster": sd.cluster,
            "source_details.project": sd.project,
            "source_details.service_name": sd.service_name,
        })

    elif isinstance(sd, EvolvenSourceModel):
        base.update({
            "source_details.host": sd.host,
            "source_details.path": sd.path,
        })

    elif isinstance(sd, ApigeeSourceModel):
        base.update({
            "source_details.host": sd.host,
            "source_details.path": sd.path,
            "source_details.domain": sd.domain,
        })

    elif isinstance(sd, AkamaiSourceModel):
        # Akamai is per-cert; use the first certificate's serial number
        first_sn = doc.certificates[0].serial_number if doc.certificates else None
        base.update({"source_details.serial_number": first_sn})

    elif isinstance(sd, SSLTrackerSourceModel):
        first_sn = doc.certificates[0].serial_number if doc.certificates else None
        base.update({"source_details.serial_number": first_sn})

    return base


# ---------------------------------------------------------------------------
# Upsert logic
# ---------------------------------------------------------------------------


async def upsert_alert_documents(
    alert_repo,       # Your CertificateAlerts Motor repository
    documents: list[CertificateAlertDocument],
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Upsert a list of ``CertificateAlertDocument`` objects into MongoDB.

    Each document is upserted individually using the per-source filter key.
    Errors for individual documents are logged and counted but do not abort
    the entire batch.

    Parameters
    ----------
    alert_repo:
        An instance of your CertificateAlerts Motor repository.
        Must expose ``upsert_one(filter, document)`` (adjust to your base
        class method name).  This is a placeholder call — wire in your actual
        repository method.
    documents:
        List of fully assembled alert documents to persist.
    dry_run:
        If ``True``, skip the actual MongoDB write (log only).

    Returns
    -------
    dict[str, int]
        Summary counts: {"upserted": N, "errors": M}.
    """
    counts = {"upserted": 0, "errors": 0}

    for doc in documents:
        try:
            filter_doc = _build_upsert_filter(doc)
            doc_dict = doc.model_dump(mode="json")

            if dry_run:
                logger.info(
                    "[DRY RUN] Would upsert %s document for filter=%s",
                    doc.source.value, filter_doc,
                )
                counts["upserted"] += 1
                continue

            # ── PLACEHOLDER: replace with your actual repository upsert call ──
            # Example:
            #   await alert_repo.upsert_one(
            #       filter_query=filter_doc,
            #       update={"$set": doc_dict},
            #   )
            logger.debug(
                "Upserted %s document (filter=%s).", doc.source.value, filter_doc
            )
            # ── END PLACEHOLDER ───────────────────────────────────────────────

            counts["upserted"] += 1

        except Exception as exc:
            logger.error(
                "Failed to upsert %s document: %r — skipping.",
                doc.source.value, exc,
            )
            counts["errors"] += 1

    logger.info(
        "Upsert complete: %d upserted, %d errors.",
        counts["upserted"], counts["errors"],
    )
    return counts
