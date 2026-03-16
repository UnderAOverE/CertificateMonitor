"""
Per-source email orchestration.

Sends one email per active source to the configured recipients for that
source.  The email is always sent — even if there are zero alert certs for
that source — so support teams receive a "nothing to report" confirmation.

HTML reports are saved to ``reports/{source_name}.html`` (overwrites).
"""

from __future__ import annotations

import logging
from datetime import datetime

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import CertificateAlertDocument
from src.batch.models.enums import SourceName
from src.batch.services.email.builder import build_per_source_email
from src.batch.services.email.sender import send_email

logger = logging.getLogger(__name__)


async def send_per_source_emails(
    source_documents: dict[SourceName, list[CertificateAlertDocument]],
    settings: CMSettings,
    run_time: datetime,
    dry_run: bool = False,
) -> dict[str, bool]:
    """
    Build, save, and send one email per active source.

    Always sends, even for sources with no alert certs.

    Parameters
    ----------
    source_documents:
        Mapping of source → list of alert documents for that source.
        Sources with no documents should still be present as empty lists.
    settings:
        Populated ``CMSettings``.
    run_time:
        UTC datetime of this pipeline run (for email timestamp).
    dry_run:
        Skip SMTP send if ``True`` (still saves HTML reports).

    Returns
    -------
    dict[str, bool]
        Map of source name → send success flag.
    """
    results: dict[str, bool] = {}

    for source in SourceName:
        # Only process active sources
        if source.value not in settings.sources.active_sources:
            continue

        documents = source_documents.get(source, [])

        # Build HTML
        html_body = build_per_source_email(
            source=source,
            documents=documents,
            run_time=run_time,
            contact_email=settings.email.contact_email,
            app_version=settings.email.app_version,
        )

        # Save HTML report
        report_path = settings.paths.per_source_html_path(source.value)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(html_body, encoding="utf-8")
        logger.info(
            "Per-source HTML report for '%s' saved to '%s'.",
            source.value, report_path,
        )

        # Get recipients for this source
        recipients = settings.email.per_source_recipients.get(source.value, [])
        if not recipients:
            logger.info(
                "No per_source_recipients configured for '%s' — skipping email send.",
                source.value,
            )
            results[source.value] = True
            continue

        # Build subject
        run_date = run_time.strftime("%Y-%m-%d")
        cert_count = sum(len(doc.certificates) for doc in documents)
        subject = (
            f"{settings.email.per_source_subject_prefix} "
            f"{source.value} Certificate Report — {run_date} "
            f"({cert_count} alert{'s' if cert_count != 1 else ''})"
        )

        success = await send_email(
            settings=settings,
            to_addresses=recipients,
            subject=subject,
            html_body=html_body,
            dry_run=dry_run,
        )
        results[source.value] = success

    return results
