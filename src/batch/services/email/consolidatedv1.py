"""
Consolidated email orchestration.

Coordinates building and sending the consolidated action-required email
and saving the HTML report to disk.

The consolidated email:
* Only shows certificates with ``attention_required = True`` (not acknowledged,
  no renewal found).
* Only includes sources listed in ``settings.sources.consolidated_sources``.
* Saves the rendered HTML to ``reports/consolidated.html`` (overwrites).
"""

from __future__ import annotations

import logging

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import CertificateAlertDocument, RunSummary
from src.batch.models.enums import SourceName
from src.batch.services.email.builder import build_consolidated_email
from src.batch.services.email.sender import send_email

logger = logging.getLogger(__name__)


async def send_consolidated_email(
    all_documents: list[CertificateAlertDocument],
    run_summary: RunSummary,
    settings: CMSettings,
    dry_run: bool = False,
    jira_details_fn=None,
) -> bool:
    """
    Build, save, and send the consolidated certificate expiration email.

    Parameters
    ----------
    all_documents:
        All alert documents from all sources (will be filtered to
        ``consolidated_sources`` internally).
    run_summary:
        Run statistics for the snapshot section.
    settings:
        Populated ``CMSettings``.
    dry_run:
        Skip SMTP send if ``True`` (still saves HTML report).
    jira_details_fn:
        Optional callable(cert) → HTML for the JIRA Details column.

    Returns
    -------
    bool
        ``True`` if the email was sent (or dry-run), ``False`` on failure.
    """
    consolidated_sources = set(settings.sources.consolidated_sources)

    # Filter to only sources configured for the consolidated email
    filtered_docs = [
        doc for doc in all_documents
        if doc.source.value in consolidated_sources
    ]

    # Check if there is anything action-required to report
    has_action_required = any(
        cert.attention_required and not cert.acknowledged
        for doc in filtered_docs
        for cert in doc.certificates
    )

    if not has_action_required:
        logger.info(
            "No action-required certificates across consolidated sources — "
            "consolidated email will note clean status."
        )

    # Build the HTML
    settings_snapshot = {
        "alert_days_threshold": settings.thresholds.alert_days_threshold,
        "renewal_min_days": settings.thresholds.renewal_min_days,
        "renewal_score_threshold": settings.thresholds.renewal_score_threshold,
        "possible_match_score_threshold": settings.thresholds.possible_match_score_threshold,
        "length_ratio_min": settings.thresholds.length_ratio_min,
        "environments": settings.sources.environments,
        "active_sources": settings.sources.active_sources,
    }

    html_body = build_consolidated_email(
        all_documents=filtered_docs,
        run_summary=run_summary,
        settings_snapshot=settings_snapshot,
        jira_details_fn=jira_details_fn,
        contact_email=settings.email.contact_email,
        app_version=settings.email.app_version,
        table1_sort_by=settings.email.table1_sort_by,
    )

    # Save HTML report to disk
    report_path = settings.paths.consolidated_html_path
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(html_body, encoding="utf-8")
    logger.info("Consolidated HTML report saved to '%s'.", report_path)

    # Send email
    recipients = settings.email.consolidated_recipients
    if not recipients:
        logger.warning(
            "No consolidated_recipients configured — skipping consolidated email send."
        )
        return True

    subject = settings.email.consolidated_subject_prefix
    run_date = run_summary.run_datetime.strftime("%Y-%m-%d")
    full_subject = f"{subject} — {run_date}"

    return await send_email(
        settings=settings,
        to_addresses=recipients,
        subject=full_subject,
        html_body=html_body,
        dry_run=dry_run,
    )
