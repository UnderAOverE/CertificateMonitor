"""
Main pipeline runner.

Orchestrates the full certificate-monitoring pipeline in the correct order:

    1.  Load / build the parquet cache (FETCH ONCE).
    2.  Load IgnoreAlerts acknowledgements.
    3.  Run each source processor concurrently (within-source sequential).
    4.  Upsert results to MongoDB ``CertificateAlerts``.
    5.  Build the run summary.
    6.  Send per-source emails (always).
    7.  Send consolidated email (action-required only).
    8.  Save HTML reports and ``summary.json``.

Error handling
--------------
* Per-source processor failures are caught, logged, and appended to
  ``run_errors``.  The pipeline continues with remaining sources.
* On catastrophic failure (cache build, MongoDB connection), a developer
  alert email is sent and the process exits non-zero.
* ``--dry-run`` skips MongoDB upserts and SMTP sends but runs all processing
  and saves HTML reports.

Usage
-----
    python -m src.batch.services.runner [OPTIONS]

See ``runner_cli.py`` for the Typer CLI wrapper.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import polars as pl

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import CertificateAlertDocument
from src.batch.models.enums import SourceName
from src.batch.services.alerts import upsert_alert_documents
from src.batch.services.cache import get_or_build_cache
from src.batch.services.email.consolidated import send_consolidated_email
from src.batch.services.email.per_source import send_per_source_emails
from src.batch.services.email.sender import send_developer_alert
from src.batch.services.ignore import load_ignore_set
from src.batch.services.sources import get_processor_registry
from src.batch.services.summary import build_run_summary, save_run_summary

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-source runner (called concurrently via gather)
# ---------------------------------------------------------------------------


async def _run_source(
    source: SourceName,
    processor,
    parquet_df: pl.DataFrame,
    ignore_set,
    run_errors: list[str],
) -> tuple[SourceName, list[CertificateAlertDocument]]:
    """
    Run a single source processor and return its documents.

    Failures are caught, logged, and appended to ``run_errors``.
    Returns an empty list on failure so the pipeline continues.
    """
    try:
        logger.info("▶ Processing source: %s", source.value)
        docs = await processor.process(parquet_df, ignore_set)
        logger.info(
            "✔ Source %s: %d document(s) produced.", source.value, len(docs)
        )
        return source, docs
    except Exception as exc:
        msg = f"Source '{source.value}' failed: {exc!r}\n{traceback.format_exc()}"
        logger.error(msg)
        run_errors.append(msg)
        return source, []


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------


async def run_pipeline(
    settings: CMSettings,
    consolidated_repo=None,   # CMConsolidatedDataMotorRepository instance
    ignore_repo=None,         # IgnoreAlerts repository instance
    alert_repo=None,          # CertificateAlerts repository instance
    force_refresh: bool = False,
    dry_run: bool = False,
    source_filter: list[str] | None = None,
) -> int:
    """
    Execute the full certificate-monitoring pipeline.

    Parameters
    ----------
    settings:
        Populated ``CMSettings``.
    consolidated_repo:
        ``CMConsolidatedDataMotorRepository`` instance for MongoDB reads.
    ignore_repo:
        IgnoreAlerts repository instance.
    alert_repo:
        CertificateAlerts repository instance for upserts.
    force_refresh:
        If ``True``, rebuild the parquet cache from MongoDB even if fresh.
    dry_run:
        If ``True``, skip MongoDB upserts and SMTP sends.
    source_filter:
        If provided, only run processors for the listed source names.

    Returns
    -------
    int
        Exit code: 0 = success, 1 = partial failure, 2 = fatal failure.
    """
    run_time = datetime.now(tz=timezone.utc)
    run_errors: list[str] = []

    logger.info(
        "═══════════════════════════════════════════════════════\n"
        "  Certificate Monitor Pipeline — %s\n"
        "  dry_run=%s  force_refresh=%s  source_filter=%s\n"
        "═══════════════════════════════════════════════════════",
        run_time.strftime("%Y-%m-%d %H:%M UTC"),
        dry_run, force_refresh, source_filter,
    )

    # ── Step 0: ensure output directories exist ──────────────────────────────
    settings.paths.cache_dir.mkdir(parents=True, exist_ok=True)
    settings.paths.reports_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Load / build parquet cache ───────────────────────────────────
    try:
        parquet_df = await get_or_build_cache(
            repository=consolidated_repo,
            settings=settings,
            force_refresh=force_refresh,
        )
        parquet_row_count = parquet_df.height
        logger.info("Cache ready: %d rows.", parquet_row_count)
    except Exception as exc:
        msg = f"FATAL: Could not build/load parquet cache: {exc!r}\n{traceback.format_exc()}"
        logger.critical(msg)
        await send_developer_alert(
            settings, "Parquet cache failure", msg, dry_run=dry_run
        )
        return 2

    # ── Step 2: Load IgnoreAlerts ─────────────────────────────────────────────
    try:
        ignore_set = await load_ignore_set(ignore_repo, settings)
    except Exception as exc:
        msg = f"IgnoreAlerts load failed (continuing without ack data): {exc!r}"
        logger.warning(msg)
        run_errors.append(msg)
        ignore_set = frozenset()

    # ── Step 3: Build processor registry ─────────────────────────────────────
    registry = get_processor_registry(settings, source_filter=source_filter)
    logger.info("Active processors: %s", [s.value for s in registry])

    # ── Step 4: Run all source processors concurrently ───────────────────────
    # Each processor is an async coroutine; we gather them all.
    # Within each processor, operations are sequential (no nested gather).
    tasks = [
        _run_source(source, processor, parquet_df, ignore_set, run_errors)
        for source, processor in registry.items()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    # Collect results per source
    source_documents: dict[SourceName, list[CertificateAlertDocument]] = {}
    all_documents: list[CertificateAlertDocument] = []

    for source, docs in results:
        source_documents[source] = docs
        all_documents.extend(docs)

    logger.info(
        "Processing complete: %d total documents across %d source(s).",
        len(all_documents), len(source_documents),
    )

    # ── Step 5: Upsert to MongoDB ─────────────────────────────────────────────
    if all_documents:
        try:
            upsert_counts = await upsert_alert_documents(
                alert_repo=alert_repo,
                documents=all_documents,
                dry_run=dry_run,
            )
            logger.info("Upsert results: %s", upsert_counts)
            if upsert_counts["errors"]:
                run_errors.append(
                    f"Upsert errors: {upsert_counts['errors']} document(s) failed."
                )
        except Exception as exc:
            msg = f"Upsert phase failed: {exc!r}\n{traceback.format_exc()}"
            logger.error(msg)
            run_errors.append(msg)
    else:
        logger.info("No documents to upsert.")

    # ── Step 6: Build run summary ─────────────────────────────────────────────
    run_summary = build_run_summary(
        all_documents=all_documents,
        parquet_row_count=parquet_row_count,
        settings=settings,
        run_datetime=run_time,
        errors=run_errors,
    )
    save_run_summary(run_summary, settings)

    logger.info(
        "Summary — Action Required: %d | Matched Renewal: %d | Missing Service: %d",
        run_summary.total_action_required,
        run_summary.total_matched_renewal,
        run_summary.total_missing_service,
    )

    # ── Step 7: Per-source emails ─────────────────────────────────────────────
    try:
        email_results = await send_per_source_emails(
            source_documents=source_documents,
            settings=settings,
            run_time=run_time,
            dry_run=dry_run,
        )
        failed_sources = [s for s, ok in email_results.items() if not ok]
        if failed_sources:
            run_errors.append(f"Per-source email failures: {failed_sources}")
    except Exception as exc:
        msg = f"Per-source email phase failed: {exc!r}"
        logger.error(msg)
        run_errors.append(msg)

    # ── Step 8: Consolidated email ────────────────────────────────────────────
    try:
        await send_consolidated_email(
            all_documents=all_documents,
            run_summary=run_summary,
            settings=settings,
            dry_run=dry_run,
        )
    except Exception as exc:
        msg = f"Consolidated email failed: {exc!r}\n{traceback.format_exc()}"
        logger.error(msg)
        run_errors.append(msg)
        await send_developer_alert(
            settings, "Consolidated email failure", msg, dry_run=dry_run
        )

    # ── Final status ──────────────────────────────────────────────────────────
    if run_errors:
        logger.warning(
            "Pipeline completed with %d non-fatal error(s). See summary.json.",
            len(run_errors),
        )
        await send_developer_alert(
            settings=settings,
            subject=f"Pipeline completed with {len(run_errors)} error(s)",
            body="\n\n".join(run_errors),
            dry_run=dry_run,
        )
        return 1

    logger.info("Pipeline completed successfully. ✔")
    return 0
