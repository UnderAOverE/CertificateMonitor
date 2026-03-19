"""
Run summary — builds and writes the ``reports/summary.json`` file.

The summary provides a machine-readable snapshot of each pipeline run:
counts per source, settings used, errors encountered, and cache stats.
It overwrites the previous run's file each time.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import (
    CertificateAlertDocument,
    RunSummary,
    SourceSummary,
)
from src.batch.models.enums import AlertStatus, SourceName

logger = logging.getLogger(__name__)


def build_run_summary(
    all_documents: list[CertificateAlertDocument],
    parquet_row_count: int,
    settings: CMSettings,
    run_datetime: datetime,
    errors: list[str] | None = None,
) -> RunSummary:
    """
    Compute a ``RunSummary`` from the processed alert documents.

    Parameters
    ----------
    all_documents:
        All ``CertificateAlertDocument`` objects produced this run.
    parquet_row_count:
        Number of rows in the parquet cache.
    settings:
        Populated ``CMSettings`` (used for settings_snapshot).
    run_datetime:
        UTC datetime of this pipeline run.
    errors:
        Optional list of non-fatal error strings encountered during the run.

    Returns
    -------
    RunSummary
    """
    t = settings.thresholds
    s = settings.sources

    # Build per-source summaries
    source_summaries: list[SourceSummary] = []

    for source in SourceName:
        if source.value not in s.active_sources:
            continue

        source_docs = [d for d in all_documents if d.source == source]
        action = sum(
            1 for d in source_docs for c in d.certificates
            if c.status == AlertStatus.ACTION_REQUIRED
        )
        renewal = sum(
            1 for d in source_docs for c in d.certificates
            if c.status == AlertStatus.MATCHED_RENEWAL
        )
        missing = sum(
            1 for d in source_docs for c in d.certificates
            if c.status == AlertStatus.MISSING_SERVICE
        )
        total = action + renewal + missing

        source_summaries.append(SourceSummary(
            source=source,
            action_required=action,
            matched_renewal=renewal,
            missing_service=missing,
            total_alerts=total,
        ))

    total_action = sum(s.action_required for s in source_summaries)
    total_renewal = sum(s.matched_renewal for s in source_summaries)
    total_missing = sum(s.missing_service for s in source_summaries)

    settings_snapshot = {
        "alert_days_threshold": t.alert_days_threshold,
        "alert_days_max": t.alert_days_max,
        "log_date_staleness_days": t.log_date_staleness_days,
        "renewal_min_days": t.renewal_min_days,
        "renewal_score_threshold": t.renewal_score_threshold,
        "possible_match_score_threshold": t.possible_match_score_threshold,
        "length_ratio_min": t.length_ratio_min,
        "max_possible_candidates": t.max_possible_candidates,
        "max_possible_display": t.max_possible_display,
        "ignore_alert_lookback_days": t.ignore_alert_lookback_days,
        "environments": s.environments,
        "active_sources": s.active_sources,
        "consolidated_sources": s.consolidated_sources,
    }

    return RunSummary(
        run_datetime=run_datetime,
        parquet_cache_path=str(settings.paths.parquet_path),
        parquet_row_count=parquet_row_count,
        total_action_required=total_action,
        total_matched_renewal=total_renewal,
        total_missing_service=total_missing,
        sources=source_summaries,
        settings_snapshot=settings_snapshot,
        errors=errors or [],
    )


def save_run_summary(summary: RunSummary, settings: CMSettings) -> None:
    """
    Serialize and write the ``RunSummary`` to ``reports/summary.json``.

    Overwrites the previous run's file.

    Parameters
    ----------
    summary:
        Populated ``RunSummary`` object.
    settings:
        Populated ``CMSettings`` (provides the output path).
    """
    path = settings.paths.summary_json_path
    path.parent.mkdir(parents=True, exist_ok=True)

    # Use Pydantic's JSON serializer for datetime handling
    json_str = summary.model_dump_json(indent=2)

    path.write_text(json_str, encoding="utf-8")
    logger.info("Run summary written to '%s'.", path)
