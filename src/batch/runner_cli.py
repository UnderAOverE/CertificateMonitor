"""
Typer CLI entry point for the certificate-monitoring pipeline.

Usage
-----
    # Normal daily run
    python -m src.batch.runner_cli

    # Force rebuild parquet cache from MongoDB
    python -m src.batch.runner_cli --force-refresh

    # Skip SMTP and MongoDB writes (log only)
    python -m src.batch.runner_cli --dry-run

    # Process only specific sources
    python -m src.batch.runner_cli --source SSG --source HashiCorp

    # Create MongoDB indexes (run once after deployment)
    python -m src.batch.runner_cli create-indexes

    # Combine flags
    python -m src.batch.runner_cli --force-refresh --dry-run

Shell script integration
------------------------
Your shell script can call this as:
    python -m src.batch.runner_cli "$@"

and pass through any flags, e.g.:
    ./run_cert_monitor.sh --force-refresh
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Annotated

import typer

from src.batch.config.settings.cm import CMSettings

app = typer.Typer(
    name="cert-monitor",
    help="Production certificate expiration monitoring pipeline.",
    add_completion=False,
)


def _configure_logging(verbose: bool) -> None:
    """Set up root logger with a clean format."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# Main run command
# ---------------------------------------------------------------------------


@app.command()
def run(
    force_refresh: Annotated[
        bool,
        typer.Option(
            "--force-refresh",
            help="Ignore cache age and rebuild the parquet cache from MongoDB.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Process everything but skip MongoDB upserts and SMTP sends.",
        ),
    ] = False,
    source: Annotated[
        list[str] | None,
        typer.Option(
            "--source",
            help="Run for a specific source only. Can be repeated. "
                 "Example: --source SSG --source HashiCorp",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable DEBUG logging."),
    ] = False,
) -> None:
    """
    Run the certificate-monitoring pipeline.

    Loads certificates from MongoDB (or parquet cache), detects expiring
    production certificates, runs fuzzy renewal matching, upserts results,
    and sends alert emails.
    """
    _configure_logging(verbose)
    logger = logging.getLogger(__name__)

    try:
        settings = CMSettings()
    except Exception as exc:
        typer.echo(f"[FATAL] Failed to load settings: {exc}", err=True)
        raise typer.Exit(code=2)

    # ── Wire up your repositories here ───────────────────────────────────────
    # Replace these None values with your actual Motor repository instances.
    # Example:
    #   client = AsyncIOMotorClient(settings.mongo.uri)
    #   db = client[settings.mongo.database]
    #   consolidated_repo = CMConsolidatedDataMotorRepository(db)
    #   ignore_repo = IgnoreAlertsMotorRepository(db)
    #   alert_repo = CertificateAlertsMotorRepository(db)
    # ─────────────────────────────────────────────────────────────────────────
    consolidated_repo = None  # REPLACE WITH YOUR REPO
    ignore_repo = None        # REPLACE WITH YOUR REPO
    alert_repo = None         # REPLACE WITH YOUR REPO

    source_filter = list(source) if source else None

    logger.info(
        "Starting pipeline | force_refresh=%s dry_run=%s source_filter=%s",
        force_refresh, dry_run, source_filter,
    )

    exit_code = asyncio.run(
        _run(
            settings=settings,
            consolidated_repo=consolidated_repo,
            ignore_repo=ignore_repo,
            alert_repo=alert_repo,
            force_refresh=force_refresh,
            dry_run=dry_run,
            source_filter=source_filter,
        )
    )

    raise typer.Exit(code=exit_code)


async def _run(
    settings: CMSettings,
    consolidated_repo,
    ignore_repo,
    alert_repo,
    force_refresh: bool,
    dry_run: bool,
    source_filter: list[str] | None,
) -> int:
    """Async wrapper so we can ``asyncio.run`` from the sync Typer command."""
    from src.batch.services.runner import run_pipeline
    return await run_pipeline(
        settings=settings,
        consolidated_repo=consolidated_repo,
        ignore_repo=ignore_repo,
        alert_repo=alert_repo,
        force_refresh=force_refresh,
        dry_run=dry_run,
        source_filter=source_filter,
    )


# ---------------------------------------------------------------------------
# Create indexes command
# ---------------------------------------------------------------------------


@app.command(name="create-indexes")
def create_indexes(
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable DEBUG logging."),
    ] = False,
) -> None:
    """
    Create all required MongoDB indexes.

    Run this ONCE after deployment.  Safe to re-run — existing indexes
    are skipped silently.
    """
    _configure_logging(verbose)

    try:
        settings = CMSettings()
    except Exception as exc:
        typer.echo(f"[FATAL] Failed to load settings: {exc}", err=True)
        raise typer.Exit(code=2)

    from src.batch.repositories.indexes import create_all_indexes
    asyncio.run(create_all_indexes(settings))
    typer.echo("Indexes created / confirmed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    app()
