"""
Email sender — SMTP abstraction layer.

Provides a single ``send_email`` coroutine that dispatches HTML emails via
SMTP.  All connection parameters are read from ``EmailSettings``.

Failure handling
----------------
* Transient SMTP errors on individual emails are caught and logged; they do
  NOT abort the pipeline.
* If the consolidated email fails to send, a plain-text fallback alert is
  sent to the developer address.
* ``send_developer_alert`` is also called directly by the runner on pipeline-level failures (uncaught exceptions, MongoDB connection errors, etc.).

Design notes
------------
* Uses Python's built-in ``smtplib`` with ``email.mime`` — no third-party
  SMTP library required.
* Runs in a thread pool executor (``asyncio.get_event_loop().run_in_executor``)
  so the blocking SMTP calls do not stall the async event loop.
* Both STARTTLS (port 587) and plain (port 25) are supported.
"""

from __future__ import annotations

import asyncio
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import partial

from src.batch.config.settings.cm import CMSettings, EmailSettings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Low-level SMTP send (synchronous — run in executor)
# ---------------------------------------------------------------------------


def _smtp_send_sync(
    smtp_settings: EmailSettings,
    to_addresses: list[str],
    subject: str,
    html_body: str,
    plain_body: str = "",
) -> None:
    """
    Blocking SMTP send.  Intended to be called via ``run_in_executor``.

    Raises
    ------
    smtplib.SMTPException
        On any SMTP-level error.
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_settings.sender_address
    msg["To"] = ", ".join(to_addresses)

    # Attach plain-text fallback first (email clients prefer last part)
    if plain_body:
        msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(smtp_settings.smtp_host, smtp_settings.smtp_port) as server:
        if smtp_settings.smtp_use_tls:
            server.starttls()
        if smtp_settings.smtp_username and smtp_settings.smtp_password:
            server.login(smtp_settings.smtp_username, smtp_settings.smtp_password)
        server.sendmail(
            smtp_settings.sender_address,
            to_addresses,
            msg.as_string(),
        )


# ---------------------------------------------------------------------------
# Public async API
# ---------------------------------------------------------------------------


async def send_email(
    settings: CMSettings,
    to_addresses: list[str],
    subject: str,
    html_body: str,
    plain_body: str = "",
    dry_run: bool = False,
) -> bool:
    """
    Send an HTML email asynchronously.
    """
    if not to_addresses:
        logger.info("No recipients for '%s' — skipping.", subject)
        return True

    if dry_run:
        logger.info(
            "[DRY RUN] Would send email '%s' to %s.",
            subject, to_addresses,
        )
        return True

    loop = asyncio.get_event_loop()
    try:
        # Pass the function and arguments directly to the executor.
        # This resolves the 'args unfilled' linter error.
        await loop.run_in_executor(
            None,               # Use default ThreadPoolExecutor
            _smtp_send_sync,    # The synchronous function to run
            settings.email,     # Arg 1
            to_addresses,       # Arg 2
            subject,            # Arg 3
            html_body,          # Arg 4
            plain_body,         # Arg 5
        )
        logger.info("Email sent: '%s' → %s", subject, to_addresses)
        return True

    except Exception as exc:
        logger.error(
            "Failed to send email '%s' to %s: %r",
            subject, to_addresses, exc,
        )
        return False


async def send_developer_alert(
    settings: CMSettings,
    subject: str,
    body: str,
    dry_run: bool = False,
) -> None:
    """
    Send a plain-text alert email to the configured developer address.

    Used for pipeline-level failures and critical errors.  Never raises —
    failures are silently logged so they don't mask the original error.

    Parameters
    ----------
    settings:
        Populated ``CMSettings``.
    subject:
        Alert subject line.
    body:
        Plain-text description of the failure.
    dry_run:
        If ``True``, log only.
    """
    dev_address = settings.email.developer_alert_email
    if not dev_address:
        logger.warning("No developer_alert_email configured — cannot send failure alert.")
        return

    html_body = (
        f"<html><body><pre style='font-family:monospace;'>"
        f"{body}"
        f"</pre></body></html>"
    )

    try:
        await send_email(
            settings=settings,
            to_addresses=[dev_address],
            subject=f"[CERT MONITOR FAILURE] {subject}",
            html_body=html_body,
            plain_body=body,
            dry_run=dry_run,
        )
    except Exception as exc:
        # Swallow — we cannot let an alert failure propagate
        logger.error("Developer alert send failed: %r", exc)
