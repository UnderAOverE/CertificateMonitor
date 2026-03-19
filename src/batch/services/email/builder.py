"""
HTML email template builder.

Generates modern, responsive, email-client-compatible HTML for both the
consolidated and per-source certificate alert emails.

Design principles
-----------------
* Inline CSS only — email clients strip ``<style>`` blocks.
* Table-based layout for Outlook compatibility.
* Colour-coded status badges (Action Required = red, Matched Renewal = green,
  Missing Service = amber).
* Highlighted cells when CSI ID changes or ssl_cm_status is not 'Activated'.
* No external resources (fonts, images) — fully self-contained.

All template functions return raw HTML strings suitable for sending via SMTP
as the ``text/html`` alternative.
"""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any

from src.batch.models.alerts import (
    AkamaiSourceModel,
    ApigeeSourceModel,
    CertificateAlertDocument,
    CertificateModel,
    EvolvenSourceModel,
    HashiCorpSourceModel,
    PossibleMatchModel,
    RunSummary,
    SSGSourceModel,
    SSLTrackerSourceModel,
)
from src.batch.models.enums import AlertStatus, SourceName

# ---------------------------------------------------------------------------
# Colour palette and shared styles
# ---------------------------------------------------------------------------

_COLOURS: dict[str, str] = {
    "bg_page": "#f0f4f8",
    "bg_card": "#ffffff",
    "bg_header": "#1a2744",
    "bg_header_accent": "#2563eb",
    "bg_table_head": "#1e3a5f",
    "bg_row_even": "#f8fafc",
    "bg_row_odd": "#ffffff",
    "action_required": "#dc2626",
    "action_required_bg": "#fef2f2",
    "matched_renewal": "#16a34a",
    "matched_renewal_bg": "#f0fdf4",
    "missing_service": "#d97706",
    "missing_service_bg": "#fffbeb",
    "highlight_warn": "#fff3cd",
    "highlight_warn_border": "#f59e0b",
    "text_primary": "#1e293b",
    "text_secondary": "#64748b",
    "text_white": "#ffffff",
    "border": "#e2e8f0",
    "border_strong": "#cbd5e1",
    "link": "#2563eb",
}

_FONT = "font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;"
_CELL = (
    f"padding: 10px 14px; border-bottom: 1px solid {_COLOURS['border']}; "
    f"{_FONT} font-size: 13px; color: {_COLOURS['text_primary']}; vertical-align: top;"
)
_HEAD_CELL = (
    f"padding: 10px 14px; background-color: {_COLOURS['bg_table_head']}; "
    f"color: {_COLOURS['text_white']}; {_FONT} font-size: 12px; font-weight: 600; "
    f"text-transform: uppercase; letter-spacing: 0.05em; text-align: left;"
)

# Valid values for the Table 1 sort setting
_VALID_TABLE1_SORT_KEYS: frozenset[str] = frozenset({"days_to_expiration", "source", "csi_id"})


# ---------------------------------------------------------------------------
# Shared micro-components
# ---------------------------------------------------------------------------


def _esc(value: Any) -> str:
    """HTML-escape a value for safe injection into HTML."""
    if value is None:
        return '<span style="color:#94a3b8;font-style:italic;">—</span>'
    return html.escape(str(value))


def _status_badge(status: AlertStatus) -> str:
    colour_map = {
        AlertStatus.ACTION_REQUIRED: (_COLOURS["action_required"], _COLOURS["action_required_bg"]),
        AlertStatus.MATCHED_RENEWAL: (_COLOURS["matched_renewal"], _COLOURS["matched_renewal_bg"]),
        AlertStatus.MISSING_SERVICE: (_COLOURS["missing_service"], _COLOURS["missing_service_bg"]),
    }
    colour, bg = colour_map.get(status, ("#64748b", "#f1f5f9"))
    label = html.escape(status.value)
    return (
        f'<span style="display:inline-block;padding:3px 10px;border-radius:12px;'
        f'background:{bg};color:{colour};font-weight:600;font-size:11px;'
        f'border:1px solid {colour};">{label}</span>'
    )


def _days_badge(days: int) -> str:
    """Colour-coded days-to-expiration pill."""
    if days <= 3:
        bg, colour = "#fef2f2", "#dc2626"
    elif days <= 7:
        bg, colour = "#fff7ed", "#ea580c"
    elif days <= 30:
        bg, colour = "#fffbeb", "#d97706"
    else:
        bg, colour = "#f0fdf4", "#16a34a"
    return (
        f'<span style="display:inline-block;padding:2px 9px;border-radius:10px;'
        f'background:{bg};color:{colour};font-weight:700;font-size:12px;">'
        f'{days}d</span>'
    )


def _format_date(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d")


def _section_header(title: str, subtitle: str = "") -> str:
    sub = (
        f'<div style="{_FONT} font-size:12px;color:{_COLOURS["text_secondary"]};margin-top:4px;">'
        f'{html.escape(subtitle)}</div>'
    ) if subtitle else ""
    return (
        f'<div style="margin:28px 0 12px 0;">'
        f'<div style="{_FONT} font-size:16px;font-weight:700;color:{_COLOURS["text_primary"]};'
        f'border-left:4px solid {_COLOURS["bg_header_accent"]};padding-left:12px;">'
        f'{html.escape(title)}</div>{sub}</div>'
    )


def _stat_pill(label: str, value: int | str, colour: str) -> str:
    return (
        f'<div style="display:inline-block;background:#f8fafc;border:1px solid {colour}33;'
        f'border-radius:8px;padding:10px 16px;margin:4px;min-width:100px;text-align:center;">'
        f'<div style="{_FONT} font-size:22px;font-weight:800;color:{colour};">{value}</div>'
        f'<div style="{_FONT} font-size:11px;color:{_COLOURS["text_secondary"]};margin-top:2px;">'
        f'{html.escape(label)}</div>'
        f'</div>'
    )


# ---------------------------------------------------------------------------
# Email page shell
# ---------------------------------------------------------------------------


def _email_shell(
    title: str,
    body: str,
    run_time: datetime,
    contact_email: str = "shanereddy@email.com",
    app_version: str = "1.0.0",
) -> str:
    """
    Wrap body HTML in a complete email-safe outer shell.

    Parameters
    ----------
    title:
        Email title shown in the header banner.
    body:
        Pre-built HTML body content.
    run_time:
        UTC datetime of the pipeline run (shown in header).
    contact_email:
        Address linked in the footer contact banner
        (``CM_EMAIL__CONTACT_EMAIL``).
    app_version:
        Version string shown in the footer banner
        (``CM_EMAIL__APP_VERSION``).
    """
    run_str = run_time.strftime("%Y-%m-%d %H:%M UTC")
    contact_href = "mailto:" + html.escape(contact_email)
    parts: list[str] = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="UTF-8">',
        '  <meta name="viewport" content="width=device-width,initial-scale=1">',
        f"  <title>{html.escape(title)}</title>",
        "</head>",
        f'<body style="margin:0;padding:0;background:{_COLOURS["bg_page"]};{_FONT}">',
        '  <table width="100%" cellpadding="0" cellspacing="0" border="0"',
        f'         style="background:{_COLOURS["bg_page"]};padding:24px 16px;">',
        "    <tr><td align=\"center\">",
        '      <table width="900" cellpadding="0" cellspacing="0" border="0"',
        '             style="max-width:900px;width:100%;">',
        # header
        "        <tr>",
        f'          <td style="background:{_COLOURS["bg_header"]};border-radius:8px 8px 0 0;padding:24px 28px;">',
        f'            <div style="{_FONT} font-size:11px;font-weight:600;letter-spacing:0.1em;color:#93c5fd;text-transform:uppercase;margin-bottom:6px;">Certificate Monitor</div>',
        f'            <div style="{_FONT} font-size:22px;font-weight:700;color:{_COLOURS["text_white"]};margin-bottom:4px;">{html.escape(title)}</div>',
        f'            <div style="{_FONT} font-size:12px;color:#93c5fd;">Generated: {run_str}</div>',
        "          </td>",
        "        </tr>",
        # body
        "        <tr>",
        f'          <td style="background:{_COLOURS["bg_card"]};padding:24px 28px;border-radius:0 0 8px 8px;border:1px solid {_COLOURS["border"]};border-top:none;">',
        f"            {body}",
        "          </td>",
        "        </tr>",
        # footer contact banner
        "        <tr>",
        '          <td style="padding:12px 0 4px 0;">',
        '            <table width="100%" cellpadding="0" cellspacing="0" border="0"',
        f'                   style="background:{_COLOURS["bg_header"]};border-radius:6px;">',
        "              <tr>",
        '                <td style="padding:14px 20px;text-align:center;">',
        f'                  <span style="{_FONT} font-size:12px;color:#93c5fd;">',
        "                    For any queries or concerns contact &nbsp;",
        f'                    <a href="{contact_href}" style="color:#60a5fa;font-weight:600;text-decoration:underline;">{html.escape(contact_email)}</a>',
        "                    &nbsp;&nbsp;|&nbsp;&nbsp;",
        f'                    <span style="color:#cbd5e1;">Certificate Monitoring Application v{html.escape(app_version)}</span>',
        "                  </span>",
        "                </td>",
        "              </tr>",
        "            </table>",
        "          </td>",
        "        </tr>",
        # legal footer
        "        <tr>",
        f'          <td style="padding:8px 0 16px 0;text-align:center;{_FONT} font-size:11px;color:{_COLOURS["text_secondary"]};">',
        "            This is an automated alert from the Certificate Monitoring Pipeline.",
        "            Do not reply to this email.",
        "          </td>",
        "        </tr>",
        "      </table>",
        "    </td></tr>",
        "  </table>",
        "</body>",
        "</html>",
    ]
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Source-details card builders (per source type)
# ---------------------------------------------------------------------------


def _source_card_ssg(sd: SSGSourceModel) -> str:
    instances_html = "".join(
        f'<span style="display:inline-block;padding:2px 8px;margin:2px;border-radius:4px;'
        f'background:#eff6ff;color:#1d4ed8;font-size:11px;border:1px solid #bfdbfe;">'
        f'{_esc(i)}</span>'
        for i in (sd.instances or [])
    ) or _esc(None)
    rows = [
        ("Domain", _esc(sd.domain)),
        ("Internal Domain", _esc(sd.internal_domain)),
        ("Service Name", _esc(sd.service_name)),
        ("URL In", f'<code style="font-size:11px;background:#f1f5f9;padding:2px 6px;border-radius:3px;">{_esc(sd.url_in)}</code>'),
        ("URL Out", f'<code style="font-size:11px;background:#f1f5f9;padding:2px 6px;border-radius:3px;">{_esc(sd.url_out)}</code>'),
        ("Instances", instances_html),
    ]
    return _detail_card("SSG Gateway Details", rows, "#eff6ff", "#1d4ed8")


def _source_card_hashicorp(sd: HashiCorpSourceModel) -> str:
    total = sd.replicas.get("total", "unknown")
    available = sd.replicas.get("available", "unknown")
    unknown = total == "unknown" or available == "unknown"
    replica_colour = _COLOURS["missing_service"] if unknown else _COLOURS["matched_renewal"]
    replica_html = (
        f'<span style="font-weight:700;color:{replica_colour};">'
        f'{_esc(available)} / {_esc(total)}</span>'
        f'<span style="font-size:11px;color:{_COLOURS["text_secondary"]};"> (available/total)</span>'
    )
    rows = [
        ("Cluster", _esc(sd.cluster)),
        ("Project (Namespace)", _esc(sd.project)),
        ("Service Name", _esc(sd.service_name)),
        ("Replicas", replica_html),
    ]
    return _detail_card("HashiCorp / OpenShift Details", rows, "#f5f3ff", "#6d28d9")


def _source_card_evolven(sd: EvolvenSourceModel) -> str:
    instances_html = "".join(
        f'<span style="display:inline-block;padding:2px 8px;margin:2px;border-radius:4px;'
        f'background:#ecfdf5;color:#065f46;font-size:11px;border:1px solid #6ee7b7;">'
        f'{_esc(i)}</span>'
        for i in (sd.instances or [])
    ) or _esc(None)
    rows = [
        ("Host", _esc(sd.host)),
        ("Path", _esc(sd.path)),
        ("Instances", instances_html),
    ]
    return _detail_card("Evolven Details", rows, "#ecfdf5", "#065f46")


def _source_card_apigee(sd: ApigeeSourceModel) -> str:
    rows = [
        ("Domain", _esc(sd.domain)),
        ("Host", _esc(sd.host)),
        ("Path", _esc(sd.path)),
        ("URL In", f'<code style="font-size:11px;background:#f1f5f9;padding:2px 6px;border-radius:3px;">{_esc(sd.url_in)}</code>'),
        ("URL Out", f'<code style="font-size:11px;background:#f1f5f9;padding:2px 6px;border-radius:3px;">{_esc(sd.url_out)}</code>'),
    ]
    return _detail_card("Apigee Gateway Details", rows, "#fff7ed", "#c2410c")


def _source_card_akamai(sd: AkamaiSourceModel) -> str:
    sans_html = "".join(
        f'<span style="display:inline-block;padding:2px 8px;margin:2px;border-radius:4px;'
        f'background:#fdf4ff;color:#7e22ce;font-size:11px;border:1px solid #e9d5ff;">'
        f'{_esc(s)}</span>'
        for s in (sd.san_names or [])
    ) or _esc(None)
    rows = [
        ("Certificate Owner", _esc(sd.certificate_owner)),
        ("Support Group Email", _esc(sd.support_group_email)),
        ("SAN Names", sans_html),
    ]
    return _detail_card("Akamai CDN Details", rows, "#fdf4ff", "#7e22ce")


def _source_card_ssl_tracker(sd: SSLTrackerSourceModel) -> str:
    status_warn = sd.status is not None and sd.status != "Activated"
    status_html = (
        f'<span style="color:{_COLOURS["action_required"]};font-weight:600;">{_esc(sd.status)} ⚠</span>'
        if status_warn else _esc(sd.status)
    )
    rows = [("SSL CM Status", status_html)]
    return _detail_card("SSL Tracker Details", rows, "#f8fafc", "#475569")


def _detail_card(title: str, rows: list[tuple[str, str]], bg: str, accent: str) -> str:
    rows_html = "".join(
        f'<tr>'
        f'<td style="padding:7px 12px;font-size:12px;font-weight:600;color:{_COLOURS["text_secondary"]};'
        f'white-space:nowrap;border-bottom:1px solid {_COLOURS["border"]};width:35%;">{html.escape(k)}</td>'
        f'<td style="padding:7px 12px;font-size:13px;color:{_COLOURS["text_primary"]};'
        f'border-bottom:1px solid {_COLOURS["border"]};">{v}</td>'
        f'</tr>'
        for k, v in rows
    )
    return (
        f'<div style="background:{bg};border:1px solid {accent}33;border-radius:6px;'
        f'margin-bottom:4px;overflow:hidden;">'
        f'<div style="background:{accent};padding:6px 12px;'
        f'{_FONT} font-size:11px;font-weight:700;color:white;letter-spacing:0.05em;'
        f'text-transform:uppercase;">{html.escape(title)}</div>'
        f'<table width="100%" cellpadding="0" cellspacing="0">{rows_html}</table>'
        f'</div>'
    )


def build_source_detail_card(doc: CertificateAlertDocument) -> str:
    """Dispatch to the correct source card builder."""
    sd = doc.source_details
    if isinstance(sd, SSGSourceModel):
        return _source_card_ssg(sd)
    if isinstance(sd, HashiCorpSourceModel):
        return _source_card_hashicorp(sd)
    if isinstance(sd, EvolvenSourceModel):
        return _source_card_evolven(sd)
    if isinstance(sd, ApigeeSourceModel):
        return _source_card_apigee(sd)
    if isinstance(sd, AkamaiSourceModel):
        return _source_card_akamai(sd)
    if isinstance(sd, SSLTrackerSourceModel):
        return _source_card_ssl_tracker(sd)
    return ""


def _mini_source_summary(doc: CertificateAlertDocument) -> str:
    """One-line source summary for Table 1 Source Details column."""
    sd = doc.source_details
    if isinstance(sd, SSGSourceModel):
        return f'<span style="font-size:11px;">{_esc(sd.service_name)} @ {_esc(sd.domain)}</span>'
    if isinstance(sd, HashiCorpSourceModel):
        return f'<span style="font-size:11px;">{_esc(sd.cluster)}/{_esc(sd.project)}/{_esc(sd.service_name)}</span>'
    if isinstance(sd, EvolvenSourceModel):
        return f'<span style="font-size:11px;">{_esc(sd.host)}{_esc(sd.path)}</span>'
    if isinstance(sd, ApigeeSourceModel):
        return f'<span style="font-size:11px;">{_esc(sd.domain)} — {_esc(sd.host)}</span>'
    if isinstance(sd, AkamaiSourceModel):
        return f'<span style="font-size:11px;">Owner: {_esc(sd.certificate_owner)}</span>'
    if isinstance(sd, SSLTrackerSourceModel):
        return f'<span style="font-size:11px;">Status: {_esc(sd.status)}</span>'
    return ""


# ---------------------------------------------------------------------------
# Consolidated email builder
# ---------------------------------------------------------------------------


def build_consolidated_email(
    all_documents: list[CertificateAlertDocument],
    run_summary: RunSummary,
    settings_snapshot: dict[str, Any],
    jira_details_fn=None,
    contact_email: str = "shanereddy@email.com",
    app_version: str = "1.0.0",
    table1_sort_by: str = "days_to_expiration",
) -> str:
    """
    Build the HTML for the consolidated action-required email.

    Contains:
    * Snapshot section: SOURCE | ALERTS table only (no stat pills).
    * Table 1: action-required certs, deduplicated by (DN, SN, source),
      same-source groups merged into numbered details list.
    * Table 2: possible matches grouped by dn_clean, combined SN cell.
    * Important Information section (thresholds / key settings).
    * Footer banner (contact + version).
    """
    from collections import defaultdict
    from src.batch.utilities.cm import clean_string

    run_time = run_summary.run_datetime

    # ── Snapshot: SOURCE | ALERTS table only ─────────────────────────────────
    # "ALERTS" = action_required certs that are NOT acknowledged (matches Table 1)
    snapshot_rows = "".join(
        f'<tr style="background:{"#f8fafc" if i % 2 == 0 else "#ffffff"};">'
        f'<td style="{_CELL} font-weight:600;">{_esc(s.source.value)}</td>'
        f'<td style="{_CELL} color:{_COLOURS["action_required"]};font-weight:700;">{s.action_required}</td>'
        f'</tr>'
        for i, s in enumerate(run_summary.sources)
        if s.action_required > 0
    )
    if not snapshot_rows:
        snapshot_rows = (
            f'<tr><td colspan="2" style="{_CELL} text-align:center;'
            f'color:{_COLOURS["matched_renewal"]};font-style:italic;">'
            f'✓ No action-required certificates across all sources.</td></tr>'
        )

    snapshot_html = (
        _section_header("Run Snapshot",
                        f"Pipeline run at {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
        + f'<table width="100%" cellpadding="0" cellspacing="0"'
          f' style="border-collapse:collapse;border:1px solid {_COLOURS["border"]};'
          f'border-radius:6px;overflow:hidden;margin-bottom:28px;">'
          f'<tr>'
          f'<th style="{_HEAD_CELL}">Source</th>'
          f'<th style="{_HEAD_CELL}">Alerts (Action Required)</th>'
          f'</tr>'
          f'{snapshot_rows}'
          f'</table>'
    )

    # ── Collect action-required, non-acknowledged certs ───────────────────────
    action_certs: list[tuple[CertificateAlertDocument, CertificateModel]] = [
        (doc, cert)
        for doc in all_documents
        for cert in doc.certificates
        if cert.attention_required and not cert.acknowledged
    ]

    # ── Sort ──────────────────────────────────────────────────────────────────
    _VALID_SORT = {"days_to_expiration", "source", "csi_id"}
    _sort_key = table1_sort_by if table1_sort_by in _VALID_SORT else "days_to_expiration"

    def _t1_sort(pair: tuple) -> tuple:
        d, c = pair
        days = c.days_to_expiration if c.days_to_expiration is not None else 9999
        if _sort_key == "source":
            return (d.source.value, days)
        if _sort_key == "csi_id":
            return (d.csi_id if d.csi_id is not None else 999999, days)
        return (days, d.source.value)

    action_certs.sort(key=_t1_sort)

    # ── Dedup by (DN, SN, source) — merge same-source groups ─────────────────
    # Key: (distinguished_name, serial_number, source_value)
    # Value: list of docs whose source_details we merge as numbered list
    group_map: dict[tuple, dict] = {}
    group_order: list[tuple] = []

    for doc, cert in action_certs:
        key = (cert.distinguished_name, cert.serial_number, doc.source.value)
        if key not in group_map:
            group_map[key] = {"cert": cert, "doc": doc, "all_docs": []}
            group_order.append(key)
        group_map[key]["all_docs"].append(doc)

    # ── Table 1 rows ──────────────────────────────────────────────────────────
    table1_rows_html = ""
    row_num = 1

    for key in group_order:
        entry = group_map[key]
        cert: CertificateModel = entry["cert"]
        doc: CertificateAlertDocument = entry["doc"]
        all_docs: list[CertificateAlertDocument] = entry["all_docs"]

        # Cert cell: DN + CSI/SN below
        cert_cell = (
            f'<code style="font-size:11px;background:#f1f5f9;padding:2px 5px;'
            f'border-radius:3px;">{_esc(cert.distinguished_name)}</code>'
            f'<div style="font-size:11px;color:{_COLOURS["text_secondary"]};margin-top:4px;">'
            f'CSI: {_esc(doc.csi_id)} &nbsp;|&nbsp; SN: {_esc(cert.serial_number)}</div>'
        )

        # JIRA cell
        if cert.acknowledged:
            jira_html = (
                f'<span style="color:{_COLOURS["matched_renewal"]};font-weight:600;">✓ Acknowledged</span>'
                + (f'<br><span style="font-size:11px;color:{_COLOURS["text_secondary"]};">'
                   f'by {_esc(cert.acknowledged_by)}</span>' if cert.acknowledged_by else "")
            )
        elif jira_details_fn:
            jira_html = jira_details_fn(cert)
        else:
            jira_html = f'<span style="color:{_COLOURS["text_secondary"]};font-style:italic;">N/A</span>'

        # Details cell: numbered list of source occurrences
        source_label_map = {
            "SSG": ("#eff6ff", "#1d4ed8"),
            "HashiCorp": ("#f5f3ff", "#6d28d9"),
            "Evolven": ("#ecfdf5", "#065f46"),
            "Evolven_Legacy": ("#ecfdf5", "#065f46"),
            "APIGEE": ("#fff7ed", "#c2410c"),
            "AKAMAI": ("#fdf4ff", "#7e22ce"),
            "SSL Tracker": ("#f8fafc", "#475569"),
        }
        src_bg, src_fg = source_label_map.get(doc.source.value, ("#f1f5f9", "#334155"))

        details_items = []
        for idx, d in enumerate(all_docs, 1):
            mini = _mini_source_summary(d)
            details_items.append(
                f'<div style="margin-bottom:4px;">'
                f'<span style="font-size:11px;font-weight:700;color:{_COLOURS["text_secondary"]};">{idx}.</span> '
                f'{mini}'
                f'</div>'
            )

        details_cell = (
            f'<div style="margin-bottom:6px;">'
            f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
            f'background:{src_bg};color:{src_fg};font-size:11px;font-weight:600;">'
            f'{_esc(doc.source.value)}</span>'
            f'</div>'
            + "".join(details_items)
        )

        bg = "#ffffff" if row_num % 2 == 0 else "#f8fafc"
        table1_rows_html += (
            f'<tr style="background:{bg};">'
            f'<td style="{_CELL} font-weight:600;color:{_COLOURS["text_secondary"]};width:32px;">{row_num}</td>'
            f'<td style="{_CELL}">{cert_cell}</td>'
            f'<td style="{_CELL} text-align:center;">{_days_badge(cert.days_to_expiration)}</td>'
            f'<td style="{_CELL} font-size:12px;">{_format_date(cert.expiration_date)}</td>'
            f'<td style="{_CELL}">{jira_html}</td>'
            f'<td style="{_CELL}">{details_cell}</td>'
            f'</tr>'
        )
        row_num += 1

    if not table1_rows_html:
        table1_rows_html = (
            f'<tr><td colspan="6" style="{_CELL} text-align:center;'
            f'color:{_COLOURS["text_secondary"]};font-style:italic;">'
            f'No action-required certificates at this time.</td></tr>'
        )

    sort_label = {
        "days_to_expiration": "Days to Expiration (most urgent first)",
        "source": "Source (A→Z), then Days",
        "csi_id": "CSI ID (asc), then Days",
    }.get(_sort_key, _sort_key)

    table1_html = (
        _section_header("Table 1 — Action Required Certificates",
                        f"{row_num - 1} certificate(s) · Sorted by: {sort_label}")
        + f'<table width="100%" cellpadding="0" cellspacing="0"'
          f' style="border-collapse:collapse;border:1px solid {_COLOURS["border"]};'
          f'border-radius:6px;overflow:hidden;margin-bottom:32px;">'
          f'<tr>'
          f'<th style="{_HEAD_CELL}">#</th>'
          f'<th style="{_HEAD_CELL}">Certificate (DN / CSI / SN)</th>'
          f'<th style="{_HEAD_CELL}">Days</th>'
          f'<th style="{_HEAD_CELL}">Expiration</th>'
          f'<th style="{_HEAD_CELL}">JIRA</th>'
          f'<th style="{_HEAD_CELL}">Source Details</th>'
          f'</tr>'
          f'{table1_rows_html}'
          f'</table>'
    )

    # ── Table 2: group expiring certs by dn_clean, combined SN cell ───────────
    # Build noise tuple for clean_string — use noise words from settings_snapshot
    # (passed as a list; fall back to empty tuple if not present)
    noise_words = tuple(settings_snapshot.get("noise_words", []))

    # Group action certs by dn_clean
    dn_groups: dict[str, list[tuple[CertificateAlertDocument, CertificateModel]]] = defaultdict(list)
    for doc, cert in action_certs:
        dn_c = clean_string(cert.distinguished_name or "", noise_words)
        dn_groups[dn_c].append((doc, cert))

    MAX_DISPLAY = 3
    table2_rows_html = ""
    pm_row_num = 1

    # Process each unique dn_clean group
    seen_dn_groups: set[str] = set()
    for doc, cert in action_certs:
        dn_c = clean_string(cert.distinguished_name or "", noise_words)
        if dn_c in seen_dn_groups:
            continue
        seen_dn_groups.add(dn_c)

        group_pairs = dn_groups[dn_c]

        # Collect possible matches — use the first cert that has any
        possible_matches: list[PossibleMatchModel] = []
        for _, c in group_pairs:
            if c.possible_matches:
                possible_matches = c.possible_matches
                break

        if not possible_matches:
            continue

        # Expiring cert cell — show DN once, then each unique SN
        unique_sns = list(dict.fromkeys(
            (c.serial_number, d.csi_id, c.days_to_expiration, c.expiration_date)
            for d, c in group_pairs
        ))

        sn_lines = "".join(
            f'<div style="font-size:11px;color:{_COLOURS["text_secondary"]};margin-top:3px;">'
            f'SN: {_esc(sn)} &nbsp;·&nbsp; CSI: {_esc(csi)} &nbsp;·&nbsp; '
            f'{_days_badge(days)} {_format_date(exp)}'
            f'</div>'
            for sn, csi, days, exp in unique_sns
        )

        alert_cell = (
            f'<div style="font-size:12px;">'
            f'<code style="background:#fef2f2;padding:2px 5px;border-radius:3px;'
            f'color:{_COLOURS["action_required"]};font-size:11px;">'
            f'{_esc(group_pairs[0][1].distinguished_name)}</code>'
            f'</div>'
            + sn_lines
        )

        # Match cells
        def _match_cell(pm: PossibleMatchModel | None, alert_csi: int | None) -> str:
            if pm is None:
                return f'<td style="{_CELL} color:{_COLOURS["text_secondary"]};font-style:italic;vertical-align:top;">—</td>'
            csi_changed = pm.csi_id is not None and alert_csi is not None and pm.csi_id != alert_csi
            status_warn = pm.ssl_cm_status is not None and pm.ssl_cm_status != "Activated"
            warn_bg = _COLOURS["highlight_warn"] if (csi_changed or status_warn) else ""
            warn_border = f"border-left:3px solid {_COLOURS['highlight_warn_border']};" if (csi_changed or status_warn) else ""
            content = (
                f'<div style="font-size:12px;font-weight:700;color:{_COLOURS["bg_header_accent"]};">'
                f'{pm.similarity_score:.1f}% match</div>'
                f'<div style="font-size:11px;margin:3px 0;">'
                f'<code style="background:#f1f5f9;padding:1px 4px;border-radius:2px;">'
                f'{_esc(pm.distinguished_name)}</code></div>'
                f'<div style="font-size:11px;color:{_COLOURS["text_secondary"]};">SN: {_esc(pm.serial_number)}</div>'
                f'<div style="font-size:11px;">Exp: {_format_date(pm.expiration_date)} ({pm.days_to_expiration}d)</div>'
                f'<div style="font-size:11px;color:{_COLOURS["text_secondary"]};">CSI: {_esc(pm.csi_id)}</div>'
                + (f'<div style="font-size:11px;background:{_COLOURS["highlight_warn"]};'
                   f'border-left:3px solid {_COLOURS["highlight_warn_border"]};'
                   f'padding:2px 6px;margin-top:3px;">⚠ CSI changed</div>' if csi_changed else "")
                + (f'<div style="font-size:11px;background:{_COLOURS["action_required_bg"]};'
                   f'border-left:3px solid {_COLOURS["action_required"]};'
                   f'padding:2px 6px;margin-top:3px;">⚠ Status: {_esc(pm.ssl_cm_status)}</div>' if status_warn else "")
            )
            return f'<td style="{_CELL} background:{warn_bg};{warn_border}vertical-align:top;">{content}</td>'

        matches = list(possible_matches[:MAX_DISPLAY])
        while len(matches) < MAX_DISPLAY:
            matches.append(None)

        first_csi = group_pairs[0][0].csi_id
        table2_rows_html += (
            f'<tr style="background:{"#f8fafc" if pm_row_num % 2 == 0 else "#ffffff"};">'
            f'<td style="{_CELL} font-weight:600;color:{_COLOURS["text_secondary"]};vertical-align:top;">{pm_row_num}</td>'
            f'<td style="{_CELL} vertical-align:top;">{alert_cell}</td>'
            + "".join(_match_cell(m, first_csi) for m in matches)
            + '</tr>'
        )
        pm_row_num += 1

    if table2_rows_html:
        match_headers = "".join(
            f'<th style="{_HEAD_CELL}">Possible Match {i}</th>'
            for i in range(1, MAX_DISPLAY + 1)
        )
        table2_html = (
            _section_header("Table 2 — Possible Matches",
                            "Certificates that may be renewals (cross-source, DN similarity ≥ threshold)")
            + f'<table width="100%" cellpadding="0" cellspacing="0"'
              f' style="border-collapse:collapse;border:1px solid {_COLOURS["border"]};'
              f'border-radius:6px;overflow:hidden;margin-bottom:32px;">'
              f'<tr>'
              f'<th style="{_HEAD_CELL}">#</th>'
              f'<th style="{_HEAD_CELL}">Expiring Certificate</th>'
              f'{match_headers}'
              f'</tr>'
              f'{table2_rows_html}'
              f'</table>'
        )
    else:
        table2_html = ""

    # ── Important Information ─────────────────────────────────────────────────
    info_rows = [
        ("Alert threshold",         f"{settings_snapshot.get('alert_days_threshold', '—')} days"),
        ("Renewal minimum days",     f"{settings_snapshot.get('renewal_min_days', '—')} days"),
        ("Renewal match threshold",  f"{settings_snapshot.get('renewal_score_threshold', '—')}%"),
        ("Possible match threshold", f"{settings_snapshot.get('possible_match_score_threshold', '—')}%"),
        ("Length ratio gate",        f"{settings_snapshot.get('length_ratio_min', '—')}"),
        ("Ignore lookback",          f"{settings_snapshot.get('ignore_alert_lookback_days', '—')} days"),
        ("Log date staleness",       f"{settings_snapshot.get('log_date_staleness_days', '—')} days"),
        ("Active sources",           ", ".join(settings_snapshot.get("active_sources", [])) or "—"),
        ("Environments",             ", ".join(settings_snapshot.get("environments", [])) or "—"),
        ("Table 1 sort",             sort_label),
    ]
    info_rows_html = "".join(
        f'<tr style="background:{"#f8fafc" if i % 2 == 0 else "#ffffff"};">'
        f'<td style="padding:7px 14px;border-bottom:1px solid {_COLOURS["border"]};'
        f'font-size:12px;font-weight:600;color:{_COLOURS["text_secondary"]};white-space:nowrap;width:30%;">'
        f'{html.escape(label)}</td>'
        f'<td style="padding:7px 14px;border-bottom:1px solid {_COLOURS["border"]};'
        f'font-size:12px;color:{_COLOURS["text_primary"]};">{html.escape(value)}</td>'
        f'</tr>'
        for i, (label, value) in enumerate(info_rows)
    )
    important_info_html = (
        _section_header("Important Information",
                        "Thresholds and settings applied for this pipeline run")
        + f'<table width="100%" cellpadding="0" cellspacing="0"'
          f' style="border-collapse:collapse;border:1px solid {_COLOURS["border"]};'
          f'border-radius:6px;overflow:hidden;margin-bottom:32px;">'
          f'{info_rows_html}'
          f'</table>'
    )

    body = snapshot_html + table1_html + table2_html + important_info_html
    return _email_shell(
        "Production Certificate Expiration Report",
        body,
        run_time,
        contact_email=contact_email,
        app_version=app_version,
    )



def build_per_source_email(
    source: SourceName,
    documents: list[CertificateAlertDocument],
    run_time: datetime,
    contact_email: str = "shanereddy@email.com",
    app_version: str = "1.0.0",
) -> str:
    """
    Build the HTML for a per-source certificate alert email.

    Contains one section per logical source group, each with:
    * Source details card.
    * Certificate details (DN, SN, renewal match or warning action required).
    * Status badge.

    If ``documents`` is empty, returns a polite "nothing to report" email.

    Parameters
    ----------
    source:
        The source name (used in the title).
    documents:
        All ``CertificateAlertDocument`` objects for this source.
    run_time:
        UTC datetime of this pipeline run.
    contact_email:
        Address linked in the footer contact banner.
    app_version:
        Version string shown in the footer banner.

    Returns
    -------
    str
        Complete HTML email string.
    """
    title = f"{source.value} — Certificate Alert Report"

    if not documents:
        body = (
            f'<div style="text-align:center;padding:40px 20px;">'
            f'<div style="font-size:48px;margin-bottom:16px;">✅</div>'
            f'<div style="{_FONT} font-size:18px;font-weight:600;color:{_COLOURS["matched_renewal"]};">'
            f'No certificates require attention</div>'
            f'<div style="{_FONT} font-size:13px;color:{_COLOURS["text_secondary"]};margin-top:8px;">'
            f'All {source.value} certificates are in good standing as of this run.</div>'
            f'</div>'
        )
        return _email_shell(title, body, run_time, contact_email=contact_email, app_version=app_version)

    action_count = sum(1 for doc in documents for c in doc.certificates if c.status == AlertStatus.ACTION_REQUIRED)
    renewal_count = sum(1 for doc in documents for c in doc.certificates if c.status == AlertStatus.MATCHED_RENEWAL)
    missing_count = sum(1 for doc in documents for c in doc.certificates if c.status == AlertStatus.MISSING_SERVICE)

    pills_html = "".join(
        _stat_pill(label, value, colour)
        for label, value, colour in [
            ("Action Required", action_count, _COLOURS["action_required"]),
            ("Matched Renewal", renewal_count, _COLOURS["matched_renewal"]),
            ("Missing Service", missing_count, _COLOURS["missing_service"]),
        ]
    )

    sections_html = ""
    row_num = 1

    for doc in documents:
        source_card = build_source_detail_card(doc)
        for cert in doc.certificates:
            cert_details = (
                f'<div style="margin-bottom:6px;">'
                f'<span style="font-size:11px;font-weight:600;color:{_COLOURS["text_secondary"]};">DN</span><br>'
                f'<code style="font-size:11px;background:#f1f5f9;padding:2px 5px;border-radius:3px;">{_esc(cert.distinguished_name)}</code></div>'
                f'<div style="font-size:11px;color:{_COLOURS["text_secondary"]};">SN: {_esc(cert.serial_number)}</div>'
                f'<div style="font-size:11px;">Exp: {_format_date(cert.expiration_date)} {_days_badge(cert.days_to_expiration)}</div>'
            )

            if cert.status == AlertStatus.MATCHED_RENEWAL and cert.renewed_distinguished_name:
                renewal_html = (
                    f'<div style="color:{_COLOURS["matched_renewal"]};font-weight:600;font-size:12px;">✓ Renewal Found ({cert.similarity_score:.0f}% match)</div>'
                    f'<div style="font-size:11px;margin-top:4px;"><code style="background:#f0fdf4;padding:2px 5px;border-radius:3px;">{_esc(cert.renewed_distinguished_name)}</code></div>'
                    f'<div style="font-size:11px;color:{_COLOURS["text_secondary"]};">SN: {_esc(cert.renewed_serial_number)}</div>'
                    f'<div style="font-size:11px;">Exp: {_format_date(cert.renewed_expiration_date)} ({cert.renewed_days_to_expiration}d)</div>'
                )
            elif cert.acknowledged:
                renewal_html = (
                    f'<div style="color:{_COLOURS["matched_renewal"]};font-weight:600;font-size:12px;">✓ Acknowledged</div>'
                )
            else:
                renewal_html = (
                    f'<div style="color:{_COLOURS["action_required"]};font-weight:600;font-size:12px;">⚠ No valid renewal found in database</div>'
                )

            row_bg = "#ffffff" if row_num % 2 == 0 else "#f8fafc"
            sections_html += (
                f'<tr style="background:{row_bg};">'
                f'<td style="{_CELL} font-weight:600;color:{_COLOURS["text_secondary"]};width:40px;">{row_num}</td>'
                f'<td style="{_CELL}">{source_card}</td>'
                f'<td style="{_CELL}">{cert_details}</td>'
                f'<td style="{_CELL}">{renewal_html}</td>'
                f'<td style="{_CELL} text-align:center;">{_status_badge(cert.status)}</td>'
                f'</tr>'
            )
            row_num += 1

    body = (
        f'<div style="margin-bottom:20px;">{pills_html}</div>'
        + f'<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid {_COLOURS["border"]};border-radius:6px;overflow:hidden;">'
        + f'<tr><th style="{_HEAD_CELL}">#</th><th style="{_HEAD_CELL}">Source Details</th><th style="{_HEAD_CELL}">Certificate Details</th><th style="{_HEAD_CELL}">Renewal / Match</th><th style="{_HEAD_CELL}">Status</th></tr>'
        + sections_html
        + "</table>"
    )

    return _email_shell(title, body, run_time, contact_email=contact_email, app_version=app_version)
