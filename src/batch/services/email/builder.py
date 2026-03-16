"""
HTML email template builder.

Generates modern, responsive, email-client-compatible HTML for both the
consolidated and per-source certificate alert emails.
"""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any

from src.batch.models.alerts import (
    CertificateAlertDocument,
    CertificateModel,
    PossibleMatchModel,
    RunSummary,
)
from src.batch.models.enums import AlertStatus, SourceName

# ---------------------------------------------------------------------------
# color palette and shared styles
# ---------------------------------------------------------------------------

_COLORS = {
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
_CELL = f"padding: 10px 14px; border-bottom: 1px solid {_COLORS['border']}; {_FONT} font-size: 13px; color: {_COLORS['text_primary']}; vertical-align: top;"
_HEAD_CELL = f"padding: 10px 14px; background-color: {_COLORS['bg_table_head']}; color: {_COLORS['text_white']}; {_FONT} font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; text-align: left;"


# ---------------------------------------------------------------------------
# Shared micro-components
# ---------------------------------------------------------------------------


def _esc(value: Any) -> str:
    """HTML-escape a value for safe injection into HTML."""
    if value is None:
        return '<span style="color:#94a3b8;font-style:italic;">—</span>'
    return html.escape(str(value))


def _status_badge(status: AlertStatus) -> str:
    color_map = {
        AlertStatus.ACTION_REQUIRED: (_COLORS["action_required"], _COLORS["action_required_bg"]),
        AlertStatus.MATCHED_RENEWAL: (_COLORS["matched_renewal"], _COLORS["matched_renewal_bg"]),
        AlertStatus.MISSING_SERVICE: (_COLORS["missing_service"], _COLORS["missing_service_bg"]),
    }
    color, bg = color_map.get(status, ("#64748b", "#f1f5f9"))
    label = html.escape(status.value)
    return (
        f'<span style="display:inline-block;padding:3px 10px;border-radius:12px;'
        f'background:{bg};color:{color};font-weight:600;font-size:11px;'
        f'border:1px solid {color};">{label}</span>'
    )


def _days_badge(days: int | None) -> str:
    """color-coded days-to-expiration pill."""
    if days is None:
        return _esc(None)
    if days <= 3:
        bg, color = "#fef2f2", "#dc2626"
    elif days <= 7:
        bg, color = "#fff7ed", "#ea580c"
    elif days <= 30:
        bg, color = "#fffbeb", "#d97706"
    else:
        bg, color = "#f0fdf4", "#16a34a"
    return (
        f'<span style="display:inline-block;padding:2px 9px;border-radius:10px;'
        f'background:{bg};color:{color};font-weight:700;font-size:12px;">'
        f'{days}d</span>'
    )


def _format_date(dt: datetime | None) -> str:

    if dt is None:
        return "—"

    # endIf

    return dt.strftime("%Y-%m-%d")

# endDef


def _section_header(title: str, subtitle: str = "") -> str:

    sub = f'<div style="{_FONT} font-size:12px;color:{_COLORS["text_secondary"]};margin-top:4px;">{html.escape(subtitle)}</div>' if subtitle else ""

    return (
        f'<div style="margin:28px 0 12px 0;">'
        f'<div style="{_FONT} font-size:16px;font-weight:700;color:{_COLORS["text_primary"]};'
        f'border-left:4px solid {_COLORS["bg_header_accent"]};padding-left:12px;">'
        f'{html.escape(title)}</div>{sub}</div>'
    )

# endDef


def _stat_pill(label: str, value: int | str, color: str) -> str:

    return (
        f'<div style="display:inline-block;background:#f8fafc;border:1px solid {color}33;'
        f'border-radius:8px;padding:10px 16px;margin:4px;min-width:100px;text-align:center;">'
        f'<div style="{_FONT} font-size:22px;font-weight:800;color:{color};">{value}</div>'
        f'<div style="{_FONT} font-size:11px;color:{_COLORS["text_secondary"]};margin-top:2px;">'
        f'{html.escape(label)}</div>'
        f'</div>'
    )

# endDef


# ---------------------------------------------------------------------------
# Email page shell
# ---------------------------------------------------------------------------


def _email_shell(title: str, body: str, run_time: datetime, contact_email: str = "", app_version: str = "") -> str:

    """Wrap body HTML in a complete email-safe outer shell."""

    run_str = run_time.strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{html.escape(title)}</title>
</head>
<body style="margin:0;padding:0;background:{_COLORS['bg_page']};{_FONT}">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background:{_COLORS['bg_page']};padding:24px 16px;">
    <tr><td align="center">
      <table width="720" cellpadding="0" cellspacing="0" border="0"
             style="max-width:720px;width:100%;">

        <!-- ── Header ── -->
        <tr>
          <td style="background:{_COLORS['bg_header']};border-radius:8px 8px 0 0;
                     padding:24px 28px;">
            <div style="{_FONT} font-size:11px;font-weight:600;letter-spacing:0.1em;
                         color:#93c5fd;text-transform:uppercase;margin-bottom:6px;">
              Certificate Monitor
            </div>
            <div style="{_FONT} font-size:22px;font-weight:700;color:{_COLORS['text_white']};
                         margin-bottom:4px;">
              {html.escape(title)}
            </div>
            <div style="{_FONT} font-size:12px;color:#93c5fd;">
              Generated: {run_str}
            </div>
          </td>
        </tr>

        <!-- ── Body ── -->
        <tr>
          <td style="background:{_COLORS['bg_card']};padding:24px 28px;
                     border-radius:0 0 8px 8px;
                     border:1px solid {_COLORS['border']};border-top:none;">
            {body}
          </td>
        </tr>

        <!-- ── Footer ── -->
        <tr>
          <td style="padding:16px 0;text-align:center;
                     {_FONT} font-size:11px;color:{_COLORS['text_secondary']};">
            This is an automated alert from the Certificate Monitoring Pipeline v{html.escape(app_version)}.<br>
            Contact: {html.escape(contact_email)} | Do not reply to this email.
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

# endDef


# ---------------------------------------------------------------------------
# Source-details card builders
# ---------------------------------------------------------------------------


def _detail_card(title: str, rows: list[tuple[str, str]], bg: str, accent: str) -> str:

    rows_html = "".join(
        f'<tr>'
        f'<td style="padding:7px 12px;font-size:12px;font-weight:600;color:{_COLORS["text_secondary"]};'
        f'white-space:nowrap;border-bottom:1px solid {_COLORS["border"]};width:35%;">{html.escape(k)}</td>'
        f'<td style="padding:7px 12px;font-size:13px;color:{_COLORS["text_primary"]};'
        f'border-bottom:1px solid {_COLORS["border"]};">{v}</td>'
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

# endDef


def build_source_detail_card(doc: CertificateAlertDocument) -> str:

    """Dispatch to the correct source card builder."""

    from src.batch.models.alerts import (
        AkamaiSourceModel, ApigeeSourceModel, EvolvenSourceModel,
        HashiCorpSourceModel, SSGSourceModel, SSLTrackerSourceModel,
    )

    sd = doc.source_details

    if isinstance(sd, SSGSourceModel):
        rows = [("Domain", _esc(sd.domain)), ("Service", _esc(sd.service_name)), ("URL In", _esc(sd.url_in))]
        return _detail_card(title="SSG Details", rows=rows, bg="#eff6ff", accent="#1d4ed8")

    # endIf

    if isinstance(sd, HashiCorpSourceModel):
        replica_html = f"<b>{_esc(sd.replicas.get('available'))} / {_esc(sd.replicas.get('total'))}</b>"
        rows = [("Cluster", _esc(sd.cluster)), ("Project", _esc(sd.project)), ("Replicas", replica_html)]
        return _detail_card(title="OpenShift Details", rows=rows, bg="#f5f3ff", accent="#6d28d9")

    # endIf

    if isinstance(sd, SSLTrackerSourceModel):
        rows = [("SSL CM Status", _esc(sd.status))]
        return _detail_card(title="SSL Tracker Details", rows=rows, bg="#f8fafc", accent="#475569")

    # endIf

    # Simple fallback for others
    return f'<div style="font-size:12px; color:{_COLORS["text_secondary"]};">Source detail view not implemented for {doc.source.value}</div>'

# endDef


def _mini_source_summary(doc: CertificateAlertDocument) -> str:

    """One-line source summary for Table 1 Source Details column."""

    from src.batch.models.alerts import (
        AkamaiSourceModel, ApigeeSourceModel, EvolvenSourceModel,
        HashiCorpSourceModel, SSGSourceModel, SSLTrackerSourceModel,
    )

    sd = doc.source_details

    if isinstance(sd, SSGSourceModel):
        return f'<span style="font-size:11px;">{_esc(sd.service_name)} @ {_esc(sd.domain)}</span>'

    # endIf

    if isinstance(sd, HashiCorpSourceModel):
        return f'<span style="font-size:11px;">{_esc(sd.cluster)}/{_esc(sd.project)}</span>'
    # endIf

    if isinstance(sd, SSLTrackerSourceModel):
        return f'<span style="font-size:11px;">Status: {_esc(sd.status)}</span>'

    # endIf

    return f'<span style="font-size:11px;">{_esc(doc.source.value)}</span>'

# endDef


# ---------------------------------------------------------------------------
# Consolidated email builder
# ---------------------------------------------------------------------------


def build_consolidated_email(
    all_documents: list[CertificateAlertDocument],
    run_summary: RunSummary,
    settings_snapshot: dict[str, Any],
    jira_details_fn=None,
    contact_email: str = "",
    app_version: str = "",
    table1_sort_by: str = "days_to_expiration",
) -> str:
    
    run_time = run_summary.run_datetime

    # ── Snapshot ─────────────────────────────────────────────────────────────
    total_alerts = (
        run_summary.total_action_required
        + run_summary.total_matched_renewal
        + run_summary.total_missing_service
    )

    snapshot_rows = "".join(
        f'<tr style="background:{"#f8fafc" if i % 2 == 0 else "#ffffff"};">'
        f'<td style="{_CELL} font-weight:600;">{_esc(s.source.value)}</td>'
        f'<td style="{_CELL} color:{_COLORS["action_required"]};font-weight:700;">{s.action_required}</td>'
        f'<td style="{_CELL} color:{_COLORS["matched_renewal"]};">{s.matched_renewal}</td>'
        f'<td style="{_CELL} color:{_COLORS["missing_service"]};">{s.missing_service}</td>'
        f'<td style="{_CELL}">{s.total_alerts}</td>'
        f'</tr>'
        for i, s in enumerate(run_summary.sources)
    )

    snapshot_html = f"""
    {_section_header(title="Run Snapshot", subtitle=f"Pipeline run at {run_time.strftime('%Y-%m-%d %H:%M UTC')}")}
    <div style="margin-bottom:20px;">
      {_stat_pill(label="Action Required", value=run_summary.total_action_required, color=_COLORS["action_required"])}
      {_stat_pill(label="Matched Renewal", value=run_summary.total_matched_renewal, color=_COLORS["matched_renewal"])}
      {_stat_pill(label="Missing Service", value=run_summary.total_missing_service, color=_COLORS["missing_service"])}
      {_stat_pill(label="Total Alerts", value=total_alerts, color=_COLORS["text_secondary"])}
    </div>
    <table width="100%" cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;border:1px solid {_COLORS['border']};margin-bottom:24px;">
      <tr>
        <th style="{_HEAD_CELL}">Source</th>
        <th style="{_HEAD_CELL}">Action Required</th>
        <th style="{_HEAD_CELL}">Matched Renewal</th>
        <th style="{_HEAD_CELL}">Missing Service</th>
        <th style="{_HEAD_CELL}">Total</th>
      </tr>
      {snapshot_rows}
    </table>
    """

    # ── Table 1 Logic ────────────────────────────────────────────────────────
    action_certs: list[tuple[CertificateAlertDocument, CertificateModel]] = []
    for doc in all_documents:
        for cert in doc.certificates:
            if cert.attention_required and not cert.acknowledged:
                action_certs.append((doc, cert))
                
            # endIf
            
        # endFor
        
    # endFor

    # Sorting
    def _sort_key(pair) -> tuple:
        inner_document, inner_certificate = pair
        days = inner_certificate.days_to_expiration if inner_certificate.days_to_expiration is not None else 999
        if table1_sort_by == "source": return inner_document.source.value, days
        if table1_sort_by == "csi_id": return inner_document.csi_id or 999999, days
        return days, inner_document.source.value
    
    # endDef

    action_certs.sort(key=_sort_key)

    table1_rows = ""
    for i, (doc, cert) in enumerate(action_certs, 1):
        jira_html = jira_details_fn(cert) if jira_details_fn else "—"
        table1_rows += (
            f'<tr style="background:{"#f8fafc" if i % 2 == 0 else "#ffffff"};">'
            f'<td style="{_CELL}">{i}</td>'
            f'<td style="{_CELL}">{_esc(doc.source.value)}</td>'
            f'<td style="{_CELL}">{_esc(doc.csi_id)}</td>'
            f'<td style="{_CELL}"><code style="font-size:11px;">{_esc(cert.distinguished_name)}</code></td>'
            f'<td style="{_CELL}">{_days_badge(cert.days_to_expiration)}</td>'
            f'<td style="{_CELL}">{_format_date(cert.expiration_date)}</td>'
            f'<td style="{_CELL}">{jira_html}</td>'
            f'<td style="{_CELL}">{_mini_source_summary(doc)}</td>'
            f'</tr>'
        )
        
    # endFor

    table1_html = f"""
    {_section_header(title="Table 1 — Action Required Certificates", subtitle=f"{len(action_certs)} certificate(s)")}
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid {_COLORS['border']};margin-bottom:32px;">
      <tr>
        <th style="{_HEAD_CELL}">#</th><th style="{_HEAD_CELL}">Source</th><th style="{_HEAD_CELL}">CSI</th>
        <th style="{_HEAD_CELL}">DN</th><th style="{_HEAD_CELL}">Days</th><th style="{_HEAD_CELL}">Exp</th>
        <th style="{_HEAD_CELL}">Jira</th><th style="{_HEAD_CELL}">Details</th>
      </tr>
      {table1_rows if table1_rows else f'<tr><td colspan="8" style="{_CELL} text-align:center;">No actions.</td></tr>'}
    </table>
    """

    # ── Table 2 Logic ────────────────────────────────────────────────────────
    table2_rows = ""
    pm_idx = 1
    for doc, cert in action_certs:
        if not cert.possible_matches:
            continue
            
        # endIf

        matches_html = ""
        for m in (cert.possible_matches[:3]):
            csi_warn = m.csi_id != doc.csi_id if m.csi_id and doc.csi_id else False
            bg = _COLORS["highlight_warn"] if csi_warn else ""
            matches_html += (
                f'<td style="{_CELL} background:{bg};">'
                f'<div style="font-weight:700; color:{_COLORS["bg_header_accent"]};">{m.similarity_score}%</div>'
                f'<div style="font-size:11px;">{_esc(m.distinguished_name)}</div>'
                f'<div style="font-size:11px; color:{_COLORS["text_secondary"]};">CSI: {_esc(m.csi_id)}</div>'
                f'</td>'
            )
            
        # endFor
        
        # Pad empty match columns
        for _ in range(3 - len(cert.possible_matches[:3])):
            matches_html += f'<td style="{_CELL} color:{_COLORS["text_secondary"]}; font-style:italic;">—</td>'
            
        # endFor

        table2_rows += (
            f'<tr><td style="{_CELL}">{pm_idx}</td>'
            f'<td style="{_CELL}"><code>{_esc(cert.distinguished_name)}</code></td>'
            f'{matches_html}</tr>'
        )
        pm_idx += 1
        
    # endFor

    table2_html = ""
    if table2_rows:
        table2_html = f"""
        {_section_header(title="Table 2 — Possible Matches", subtitle="Top candidates (DN similarity ≥ threshold)")}
        <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid {_COLORS['border']};margin-bottom:32px;">
          <tr><th style="{_HEAD_CELL}">#</th><th style="{_HEAD_CELL}">Expiring DN</th>
          <th style="{_HEAD_CELL}">Match 1</th><th style="{_HEAD_CELL}">Match 2</th><th style="{_HEAD_CELL}">Match 3</th></tr>
          {table2_rows}
        </table>
        """
        
    # endIf

    # ── Important Info ───────────────────────────────────────────────────────
    info_rows = [
        ("Alert Threshold", f"{settings_snapshot.get('alert_days_threshold')} days"),
        ("Match Threshold", f"{settings_snapshot.get('renewal_score_threshold')}%"),
        ("Active Sources", ", ".join(settings_snapshot.get("active_sources", []))),
    ]
    info_rows_html = "".join(f'<tr><td style="{_CELL} font-weight:600;">{k}</td><td style="{_CELL}">{v}</td></tr>' for k, v in info_rows)
    important_info_html = f"""
    {_section_header("Important Information")}
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid {_COLORS['border']};">
      {info_rows_html}
    </table>
    """

    body = snapshot_html + table1_html + table2_html + important_info_html
    return _email_shell(title="Production Certificate Expiration Report", body=body, run_time=run_time, contact_email=contact_email, app_version=app_version)

# endDef


# ---------------------------------------------------------------------------
# Per-source email builder
# ---------------------------------------------------------------------------


def build_per_source_email(
    source: SourceName,
    documents: list[CertificateAlertDocument],
    run_time: datetime,
    contact_email: str = "",
    app_version: str = "",
) -> str:
    
    title = f"{source.value} — Certificate Alert Report"

    if not documents:
        body = f'<div style="text-align:center; padding:40px;">✅ No certificates require attention for {source.value}.</div>'
        return _email_shell(title, body, run_time, contact_email, app_version)
    
    # endIf

    rows_html = ""
    idx = 1
    for doc in documents:
        source_card = build_source_detail_card(doc)
        for cert in doc.certificates:
            rows_html += (
                f'<tr style="background:{"#f8fafc" if idx % 2 == 0 else "#ffffff"};">'
                f'<td style="{_CELL}">{idx}</td>'
                f'<td style="{_CELL}">{source_card}</td>'
                f'<td style="{_CELL}"><div style="font-weight:600;">{_esc(cert.distinguished_name)}</div>'
                f'<div style="font-size:11px; color:{_COLORS["text_secondary"]};">SN: {_esc(cert.serial_number)}</div></td>'
                f'<td style="{_CELL}">{_days_badge(cert.days_to_expiration)}</td>'
                f'<td style="{_CELL} text-align:center;">{_status_badge(cert.status)}</td>'
                f'</tr>'
            )
            idx += 1
            
        # endFor
        
    # endFor

    body = f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; border:1px solid {_COLORS['border']};">
      <tr>
        <th style="{_HEAD_CELL}">#</th><th style="{_HEAD_CELL}">Source Details</th>
        <th style="{_HEAD_CELL}">Certificate</th><th style="{_HEAD_CELL}">Urgency</th>
        <th style="{_HEAD_CELL}">Status</th>
      </tr>
      {rows_html}
    </table>
    """
    
    return _email_shell(title, body, run_time, contact_email, app_version)

# endDef