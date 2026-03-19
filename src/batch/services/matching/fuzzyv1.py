"""
Fuzzy matching engine.

Provides two matching modes used during alert processing:

1. ``find_renewal``  — same-source, exact key-field match, DN similarity
   ─────────────────────────────────────────────────────────────────────
   For a given alert certificate, searches within the *same source* for a
   certificate that:
   * Has all source-specific key fields matching exactly (e.g. SSG domain +
     microservice_name for SSG).
   * Has ``days_to_expiration >= renewal_min_days``.
   * Has a cleaned-DN similarity score >= ``renewal_score_threshold``.
   * Is NOT the alert certificate itself (excluded by serial number).

   Returns the single best-scoring match or ``None``.

2. ``find_possible_matches``  — cross-source, DN similarity only
   ──────────────────────────────────────────────────────────────
   For a given alert certificate that has NO renewal match, searches the
   *entire* parquet (all sources) for candidates with DN similarity >=
   ``possible_match_score_threshold``.

   Uses a two-stage filter before scoring:
   Stage 1 — Length-ratio gate (pure Polars, runs in Rust):
       ``len(shorter) / len(longer) >= length_ratio_min``
       Eliminates ``CRSFSVC`` → ``163242crsfsvcprodsecret`` false positives
       before any Python-level work.
   Stage 2 — rapidfuzz cdist (NumPy matrix, token_sort_ratio):
       Scores all remaining candidates in one vectorized call.

   Stops collecting after ``max_possible_candidates`` matches (or immediately
   if a perfect score is found, and we already have >= ``max_possible_display``
   matches).

   Returns up to ``max_possible_display`` matches sorted by score descending.

Performance notes
-----------------
* The full parquet is ~5 M rows but the candidate pool for possible-match
  search is pre-filtered to valid certs with days_to_expiration >= 90
  *before* cdist runs.  In practice this is 50 k–200 k rows.
* ``rapidfuzz.process.cdist`` computes an (n_alerts × n_candidates) score
  matrix in one C-extension call — no Python loop per pair.
* For bulk renewal searches (many alert certs at once) we batch-call cdist
  over all alert DNs simultaneously rather than one cert at a time.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import polars as pl
from rapidfuzz import process as rfprocess
from rapidfuzz.distance import Indel
from rapidfuzz.fuzz import token_sort_ratio

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import PossibleMatchModel
from src.batch.models.enums import SourceName
from src.batch.utilities.cm import clean_string

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source-specific key fields used for renewal matching
# (must all match exactly for a candidate to qualify as a renewal)
# ---------------------------------------------------------------------------

SOURCE_KEY_FIELDS: dict[str, list[str]] = {
    SourceName.SSG: ["sp_ssg_domain", "sp_microservice_name", "sp_internal_ssg_domain"],
    SourceName.HASHICORP: ["sp_openshift_namespace", "sp_microservice_name"],
    SourceName.EVOLVEN: ["sp_evolven_host", "sp_evolven_path"],
    SourceName.APIGEE: ["sp_evolven_host", "sp_evolven_path", "sp_ssg_domain"],
    SourceName.AKAMAI: ["sp_serial_number"],       # per-cert, no natural group key
    SourceName.SSL_TRACKER: ["sp_serial_number"],  # per-cert
}


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class RenewalMatch:
    """The single best same-source renewal candidate for an alert cert."""

    distinguished_name: str
    serial_number: str
    days_to_expiration: int
    expiration_date: Any          # datetime
    similarity_score: float
    csi_id: int | None = None
    ssl_cm_status: str | None = None


@dataclass
class MatchingResult:
    """
    Outcome of the full renewal + possible-match search for one alert cert.
    """

    serial_number: str             # identity of the alert cert
    renewal: RenewalMatch | None = None
    possible_matches: list[PossibleMatchModel] = field(default_factory=list)

    @property
    def has_renewal(self) -> bool:
        return self.renewal is not None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _length_ratio(a: str, b: str) -> float:
    """Return min/max length ratio for two strings."""
    la, lb = len(a), len(b)
    if la == 0 and lb == 0:
        return 1.0
    if la == 0 or lb == 0:
        return 0.0
    return min(la, lb) / max(la, lb)


def _add_length_ratio_column(df: pl.DataFrame, alert_clean: str) -> pl.DataFrame:
    """
    Add a ``length_ratio`` column to ``df`` comparing each row's ``dn_clean``
    against ``alert_clean``.  Runs entirely in Polars (Rust).
    """
    alert_len = len(alert_clean)
    return df.with_columns(
        pl.when(
            (pl.col("dn_clean").str.len_chars() == 0) | (alert_len == 0)
        )
        .then(pl.lit(0.0))
        .otherwise(
            pl.min_horizontal(
                pl.col("dn_clean").str.len_chars().cast(pl.Float64),
                pl.lit(float(alert_len)),
            )
            / pl.max_horizontal(
                pl.col("dn_clean").str.len_chars().cast(pl.Float64),
                pl.lit(float(alert_len)),
            )
        )
        .alias("length_ratio")
    )


def _row_to_possible_match(row: dict[str, Any], score: float) -> PossibleMatchModel:
    """Convert a parquet row dict to a ``PossibleMatchModel``."""
    return PossibleMatchModel(
        distinguished_name=row.get("distinguished_name") or "",
        days_to_expiration=int(row.get("days_to_expiration") or 0),
        expiration_date=row["expiration_date"],
        serial_number=row.get("serial_number_upper") or row.get("sp_serial_number") or "",
        similarity_score=round(float(score), 2),
        csi_id=row.get("csi_application_id"),
        ssl_cm_status=row.get("sp_ssl_cm_status"),
        source=row.get("sp_name"),
    )


# ---------------------------------------------------------------------------
# Core scoring function (cdist wrapper)
# ---------------------------------------------------------------------------


def _score_candidates(
    query_dn_clean: str,
    candidate_dns_clean: list[str],
) -> np.ndarray:
    """
    Score ``query_dn_clean`` against every string in ``candidate_dns_clean``
    using ``rapidfuzz token_sort_ratio``.

    Returns a 1-D float32 NumPy array of scores (0–100).
    """
    if not candidate_dns_clean:
        return np.array([], dtype=np.float32)

    # cdist returns shape (1, n_candidates) when query is a list of 1
    matrix = rfprocess.cdist(
        [query_dn_clean],
        candidate_dns_clean,
        scorer=token_sort_ratio,
        dtype=np.float32,
        workers=1,   # single-threaded; caller manages concurrency
    )
    return matrix[0]  # shape (n_candidates,)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def find_renewal(
    alert_row: dict[str, Any],
    parquet_df: pl.DataFrame,
    settings: CMSettings,
) -> RenewalMatch | None:
    """
    Search for a same-source renewal certificate for a single alert cert.

    Algorithm
    ---------
    1. Filter parquet to same ``sp_name`` (source).
    2. Exclude the alert cert itself (by uppercased serial number).
    3. Apply exact match on all source-specific key fields.
    4. Apply ``days_to_expiration >= renewal_min_days`` filter.
    5. Apply length-ratio gate (Polars, Rust).
    6. Score remaining candidates with rapidfuzz cdist.
    7. Return the highest-scoring candidate if score >= threshold.

    Parameters
    ----------
    alert_row:
        Single row from the parquet as a plain dict.
    parquet_df:
        Full certificate cache DataFrame.
    settings:
        Pipeline settings (thresholds, noise words).

    Returns
    -------
    RenewalMatch | None
    """
    t = settings.thresholds
    noise = tuple(settings.sources.noise_words)

    source = alert_row.get("sp_name") or ""
    alert_serial_upper = (alert_row.get("serial_number_upper") or "").upper()
    alert_dn_clean = clean_string(
        alert_row.get("distinguished_name") or "", noise
    )

    if not alert_dn_clean:
        logger.debug("Alert cert has empty cleaned DN — skipping renewal search.")
        return None

    # ── Step 1 + 2: same source, exclude self ────────────────────────────────
    candidates = parquet_df.filter(
        (pl.col("sp_name") == source)
        & (pl.col("serial_number_upper") != alert_serial_upper)
        & (pl.col("days_to_expiration") >= t.renewal_min_days)
    )

    # ── Step 3: exact key-field match ────────────────────────────────────────
    key_fields = SOURCE_KEY_FIELDS.get(source, [])
    for kf in key_fields:
        if kf not in candidates.columns:
            continue
        kf_value = alert_row.get(kf)
        if kf_value:
            # Both sides can be None/null — only filter when the alert has a value
            candidates = candidates.filter(pl.col(kf) == kf_value)

    if candidates.is_empty():
        logger.debug("No same-source key-field candidates for '%s'.", alert_row.get("distinguished_name"))
        return None

    # ── Step 4+5: length-ratio gate ─────────────────────────────────────────
    candidates = _add_length_ratio_column(candidates, alert_dn_clean)
    candidates = candidates.filter(pl.col("length_ratio") >= t.length_ratio_min)

    if candidates.is_empty():
        return None

    # ── Step 6: rapidfuzz cdist ─────────────────────────────────────────────
    candidate_dns_clean = candidates["dn_clean"].to_list()
    scores = _score_candidates(alert_dn_clean, candidate_dns_clean)

    best_idx = int(np.argmax(scores))
    best_score = float(scores[best_idx])

    if best_score < t.renewal_score_threshold:
        logger.debug(
            "Best renewal score %.1f < threshold %.1f for '%s'.",
            best_score, t.renewal_score_threshold, alert_row.get("distinguished_name"),
        )
        return None

    best_row = candidates.row(best_idx, named=True)
    logger.debug(
        "Renewal found for '%s' → '%s' (score=%.1f)",
        alert_row.get("distinguished_name"),
        best_row.get("distinguished_name"),
        best_score,
    )

    return RenewalMatch(
        distinguished_name=best_row.get("distinguished_name") or "",
        serial_number=(best_row.get("serial_number_upper") or "").upper(),
        days_to_expiration=int(best_row.get("days_to_expiration") or 0),
        expiration_date=best_row["expiration_date"],
        similarity_score=round(best_score, 2),
        csi_id=best_row.get("csi_application_id"),
        ssl_cm_status=best_row.get("sp_ssl_cm_status"),
    )


def find_possible_matches(
    alert_row: dict[str, Any],
    parquet_df: pl.DataFrame,
    settings: CMSettings,
) -> list[PossibleMatchModel]:
    """
    Search the *entire* parquet for possible renewal candidates (cross-source).

    Used only when ``find_renewal`` returns ``None``.

    Algorithm
    ---------
    1. Exclude the alert cert itself.
    2. Filter to ``days_to_expiration >= renewal_min_days``.
    3. Apply length-ratio gate (Polars, Rust).
    4. Score with rapidfuzz cdist.
    5. Collect up to ``max_possible_candidates`` matches above threshold.
    6. Apply early-stop: if a perfect score is found, and we already have
       ``>= max_possible_display`` matches, stop immediately.
    7. Return top ``max_possible_display`` matches sorted by score desc.

    Parameters
    ----------
    alert_row:
        Single row from the parquet as a plain dict.
    parquet_df:
        Full certificate cache DataFrame.
    settings:
        Pipeline settings.

    Returns
    -------
    list[PossibleMatchModel]
        Up to ``max_possible_display`` matches, score descending.
    """
    t = settings.thresholds
    noise = tuple(settings.sources.noise_words)

    alert_serial_upper = (alert_row.get("serial_number_upper") or "").upper()
    alert_dn_clean = clean_string(
        alert_row.get("distinguished_name") or "", noise
    )

    if not alert_dn_clean:
        return []

    # ── Step 1+2: exclude self, min days ─────────────────────────────────────
    pool = parquet_df.filter(
        (pl.col("serial_number_upper") != alert_serial_upper)
        & (pl.col("days_to_expiration") >= t.renewal_min_days)
    )

    if pool.is_empty():
        return []

    # ── Step 3: length-ratio gate (Polars/Rust) ───────────────────────────────
    pool = _add_length_ratio_column(pool, alert_dn_clean)
    pool = pool.filter(pl.col("length_ratio") >= t.length_ratio_min)

    if pool.is_empty():
        return []

    logger.debug(
        "Possible-match pool for '%s': %d candidates after length-ratio gate.",
        alert_row.get("distinguished_name"), pool.height,
    )

    # ── Step 4: rapidfuzz cdist ──────────────────────────────────────────────
    candidate_dns_clean = pool["dn_clean"].to_list()
    scores = _score_candidates(alert_dn_clean, candidate_dns_clean)

    # ── Step 5+6: collect matches with early stop ────────────────────────────
    # Sort indices by score descending for efficient early-stop
    sorted_indices = np.argsort(scores)[::-1]

    collected: list[PossibleMatchModel] = []

    for idx in sorted_indices:
        score = float(scores[idx])

        if score < t.possible_match_score_threshold:
            break  # sorted descending — everything below is also below threshold

        row = pool.row(int(idx), named=True)
        collected.append(_row_to_possible_match(row, score))

        # Early stop: perfect score and already have enough to display
        if (
            score >= t.perfect_score_early_stop
            and len(collected) >= t.max_possible_display
        ):
            logger.debug("Early stop: perfect score reached with %d matches.", len(collected))
            break

        if len(collected) >= t.max_possible_candidates:
            break

    # Return top N sorted by score desc (already sorted since we iterated desc)
    result = collected[: t.max_possible_display]
    logger.debug(
        "Possible matches for '%s': %d returned.",
        alert_row.get("distinguished_name"), len(result),
    )
    return result


def run_matching_for_source(
    alert_rows: list[dict[str, Any]],
    parquet_df: pl.DataFrame,
    settings: CMSettings,
) -> list[MatchingResult]:
    """
    Run the full renewal + possible-match search for a list of alert certs
    from the *same* source.

    This is the main entry point called by each source processor.

    Parameters
    ----------
    alert_rows:
        Rows from the alert DataFrame (dicts) for one source.
    parquet_df:
        Full certificate cache.
    settings:
        Pipeline settings.

    Returns
    -------
    list[MatchingResult]
        One result per alert cert, same order as ``alert_rows``.
    """
    results: list[MatchingResult] = []

    for row in alert_rows:
        serial = (row.get("serial_number_upper") or row.get("sp_serial_number") or "").upper()

        renewal = find_renewal(row, parquet_df, settings)

        if renewal:
            results.append(MatchingResult(serial_number=serial, renewal=renewal))
        else:
            possible = find_possible_matches(row, parquet_df, settings)
            results.append(MatchingResult(serial_number=serial, possible_matches=possible))

    return results
