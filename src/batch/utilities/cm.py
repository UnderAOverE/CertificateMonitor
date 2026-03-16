"""
String cleaning utilities for distinguished-name (DN) normalization.

Two strategies are provided, each optimized for a different use-case:

1. ``build_dn_clean_expression``  (Polars-native, Rust speed)
   ─────────────────────────────────────────────────────────
   Returns a ``pl.Expr`` that can be used inside ``df.with_columns(...)``
   to compute a ``dn_clean`` column across millions of rows in one pass.
   All operations run in the Polars Rust runtime — no Python GIL overhead.

   Usage::

       df = df.with_columns(
           build_dn_clean_expression(noise_words).alias("dn_clean")
       )

2. ``clean_string``  (Python + ``lru_cache``, for small-batch matching)
   ─────────────────────────────────────────────────────────────────────
   The original ``_clean_string`` logic, promoted to module-level (so the
   compiled regex is not re-created on every call) and wrapped with
   ``lru_cache``.  Used at matching time when we need to clean individual
   alert-cert strings on the fly without spinning up a Polars DataFrame.

Notes
-----
* Both strategies produce *identical* output for the same input.  Unit-test
  them together to ensure parity if you change either implementation.
* ``_CN_RE`` is compiled once at module import — not inside any function.
* ``noise_words`` must be passed as a ``tuple`` (not list) to ``clean_string``
  so that ``lru_cache`` can hash the argument.
"""

from __future__ import annotations

import re
from functools import lru_cache

import polars as pl


# ---------------------------------------------------------------------------
# Module-level compiled regex (fix: was previously inside the function body)
# ---------------------------------------------------------------------------

_CN_RE: re.Pattern[str] = re.compile(r"CN=([^,]+)", re.IGNORECASE)

# Characters to replace with a space during normalization.
_REPLACE_CHARS: str = r"[.\-_]"


# ---------------------------------------------------------------------------
# 1. Polars-native expression (for bulk column computation)
# ---------------------------------------------------------------------------


def build_dn_clean_expression(
    noise_words: list[str] | tuple[str, ...],
    source_column: str = "distinguished_name",
) -> pl.Expr:
    """
    Build a Polars expression that normalizes a DN column for fuzzy matching.

    Steps (all executed in Rust via the Polars engine):
        1. Extract the CN= value if the string looks like a full DN.
        2. Lowercase the result.
        3. Replace ``.``, ``-``, ``_`` with a space.
        4. Strip each noise word (whole-word-ish replacement via regex).
        5. Collapse multiple spaces and strip leading/trailing whitespace.

    Parameters
    ----------
    noise_words:
        Words to remove from the normalized DN.  Order does not matter.
    source_column:
        Name of the input column containing raw distinguished names.

    Returns
    -------
    pl.Expr
        Expression to pass to ``df.with_columns(...).alias("dn_clean")``.

    Example
    -------
    >>> df = df.with_columns(
    ...     build_dn_clean_expression(settings.sources.noise_words).alias("dn_clean")
    ... )
    """
    # Step 1: extract CN= value when present; otherwise keep original
    #   Polars str.extract returns the first capture group (index 1).
    expr: pl.Expr = (
        pl.col(source_column)
        .str.extract(r"(?i)CN=([^,]+)", group_index=1)
        .fill_null(pl.col(source_column))  # fallback to original if no CN= found
    )

    # Step 2: lowercase
    expr = expr.str.to_lowercase()

    # Step 3: replace punctuation with spaces
    expr = expr.str.replace_all(_REPLACE_CHARS, " ", literal=False)

    # Step 4: remove noise words (whole-word boundaries via \b…\b regex)
    for word in noise_words:
        if word:  # guard against empty strings in the noise list
            # \b word-boundary ensures we don't clip substrings
            # e.g. 'net' won't strip 'netapp' → 'app'
            pattern = rf"\b{re.escape(word.lower())}\b"
            expr = expr.str.replace_all(pattern, " ", literal=False)

    # Step 5: collapse multiple spaces and strip
    expr = expr.str.replace_all(r"\s+", " ", literal=False).str.strip_chars()

    return expr


# ---------------------------------------------------------------------------
# 2. Python lru_cache version (for single-string matching-time use)
# ---------------------------------------------------------------------------


@lru_cache(maxsize=50_000)
def clean_string(distinguished_name: str, noise_words: tuple[str, ...]) -> str:
    """
    Normalize a distinguished name for fuzzy comparison.

    Identical in behavior to ``build_dn_clean_expression`` but operates on a
    single Python string.  The ``lru_cache`` avoids redundant work when the
    same DN appears across multiple alert certs.

    Parameters
    ----------
    distinguished_name:
        Raw DN string (e.g. ``"CN=rfa.afore.mexico.chase.net"``).
    noise_words:
        *Tuple* of words to strip (must be a tuple for ``lru_cache`` hashability).

    Returns
    -------
    str
        Normalized DN suitable for fuzzy comparison.

    Notes
    -----
    Pass ``tuple(settings.sources.noise_words)`` when calling from the
    matching engine to satisfy the tuple requirement.
    """
    # Step 1: extract CN= portion
    m = _CN_RE.search(distinguished_name)
    if m:
        distinguished_name = m.group(1)

    # Step 2: lowercase
    text = distinguished_name.casefold()

    # Step 3: replace punctuation
    text = re.sub(_REPLACE_CHARS, " ", text)

    # Step 4: remove noise words (word-boundary safe)
    for word in noise_words:
        if word:
            pattern = rf"\b{re.escape(word.lower())}\b"
            text = re.sub(pattern, " ", text)

    # Step 5: collapse and strip
    text = re.sub(r"\s+", " ", text).strip()

    return text


# ---------------------------------------------------------------------------
# Helpers used by source processors
# ---------------------------------------------------------------------------


def split_instance_name(raw: str | None) -> list[str]:
    """
    Split a comma-separated ``instance_name`` string into a clean list.

    Handles:
    * ``None`` / missing values  → empty list
    * Literal string ``'null'``  → empty list
    * Trailing commas            → ignored
    * Surrounding whitespace     → stripped per element

    Parameters
    ----------
    raw:
        The raw ``instance_name`` value as stored in MongoDB / parquet.

    Returns
    -------
    list[str]
        Cleaned list of instance name strings.
    """
    if not raw or (isinstance(raw, str) and raw.strip().lower() == "null"):
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def parse_san_names(raw: str | None) -> list[str]:
    """
    Parse the Akamai ``san_names`` field from its string-encoded list form.

    The field is stored as a Python list literal string, e.g.
    ``"['a.example.com', 'b.example.com']"``.  We use ``ast.literal_eval``
    to parse it safely (no ``eval``).

    Falls back to an empty list on any parse error.

    Parameters
    ----------
    raw:
        Raw string value from MongoDB / parquet.

    Returns
    -------
    list[str]
        Parsed SAN names, or ``[]`` on failure.
    """
    import ast  # stdlib — import here to keep top-level imports minimal

    if not raw or (isinstance(raw, str) and raw.strip().lower() == "null"):
        return []
    try:
        result = ast.literal_eval(raw)
        if isinstance(result, list):
            return [str(item).strip() for item in result if item]
        return [str(result).strip()]
    except (ValueError, SyntaxError):
        # If it's just a plain comma-separated string, fall back to splitting
        if "," in raw:
            return [item.strip().strip("'\"") for item in raw.split(",") if item.strip()]
        return [raw.strip()] if raw.strip() else []


def normalise_serial_number(serial: str | None) -> str | None:
    """
    Normalize a certificate serial number to uppercase for consistent
    comparison across sources that store it in mixed case.

    Parameters
    ----------
    serial:
        Raw serial number string.

    Returns
    -------
    str | None
        Uppercased serial number, or ``None`` if the input is falsy / 'null'.
    """
    if not serial or (isinstance(serial, str) and serial.strip().lower() == "null"):
        return None
    return serial.strip().upper()
