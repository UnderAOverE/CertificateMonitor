"""
Parquet cache manager.

Responsibility
--------------
* Decide whether the on-disk parquet cache is fresh enough to reuse.
* If stale (or missing), stream eligible certificate records from MongoDB
  using a single seek-paginated cursor, flatten them into a Polars DataFrame,
  and persist the result as a parquet file.
* Add computed columns (``dn_clean``, ``serial_number_upper``,
  ``days_to_expiration_live``) that downstream processors rely on.
* Expose a ``load_cache`` function that callers use to get the DataFrame;
  they never touch parquet I/O directly.

Cache freshness
---------------
The parquet file is considered stale when:
    now() - file mtime > PARQUET_CACHE_MAX_AGE_HOURS

The ``--force-refresh`` CLI flag bypasses this check.

Schema strategy
---------------
MongoDB documents contain deeply nested ``source_properties`` sub-documents
whose fields are inconsistent across sources (missing fields, numeric strings,
literal ``'null'`` strings, ``NaN``).  We flatten them during cursor
iteration *before* handing data to Polars, normalizing every
``source_properties.*`` leaf to a string (or ``None``).  Top-level numeric
fields (``csi_application_id``, ``days_to_expiration``) are preserved as
integers.

The explicit ``PARQUET_SCHEMA`` definition ensures Polars never infers the
wrong type from a batch that happens to be missing a field.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from src.batch.config.settings.cm import CMSettings
from src.batch.utilities.cm import build_dn_clean_expression, normalise_serial_number

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Explicit Polars schema for the parquet file
# ---------------------------------------------------------------------------
# All source_properties.* fields are stored as Utf8 (string).
# Top-level numeric fields keep their natural types.
# Datetime fields are stored as Datetime(time_unit="us", time_zone="UTC").

_DT = pl.Datetime(time_unit="us", time_zone="UTC")

PARQUET_SCHEMA: dict[str, pl.PolarsDataType] = {
    # ── Top-level fields ──────────────────────────────────────────────────
    "distinguished_name": pl.Utf8,
    "start_date": _DT,
    "expiration_date": _DT,
    "csi_application_id": pl.Int64,
    "status": pl.Utf8,
    "days_to_expiration": pl.Int64,
    "log_date": _DT,
    # ── source_properties.* (all Utf8; normalized from mixed types) ───────
    "sp_name": pl.Utf8,
    "sp_serial_number": pl.Utf8,
    "sp_certificate_type": pl.Utf8,
    "sp_certificate_owner": pl.Utf8,
    "sp_certificate_name": pl.Utf8,
    "sp_owner_email": pl.Utf8,
    "sp_support_group": pl.Utf8,
    "sp_support_group_email": pl.Utf8,
    "sp_application_manager": pl.Utf8,
    "sp_l3_application_head": pl.Utf8,
    "sp_l4_application_head": pl.Utf8,
    "sp_environment": pl.Utf8,
    "sp_evolven_host": pl.Utf8,
    "sp_evolven_path": pl.Utf8,
    "sp_ssg_domain": pl.Utf8,
    "sp_ssg_url_in": pl.Utf8,
    "sp_ssg_url_out": pl.Utf8,
    "sp_internal_ssg_domain": pl.Utf8,
    "sp_ssg_url": pl.Utf8,
    "sp_san_names": pl.Utf8,
    "sp_instance_name": pl.Utf8,
    "sp_microservice_name": pl.Utf8,
    "sp_openshift_namespace": pl.Utf8,
    "sp_openshift_container": pl.Utf8,
    "sp_ssl_cm_region": pl.Utf8,
    "sp_ssl_cm_sector": pl.Utf8,
    "sp_ssl_cm_status": pl.Utf8,
    "sp_lob_domain": pl.Utf8,
    # ── Computed columns (added after load) ──────────────────────────────
    "dn_clean": pl.Utf8,
    "serial_number_upper": pl.Utf8,
}

# Fields that live inside source_properties and should be flattened.
# Keys = MongoDB field names;  Values = parquet column names (prefixed sp_).
_SP_FIELD_MAP: dict[str, str] = {
    "name": "sp_name",
    "serial_number": "sp_serial_number",
    "certificate_type": "sp_certificate_type",
    "certificate_owner": "sp_certificate_owner",
    "certificate_name": "sp_certificate_name",
    "owner_email": "sp_owner_email",
    "support_group": "sp_support_group",
    "support_group_email": "sp_support_group_email",
    "application_manager": "sp_application_manager",
    "l3_application_head": "sp_l3_application_head",
    "l4_application_head": "sp_l4_application_head",
    "environment": "sp_environment",
    "evolven_host": "sp_evolven_host",
    "evolven_path": "sp_evolven_path",
    "ssg_domain": "sp_ssg_domain",
    "ssg_url_in": "sp_ssg_url_in",
    "ssg_url_out": "sp_ssg_url_out",
    "internal_ssg_domain": "sp_internal_ssg_domain",
    "ssg_url": "sp_ssg_url",
    "san_names": "sp_san_names",
    "instance_name": "sp_instance_name",
    "microservice_name": "sp_microservice_name",
    "openshift_namespace": "sp_openshift_namespace",
    "openshift_container": "sp_openshift_container",
    "ssl_cm_region": "sp_ssl_cm_region",
    "ssl_cm_sector": "sp_ssl_cm_sector",
    "ssl_cm_status": "sp_ssl_cm_status",
    "lob_domain": "sp_lob_domain",
}


# ---------------------------------------------------------------------------
# Raw document flattener
# ---------------------------------------------------------------------------


def _safe_str(value: Any) -> str | None:
    """
    Convert any value to a clean string, normalizing nulls.

    * Python ``None``        → ``None``
    * String ``'null'``      → ``None``
    * ``float('nan')``       → ``None``
    * Everything else        → ``str(value).strip()``
    """
    if value is None:
        return None
    if isinstance(value, float) and (value != value):  # NaN check without math import
        return None
    s = str(value).strip()
    if s.lower() == "null" or s == "":
        return None
    return s


def _safe_int(value: Any) -> int | None:
    """Convert a value to int, returning None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_datetime(value: Any) -> datetime | None:
    """
    Coerce a value to a UTC-aware datetime.

    Motor returns BSON Date as timezone-aware ``datetime``.  If for any
    reason it arrives as naive, we attach UTC explicitly.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    return None


def _flatten_document(doc: dict[str, Any]) -> dict[str, Any]:
    """
    Flatten a raw MongoDB certificate document into a single-level dict
    whose keys match ``PARQUET_SCHEMA``.

    All ``source_properties.*`` sub-fields are extracted with the ``sp_``
    prefix.  Missing fields default to ``None`` so Polars can build a
    consistent schema.
    """
    sp: dict[str, Any] = doc.get("source_properties") or {}

    row: dict[str, Any] = {
        # Top-level
        "distinguished_name": _safe_str(doc.get("distinguished_name")),
        "start_date": _safe_datetime(doc.get("start_date")),
        "expiration_date": _safe_datetime(doc.get("expiration_date")),
        "csi_application_id": _safe_int(doc.get("csi_application_id")),
        "status": _safe_str(doc.get("status")),
        "days_to_expiration": _safe_int(doc.get("days_to_expiration")),
        "log_date": _safe_datetime(doc.get("log_date")),
    }

    # Flatten source_properties fields
    for mongo_key, parquet_col in _SP_FIELD_MAP.items():
        raw_val = sp.get(mongo_key)
        # instance_name for Evolven can legitimately be numeric — keep as str
        row[parquet_col] = _safe_str(raw_val)

    return row


# ---------------------------------------------------------------------------
# Batch → DataFrame builder
# ---------------------------------------------------------------------------


def _build_dataframe_from_rows(
    rows: list[dict[str, Any]],
    noise_words: list[str],
) -> pl.DataFrame:
    """
    Convert a list of flattened row dicts to a typed Polars DataFrame.

    Applies the explicit schema, fills structural nulls, and adds computed
    columns (``dn_clean``, ``serial_number_upper``).

    Parameters
    ----------
    rows:
        Output of ``_flatten_document`` for each MongoDB document.
    noise_words:
        DN noise words from ``SourceSettings``.
    """
    if not rows:
        # Return an empty DataFrame with the full schema
        return pl.DataFrame(schema=PARQUET_SCHEMA)

    # Build the DataFrame — Polars infers types from the dicts first…
    df = pl.from_dicts(rows, infer_schema_length=500)

    # …then we cast to the canonical schema column-by-column (safe cast).
    cast_exprs: list[pl.Expr] = []
    for col_name, dtype in PARQUET_SCHEMA.items():
        if col_name in df.columns:
            cast_exprs.append(pl.col(col_name).cast(dtype, strict=False))
        else:
            # Column entirely absent from this batch — add as null column
            cast_exprs.append(pl.lit(None).cast(dtype).alias(col_name))

    df = df.with_columns(cast_exprs)

    # Keep only schema columns in the defined order
    df = df.select(list(PARQUET_SCHEMA.keys()))

    # Computed: dn_clean (Polars-native Rust expressions)
    df = df.with_columns(
        build_dn_clean_expression(noise_words, source_column="distinguished_name").alias("dn_clean")
    )

    # Computed: serial_number_upper
    df = df.with_columns(
        pl.col("sp_serial_number").str.to_uppercase().alias("serial_number_upper")
    )

    return df


# ---------------------------------------------------------------------------
# Cache freshness check
# ---------------------------------------------------------------------------


def is_cache_fresh(path: Path, max_age_hours: float) -> bool:
    """
    Return ``True`` if the parquet file at ``path`` exists and its
    modification time is within ``max_age_hours`` of now.

    Parameters
    ----------
    path:
        Absolute or relative path to the parquet file.
    max_age_hours:
        Maximum allowed age in hours.
    """
    if not path.exists():
        logger.info("Parquet cache not found at '%s'.", path)
        return False

    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    age_hours = (datetime.now(tz=timezone.utc) - mtime).total_seconds() / 3600

    if age_hours > max_age_hours:
        logger.info(
            "Parquet cache is %.1f h old (threshold %.1f h) — will rebuild.",
            age_hours,
            max_age_hours,
        )
        return False

    logger.info(
        "Parquet cache is %.1f h old — reusing '%s'.",
        age_hours,
        path,
    )
    return True


# ---------------------------------------------------------------------------
# Main public API
# ---------------------------------------------------------------------------


async def build_and_save_cache(
    repository,  # CMConsolidatedDataMotorRepository
    settings: CMSettings,
) -> pl.DataFrame:
    """
    Stream eligible certificate records from MongoDB, flatten them, build a
    Polars DataFrame, and persist it as a parquet file.

    This is the "expensive" path — called only when the cache is stale or
    missing.  A single seek-paginated cursor is used so only one connection
    slot is consumed regardless of collection size.

    Parameters
    ----------
    repository:
        An instance of ``CMConsolidatedDataMotorRepository`` (your existing
        repository class).
    settings:
        Populated ``CMSettings``.

    Returns
    -------
    pl.DataFrame
        The freshly built cache DataFrame (also persisted to disk).
    """
    t = settings.thresholds
    s = settings.sources
    paths = settings.paths

    logger.info(
        "Building parquet cache: environments=%s sources=%s "
        "days=[%d, %d] log_staleness=%dd",
        s.environments,
        s.active_sources,
        t.alert_days_min,
        t.alert_days_max,
        t.log_date_staleness_days,
    )

    # Ensure cache directory exists
    paths.cache_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, Any]] = []
    batch_count = 0
    total_docs = 0

    # ── Single cursor, one connection slot ──────────────────────────────────
    async for batch in repository.find_eligible_certificates(
        environments=s.environments,
        log_date_threshold=t.log_date_staleness_days,
        expiry_threshold=t.alert_days_min,
        validity_threshold=t.alert_days_max,
        source_names=s.active_sources if s.active_sources else None,
    ):
        batch_count += 1
        total_docs += len(batch)

        for doc in batch:
            # Motor returns model objects; convert back to dict for flattening
            raw = doc.model_dump() if hasattr(doc, "model_dump") else dict(doc)
            all_rows.append(_flatten_document(raw))

        if batch_count % 10 == 0:
            logger.info("  … processed %d batches (%d docs so far)", batch_count, total_docs)

    logger.info("Cursor exhausted: %d total documents fetched.", total_docs)

    # ── Build DataFrame ──────────────────────────────────────────────────────
    logger.info("Building Polars DataFrame …")
    df = _build_dataframe_from_rows(all_rows, list(s.noise_words))

    logger.info("DataFrame shape: %d rows × %d columns", df.height, df.width)

    # ── Persist to parquet ───────────────────────────────────────────────────
    parquet_path = paths.parquet_path
    df.write_parquet(parquet_path, compression="snappy")
    logger.info("Parquet cache written to '%s'.", parquet_path)

    return df


def load_cache(settings: CMSettings) -> pl.DataFrame:
    """
    Load the parquet cache from disk.

    Raises
    ------
    FileNotFoundError
        If the parquet file does not exist (caller should have built it first).
    """
    path = settings.paths.parquet_path
    if not path.exists():
        raise FileNotFoundError(
            f"Parquet cache not found at '{path}'. "
            "Run with --force-refresh or wait for the cache to be built."
        )
    logger.info("Loading parquet cache from '%s' …", path)
    df = pl.read_parquet(path)
    logger.info("Cache loaded: %d rows × %d columns.", df.height, df.width)
    return df


async def get_or_build_cache(
    repository,
    settings: CMSettings,
    force_refresh: bool = False,
) -> pl.DataFrame:
    """
    Return the certificate cache DataFrame, building it from MongoDB if needed.

    This is the single entry-point used by the runner.

    Parameters
    ----------
    repository:
        ``CMConsolidatedDataMotorRepository`` instance.
    settings:
        Populated ``CMSettings``.
    force_refresh:
        If ``True``, ignore the cache age and always rebuild from MongoDB.

    Returns
    -------
    pl.DataFrame
        The certificate cache, either loaded from disk or freshly built.
    """
    path = settings.paths.parquet_path
    max_age = settings.thresholds.parquet_cache_max_age_hours

    if not force_refresh and is_cache_fresh(path, max_age):
        return load_cache(settings)

    return await build_and_save_cache(repository, settings)
