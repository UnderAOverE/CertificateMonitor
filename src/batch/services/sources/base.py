"""
Abstract base class for source certificate processors.

Every source (SSG, HashiCorp, Evolven, Apigee, Akamai, SSL Tracker) has its
own processor module that inherits from ``BaseSourceProcessor``.  The base
class enforces a consistent interface so the runner can iterate over all
processors polymorphically without knowing which source it is dealing with.

Extension guide
---------------
To add a new source:
    1. Create ``src/batch/services/sources/my_source.py``.
    2. Define a class ``MySourceProcessor(BaseSourceProcessor)``.
    3. Implement ``source_name``, ``key_fields``, and ``process``.
    4. Register it in ``src/batch/services/sources/__init__.py``.
    5. Add the new ``SourceName`` enum value.
    6. Done — the runner picks it up automatically.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

import polars as pl

from src.batch.config.settings.cm import CMSettings
from src.batch.models.alerts import CertificateAlertDocument
from src.batch.models.enums import SourceName
from src.batch.services.ignore import IgnoreSet
from src.batch.services.matching.fuzzy import run_matching_for_source

logger = logging.getLogger(__name__)


class BaseSourceProcessor(ABC):
    """
    Abstract base class for per-source certificate processors.

    Subclasses implement ``source_name``, ``key_fields``, and ``process``.
    The base class provides shared utilities for extracting alert certs from
    the parquet cache and running the fuzzy matching engine.
    """

    def __init__(self, settings: CMSettings) -> None:
        self.settings = settings
        self._logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}"
        )

    # ------------------------------------------------------------------
    # Abstract interface — subclasses must implement these
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def source_name(self) -> SourceName:
        """The ``SourceName`` enum value this processor handles."""
        ...

    @property
    @abstractmethod
    def key_fields(self) -> list[str]:
        """
        Parquet column names used to group certificates for this source.

        Used to:
        * Deduplicate OSE/API calls (HashiCorp).
        * Build upsert filter keys for MongoDB.
        * Group certs in email tables.
        """
        ...

    @abstractmethod
    async def process(
        self,
        parquet_df: pl.DataFrame,
        ignore_set: IgnoreSet,
    ) -> list[CertificateAlertDocument]:
        """
        Full processing pipeline for this source.

        Steps (implemented in subclasses with help from base utilities):
        1. Extract alert certs from parquet.
        2. Group by key fields.
        3. Enrich with source-specific details (API calls, field extraction).
        4. Run fuzzy matching (renewal + possible matches).
        5. Apply to ignore/acknowledgement check.
        6. Build and return ``CertificateAlertDocument`` list.

        Parameters
        ----------
        parquet_df:
            Full certificate cache DataFrame (all sources).
        ignore_set:
            Pre-loaded acknowledgement lookup set.

        Returns
        -------
        list[CertificateAlertDocument]
            One document per logical source group (e.g. one per SSG
            domain+service).
        """
        ...

    # ------------------------------------------------------------------
    # Shared utility methods available to all subclasses
    # ------------------------------------------------------------------

    def extract_alert_certs(self, parquet_df: pl.DataFrame) -> pl.DataFrame:
        """
        Filter the parquet cache to alert certificates for this source.

        A certificate is an "alert cert" if:
        * Its source (``sp_name``) matches ``self.source_name``.
        * Its ``days_to_expiration`` is <= ``alert_days_threshold``.

        Parameters
        ----------
        parquet_df:
            Full certificate cache.

        Returns
        -------
        pl.DataFrame
            Subset of the cache for this source's alert certs only.
        """
        threshold = self.settings.thresholds.alert_days_threshold

        alerts = parquet_df.filter(
            (pl.col("sp_name") == self.source_name.value)
            & (pl.col("days_to_expiration") <= threshold)
            & (pl.col("days_to_expiration") >= 0)
        )

        self._logger.info(
            "Source '%s': %d alert certificate(s) found (days_to_expiration <= %d).",
            self.source_name,
            alerts.height,
            threshold,
        )
        return alerts

    def group_alert_certs(
        self, alert_df: pl.DataFrame
    ) -> dict[tuple[str | None, ...], list[dict[str, Any]]]:
        """
        Group alert certificates by the source-specific key fields.

        Returns a dict mapping a key-tuple → list of row dicts in that group.
        The key tuple has one element per field in ``self.key_fields``, in
        order.  ``None`` values in the key represent missing/null fields.

        Example (SSG with key_fields = ["sp_ssg_domain", "sp_microservice_name"]):
            {
                ("sogateway.retail.chase.net", "GCB-AforeBNMX-QAID-154958"): [row1, row2],
                (None, "SomeOtherService"): [row3],
            }
        """
        groups: dict[tuple[str | None, ...], list[dict[str, Any]]] = {}

        for row in alert_df.iter_rows(named=True):
            key = tuple(
                (row.get(f) or None)
                for f in self.key_fields
                if f in alert_df.columns
            )
            groups.setdefault(key, []).append(row)

        self._logger.debug(
            "Source '%s': %d group(s) from %d alert cert(s).",
            self.source_name,
            len(groups),
            alert_df.height,
        )
        return groups

    def run_matching(
        self,
        alert_rows: list[dict[str, Any]],
        parquet_df: pl.DataFrame,
    ):
        """
        Run the fuzzy matching engine for a list of alert cert rows.

        Delegates to ``run_matching_for_source`` in the fuzzy module.

        Parameters
        ----------
        alert_rows:
            List of row dicts from the alert DataFrame.
        parquet_df:
            Full certificate cache.

        Returns
        -------
        list[MatchingResult]
        """
        return run_matching_for_source(alert_rows, parquet_df, self.settings)
