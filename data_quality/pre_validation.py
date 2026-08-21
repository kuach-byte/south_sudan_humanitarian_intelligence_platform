"""
pre_validation.py

Pre-load validation gate for the humanitarian data engineering pipeline.

This module answers ONE question:

    "Is this DataFrame technically and structurally safe to load
     into the raw database layer?"

It deliberately does NOT answer:

    "Is this dataset clean, complete, or analytics-ready?"

That responsibility belongs downstream (dbt staging, cleaning,
transformation, analytics preparation).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised by callers when a ValidationResult is not valid.

    The validator itself never raises this - it only reports.
    Raising is the orchestrator's decision, e.g.:

        result = validator.validate(df)
        if not result.is_valid:
            raise DataQualityError(result.errors)
    """


@dataclass
class ValidationResult:
    """Outcome of a single pre-load validation run.

    Attributes:
        is_valid: True if the DataFrame is safe to pass to the loader.
        errors: Conditions that should block loading.
        warnings: Observations worth noting but not blocking.
        checked_rows: Number of rows that were present at check time.
    """

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checked_rows: int = 0


class PreValidator:
    """A minimal, metadata-friendly pre-load validation gate.

    The validator holds no dataset-specific knowledge. Any value
    range or uniqueness rules must be supplied explicitly per call
    via the ``rules`` argument (typically sourced from external
    metadata/configuration). If no rules are supplied, no rule-based
    checks are performed - the validator never guesses at intent
    from column names.

    Example:
        >>> validator = PreValidator()
        >>> result = validator.validate(df)
        >>> if not result.is_valid:
        ...     raise DataQualityError(result.errors)
    """

    def validate(
        self,
        df: pd.DataFrame | None,
        rules: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> ValidationResult:
        """Validate a single DataFrame against structural/technical checks.

        Args:
            df: The DataFrame to validate. Treated as read-only; never
                modified.
            rules: Optional per-column rules, e.g.::

                    {
                        "latitude": {"min": -90, "max": 90},
                        "record_id": {"unique": True},
                    }

                Only columns explicitly listed receive rule checks.
                Supported keys per column: "min", "max", "unique".

        Returns:
            A ValidationResult describing whether the DataFrame is
            safe to load, plus any errors/warnings collected.
        """
        logger.info("Validation started")
        errors: list[str] = []
        warnings: list[str] = []

        # --- Input sanity -------------------------------------------------
        if df is None:
            errors.append("DataFrame is None")
            logger.warning("Validation failed: DataFrame is None")
            return ValidationResult(is_valid=False, errors=errors, checked_rows=0)

        checked_rows = len(df)

        if df.columns.empty:
            errors.append("DataFrame has no columns")

        if df.empty:
            errors.append("DataFrame is empty (no rows)")

        # If structurally empty, no point running further checks.
        if errors:
            logger.warning(
                "Validation failed at input-sanity stage: %s", "; ".join(errors)
            )
            return ValidationResult(
                is_valid=False, errors=errors, warnings=warnings, checked_rows=checked_rows
            )

        # --- Structural sanity ---------------------------------------------
        duplicate_columns = df.columns[df.columns.duplicated()].unique().tolist()
        if duplicate_columns:
            errors.append(
                f"Duplicate column names create ambiguity: {duplicate_columns}"
            )



        # --- Missing values: informational warning only ---------------------
        columns_with_nulls = df.columns[df.isna().any()].tolist()
        if columns_with_nulls:
            warnings.append(
                f"Missing values present in columns: {columns_with_nulls}"
            )

        # --- Geometry (GeoJSON/GeoDataFrame) checks --------------------------
        # Only runs when `df` is a GeoDataFrame - the strongest available
        # signal that this is geospatial/GeoJSON-derived data. Ordinary
        # tabular DataFrames (CSV/Excel) are completely unaffected.
        if self._is_geodataframe(df):
            geometry_errors, geometry_warnings = self._validate_geometry(df, rules)
            errors.extend(geometry_errors)
            warnings.extend(geometry_warnings)

        # --- Optional externally supplied rules -----------------------------
        if rules:
            rule_errors = self._apply_rules(df, rules)
            errors.extend(rule_errors)

        is_valid = len(errors) == 0

        if is_valid:
            logger.info(
                "Validation passed (rows=%d, warnings=%d)", checked_rows, len(warnings)
            )
        else:
            logger.warning(
                "Validation failed (rows=%d, errors=%d): %s",
                checked_rows,
                len(errors),
                "; ".join(errors),
            )

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            checked_rows=checked_rows,
        )

    @staticmethod
    def _apply_rules(
        df: pd.DataFrame, rules: Mapping[str, Mapping[str, Any]]
    ) -> list[str]:
        """Evaluate explicitly supplied per-column rules.

        Only columns named in ``rules`` are checked. Supported rule
        keys: "min", "max", "unique". Unknown columns in ``rules``
        that are absent from ``df`` are skipped silently, since schema
        evolution is expected upstream.
        """
        rule_errors: list[str] = []

        for column, column_rules in rules.items():
            if column not in df.columns:
                continue

            series = df[column]

            min_value = column_rules.get("min")
            max_value = column_rules.get("max")
            if min_value is not None or max_value is not None:
                numeric_series = pd.to_numeric(series, errors="coerce")
                if min_value is not None:
                    violations = numeric_series < min_value
                    count = int(violations.sum())
                    if count:
                        rule_errors.append(
                            f"Column '{column}' has {count} value(s) below "
                            f"minimum {min_value}"
                        )
                if max_value is not None:
                    violations = numeric_series > max_value
                    count = int(violations.sum())
                    if count:
                        rule_errors.append(
                            f"Column '{column}' has {count} value(s) above "
                            f"maximum {max_value}"
                        )

            if column_rules.get("unique"):
                duplicate_count = int(series.duplicated().sum())
                if duplicate_count:
                    rule_errors.append(
                        f"Column '{column}' violates uniqueness rule: "
                        f"{duplicate_count} duplicate value(s)"
                    )

        return rule_errors

    # -- Geometry / GeoJSON validation ---------------------------------------
    #
    # These checks apply only when `df` is a GeoDataFrame, i.e. the extractor
    # produced it from a geospatial source (GeoJSON, GeoPackage, etc.). They
    # run *in addition to* the generic checks above and follow the same
    # error/warning split: only dataset-level, unloadable conditions are
    # errors (fatal - block `is_valid`); real-world geometry/attribute
    # imperfections that don't prevent loading are warnings.
    #
    # Geometry-specific expectations (expected geometry type, expected CRS)
    # are opt-in and metadata-driven: reuse the existing `rules` mapping,
    # keyed by the geometry column name, e.g.::
    #
    #     rules = {
    #         "geometry": {
    #             "expected_geometry_type": "Polygon",
    #             "expected_crs": "EPSG:4326",
    #         }
    #     }
    #
    # No dataset-name or dataset-specific branching is used anywhere below.

    @staticmethod
    def _is_geodataframe(df: pd.DataFrame) -> bool:
        """True if `df` is a geopandas GeoDataFrame.

        geopandas is an optional dependency (as in extractor.py), so it is
        imported lazily and its absence is treated as "not geospatial"
        rather than an error.
        """
        try:
            import geopandas as gpd
        except ImportError:
            return False
        return isinstance(df, gpd.GeoDataFrame)

    @classmethod
    def _validate_geometry(
        cls, df: pd.DataFrame, rules: Mapping[str, Mapping[str, Any]] | None
    ) -> tuple[list[str], list[str]]:
        """Run geometry/GeoJSON-specific checks on a GeoDataFrame.

        Operates on the actual Shapely geometry objects (via the
        GeoDataFrame produced by the extractor), before any WKT/WKB
        conversion happens in the loader.

        Returns:
            A (errors, warnings) tuple to be merged into the caller's lists.
        """
        errors: list[str] = []
        warnings: list[str] = []

        try:
            geometry_col = df.geometry.name
        except (AttributeError, ValueError) as exc:
            errors.append(f"GeoDataFrame has no usable geometry column: {exc}")
            return errors, warnings

        geom_series = df[geometry_col]
        total = len(geom_series)

        # -- Null / empty geometry -------------------------------------------
        unusable_mask = geom_series.isna() | geom_series.is_empty
        unusable_count = int(unusable_mask.sum())

        if unusable_count == total:
            errors.append(
                f"Column '{geometry_col}' has no usable geometry: all "
                f"{total} feature(s) are null or empty"
            )
            # Nothing further can be checked without any geometry.
            return errors, warnings
        elif unusable_count:
            warnings.append(
                f"Column '{geometry_col}' has {unusable_count} feature(s) "
                "with null or empty geometry"
            )

        usable = geom_series[~unusable_mask]

        # -- Structural/geometric validity ------------------------------------
        invalid_mask = ~usable.is_valid
        invalid_count = int(invalid_mask.sum())
        if invalid_count == len(usable):
            errors.append(
                f"Column '{geometry_col}' has no usable geometry: all "
                f"{len(usable)} present geometrie(s) are structurally invalid"
            )
            return errors, warnings
        elif invalid_count:
            invalid_indices = usable.index[invalid_mask].tolist()
            sample = invalid_indices[:10]
            suffix = "..." if len(invalid_indices) > len(sample) else ""
            warnings.append(
                f"Column '{geometry_col}' has {invalid_count} feature(s) "
                f"with invalid geometry (indices: {sample}{suffix})"
            )

        # -- Geometry type consistency -----------------------------------------
        geom_types_present = sorted(usable.geom_type.dropna().unique().tolist())
        expected_type = cls._rule_value(rules, geometry_col, "expected_geometry_type")

        if expected_type:
            mismatched = [t for t in geom_types_present if t != expected_type]
            if mismatched:
                errors.append(
                    f"Column '{geometry_col}' geometry type mismatch: expected "
                    f"'{expected_type}', found {geom_types_present}"
                )
        elif len(geom_types_present) > 1:
            warnings.append(
                f"Column '{geometry_col}' contains mixed geometry types: "
                f"{geom_types_present}"
            )

        # -- CRS ------------------------------------------------------------
        crs = df.crs
        expected_crs = cls._rule_value(rules, geometry_col, "expected_crs")

        if crs is None:
            warnings.append(f"Column '{geometry_col}' has no CRS defined")
        elif expected_crs:
            try:
                import pyproj

                matches = pyproj.CRS.from_user_input(crs) == pyproj.CRS.from_user_input(
                    expected_crs
                )
            except ImportError:
                matches = str(crs) == str(expected_crs)
            if not matches:
                errors.append(
                    f"Column '{geometry_col}' CRS mismatch: expected "
                    f"'{expected_crs}', found '{crs}'"
                )

        # -- Coordinate ranges (geographic CRS only) -----------------------------
        is_geographic = bool(crs is not None and getattr(crs, "is_geographic", False))
        if is_geographic and len(usable[~invalid_mask]) > 0:
            valid_usable = usable[~invalid_mask]
            bounds = valid_usable.bounds
            out_of_range_mask = (
                (bounds["minx"] < -180)
                | (bounds["maxx"] > 180)
                | (bounds["miny"] < -90)
                | (bounds["maxy"] > 90)
            )
            out_of_range_count = int(out_of_range_mask.sum())
            if out_of_range_count == len(valid_usable):
                errors.append(
                    f"Column '{geometry_col}' has no usable geometry: all "
                    f"{len(valid_usable)} feature(s) fall outside the valid "
                    "geographic coordinate range (lon -180..180, lat -90..90)"
                )
                return errors, warnings
            elif out_of_range_count:
                out_of_range_indices = bounds.index[out_of_range_mask].tolist()
                sample = out_of_range_indices[:10]
                suffix = "..." if len(out_of_range_indices) > len(sample) else ""
                warnings.append(
                    f"Column '{geometry_col}' has {out_of_range_count} "
                    "feature(s) with coordinates outside the valid geographic "
                    f"range (lon -180..180, lat -90..90); indices: {sample}{suffix}"
                )

        # -- Duplicate features (geometry + attributes) -----------------------
        # O(n) via hashing (WKB bytes + attribute tuple), never O(n^2)
        # pairwise geometry comparison.
        attribute_cols = [c for c in df.columns if c != geometry_col]
        try:
            geom_keys = geom_series.apply(
                lambda g: g.wkb if g is not None and not g.is_empty else None
            )
            if attribute_cols:
                attr_keys = df[attribute_cols].apply(tuple, axis=1)
            else:
                attr_keys = pd.Series([()] * total, index=df.index)
            combined_keys = pd.Series(
                list(zip(geom_keys, attr_keys)), index=df.index
            )
            duplicate_count = int(combined_keys.duplicated().sum())
            if duplicate_count:
                warnings.append(
                    f"{duplicate_count} duplicate feature(s) detected "
                    "(matching geometry and attributes)"
                )
        except TypeError:
            # Unhashable attribute values (e.g. nested lists/dicts) - skip
            # duplicate detection rather than failing validation over it.
            pass

        # -- Empty properties -------------------------------------------------
        if attribute_cols:
            empty_props_mask = df[attribute_cols].apply(
                lambda row: all(
                    pd.isna(v) or (isinstance(v, str) and v.strip() == "")
                    for v in row
                ),
                axis=1,
            )
            empty_props_count = int(empty_props_mask.sum())
            if empty_props_count:
                warnings.append(
                    f"{empty_props_count} feature(s) have empty properties"
                )

        return errors, warnings

    @staticmethod
    def _rule_value(
        rules: Mapping[str, Mapping[str, Any]] | None, column: str, key: str
    ) -> Any:
        """Look up a single metadata-supplied rule value for `column`.

        Thin helper so geometry-specific rule lookups (expected geometry
        type, expected CRS) read the same `rules` mapping already used by
        `_apply_rules`, rather than introducing a second configuration path.
        """
        if not rules:
            return None
        return rules.get(column, {}).get(key)