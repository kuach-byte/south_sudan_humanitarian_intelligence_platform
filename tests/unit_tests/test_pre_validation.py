"""Unit tests for the pre-load validation gate (pre_validation.py)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data_quality.pre_validation import PreValidator, ValidationResult

try:
    import geopandas as gpd
    from shapely.geometry import Point, Polygon
except ImportError:  # pragma: no cover - geopandas is an optional dependency,
    gpd = None       # mirroring extractor.py's lazy/optional geopandas import.


@pytest.fixture
def validator() -> PreValidator:
    return PreValidator()


# ---------------------------------------------------------------------------
# Valid cases
# ---------------------------------------------------------------------------


def test_normal_valid_dataframe(validator: PreValidator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
    result = validator.validate(df)
    assert result.is_valid is True
    assert result.errors == []
    assert result.checked_rows == 3


def test_dataframe_with_legitimate_nulls(validator: PreValidator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "notes": ["a", None, "c"]})
    result = validator.validate(df)
    assert result.is_valid is True
    assert result.errors == []
    assert any("notes" in w for w in result.warnings)


def test_dataframe_with_whitespace_strings(validator: PreValidator) -> None:
    df = pd.DataFrame({"id": [1, 2], "name": [" Alice ", "Bob  "]})
    result = validator.validate(df)
    assert result.is_valid is True
    assert result.errors == []


def test_dataframe_with_mixed_object_values(validator: PreValidator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "mixed": ["text", 42, None]})
    result = validator.validate(df)
    assert result.is_valid is True


def test_dataframe_with_unexpected_extra_columns(validator: PreValidator) -> None:
    df = pd.DataFrame(
        {"id": [1, 2], "name": ["Alice", "Bob"], "brand_new_source_column": [1, 2]}
    )
    result = validator.validate(df)
    assert result.is_valid is True
    assert result.errors == []


def test_dataframe_with_unusual_but_valid_values(validator: PreValidator) -> None:
    # Large but finite population figure - not the validator's job to flag.
    df = pd.DataFrame({"region": ["X"], "population": [5_000_000_000]})
    result = validator.validate(df)
    assert result.is_valid is True
    assert result.errors == []


def test_missing_optional_values_do_not_fail(validator: PreValidator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "optional_field": [np.nan, np.nan, np.nan]})
    result = validator.validate(df)
    assert result.is_valid is True


# ---------------------------------------------------------------------------
# Invalid cases
# ---------------------------------------------------------------------------


def test_none_dataframe_fails(validator: PreValidator) -> None:
    result = validator.validate(None)
    assert result.is_valid is False
    assert "None" in result.errors[0]
    assert result.checked_rows == 0




def test_empty_dataframe_fails(validator: PreValidator) -> None:
    df = pd.DataFrame({"id": [], "name": []})
    result = validator.validate(df)
    assert result.is_valid is False
    assert any("empty" in e for e in result.errors)


def test_dataframe_with_no_columns_fails(validator: PreValidator) -> None:
    df = pd.DataFrame()
    result = validator.validate(df)
    assert result.is_valid is False
    assert any("no columns" in e for e in result.errors)


def test_duplicate_column_names_fail(validator: PreValidator) -> None:
    df = pd.DataFrame([[1, 2], [3, 4]], columns=["id", "id"])
    result = validator.validate(df)
    assert result.is_valid is False
    assert any("Duplicate column" in e for e in result.errors)



def test_explicit_numeric_range_violation_fails(validator: PreValidator) -> None:
    df = pd.DataFrame({"latitude": [10.0, 45.0, 999.0]})
    rules = {"latitude": {"min": -90, "max": 90}}
    result = validator.validate(df, rules=rules)
    assert result.is_valid is False
    assert any("latitude" in e for e in result.errors)


def test_explicit_uniqueness_violation_fails(validator: PreValidator) -> None:
    df = pd.DataFrame({"record_id": [1, 2, 2, 3]})
    rules = {"record_id": {"unique": True}}
    result = validator.validate(df, rules=rules)
    assert result.is_valid is False
    assert any("uniqueness" in e for e in result.errors)


# ---------------------------------------------------------------------------
# Non-mutation
# ---------------------------------------------------------------------------


def test_validation_does_not_mutate_input(validator: PreValidator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "value": [1.0, np.inf, 3.0]})
    df_before = df.copy(deep=True)

    validator.validate(df, rules={"id": {"min": 0, "max": 100, "unique": True}})

    pd.testing.assert_frame_equal(df, df_before)


# ---------------------------------------------------------------------------
# Rule behavior
# ---------------------------------------------------------------------------


def test_range_rule_only_applies_to_named_column(validator: PreValidator) -> None:
    df = pd.DataFrame({"latitude": [500.0], "longitude": [500.0]})
    rules = {"latitude": {"min": -90, "max": 90}}
    result = validator.validate(df, rules=rules)

    assert result.is_valid is False
    assert any("latitude" in e for e in result.errors)
    # longitude was never given a rule, so it must not be flagged.
    assert not any("longitude" in e for e in result.errors)


def test_no_rules_supplied_means_no_rule_checks(validator: PreValidator) -> None:
    df = pd.DataFrame({"latitude": [500.0], "longitude": [500.0]})
    result = validator.validate(df)
    assert result.is_valid is True
    assert result.errors == []


def test_rule_for_column_not_present_is_skipped(validator: PreValidator) -> None:
    df = pd.DataFrame({"id": [1, 2, 3]})
    rules = {"missing_column": {"min": 0, "max": 10}}
    result = validator.validate(df, rules=rules)
    assert result.is_valid is True


def test_validation_result_is_dataclass_shaped() -> None:
    result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_rows=5)
    assert result.is_valid is True
    assert result.checked_rows == 5


# ---------------------------------------------------------------------------
# GeoJSON / GeoDataFrame geometry checks
#
# These exercise the geometry-specific validation added to
# `PreValidator.validate()`. It only activates for GeoDataFrame input, so
# every plain-DataFrame test above continues to pass unmodified and proves
# that non-geospatial behavior is unaffected.
# ---------------------------------------------------------------------------

pytestmark_geo = pytest.mark.skipif(gpd is None, reason="geopandas is not installed")


@pytestmark_geo
def test_valid_geojson_geodataframe_passes(validator: PreValidator) -> None:
    gdf = gpd.GeoDataFrame(
        {"name": ["north", "south"], "geometry": [Point(1, 1), Point(2, 2)]},
        crs="EPSG:4326",
    )
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []


@pytestmark_geo
def test_valid_geojson_polygon_geodataframe_passes(validator: PreValidator) -> None:
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame({"name": ["region"], "geometry": [polygon]}, crs="EPSG:4326")
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []


@pytestmark_geo
def test_empty_geojson_geodataframe_fails(validator: PreValidator) -> None:
    gdf = gpd.GeoDataFrame({"name": [], "geometry": []}, crs="EPSG:4326")
    result = validator.validate(gdf)
    assert result.is_valid is False
    assert any("empty" in e for e in result.errors)


@pytestmark_geo
def test_geodataframe_with_some_invalid_geometry_warns_not_fatal(
    validator: PreValidator,
) -> None:
    # Self-intersecting "bowtie" polygon is structurally invalid per Shapely.
    bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
    gdf = gpd.GeoDataFrame(
        {"name": ["a", "b", "c"], "geometry": [Point(1, 1), bowtie, Point(2, 2)]},
        crs="EPSG:4326",
    )
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []
    assert any("invalid geometry" in w for w in result.warnings)


@pytestmark_geo
def test_geodataframe_with_null_geometry_warns(validator: PreValidator) -> None:
    gdf = gpd.GeoDataFrame(
        {
            "name": ["a", "b", "c", "d"],
            "geometry": [Point(1, 1), Point(2, 2), None, Point(3, 3)],
        },
        crs="EPSG:4326",
    )
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []
    assert any("null or empty geometry" in w for w in result.warnings)


@pytestmark_geo
def test_geodataframe_with_mixed_geometry_types_warns(validator: PreValidator) -> None:
    polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(
        {"name": ["a", "b", "c"], "geometry": [Point(1, 1), polygon, Point(2, 2)]},
        crs="EPSG:4326",
    )
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []
    assert any("mixed geometry types" in w for w in result.warnings)


@pytestmark_geo
def test_geodataframe_missing_crs_warns(validator: PreValidator) -> None:
    gdf = gpd.GeoDataFrame({"name": ["a"], "geometry": [Point(1, 1)]})
    assert gdf.crs is None
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []
    assert any("no CRS" in w for w in result.warnings)


@pytestmark_geo
def test_geodataframe_invalid_coordinate_range_warns(validator: PreValidator) -> None:
    gdf = gpd.GeoDataFrame(
        {"name": ["a", "b"], "geometry": [Point(400, 4.8), Point(31.5, 6.8)]},
        crs="EPSG:4326",
    )
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []
    assert any("outside the valid geographic range" in w for w in result.warnings)


@pytestmark_geo
def test_geodataframe_empty_properties_warns(validator: PreValidator) -> None:
    gdf = gpd.GeoDataFrame(
        {"name": [None, "b"], "geometry": [Point(1, 1), Point(2, 2)]}, crs="EPSG:4326"
    )
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []
    assert any("empty properties" in w for w in result.warnings)


@pytestmark_geo
def test_geodataframe_duplicate_features_warn_not_fatal(validator: PreValidator) -> None:
    gdf = gpd.GeoDataFrame(
        {"name": ["a", "a", "b"], "geometry": [Point(1, 1), Point(1, 1), Point(2, 2)]},
        crs="EPSG:4326",
    )
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []
    assert any("duplicate feature" in w for w in result.warnings)


@pytestmark_geo
def test_geodataframe_realistic_mixed_quality_passes_with_warnings(
    validator: PreValidator,
) -> None:
    """Valid geometries + a null geometry + missing attributes should still
    be loadable - imperfect real-world data is not the same as unusable
    data."""
    gdf = gpd.GeoDataFrame(
        {"name": ["alpha", None, "gamma"], "geometry": [Point(1, 1), None, Point(3, 3)]},
        crs="EPSG:4326",
    )
    result = validator.validate(gdf)
    assert result.is_valid is True
    assert result.errors == []
    assert len(result.warnings) > 0


@pytestmark_geo
def test_geodataframe_expected_geometry_type_mismatch_fails(
    validator: PreValidator,
) -> None:
    gdf = gpd.GeoDataFrame({"name": ["a"], "geometry": [Point(1, 1)]}, crs="EPSG:4326")
    rules = {"geometry": {"expected_geometry_type": "Polygon"}}
    result = validator.validate(gdf, rules=rules)
    assert result.is_valid is False
    assert any("geometry type mismatch" in e for e in result.errors)


@pytestmark_geo
def test_plain_dataframe_is_unaffected_by_geometry_checks(validator: PreValidator) -> None:
    """Ordinary tabular data (CSV/Excel-sourced) must never trigger
    geometry-specific checks."""
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    result = validator.validate(df)
    assert result.is_valid is True
    assert result.errors == []
    assert result.warnings == []