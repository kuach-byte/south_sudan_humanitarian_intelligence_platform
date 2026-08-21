"""Unit tests for the Ingestion coordinator.

Exercises the real Ingestion.run() / _resolve_loader_class() logic. All
dependencies (DataExtractor, loader classes) are mocked — no real files,
PostgreSQL, or pandas processing.
"""
from unittest.mock import MagicMock

import pytest

from data_engineering_layer.ingestion import ingestion as ing

LOADER_NAMES = ("CSVLoader", "ExcelLoader", "GeoPackageLoader", "GeoJSONLoader")


@pytest.fixture
def mock_extractor():
    return MagicMock(name="extractor")


@pytest.fixture
def mock_loader_classes(monkeypatch):
    """Patch Ingestion.LOADER_CLASSES with mock classes/instances for each
    entry. monkeypatch.setitem restores each entry automatically.
    """

    def _install():
        loaders = {}
        for name in LOADER_NAMES:
            instance = MagicMock(name=f"{name}_instance")
            cls_mock = MagicMock(name=name, return_value=instance)
            monkeypatch.setitem(ing.Ingestion.LOADER_CLASSES, name, cls_mock)
            loaders[name] = {"class": cls_mock, "instance": instance}
        return loaders

    return _install


def test_run_success(mock_extractor, mock_loader_classes):
    df = object()
    mock_extractor.extract.return_value = df
    loaders = mock_loader_classes()
    load_result = object()
    loaders["CSVLoader"]["instance"].load.return_value = load_result

    ingestion = ing.Ingestion(extractor=mock_extractor)
    result = ingestion.run(
        source_path="foo.csv",
        file_type="csv",
        loader_class="CSVLoader",
        schema="public",
        table="widgets",
        load_mode="append",
        chunk_size=500,
    )

    mock_extractor.extract.assert_called_once_with(path="foo.csv", file_type="csv")
    loaders["CSVLoader"]["class"].assert_called_once_with()
    loaders["CSVLoader"]["instance"].load.assert_called_once_with(
        df, schema="public", table="widgets", load_mode="append", chunk_size=500
    )

    assert isinstance(result, ing.IngestionResult)
    assert result.source_path == "foo.csv"
    assert result.file_type == "csv"
    assert result.loader_class == "CSVLoader"
    assert result.load_result is load_result


@pytest.mark.parametrize("name", LOADER_NAMES)
def test_resolve_loader_class_supported(name):
    assert ing.Ingestion._resolve_loader_class(name) is ing.Ingestion.LOADER_CLASSES[name]


def test_resolve_loader_class_unsupported():
    with pytest.raises(ing.UnsupportedLoaderClassError) as exc_info:
        ing.Ingestion._resolve_loader_class("ParquetLoader")

    message = str(exc_info.value)
    assert "ParquetLoader" in message
    for name in ing.Ingestion.LOADER_CLASSES:
        assert name in message


@pytest.mark.parametrize(
    "raised",
    [
        FileNotFoundError("no such file"),
        ing.UnsupportedFileTypeError("bad type"),
        ing.ExtractionError("boom"),
    ],
)
def test_run_extraction_failure(raised, mock_extractor, mock_loader_classes):
    mock_extractor.extract.side_effect = raised
    loaders = mock_loader_classes()

    ingestion = ing.Ingestion(extractor=mock_extractor)
    with pytest.raises(ing.IngestionError) as exc_info:
        ingestion.run(
            source_path="foo.csv",
            file_type="csv",
            loader_class="CSVLoader",
            schema="s",
            table="t",
        )

    assert "Extraction failed" in str(exc_info.value)
    assert exc_info.value.__cause__ is raised
    loaders["CSVLoader"]["class"].assert_not_called()


def test_run_load_failure(mock_extractor, mock_loader_classes):
    mock_extractor.extract.return_value = object()
    loaders = mock_loader_classes()
    load_error = ing.LoadError("disk full")
    loaders["CSVLoader"]["instance"].load.side_effect = load_error

    ingestion = ing.Ingestion(extractor=mock_extractor)
    with pytest.raises(ing.IngestionError) as exc_info:
        ingestion.run(
            source_path="foo.csv",
            file_type="csv",
            loader_class="CSVLoader",
            schema="public",
            table="widgets",
        )

    message = str(exc_info.value)
    assert "Load failed" in message
    assert "public.widgets" in message
    assert exc_info.value.__cause__ is load_error


def test_run_uses_default_load_mode_and_chunk_size(mock_extractor, mock_loader_classes):
    df = object()
    mock_extractor.extract.return_value = df
    loaders = mock_loader_classes()

    ingestion = ing.Ingestion(extractor=mock_extractor)
    ingestion.run(
        source_path="foo.csv", file_type="csv", loader_class="CSVLoader", schema="s", table="t"
    )

    loaders["CSVLoader"]["instance"].load.assert_called_once_with(
        df, schema="s", table="t", load_mode="replace", chunk_size=None
    )


def test_extractor_injection(monkeypatch, mock_extractor):
    default_instance = MagicMock(name="default_extractor_instance")
    default_extractor_cls = MagicMock(name="DataExtractor", return_value=default_instance)
    monkeypatch.setattr(ing, "DataExtractor", default_extractor_cls)

    injected = ing.Ingestion(extractor=mock_extractor)
    assert injected._extractor is mock_extractor
    default_extractor_cls.assert_not_called()

    default = ing.Ingestion()
    assert default._extractor is default_instance
    default_extractor_cls.assert_called_once_with()


@pytest.mark.parametrize("loader_name", LOADER_NAMES)
def test_run_selects_requested_loader(loader_name, mock_extractor, mock_loader_classes):
    mock_extractor.extract.return_value = object()
    loaders = mock_loader_classes()

    ingestion = ing.Ingestion(extractor=mock_extractor)
    ingestion.run(
        source_path="f", file_type="csv", loader_class=loader_name, schema="s", table="t"
    )

    loaders[loader_name]["class"].assert_called_once_with()
    loaders[loader_name]["instance"].load.assert_called_once()
    for other in LOADER_NAMES:
        if other != loader_name:
            loaders[other]["class"].assert_not_called()


# ---------------------------------------------------------------------------
# GeoJSON support
#
# Test A: extraction (DataExtractor -> GeoDataFrame, geometry/CRS preserved)
# Test B: loading (GeoJSONLoader -> PostGIS insert path, geometry never
#         converted to text)
# All CSV/Excel/GeoPackage/generic tests above are left unchanged; the
# GeoJSONLoader entry added to LOADER_NAMES already extends the generic
# resolve/dispatch tests above to cover GeoJSON as well.
# ---------------------------------------------------------------------------


def test_geojson_extraction_preserves_geometry_and_crs(tmp_path):
    """Test A: extracting a .geojson file yields a GeoDataFrame with an
    intact geometry column and CRS -- geometry must never be stringified.
    """
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import Point

    from data_engineering_layer.ingestion.extractor import DataExtractor

    gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2],
            "name": ["alpha", "beta"],
            "geometry": [Point(0.0, 0.0), Point(1.0, 1.0)],
        },
        crs="EPSG:4326",
    )
    fixture_path = tmp_path / "fixture.geojson"
    gdf.to_file(fixture_path, driver="GeoJSON")

    result = DataExtractor().extract(path=str(fixture_path), file_type="geojson")

    assert isinstance(result, gpd.GeoDataFrame)
    assert "geometry" in result.columns
    assert result.geometry.notna().all()
    assert not result["geometry"].apply(lambda g: isinstance(g, str)).any()
    assert result.crs is not None
    assert result.crs.to_epsg() == 4326
    assert len(result) == 2


def test_geojson_loader_creates_table_when_absent(monkeypatch):
    """Test B1: with no existing table, replace mode creates the spatial
    table fresh and drives the same PostGIS insert path as GeoPackageLoader
    -- geometry is handed to the spatial insert routine untouched (never
    cast to str/WKT), and the configured schema/table/chunk_size are
    respected. Database interaction is mocked; no real PostgreSQL is used.
    """
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import Point

    from data_engineering_layer.ingestion import loader as ld

    gdf = gpd.GeoDataFrame(
        {"name": ["a", "b"], "geometry": [Point(0.0, 0.0), Point(1.0, 1.0)]},
        crs="EPSG:4326",
    )
    original_geometry = list(gdf.geometry)

    fake_conn = MagicMock(name="fake_pg_connection")
    monkeypatch.setattr(ld.psycopg2, "connect", MagicMock(return_value=fake_conn))

    # No table exists yet -> replace mode must create it.
    monkeypatch.setattr(ld.GeoJSONLoader, "_table_exists", MagicMock(return_value=False))

    create_spatial_table_mock = MagicMock(name="_create_spatial_table")
    truncate_table_mock = MagicMock(name="_truncate_table")
    insert_spatial_rows_mock = MagicMock(name="_insert_spatial_rows", return_value=len(gdf))
    create_spatial_index_mock = MagicMock(name="_create_spatial_index")

    monkeypatch.setattr(ld.GeoJSONLoader, "_create_spatial_table", create_spatial_table_mock)
    monkeypatch.setattr(ld.GeoJSONLoader, "_truncate_table", truncate_table_mock)
    monkeypatch.setattr(ld.GeoJSONLoader, "_insert_spatial_rows", insert_spatial_rows_mock)
    monkeypatch.setattr(ld.GeoJSONLoader, "_create_spatial_index", create_spatial_index_mock)

    loader = ld.GeoJSONLoader(connection_params={"host": "test", "port": 5432, "dbname": "test_db", "user": "test_user", "password": "test_pass"})
    result = loader.load(
        gdf, schema="raw_data", table="admin1_boundary", load_mode="replace", chunk_size=250
    )

    assert isinstance(result, ld.LoadResult)
    assert result.schema == "raw_data"
    assert result.table == "admin1_boundary"
    assert result.load_mode == "replace"
    assert result.rows_loaded == len(gdf)

    create_spatial_table_mock.assert_called_once()
    truncate_table_mock.assert_not_called()
    insert_spatial_rows_mock.assert_called_once()
    create_spatial_index_mock.assert_called_once()

    # geometry passed through to the spatial insert routine must still be
    # real shapely geometry objects, not strings/WKT.
    inserted_gdf = insert_spatial_rows_mock.call_args.kwargs.get("gdf")
    assert inserted_gdf is not None
    assert list(inserted_gdf.geometry) == original_geometry
    assert not any(isinstance(g, str) for g in inserted_gdf.geometry)


def test_geojson_loader_truncates_when_table_exists(monkeypatch):
    """Test B2: with an existing table, replace mode TRUNCATEs in place
    instead of dropping and recreating -- this is what keeps a downstream
    dbt view built on this table alive across reloads. _create_spatial_table
    must NOT be called in this branch.
    """
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import Point

    from data_engineering_layer.ingestion import loader as ld

    gdf = gpd.GeoDataFrame(
        {"name": ["a", "b"], "geometry": [Point(0.0, 0.0), Point(1.0, 1.0)]},
        crs="EPSG:4326",
    )
    original_geometry = list(gdf.geometry)

    fake_conn = MagicMock(name="fake_pg_connection")
    monkeypatch.setattr(ld.psycopg2, "connect", MagicMock(return_value=fake_conn))

    # Table already exists -> replace mode must truncate, not recreate.
    monkeypatch.setattr(ld.GeoJSONLoader, "_table_exists", MagicMock(return_value=True))

    create_spatial_table_mock = MagicMock(name="_create_spatial_table")
    truncate_table_mock = MagicMock(name="_truncate_table")
    insert_spatial_rows_mock = MagicMock(name="_insert_spatial_rows", return_value=len(gdf))
    create_spatial_index_mock = MagicMock(name="_create_spatial_index")

    monkeypatch.setattr(ld.GeoJSONLoader, "_create_spatial_table", create_spatial_table_mock)
    monkeypatch.setattr(ld.GeoJSONLoader, "_truncate_table", truncate_table_mock)
    monkeypatch.setattr(ld.GeoJSONLoader, "_insert_spatial_rows", insert_spatial_rows_mock)
    monkeypatch.setattr(ld.GeoJSONLoader, "_create_spatial_index", create_spatial_index_mock)

    loader = ld.GeoJSONLoader(connection_params={"host": "test", "port": 5432, "dbname": "test_db", "user": "test_user", "password": "test_pass"})
    result = loader.load(
        gdf, schema="raw_data", table="admin1_boundary", load_mode="replace", chunk_size=250
    )

    assert isinstance(result, ld.LoadResult)
    assert result.schema == "raw_data"
    assert result.table == "admin1_boundary"
    assert result.load_mode == "replace"
    assert result.rows_loaded == len(gdf)

    truncate_table_mock.assert_called_once()
    create_spatial_table_mock.assert_not_called()
    insert_spatial_rows_mock.assert_called_once()
    create_spatial_index_mock.assert_called_once()

    inserted_gdf = insert_spatial_rows_mock.call_args.kwargs.get("gdf")
    assert inserted_gdf is not None
    assert list(inserted_gdf.geometry) == original_geometry
    assert not any(isinstance(g, str) for g in inserted_gdf.geometry)


def test_geojson_loader_requires_geodataframe():
    """GeoJSONLoader inherits GeoPackageLoader's guard rejecting plain
    (non-spatial) DataFrames, since a GeoJSON source always yields a
    GeoDataFrame -- a plain DataFrame here signals a wiring error.
    """
    pd = pytest.importorskip("pandas")
    from data_engineering_layer.ingestion import loader as ld

    loader = ld.GeoJSONLoader(connection_params={"host": "test", "port": 5432, "dbname": "test_db", "user": "test_user", "password": "test_pass"})
    with pytest.raises(ld.LoadError):
        loader.load(pd.DataFrame({"a": [1]}), schema="s", table="t")