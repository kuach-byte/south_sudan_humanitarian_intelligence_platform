# Ingestion

Moves files from `i_p_f_w/data/landing/` into the `raw_data` schema of `humanitarian_db` without altering their content, and proves it with SHA-256.

| File | Role |
|---|---|
| `i_p_f_w/ingestion/prevalidation_job.py` | Discovers files, reads them faithfully, checks they are safe to load, hashes the source file. |
| `i_p_f_w/ingestion/loader_job.py` | Creates the raw table, copies the rows in, verifies them by hash, moves the files to `achieved/`. |
| `i_p_f_w/ingestion/integrity.py` | Shared helpers: source file hash and the row digest. |

## Flow

```
data/landing/<file> (+ <file>.yml)
   │  Prevalidationjob.run()
   │    hash source ─ extract ─ validate ─ hash source again
   ├─ errors   → data/rejected/   (data file, shapefile siblings, YAML)
   └─ valid    → result {data, metadata, metadata["integrity"]}
         │  Loader.run(metadata, dataframe)
         │    check source hash ─ DROP/CREATE ─ COPY ─ read back and compare digest ─ commit
         └─ data/achieved/      (data file, shapefile siblings, YAML)
```

The caller passes `result["metadata"]` and `result["data"]` from prevalidation to `Loader.run`. Registration of the YAML metadata (`MetadataRegistry`, `YamlMetadataParser`) is separate and only reads `source`, `dataset` and `target` from the metadata dict. The `integrity` key is ignored there.

## Fidelity rules

The raw layer is a faithful copy. Nothing is typed, cleaned or deduplicated here; that belongs to dbt staging.

| Source | How it is read | Result in `raw_data` |
|---|---|---|
| CSV | `dtype=str`, `keep_default_na=False`, `na_values=[""]` | All columns `text`. `007` stays `007`, `NA` and `null` stay as written. Only a blank cell is NULL. |
| Excel (`.xlsx`, `.xls`) | Same options, `header=None`, first sheet only | All columns `text`. Excel's own typing still applies (a date arrives as `2025-01-01 00:00:00`). |
| JSON | `json.load` into an object DataFrame | Values as written. Ints stay ints, big ints stay exact, nested objects stored as JSON text. |
| Parquet | `read_parquet(dtype_backend="numpy_nullable")` | Native types kept (ints with nulls stay ints). Empty strings stay empty strings. |
| `.geojson`, `.shp`, `.gpkg` | `pyogrio.read_dataframe` (default layer) | Geometry loaded as `geometry(Geometry, srid)`, attributes keep the file's types. No reprojection or repair. |

Column names are kept exactly as written. Only empty names (`unnamed_N`) and duplicates (`name_2`) are renamed, because a table cannot have them. Each rename is logged as a warning.

Known, deliberate or unavoidable changes:

- A blank CSV cell becomes NULL. `,,` and `,"",` cannot be told apart by pandas. `empty_string_count` in `data_profiling.column_profile` is therefore 0 for CSV and Excel sources.
- A float NaN loads as NULL.
- Shapefile field names are limited to 10 characters by the format itself.
- A CRS without an EPSG code loads as SRID 0.

## `prevalidation_job.py`

### Module-level

| Name | Purpose |
|---|---|
| `TEXT_READ` | Read options for CSV and Excel (see table above). |
| `chronological_key(stem)` | Sort key from the date at the end of the dataset name. Names without a valid date sort last. |
| `make_unique_columns(names)` | Returns `(unique_names, renamed)`. Only fixes empty and duplicate names. |
| `verify_csv(df, path)` | Re-reads the CSV with the `csv` module and compares its cell digest with the DataFrame's. Raises `ValueError` on any difference. |
| `validate_general(df)` | Errors: no columns, no rows, duplicate names. Warning: renamed columns. |
| `validate_geospatial(gdf)` | Errors: no features, no geometry type. Warnings: no geometry column, no CRS. |

### `Prevalidationjob`

| Method | Behaviour |
|---|---|
| `discover_datasets()` | Supported files under `landing/`, grouped by folder, oldest first by name date. Never uses file timestamps. |
| `list_spatial_layers(path)` | Lists layers and rejects files with none or more than 5. Only the default layer is read afterwards. |
| `extract_dataset(path)` | Reads a file according to the fidelity table. For CSV it also runs `verify_csv`. |
| `prevalidate(data, path)` | Dispatches to the general or geospatial validation. |
| `reject_dataset(path)` | Moves the data file, shapefile siblings and YAML sidecar to `rejected/`, keeping the sub-path. |
| `build_metadata(path)` | Metadata dict: `dataset`, `source` (`local_file`, relative path, extension), `target` (`humanitarian_db.raw_data.<stem>`). Also used by `YamlMetadataParser`. |
| `run()` | Processes every dataset and returns one result dict per file. |

`run()` per dataset:

1. Skip if the file already exists in `achieved/` (`status: skipped`).
2. `source_sha256(path)`.
3. Extract, then validate.
4. `source_sha256(path)` again. A different hash adds the error `Source file changed during extraction`.
5. Errors → `reject_dataset`, `status: rejected`. Otherwise `status: valid`, with `data`, `metadata` and `metadata["integrity"] = {"source_sha256": ...}`.

Result keys: `path`, `status`, `data`, `metadata`, `crs`, `errors`, `warnings`, `timing`.

### Checks that reject a file

| Check | Catches |
|---|---|
| Extraction exception | Unreadable file, parser error, more than 5 layers. |
| `verify_csv` | Rows with extra fields that pandas would silently shift into an index. |
| Source hash before and after | File modified while being read. |
| `validate_general` / `validate_geospatial` | See above. |

## `loader_job.py`

### `Loader(project_root=None)`

Loads `.env` from the project root. Needs `DB_HOST`, `DB_USER`, `DB_PASSWORD` (`DB_PORT` defaults to 5432). The database name comes from `metadata["target"]["database"]`.

| Method | Behaviour |
|---|---|
| `_target(metadata)` | Returns `(database, schema, table)` or raises `LoaderError`. |
| `_pg_type(dtype)` | bool → `boolean`, integer → `bigint`, float → `double precision`, datetime → `timestamp` / `timestamptz`, everything else → `text`. |
| `_text(v)` | Text form of a cell: dict/list → JSON text, NULL → `None`, everything else `str(v)`. |
| `_prepare(chunk, geom, srid)` | Converts geometry to lowercase hex EWKB and all text-typed columns with `_text`. The prepared chunk is what is both hashed and sent. |
| `run(metadata, dataframe)` | Loads the table and returns the row count. |

### `run()` steps

1. **Source check.** If `metadata["integrity"]["source_sha256"]` exists, the source file is hashed again and must match, otherwise `LoaderError` and nothing is loaded. Without it, a warning is logged and this check is skipped.
2. **Single transaction:**
   1. `DROP TABLE IF EXISTS ... CASCADE`, then `CREATE TABLE`. Each load is a full replace, and `CASCADE` also drops dependent views.
   2. For each chunk of 50,000 rows: `_prepare`, add the chunk to the sent digest, `COPY ... (FORMAT csv, NULL '\x01')`.
   3. Read the table back with a server-side cursor (geometry through `encode(ST_AsEWKB(col), 'hex')`) and compute the stored digest.
   4. Stored digest ≠ sent digest → `LoaderError`, rollback. The rollback also restores the previous table.
3. **After commit:** move the data file, shapefile siblings and YAML to `achieved/`. If the move fails, the table is already committed and `LoaderError` says so.

Nothing is added to the table: no load timestamp, row id, lineage column or deduplication.

## `integrity.py`

| Function | Purpose |
|---|---|
| `source_files(path)` | The data file, or for `.shp` every file with the same stem (YAML excluded). Also used to decide which files move to `achieved/` and `rejected/`. |
| `source_sha256(path)` | SHA-256 over the names and bytes of `source_files(path)`, read in 1 MB blocks. |
| `rows_sum(rows, start=0)` | Row digest: each row becomes a JSON list of typed cell strings, is hashed with SHA-256, and the hashes are summed mod 2²⁵⁶ (plus `start`). Independent of row order. Duplicate rows are counted. |

Cell canonical form (`_cell`): NULL, `b0/b1`, `i<int>`, `f<float.hex()>`, `t<UTC ISO timestamp>`, `s<text>`. Both the sent rows and the rows read back from Postgres go through the same function, so a changed value, type, NULL, timestamp or geometry produces a different digest.

## What the checks prove and do not prove

| Question | Answer |
|---|---|
| Was the file changed during extraction or between prevalidation and load? | Detected by the source hashes. A change during the load itself is not detected. |
| Did pandas read the CSV differently from the file? | Detected by `verify_csv`. Not available for Excel, JSON, Parquet or geospatial files. |
| Does the table hold exactly the rows that were sent? | Verified by the digest before commit. |
| Is the DataFrame a correct reading of an Excel, JSON, Parquet or geospatial file? | Not independently verified. It rests on the read options in the fidelity table. |

## Known limits

- Only the first Excel sheet and the first layer of a multi-layer `.gpkg` are loaded.
- Postgres truncates column names longer than 63 bytes without raising an error.
- A cell whose value contains `\x01` would be read as NULL. The digest check then fails the load instead of storing it wrongly.
- Integer fields containing nulls in shapefile/GeoPackage attributes may be returned as float by pyogrio. This has not been verified.
- `read_parquet(dtype_backend=...)` needs pandas 2.0 or newer.
- Hashing runs per row in Python, so very large files are slower to load than before.
