# metadata_manager

Registers dataset metadata in PostgreSQL. Two modules in `i_p_f_w/metadata_manager/`:

| Module | Role |
|---|---|
| `yaml_metadata_parser.py` | Reads and validates the YAML sidecar next to each data file in `data/landing`. Never touches the database. |
| `metadata_registry.py` | Writes metadata dicts to the `metadata_manager` schema. |

DDL for the tables lives in `i_p_f_w/schemas/metadata_schemas.sql`, executed by `database_bootstrap.py`.

## Data flow

```
data/landing/<name>.<ext> + <name>.yml
        │
        ▼
YamlMetadataParser.collect()
   ├─ parse_sidecar()              → validated descriptive source fields
   └─ Prevalidationjob.build_metadata(data_file)  → dataset, source (type/path/file_type), target
        │   (m["source"].update(fields))
        ▼
(metadata, errors)
        │
        ▼
MetadataRegistry.run(metadata)     → one transaction per dataset
        ▼
metadata_manager.dataset / source / target
```

## Metadata dict

The single structure passed from parser to registry (built by `Prevalidationjob.build_metadata()`, then enriched with the YAML fields):

```python
{
  "dataset": {"name": str, "registered_at": datetime},   # name = data file stem
  "source":  {"source", "source_id", "contributor", "type", "path",
              "file_type", "time_period_begin", "time_period_end"},
  "target":  {"database", "schema", "table"},
}
```

`source` and `target` are optional; they are only written if present and non-empty.

## Target tables (`metadata_manager`)

| Table | Key columns | Cardinality |
|---|---|---|
| `dataset` | `id`, `name` (UNIQUE), `registered_at` | one row per dataset |
| `source` | `dataset_id` (UNIQUE, FK), `source`, `source_id`, `contributor`, `type`, `path`, `file_type`, `time_period_begin`, `time_period_end` | one per dataset |
| `target` | `dataset_id` (UNIQUE, FK), `database`, `schema`, `"table"` | one per dataset |

Both FKs are `ON UPDATE CASCADE ON DELETE CASCADE`.

---

## `yaml_metadata_parser.py`

### Sidecar format

A sidecar shares the data file's folder and stem (`jiaf_south_sudan_2025.csv` + `jiaf_south_sudan_2025.yml`; `.yml` or `.yaml`). All five fields are required; any other key is rejected.

```yaml
source: "Humanitarian partners"
source_id: "bfdf0ecb-0ee1-4813-bfa2-0cf3b5b8f9c5"
contributor: "OCHA South Sudan"
time_period_begin: 01/01/2025     # DD/MM/YYYY
time_period_end: 31/12/2025
```

`type`, `path`, `file_type` and the target are not in the YAML; they come from `build_metadata()`.

### Validation rules

| Field | Rule |
|---|---|
| `source`, `source_id`, `contributor` | Non-empty string (stripped). Unquoted numbers fail, so quote them. |
| `time_period_begin`, `time_period_end` | `DD/MM/YYYY` string, or a native YAML date/datetime (so unquoted `2025-01-01` also works; a *quoted* ISO string does not). Naive values are set to UTC; timezone-aware datetimes are kept as is. |
| Cross-field | `time_period_begin` must not be later than `time_period_end`. |

### Functions

| Function | Behaviour |
|---|---|
| `parse_sidecar(yaml_path)` | Returns the validated fields dict. Collects **all** problems (unknown fields, missing fields, bad values, date order) into one `YamlMetadataError` prefixed with the file path. Also raises on unreadable file, bad encoding (`utf-8-sig` is accepted) or malformed YAML, or a non-mapping top level. |
| `sidecars_for(data_path)` | All YAML files in the same folder with the same stem, sorted. |
| `sidecar_for(data_path)` | The single sidecar or `None`; raises `YamlMetadataError` if more than one exists. |

### `YamlMetadataParser(project_root=None, job=None)`

- `project_root` defaults to `Path(__file__).resolve().parents[2]` (the directory containing the `i_p_f_w` package).
- `job` is lazily created as `Prevalidationjob(root)` from `i_p_f_w.ingestion.prevalidation_job`. It must provide `.landing`, `.discover_datasets()` and `.build_metadata(path)`.

**`collect()` → `(metadata, errors)`**

Walks every YAML under `job.landing` (ordered like the data files, i.e. chronological within a folder) and applies, in order:

| Condition | Result |
|---|---|
| No data file with the same folder and stem | warning, skipped |
| More than one YAML for that folder and stem | error |
| Several data files share that folder and stem | error |
| Same stem used by a data file in another folder | error (dataset names must be unique) |
| `parse_sidecar` fails | error |
| otherwise | `job.build_metadata(data_file)` with `m["source"].update(fields)` appended to `metadata` |

Ambiguity rejects every file involved; nothing is resolved by file timestamps. Data files without a YAML are not returned by `collect()`.

**`enrich(metadata)` → `errors`**

Merges each dataset's sidecar into an already built metadata dict in place (located via `root / m["source"]["path"]`). A missing sidecar logs a warning and leaves the dict unchanged; an invalid or ambiguous one adds an error message. Not called by `collect()` or `MetadataRegistry`.

### CLI

```
python -m i_p_f_w.metadata_manager.yaml_metadata_parser
```

Parse and validate only; nothing is registered. Exit code 1 if any file was rejected.

---

## `metadata_registry.py`

### Configuration

`MetadataRegistry()` loads `.env` (`find_dotenv()`) and builds `self.db`:

| Env var | Default |
|---|---|
| `DB_HOST` | `localhost` |
| `DB_PORT` | `5432` |
| `DB_NAME`, `DB_USER`, `DB_PASSWORD` | none |

The schema is the module constant `SCHEMA = "metadata_manager"`; `DB_SCHEMA` is not read. `database_bootstrap.py` also uses `MetadataRegistry().db` for its connection parameters. Importing the module calls `logging.basicConfig(level=INFO)`.

### Methods

All `register_*` methods take an open cursor and do not commit; the caller owns the transaction.

| Method | SQL behaviour |
|---|---|
| `register_dataset(cur, ds)` | `INSERT ... ON CONFLICT (name) DO UPDATE SET registered_at`; returns the dataset `id`. Requires `ds["name"]` and `ds["registered_at"]`. |
| `register_source(cur, dataset_id, source)` | Upsert on `dataset_id`. Writes only the columns of `SOURCE_COLUMNS` present in `source`, so the pipeline fields (`type`, `path`, `file_type`) and the YAML fields can be registered separately without overwriting each other. No-op if none are present. |
| `register_target(cur, dataset_id, target)` | `DELETE` the existing row, then `INSERT` from `database`, `schema`, `table` (missing keys become NULL). The target is replaced, not merged. |
| `run(metadata)` | Connects, then registers each dataset in its **own** transaction (`with self.conn, self.conn.cursor()`). A `psycopg2.Error` rolls back that dataset, is logged and counted; the rest continue. `source`/`target` are written only if truthy. Returns `0` on full success, `1` on connection failure or any failed dataset. The connection is always closed. |
| `register_from_yaml(project_root=None, parser=None)` | `parser.collect()` (default `YamlMetadataParser(project_root)`), logs each error, runs `run()` on the valid metadata. Returns `1` if there were parse errors or failed datasets, else `0`. Valid datasets are registered even when others are rejected. |

### CLI

```
python -m i_p_f_w.metadata_manager.metadata_registry
```

Equivalent to `sys.exit(MetadataRegistry().register_from_yaml())`.

---

## Behaviours worth knowing

- Re-running is idempotent: dataset and source are upserts, target is replaced.
- Only `psycopg2.Error` is caught per dataset in `run()`; a malformed metadata dict (e.g. missing `dataset["name"]`) raises `KeyError` and aborts the run.
- A data file without a sidecar is never registered through `register_from_yaml()`.
- Dataset identity is the file stem, enforced unique across all of `data/landing`.
