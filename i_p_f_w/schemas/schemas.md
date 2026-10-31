# Schemas

This folder (`i_p_f_w/schemas/`) holds the database DDL and the bootstrap script that applies it.

| File | Purpose |
|---|---|
| `raw_data.sql` | `raw_data` schema + PostGIS extension |
| `metadata_schemas.sql` | `metadata_manager` schema: dataset registry |
| `profiling_schemas.sql` | `data_profiling` schema: profiling results |
| `database_bootstrap.py` | Creates the database and schemas, executes the three SQL files |

The SQL files are the single source of truth for what lives inside each schema.

---

## `raw_data.sql`

```sql
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE EXTENSION IF NOT EXISTS postgis;
```

Landing zone for ingested datasets. No tables are defined here; they are created at ingestion time. PostGIS is enabled for geometry columns.

---

## `metadata_manager` (`metadata_schemas.sql`)

Registry of datasets, with one `source` row and one `target` row per dataset (both enforced by `UNIQUE (dataset_id)`).

```
dataset 1 ── 1 source
dataset 1 ── 1 target
```

### `dataset`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER, identity | PK |
| `name` | TEXT | NOT NULL, UNIQUE |
| `registered_at` | TIMESTAMPTZ | |

### `source`
Where the dataset came from.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER, identity | PK |
| `dataset_id` | INTEGER | NOT NULL, UNIQUE, FK → `dataset(id)` ON UPDATE/DELETE CASCADE |
| `source`, `source_id`, `contributor` | TEXT | Provider, provider-side identifier, contributing organisation |
| `type`, `file_type` | TEXT | |
| `path` | TEXT | Location of the source file |
| `time_period_begin`, `time_period_end` | TIMESTAMPTZ | Temporal coverage of the data |

### `target`
Where the dataset was loaded.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER, identity | PK |
| `dataset_id` | INTEGER | NOT NULL, UNIQUE, FK → `dataset(id)` ON UPDATE/DELETE CASCADE |
| `database`, `schema`, `"table"` | TEXT | `"table"` is quoted because it is a reserved word |

No separate FK indexes are created; the `UNIQUE` constraints on `dataset_id` already provide them.

---

## `data_profiling` (`profiling_schemas.sql`)

Results of profiling runs. Depends on `metadata_manager.dataset`.

```
dataset 1 ── * profile_run 1 ── * column_profile 1 ── * column_metric
                    │                    │        ├── * column_distribution
                    │                    │        ├── 0..1 spatial_profile
                    │                    │        └── * column_correlation (a and b)
                    └── * profile_issue ─┘ (column_profile_id nullable)
```

Child tables reference `column_profile` through the composite key `(run_id, column_profile_id)`, which keeps every child row in the same run as its parent profile. All FKs cascade on delete, so deleting a `profile_run` removes its entire profile.

### `profile_run`
One execution of profiling for a dataset.

| Column | Type | Notes |
|---|---|---|
| `run_id` | BIGINT, identity | PK |
| `dataset_id` | BIGINT | FK → `metadata_manager.dataset(id)` (no cascade) |
| `started_at` | TIMESTAMPTZ | NOT NULL, default `NOW()` |
| `completed_at` | TIMESTAMPTZ | NULL or ≥ `started_at` |
| `status` | TEXT | `running` (default), `completed`, `failed` |
| `row_count` | BIGINT | ≥ 0 |
| `error_message` | TEXT | |

### `column_profile`
Per-column completeness counts for a run.

| Column | Type | Notes |
|---|---|---|
| `column_profile_id` | BIGINT, identity | PK |
| `run_id` | BIGINT | NOT NULL, FK → `profile_run` ON DELETE CASCADE |
| `table_name`, `column_name` | TEXT | NOT NULL |
| `source_data_type`, `inferred_type` | TEXT | Declared vs. inferred type |
| `total_count`, `non_null_count`, `null_count`, `empty_string_count` | BIGINT | NOT NULL, default 0 |
| `distinct_count`, `sentinel_count` | BIGINT | Nullable |
| `empty_string_pct`, `whitespace_only_pct` | DOUBLE PRECISION | 0–100 |
| `whitespace_only_count` | BIGINT | Nullable |
| `created_at` | TIMESTAMPTZ | Default `NOW()` |

Constraints:
- `UNIQUE (run_id, column_name)`, `UNIQUE (run_id, column_profile_id)` (the latter is the target of the composite FKs)
- `total_count = non_null_count + null_count`
- `empty_string_count`, `whitespace_only_count`, `sentinel_count`, `distinct_count` ≤ `non_null_count`
- All counts ≥ 0

### `column_metric`
Free-form metrics per column (e.g. min, max, mean, quantiles).

| Column | Type | Notes |
|---|---|---|
| `metric_id` | BIGINT, identity | PK |
| `run_id`, `column_profile_id` | BIGINT | Composite FK → `column_profile` |
| `metric_name` | TEXT | NOT NULL |
| `metric_value_numeric` / `_text` / `_json` | DOUBLE PRECISION / TEXT / JSONB | Exactly one must be non-null (`num_nonnulls(...) = 1`) |
| `unit`, `method` | TEXT | |
| `created_at` | TIMESTAMPTZ | |

### `column_distribution`
Value frequencies or histogram bins per column.

| Column | Type | Notes |
|---|---|---|
| `distribution_id` | BIGINT, identity | PK |
| `run_id`, `column_profile_id` | BIGINT | Composite FK → `column_profile` |
| `distribution_type` | TEXT | `category`, `numeric_bin`, `date_bin`, `other` |
| `category_value` | TEXT | For `category` rows |
| `bin_lower`, `bin_upper` | DOUBLE PRECISION | `bin_lower ≤ bin_upper` when both set |
| `frequency` | BIGINT | NOT NULL, ≥ 0 |
| `percentage` | DOUBLE PRECISION | 0–100 |
| `is_other_bucket` | BOOLEAN | Default FALSE |

### `column_correlation`
Pairwise correlations between two columns of the same run.

| Column | Type | Notes |
|---|---|---|
| `correlation_id` | BIGINT, identity | PK |
| `run_id` | BIGINT | |
| `column_a_id`, `column_b_id` | BIGINT | Each a composite FK → `column_profile(run_id, column_profile_id)` |
| `method` | TEXT | NOT NULL (e.g. pearson, spearman) |
| `coefficient` | DOUBLE PRECISION | NOT NULL, −1 to 1 |
| `sample_size` | BIGINT | ≥ 0 |
| `p_value` | DOUBLE PRECISION | 0–1 |
| `created_at` | TIMESTAMPTZ | |

Constraints: `column_a_id <> column_b_id`; `UNIQUE (run_id, column_a_id, column_b_id, method)`.

### `spatial_profile`
Geometry-column profile; at most one per `column_profile`.

| Column | Type | Notes |
|---|---|---|
| `spatial_profile_id` | BIGINT, identity | PK |
| `run_id`, `column_profile_id` | BIGINT | Composite FK → `column_profile`; `UNIQUE (run_id, column_profile_id)` |
| `source_schema`, `source_table`, `geometry_column` | TEXT | NOT NULL |
| `geometry_type`, `crs_authority`, `crs_code` | TEXT | |
| `coordinate_dimension`, `srid`, `extent_srid` | INTEGER | |
| `total_count`, `missing_geometry_count`, `empty_geometry_count`, `valid_geometry_count`, `invalid_geometry_count` | BIGINT | |
| `extent_min_x`, `extent_min_y`, `extent_max_x`, `extent_max_y` | DOUBLE PRECISION | All NULL or all set, with min ≤ max |
| `spatial_completeness_pct` | DOUBLE PRECISION | 0–100 |
| `created_at` | TIMESTAMPTZ | |

### `profile_issue`
Data-quality findings, at run level or tied to a column.

| Column | Type | Notes |
|---|---|---|
| `issue_id` | BIGINT, identity | PK |
| `run_id` | BIGINT | NOT NULL, FK → `profile_run` ON DELETE CASCADE |
| `column_profile_id` | BIGINT | Nullable; composite FK → `column_profile` when set |
| `issue_type` | TEXT | NOT NULL |
| `severity` | TEXT | `info`, `warning` (default), `error`, `critical` |
| `affected_rows` | BIGINT | ≥ 0 |
| `issue_description` | TEXT | |
| `sample_values` | JSONB | |
| `status` | TEXT | `open` (default), `reviewed`, `resolved`, `accepted` |
| `created_at` | TIMESTAMPTZ | |

Because `column_profile_id` is nullable and the composite FK uses default `MATCH SIMPLE`, a NULL skips the FK check, which is what allows run-level issues.

### Indexes

| Index | Table | Columns |
|---|---|---|
| `idx_profile_run_dataset` | `profile_run` | `(dataset_id, started_at DESC)` |
| `idx_column_profile_run` | `column_profile` | `(run_id)` |
| `idx_column_metric_lookup` | `column_metric` | `(run_id, metric_name)` |
| `idx_column_metric_profile_fk` | `column_metric` | `(run_id, column_profile_id)` |
| `idx_column_distribution_lookup` | `column_distribution` | `(run_id, column_profile_id)` |
| `idx_correlation_run` | `column_correlation` | `(run_id)` |
| `idx_column_correlation_a_fk` | `column_correlation` | `(run_id, column_a_id)` |
| `idx_column_correlation_b_fk` | `column_correlation` | `(run_id, column_b_id)` |
| `idx_spatial_profile_run` | `spatial_profile` | `(run_id)` |
| `idx_profile_issue_run_status` | `profile_issue` | `(run_id, status, severity)` |
| `idx_profile_issue_profile_fk` | `profile_issue` | `(run_id, column_profile_id)` |

---

## `database_bootstrap.py`

Creates the project database if missing, then ensures all schemas and DDL objects exist.

```
python -m i_p_f_w.schemas.database_bootstrap     # CLI
run_bootstrap() -> dict                          # from code
```

### Behaviour

1. **Config**: reads connection parameters from `MetadataRegistry().db` (populated from `.env`: `DB_NAME`, `DB_USER`, `DB_PASSWORD`, plus host). Raises `BootstrapError` if `dbname`, `user` or `password` is missing.
2. **Database**: connects to the maintenance database `postgres` with autocommit and runs `CREATE DATABASE` if the name is absent from `pg_database`.
3. **Schemas and DDL**: connects to the target database and, in a single transaction (commit on success, rollback on any error):
   - creates `raw_data` if absent (tracked in the summary),
   - executes the DDL files in this order:

   | Order | Schema | File |
   |---|---|---|
   | 1 | `raw_data` | `raw_data.sql` |
   | 2 | `metadata_manager` | `metadata_schemas.sql` |
   | 3 | `data_profiling` | `profiling_schemas.sql` |

   The order matters because `profiling_schemas.sql` references `metadata_manager.dataset`.
4. **Idempotent**: nothing is dropped, truncated or overwritten. The DDL files are re-run every time (all statements use `IF NOT EXISTS`) because an existing schema does not imply its tables exist.

### Return value

```python
{"database_created": bool, "schemas_created": [...], "ddl_executed": [...]}
```

`schemas_created` only ever contains `raw_data`; the other two schemas are created inside their SQL files and are not tracked. `ddl_executed` lists the filenames run.

### Errors

All failures (connection, SQL, unreadable file) are raised as `BootstrapError`. The CLI entry point logs the message and exits with status 1.

### Notes

- `CREATE DATABASE` runs outside the schema transaction, so a DDL failure rolls back the schema work but not a newly created database.
- `CREATE EXTENSION postgis` requires PostGIS to be installed on the server and sufficient privileges for the connecting user.
- `idx_column_profile_run`, `idx_spatial_profile_run`, `idx_column_correlation_a_fk` and `idx_correlation_run` are prefix-covered by existing `UNIQUE` constraints, so they are likely redundant.
- `UNIQUE (run_id, column_name)` on `column_profile` does not include `table_name`, so a single run can only hold one column of a given name across tables.
