# Data Profiling

Profiles every dataset loaded into the `raw_data` schema and persists the results in the `data_profiling` schema.

| File | Role |
|---|---|
| `data_profiling.py` | Orchestration: selects datasets, calls the SQL functions, runs inference and statistics, writes results, records run status |
| `profiling_functions.sql` | PL/pgSQL functions that do the table scans inside PostgreSQL. Read-only on the profiled table. `data_profiling.py` loads it from its own folder (`Path(__file__).with_name(...)`), so both files must sit side by side |
| `profiling_schemas.sql` (in the schemas folder, executed by `database_bootstrap.py`) | DDL for the result tables |

**Run**

```
python -m i_p_f_w.data_profiling.data_profiling [dataset_name]
```

```python
run_profiling(dataset=None, correlations=True)
# -> {"completed": [{"dataset", "run_id", "rows"}], "failed": [{"dataset", "run_id", "error"}], "missing_tables": [names]}
```

**Prerequisites**: `database_bootstrap` has been run (schemas and tables exist, PostGIS enabled); the dataset is registered in `metadata_manager` with a target in the current database and schema `raw_data`; the table has been loaded. Python dependencies: `psycopg2`, `pandas`, `scipy`.

---

## 1. Execution flow (`run_profiling`)

1. Connect using `MetadataRegistry().db` (`.env` settings).
2. Execute `profiling_functions.sql` (`CREATE OR REPLACE`, so the functions are refreshed on every run).
3. Select targets (`TARGETS_SQL`): datasets joined to `metadata_manager.target` where `database = current_database()` and `schema = 'raw_data'`, optionally filtered by dataset name. A flag reports whether the table actually exists. Tables that do not exist are skipped and listed under `missing_tables`.
4. For each dataset, in its own transaction scope:
   - Any earlier run of that dataset still in `running` is marked `failed` ("Interrupted before completion").
   - A new `profile_run` row is inserted and **committed first**, so a failed run leaves a trace.
   - `_profile_table` writes all results and the `completed` status/`row_count` in **one transaction**: all or nothing.
   - On any exception the profiling transaction rolls back and the run is updated to `failed` with `error_message` (`ExceptionType: message`, max 2000 chars). One dataset failing does not affect the others.

Every run creates a new `profile_run`; previous runs are kept (history). Deleting a run cascades to all its child rows.

---

## 2. Configuration (constants in `data_profiling.py`)

| Constant | Default | Meaning |
|---|---|---|
| `SENTINELS` | numeric `[-9999, -999, -99]`; text `unknown, unspecified, not available, none, -, --, ?` | Placeholder values treated as missing in numeric statistics and counted as `sentinel_count`. Text matching is case-insensitive on the trimmed value |
| `INFER_THRESHOLD` | `0.95` | Share of non-blank text values that must match the numeric / datetime pattern to infer that type |
| `TOP_N` | `20` | Category values kept per column; the rest are aggregated into one `other` bucket |
| `CATEGORY_MAX_DISTINCT` | `50` | Max distinct values for a column to get a distribution |
| `CATEGORY_MAX_RATIO` | `0.5` | Max `distinct / non-null` for a distribution (keeps names, free text and identifiers out) |
| `MISSING_WARNING_PCT` / `MISSING_ERROR_PCT` | `20.0` / `50.0` | Missingness thresholds for issue severity |
| `DUPLICATE_ROW_LIMIT` | `1_000_000` | Whole-row duplicate check is skipped above this row count |
| `CORR_MAX_COLUMNS` | `40` | Max columns correlated (first 40 candidates) |
| `CORR_MIN_SAMPLE` | `10` | Min pairwise-complete observations per correlation |
| `CORR_SAMPLE_ROWS` | `50_000` | Target sample size for Spearman (Pearson always uses the full table) |
| `ID_LIKE` | regex | Column names excluded from correlation: `id, fid, uid, objectid, code, pcode, index, year, lat, lon, lng, latitude, longitude` (as whole name tokens), and anything containing `pcode` |

---

## 3. What `_profile_table` computes

### 3.1 Column scan and type inference

`profile_columns` (one scan, all columns) returns the row count and per-column counts. `data_profiling.py` then derives:

- `nulls = total - non_null`
- For **text** columns: `content = non_null - empty - whitespace_only`, then
  - `numeric_matches / content >= 0.95` → inferred `numeric`
  - else `datetime_matches / content >= 0.95` → inferred `datetime`
  - else `text`
- Non-text columns keep their native kind: `numeric → numeric`, `temporal → datetime`, otherwise `boolean`, `geometry`, `other`.

Because the raw layer is loaded largely as text, this inference is what makes numeric profiling possible for text columns.

### 3.2 `column_profile` (one row per column per run)

`total_count`, `non_null_count`, `null_count`, `empty_string_count`, `distinct_count`, `sentinel_count`, `whitespace_only_count`, plus `source_data_type` (PostgreSQL type) and `inferred_type`.

- `empty_string_pct` and `whitespace_only_pct` are percentages of **total rows**, not of non-null rows.
- `empty_string_count`/`whitespace_only_count`/`distinct_count` are only computed for the kinds that support them (see table in section 4); otherwise they are 0 / NULL as noted below.
- `distinct_count` is NULL for geometry and `other` columns. `sentinel_count` is NULL when no sentinel rule applies to the column.
- `empty_string_count` is stored as 0 for non-text columns; `empty_string_pct`/`whitespace_only_*` are NULL there.
- Distinct and sentinel counts are computed on raw values, so sentinels are still present in `distinct_count`.

### 3.3 `column_metric` (long format: one row per metric)

Exactly one of numeric / text / json value is set per row.

| Metric | Applies to | Definition |
|---|---|---|
| `null_pct` | all | `null_count / total_count × 100` |
| `distinct_ratio` | columns with `distinct_count` | `distinct_count / non_null_count` |
| `min_length`, `max_length` | text kind | Character length over all non-null values (empty strings included) |
| `numeric_value_pct` | text inferred numeric | Pattern matches / non-blank values × 100 |
| `datetime_value_pct` | text inferred datetime | Pattern matches / non-blank values × 100 |
| `min`, `max` (text value) | inferred datetime | Native temporal: min/max cast to text. Text: min/max among values matching the datetime pattern, ordered as ISO text (`COLLATE "C"`) |
| `numeric_count`, `min`, `max`, `mean`, `sum` | inferred numeric | Over valid numeric values; sentinels and non-numeric text excluded |
| `variance`, `std_dev` | inferred numeric | **Sample** statistics (n − 1): `var_samp`, `stddev_samp` |
| `q1`, `median`, `q3` | inferred numeric | `percentile_cont(0.25, 0.5, 0.75)`, linear interpolation |
| `zero_count`, `negative_count` | inferred numeric | Counts of `= 0` and `< 0` among valid values |

Numeric metrics are skipped for a column when it has no valid numeric values (`n = 0`). The `method` column documents exclusions (e.g. "sentinels and non-numeric text excluded").

### 3.4 `column_distribution`

Only for columns with inferred type `text` or `boolean` that satisfy `distinct <= 50` **and** `distinct <= 0.5 × non_null`.

- One row per top-N value (`distribution_type = 'category'`) plus at most one `other` row (`is_other_bucket = true`, `category_value` NULL).
- `percentage` = frequency / the column's **non-null** count × 100.
- Numeric and date binning (`numeric_bin`, `date_bin`) are allowed by the table's CHECK constraint but are **not produced** by this code.

### 3.5 `spatial_profile` (one row per geometry column)

From `profile_geometry`: geometry type (single type, or `MIXED(type1, type2, ...)`), max coordinate dimension, SRID, CRS authority and code, total / missing / empty / valid / invalid counts, bounding extent and `extent_srid` (set only when an extent exists).

`spatial_completeness_pct = (non_null − empty) / total × 100`.

Validity is evaluated only on non-null, non-empty geometries; NULL and empty geometries are counted in neither `valid` nor `invalid`.

### 3.6 `column_correlation`

Run only if `correlations=True` and the table has rows.

**Candidate columns**: inferred `numeric`, more than 1 distinct value, at least 10 non-null values, name not matching `ID_LIKE`; capped at the first 40 (logged as a warning when truncated). Fewer than 2 candidates → nothing is written.

| Method | Where computed | Data | Notes |
|---|---|---|---|
| `pearson` | SQL (`pearson_pairs`: `corr`, `regr_count`) | Full table | Pairwise-complete; sentinels and non-numeric text → NULL first |
| `spearman` | Python (`scipy.stats.spearmanr`) | Deterministic sample (`TABLESAMPLE BERNOULLI … REPEATABLE (42)`, target ≈ 50,000 rows) | Pairwise-complete; p-value from SciPy |

- A pair is stored only if `n >= 10` and the coefficient is not NaN. Pearson `r` is clamped to [−1, 1].
- Pearson p-value: two-sided t-test, `t = r·√((n−2)/(1−r²))`, df = n − 2, via `scipy.stats.t.sf`. `NULL` if `n <= 2`; `0.0` if `|r| >= 1`. This assumes approximately bivariate-normal data.
- p-values are **raw**: no multiple-comparison correction across the (up to 780) pairs tested. Treat them as screening values, not confirmatory evidence.
- `BERNOULLI` sampling is row-wise random, so the sample size is approximate, not exactly 50,000. The fixed seed makes it repeatable for an unchanged table.
- Column ids are stored in sorted order (`column_a_id < column_b_id`), so a pair can never appear twice for one method.

### 3.7 `profile_issue`

| `issue_type` | Severity | Condition | `affected_rows` |
|---|---|---|---|
| `empty_table` | critical | Table has 0 rows (only issue produced, then stops) | 0 |
| `excessive_missingness` | warning / error / critical | `(null + empty + whitespace) / total ≥ 20%` → warning; `≥ 50%` → error; all rows → critical | Missing rows |
| `missing_values` | info | Some missing, below 20% | Missing rows |
| `sentinel_values` | warning | `sentinel_count > 0` | Sentinel rows |
| `constant_column` | info | 1 distinct value and more than 1 non-null row | Non-null rows |
| `type_mismatch` | info | Text column inferred as numeric or datetime | Non-blank values |
| `mixed_type_values` | warning | In such a column, values not matching the pattern; up to 5 distinct samples (truncated to 40 chars) in `sample_values` | Non-matching rows |
| `duplicate_rows` | warning | Rows identical to an earlier row in every column (table ≤ 1,000,000 rows) | Extra copies |
| `missing_geometry` | warning | NULL geometries | Count |
| `empty_geometry` | warning | Empty geometries | Count |
| `invalid_geometry` | error | `ST_IsValid` fails; up to 5 distinct reasons in `sample_values` | Count |
| `missing_crs` | warning | SRID 0 | NULL |
| `coordinates_out_of_range` | error | SRID 4326 but extent outside lon ±180 / lat ±90 | NULL |

Missingness checks are skipped for geometry columns (covered by the spatial checks). Issues start with `status = 'open'`.

---

## 4. SQL functions (`profiling_functions.sql`)

All functions are read-only (`STABLE` or `IMMUTABLE`). Identifiers are built with `format(%I)` and values with `%L`, so table and column names are safely quoted. Sentinel rules are passed as `jsonb`: `{"numeric": [...], "text": [...]}`. Several queries use `OFFSET 0` inside a subquery as an optimization fence so the cleaned expression is computed once per row instead of being inlined into every aggregate.

### `value_pattern(kind text) → text`
Single source of truth for the regexes used both to infer types and to safely cast text.
- `numeric`: optional sign, digits with optional decimal part (or leading-dot decimals), optional exponent of 1–2 digits.
- `datetime`: `YYYY-MM-DD`, optionally followed by `HH:MM`, `:SS`, and fractional seconds (space or `T` separator). Only ISO-style dates are recognized.

### `table_columns(schema, table) → (ordinal, col_name, data_type, kind)`
Reads `pg_attribute` and maps each column to a coarse kind:

| PostgreSQL types | kind |
|---|---|
| `int2, int4, int8, numeric, float4, float8` | `numeric` |
| `text, varchar, bpchar` | `text` |
| `date, timestamp, timestamptz` | `temporal` |
| `bool` | `boolean` |
| `geometry` | `geometry` |
| anything else | `other` |

### `numeric_expr(ident, kind, sentinels) → text`
Builds a SQL expression returning the column as `double precision`:
- native numeric: cast directly;
- text: `btrim(x)` cast only when it matches the numeric pattern, otherwise NULL;
- any value in the numeric sentinel list → NULL.

### `profile_columns(schema, table, sentinels) → jsonb`
**Pass 1**, a single table scan for all columns. Returns `{row_count, columns: [...]}`; each column has `name, data_type, kind` plus:

| Key | Meaning | Kinds |
|---|---|---|
| `nn` | Non-null count | all |
| `dist` | Distinct count | numeric, text, temporal, boolean |
| `emp` | Empty-string count | text |
| `ws` | Whitespace-only count (non-empty) | text |
| `nm` / `dm` | Values matching numeric / datetime pattern (trimmed) | text |
| `lmin` / `lmax` | Min / max text length | text |
| `mn` / `mx` | Min / max: as text for temporal; among datetime-pattern matches for text | temporal, text |
| `sent` | Sentinel count: native numeric in numeric list; text in text list (trimmed, case-insensitive) **or** numeric-looking and in numeric list | numeric, text (only when a rule applies) |

Raises an exception if the table does not exist.

### `profile_numeric(schema, table, cols, sentinels) → jsonb`
**Pass 2**: one scan computing, per requested numeric or text column, `{n, min, max, mean, sum, var, std, zeros, neg, q: [q1, median, q3]}` on the cleaned values from `numeric_expr`. Returns `{}` if no requested column qualifies.

### `profile_categories(schema, table, cols, top_n) → table`
Returns `(column_name, category_value, frequency, percentage, is_other)` for several columns in one scan by unpivoting through a lateral `VALUES` list. Ranks values per column by frequency (ties broken by value), keeps the top `top_n`, and aggregates the rest into one row with `is_other = true`. NULLs are excluded; percentages use the column's non-null total.

### `pearson_pairs(schema, table, cols, sentinels) → table`
Returns `(col_a, col_b, pearson_r, n_pairs)` for all column pairs from one scan, using `corr()` and `regr_count()` on the cleaned values. Pairwise-complete by construction. Returns nothing for fewer than 2 usable columns. Note that it builds two aggregates per pair in one query (e.g. 40 columns → 780 pairs → 1,560 aggregates).

### `numeric_sample(schema, table, cols, sentinels, sample_pct) → jsonb`
Returns `{names: [...], values: [[col1 values], ...]}` with cleaned numeric values (NULL = missing / sentinel), used for rank statistics in Python. If `sample_pct < 100`, applies `TABLESAMPLE BERNOULLI (pct) REPEATABLE (42)`. The sampled columns are aggregated into memory via `jsonb_agg`, which is why the sample is capped.

### `duplicate_rows(schema, table) → bigint`
`count(*) − count(DISTINCT row::text)`: the number of extra copies beyond the first. Expensive on large tables, hence gated by `DUPLICATE_ROW_LIMIT` in Python. Comparison is on the row's text representation.

### `profile_geometry(schema, table, col) → jsonb`
PostGIS profile of one geometry column in one scan; `ST_IsValidDetail` is computed once per row, and only for non-null, non-empty geometries. Returns `total, non_null, empty, valid, invalid`, the first 5 distinct invalid `reasons`, distinct `types` (`GeometryType`), max `coord_dim`, extent (`xmin, ymin, xmax, ymax` via `ST_Extent`), plus `srid` (`Find_SRID`) and the `auth_name` / `auth_srid` looked up in `spatial_ref_sys`.

---

## 5. Output tables (`data_profiling` schema)

```
profile_run (run_id, dataset_id, status, started_at, completed_at, row_count, error_message)
 ├── column_profile      (run_id, column_name, counts, inferred_type)         UNIQUE (run_id, column_name)
 │    ├── column_metric        (metric_name, one of numeric / text / json value, unit, method)
 │    ├── column_distribution  (category_value, frequency, percentage, is_other_bucket)
 │    ├── spatial_profile      (one per geometry column)
 │    └── column_correlation   (column_a_id, column_b_id, method, coefficient, sample_size, p_value)
 └── profile_issue       (issue_type, severity, affected_rows, sample_values, status)
```

Child tables reference `(run_id, column_profile_id)`; everything cascades on delete of the run. `column_profile` CHECK constraints enforce `total = non_null + null`, `empty ≤ non_null`, `distinct ≤ non_null`, `sentinel ≤ non_null` and percentage bounds, so inconsistent counts fail the run rather than being stored.

Example: latest completed run's open issues for a dataset.

```sql
SELECT cp.column_name, i.issue_type, i.severity, i.affected_rows, i.issue_description
FROM data_profiling.profile_issue i
LEFT JOIN data_profiling.column_profile cp USING (run_id, column_profile_id)
WHERE i.run_id = (SELECT run_id FROM data_profiling.profile_run r
                  JOIN metadata_manager.dataset d ON d.id = r.dataset_id
                  WHERE d.name = 'my_dataset' AND r.status = 'completed'
                  ORDER BY r.started_at DESC LIMIT 1)
  AND i.status = 'open'
ORDER BY i.severity, i.affected_rows DESC;
```

---

## 6. Limitations and behaviors to know

- Type inference is based on regexes: only ISO-format dates and plain decimal/exponent numbers are recognized. Dates such as `DD/MM/YYYY`, thousands separators and decimal commas are treated as text.
- Missingness counts only NULL, empty and whitespace-only values. Sentinels are reported separately (`sentinel_values`) and are **not** included in the missingness percentage.
- Numeric sentinels (`-99`, `-999`, `-9999`) are excluded from numeric statistics and correlations but remain in `distinct_count` and distributions.
- Text sentinels such as `none` or `-` are only counted and flagged; they do not alter text metrics.
- Spearman uses a sample for tables above ~50,000 rows; Pearson, counts and numeric statistics always use the full table.
- Correlation p-values are uncorrected for multiple testing (see 3.6).
- Distributions are produced only for low-cardinality text/boolean columns; there are no histograms for numeric or date columns.
- Profiling is re-run from scratch each time; results are not incremental.
