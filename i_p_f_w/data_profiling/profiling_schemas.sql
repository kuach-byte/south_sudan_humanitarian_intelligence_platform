
-- ============================================================
-- 1. SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS data_profiling;


-- ============================================================
-- 2. PROFILING RUNS
-- One run can profile a single dataset or analyze a
-- relationship between multiple datasets.
-- ============================================================

CREATE TABLE IF NOT EXISTS data_profiling.profile_run (
    run_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id          BIGINT
        REFERENCES metadata_manager.dataset(id),
    run_scope           TEXT NOT NULL DEFAULT 'single_dataset'
        CHECK (run_scope IN ('single_dataset', 'cross_dataset')),
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed')),
    profiler_version    TEXT,
    row_count           BIGINT CHECK (row_count >= 0),
    error_message       TEXT,
    parameters          JSONB NOT NULL DEFAULT '{}'::jsonb,

    CHECK (
        (run_scope = 'single_dataset' AND dataset_id IS NOT NULL)
        OR
        (run_scope = 'cross_dataset' AND dataset_id IS NULL)
    ),
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);


-- ============================================================
-- 3. COLUMN PROFILES
-- One record per column per profiling run.
-- ============================================================

CREATE TABLE IF NOT EXISTS data_profiling.column_profile (
    column_profile_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              BIGINT NOT NULL
        REFERENCES data_profiling.profile_run(run_id)
        ON DELETE CASCADE,

    column_name         TEXT NOT NULL,
    ordinal_position    INTEGER,
    source_data_type    TEXT,
    inferred_type       TEXT,

    total_count         BIGINT NOT NULL DEFAULT 0,
    non_null_count      BIGINT NOT NULL DEFAULT 0,
    null_count          BIGINT NOT NULL DEFAULT 0,
    empty_string_count  BIGINT NOT NULL DEFAULT 0,
    distinct_count      BIGINT,
    sentinel_count      BIGINT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (run_id, column_name),
    UNIQUE (run_id, column_profile_id),

    CHECK (total_count >= 0),
    CHECK (non_null_count >= 0),
    CHECK (null_count >= 0),
    CHECK (empty_string_count >= 0),
    CHECK (distinct_count IS NULL OR distinct_count >= 0),
    CHECK (sentinel_count IS NULL OR sentinel_count >= 0),
    CHECK (total_count = non_null_count + null_count),
    CHECK (non_null_count <= total_count),
    CHECK (null_count <= total_count),
    CHECK (empty_string_count <= non_null_count),
    CHECK (distinct_count IS NULL OR distinct_count <= non_null_count),
    CHECK (sentinel_count IS NULL OR sentinel_count <= non_null_count)
);


-- ============================================================
-- 4. COLUMN METRICS
-- Flexible storage for statistical and quality metrics.
-- ============================================================

CREATE TABLE IF NOT EXISTS data_profiling.column_metric (
    metric_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              BIGINT NOT NULL,
    column_profile_id   BIGINT NOT NULL,

    metric_name         TEXT NOT NULL,
    metric_value_numeric DOUBLE PRECISION,
    metric_value_text   TEXT,
    metric_value_json   JSONB,
    unit                TEXT,
    method              TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (run_id, column_profile_id)
        REFERENCES data_profiling.column_profile
            (run_id, column_profile_id)
        ON DELETE CASCADE,

    CHECK (
        num_nonnull_values(
            metric_value_numeric,
            metric_value_text,
            metric_value_json
        ) = 1
    )
);


-- ============================================================
-- 5. COLUMN DISTRIBUTIONS
-- Supports categorical frequencies and numeric bins.
-- ============================================================

CREATE TABLE IF NOT EXISTS data_profiling.column_distribution (
    distribution_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              BIGINT NOT NULL,
    column_profile_id   BIGINT NOT NULL,

    distribution_type  TEXT NOT NULL
        CHECK (distribution_type IN
            ('category', 'numeric_bin', 'date_bin', 'other')),
    category_value      TEXT,
    bin_lower           DOUBLE PRECISION,
    bin_upper           DOUBLE PRECISION,
    frequency           BIGINT NOT NULL CHECK (frequency >= 0),
    percentage          DOUBLE PRECISION,
    is_other_bucket     BOOLEAN NOT NULL DEFAULT FALSE,

    FOREIGN KEY (run_id, column_profile_id)
        REFERENCES data_profiling.column_profile
            (run_id, column_profile_id)
        ON DELETE CASCADE,

    CHECK (percentage IS NULL OR percentage BETWEEN 0 AND 100),
    CHECK (bin_lower IS NULL OR bin_upper IS NULL OR bin_lower <= bin_upper)
);


-- ============================================================
-- 6. COLUMN CORRELATIONS
-- One row per pair of columns and correlation method.
-- ============================================================

CREATE TABLE IF NOT EXISTS data_profiling.column_correlation (
    correlation_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              BIGINT NOT NULL,

    column_a_id         BIGINT NOT NULL,
    column_b_id         BIGINT NOT NULL,

    method              TEXT NOT NULL,
    coefficient         DOUBLE PRECISION NOT NULL,
    sample_size         BIGINT,
    p_value             DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (run_id, column_a_id)
        REFERENCES data_profiling.column_profile
            (run_id, column_profile_id)
        ON DELETE CASCADE,

    FOREIGN KEY (run_id, column_b_id)
        REFERENCES data_profiling.column_profile
            (run_id, column_profile_id)
        ON DELETE CASCADE,

    CHECK (column_a_id <> column_b_id),
    CHECK (coefficient BETWEEN -1 AND 1),
    CHECK (sample_size IS NULL OR sample_size >= 0),
    CHECK (p_value IS NULL OR p_value BETWEEN 0 AND 1),

    UNIQUE (run_id, column_a_id, column_b_id, method)
);


-- ============================================================
-- 7. SPATIAL PROFILES
-- One record per spatial column per profiling run.
-- ============================================================

CREATE TABLE IF NOT EXISTS data_profiling.spatial_profile (
    spatial_profile_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                  BIGINT NOT NULL,
    column_profile_id       BIGINT NOT NULL,

    source_schema           TEXT NOT NULL,
    source_table             TEXT NOT NULL,
    geometry_column         TEXT NOT NULL,

    geometry_type           TEXT,
    coordinate_dimension    INTEGER,
    srid                    INTEGER,
    crs_authority           TEXT,
    crs_code                TEXT,

    total_count             BIGINT,
    missing_geometry_count  BIGINT,
    empty_geometry_count    BIGINT,
    valid_geometry_count    BIGINT,
    invalid_geometry_count  BIGINT,

    extent_min_x            DOUBLE PRECISION,
    extent_min_y            DOUBLE PRECISION,
    extent_max_x            DOUBLE PRECISION,
    extent_max_y            DOUBLE PRECISION,
    extent_srid             INTEGER,

    spatial_completeness_pct DOUBLE PRECISION,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (run_id, column_profile_id)
        REFERENCES data_profiling.column_profile
            (run_id, column_profile_id)
        ON DELETE CASCADE,

    UNIQUE (run_id, column_profile_id),

    CHECK (
        spatial_completeness_pct IS NULL
        OR spatial_completeness_pct BETWEEN 0 AND 100
    ),
    CHECK (
        (extent_min_x IS NULL AND extent_min_y IS NULL
         AND extent_max_x IS NULL AND extent_max_y IS NULL)
        OR
        (extent_min_x IS NOT NULL AND extent_min_y IS NOT NULL
         AND extent_max_x IS NOT NULL AND extent_max_y IS NOT NULL
         AND extent_min_x <= extent_max_x
         AND extent_min_y <= extent_max_y)
    )
);


-- ============================================================
-- 8. CROSS-DATASET SPATIAL RELATIONSHIPS
-- Stores summaries of spatial operations between two datasets.
-- ============================================================

CREATE TABLE IF NOT EXISTS data_profiling.spatial_relationship (
    relationship_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    analysis_run_id       BIGINT NOT NULL
        REFERENCES data_profiling.profile_run(run_id)
        ON DELETE CASCADE,

    left_dataset_id       BIGINT NOT NULL
        REFERENCES metadata_manager.dataset(id),
    right_dataset_id      BIGINT NOT NULL
        REFERENCES metadata_manager.dataset(id),

    left_source_schema    TEXT NOT NULL,
    left_source_table     TEXT NOT NULL,
    left_geometry_column  TEXT NOT NULL,

    right_source_schema   TEXT NOT NULL,
    right_source_table    TEXT NOT NULL,
    right_geometry_column TEXT NOT NULL,

    relationship_type     TEXT NOT NULL,
    spatial_predicate     TEXT,
    analysis_crs_srid     INTEGER,

    left_feature_count    BIGINT,
    right_feature_count   BIGINT,
    matching_pair_count  BIGINT,
    unmatched_left_count BIGINT,
    unmatched_right_count BIGINT,

    overlap_area          DOUBLE PRECISION,
    overlap_area_unit     TEXT,
    distance_value        DOUBLE PRECISION,
    distance_unit         TEXT,

    parameters            JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_summary        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (left_dataset_id <> right_dataset_id
        OR left_source_schema <> right_source_schema
        OR left_source_table <> right_source_table
        OR left_geometry_column <> right_geometry_column),

    CHECK (left_feature_count IS NULL OR left_feature_count >= 0),
    CHECK (right_feature_count IS NULL OR right_feature_count >= 0),
    CHECK (matching_pair_count IS NULL OR matching_pair_count >= 0),
    CHECK (unmatched_left_count IS NULL OR unmatched_left_count >= 0),
    CHECK (unmatched_right_count IS NULL OR unmatched_right_count >= 0),
    CHECK (overlap_area IS NULL OR overlap_area >= 0),
    CHECK (distance_value IS NULL OR distance_value >= 0)
);


-- ============================================================
-- 9. DATA QUALITY ISSUES
-- Summary findings, not copies of source records.
-- ============================================================

CREATE TABLE IF NOT EXISTS data_profiling.profile_issue (
    issue_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id               BIGINT NOT NULL
        REFERENCES data_profiling.profile_run(run_id)
        ON DELETE CASCADE,

    column_profile_id    BIGINT,
    issue_type           TEXT NOT NULL,
    severity             TEXT NOT NULL DEFAULT 'warning'
        CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    affected_rows        BIGINT,
    issue_description    TEXT,
    sample_values        JSONB,
    status               TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'reviewed', 'resolved', 'accepted')),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (run_id, column_profile_id)
        REFERENCES data_profiling.column_profile
            (run_id, column_profile_id)
        ON DELETE CASCADE,

    CHECK (affected_rows IS NULL OR affected_rows >= 0)
);


-- ============================================================
-- 10. INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_profile_run_dataset
    ON data_profiling.profile_run (dataset_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_column_profile_run
    ON data_profiling.column_profile (run_id);

CREATE INDEX IF NOT EXISTS idx_column_metric_lookup
    ON data_profiling.column_metric (run_id, metric_name);

CREATE INDEX IF NOT EXISTS idx_column_distribution_lookup
    ON data_profiling.column_distribution (run_id, column_profile_id);

CREATE INDEX IF NOT EXISTS idx_correlation_run
    ON data_profiling.column_correlation (run_id);

CREATE INDEX IF NOT EXISTS idx_spatial_profile_run
    ON data_profiling.spatial_profile (run_id);

CREATE INDEX IF NOT EXISTS idx_spatial_relationship_datasets
    ON data_profiling.spatial_relationship
        (left_dataset_id, right_dataset_id, relationship_type);

CREATE INDEX IF NOT EXISTS idx_spatial_relationship_run
    ON data_profiling.spatial_relationship (analysis_run_id);

CREATE INDEX IF NOT EXISTS idx_profile_issue_run_status
    ON data_profiling.profile_issue (run_id, status, severity);
