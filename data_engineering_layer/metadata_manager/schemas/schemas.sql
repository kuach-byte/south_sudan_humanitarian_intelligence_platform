-- ============================================================
-- Schema: metadata_manager
-- Database: humanitarian_db (assumed already exists)
-- Purpose: Pipeline/dataset metadata tables
-- ============================================================

CREATE SCHEMA IF NOT EXISTS metadata_manager;

-- ------------------------------------------------------------
-- dataset
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_manager.dataset (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    owner       TEXT,
    priority    INTEGER
);

-- ------------------------------------------------------------
-- source
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_manager.source (
    id         SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL
        REFERENCES metadata_manager.dataset(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    type       TEXT,
    path       TEXT,
    file_type  TEXT
);

-- ------------------------------------------------------------
-- target
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_manager.target (
    id         SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL
        REFERENCES metadata_manager.dataset(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    database   TEXT,
    schema     TEXT,
    "table"    TEXT
);

-- ------------------------------------------------------------
-- pipeline_config
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_manager.pipeline_config (
    pipeline_id   SERIAL PRIMARY KEY,
    dataset_id    INTEGER NOT NULL
        REFERENCES metadata_manager.dataset(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    asset_name   TEXT,
    priority      INTEGER,
    loader_class  TEXT,
    chunk_size    INTEGER,
    load_mode     TEXT,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- ------------------------------------------------------------
-- Helpful indexes on FKs (Postgres does not auto-index FK columns)
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_source_dataset_id ON metadata_manager.source(dataset_id);
CREATE INDEX IF NOT EXISTS idx_target_dataset_id ON metadata_manager.target(dataset_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_config_dataset_id ON metadata_manager.pipeline_config(dataset_id);