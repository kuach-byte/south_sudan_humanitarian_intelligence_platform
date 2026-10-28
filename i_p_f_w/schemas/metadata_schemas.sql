CREATE SCHEMA IF NOT EXISTS metadata_manager;

-- ------------------------------------------------------------
-- dataset
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_manager.dataset (
    id            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    registered_at TIMESTAMPTZ
);

-- ------------------------------------------------------------
-- source (one row per dataset)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_manager.source (
    id         INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id INTEGER NOT NULL UNIQUE
        REFERENCES metadata_manager.dataset(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    source      TEXT,
    source_id   TEXT,
    contributor TEXT,
    type       TEXT,
    path       TEXT,
    file_type  TEXT,
    time_period_begin TIMESTAMPTZ,
    time_period_end TIMESTAMPTZ
);

-- ------------------------------------------------------------
-- target (one row per dataset)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_manager.target (
    id         INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id INTEGER NOT NULL UNIQUE
        REFERENCES metadata_manager.dataset(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    database   TEXT,
    schema     TEXT,
    "table"    TEXT
);

-- The UNIQUE constraints on dataset_id create their own indexes,
-- so separate foreign-key indexes are not needed.