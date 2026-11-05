-- =============================================================
-- postgres_setup.sql
-- Creates the lab user, database and events table for the
-- real-time ingestion pipeline.
--
-- Run with psql as the postgres superuser (from the project root):
--   & "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -f sql\postgres_setup.sql
--
-- WARNING: re-running this script DELETES the database and all events.
-- =============================================================

-- 1) Start fresh (safe to re-run)
DROP DATABASE IF EXISTS ecommerce_events WITH (FORCE);
DROP ROLE IF EXISTS spark_user;

-- 2) Lab-only user for Spark (NOT the postgres superuser)
CREATE ROLE spark_user WITH LOGIN PASSWORD 'spark_lab_pass';

-- 3) Database owned by the lab user
CREATE DATABASE ecommerce_events OWNER spark_user;

-- 4) Switch into the new database, acting as the lab user
\connect ecommerce_events
SET ROLE spark_user;

-- 5) Table for cleaned events (rules mirror clean_events() in Spark)
CREATE TABLE events (
    event_id      VARCHAR(36)    PRIMARY KEY,
    event_time    TIMESTAMPTZ    NOT NULL,
    user_id       INTEGER        NOT NULL CHECK (user_id > 0),
    event_type    VARCHAR(20)    NOT NULL
                  CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    product_id    INTEGER        NOT NULL,
    product_name  VARCHAR(100),
    category      VARCHAR(50),
    price         NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    quantity      INTEGER        NOT NULL CHECK (quantity > 0),
    ingested_at   TIMESTAMPTZ    NOT NULL,                            -- when Spark processed the batch
    stored_at     TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP   -- when PostgreSQL saved the row
);

-- 6) Indexes for the queries we run most (by time, by event type)
CREATE INDEX idx_events_event_time ON events (event_time);
CREATE INDEX idx_events_event_type ON events (event_type);

-- 7) Staging table: Spark writes each micro-batch here first, then
--    INSERT ... ON CONFLICT moves only NEW events into events.
--    UNLOGGED = faster, because this data is temporary anyway.
CREATE UNLOGGED TABLE events_staging (LIKE events INCLUDING DEFAULTS);

-- 8) Show the result
\d events
\d events_staging