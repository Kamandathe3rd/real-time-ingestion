-- =============================================================
-- verification_queries.sql
-- Checks and performance measurements for the events table.
--
-- Run as the lab user (from the project root):
--   psql -h localhost -U spark_user -d ecommerce_events -f sql/verification_queries.sql
-- =============================================================

-- 1) Row count: every event_id should appear exactly once
SELECT COUNT(*)                 AS total_rows,
       COUNT(DISTINCT event_id) AS unique_event_ids
FROM events;

-- 2) Rows per event type (only view / add_to_cart / purchase expected)
SELECT event_type, COUNT(*) AS rows
FROM events
GROUP BY event_type
ORDER BY event_type;

-- 3) Data quality: all of these should be 0
SELECT COUNT(*) FILTER (WHERE price < 0)         AS negative_prices,
       COUNT(*) FILTER (WHERE quantity <= 0)     AS bad_quantities,
       COUNT(*) FILTER (WHERE user_id IS NULL)   AS missing_users,
       COUNT(*) FILTER (WHERE stored_at < event_time) AS stored_before_event
FROM events;

-- 4) End-to-end latency (event created -> row stored), in milliseconds
SELECT COUNT(*) AS rows,
       ROUND(AVG(EXTRACT(EPOCH FROM (stored_at - event_time)) * 1000))  AS avg_ms,
       ROUND(MIN(EXTRACT(EPOCH FROM (stored_at - event_time)) * 1000))  AS min_ms,
       ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP
             (ORDER BY EXTRACT(EPOCH FROM (stored_at - event_time)) * 1000))::numeric) AS p95_ms,
       ROUND(MAX(EXTRACT(EPOCH FROM (stored_at - event_time)) * 1000))  AS max_ms
FROM events;

-- 5) Where the time goes: waiting for Spark vs writing to PostgreSQL (averages, ms)
SELECT ROUND(AVG(EXTRACT(EPOCH FROM (ingested_at - event_time)) * 1000)) AS wait_for_spark_ms,
       ROUND(AVG(EXTRACT(EPOCH FROM (stored_at - ingested_at)) * 1000))  AS spark_to_db_ms,
       ROUND(AVG(EXTRACT(EPOCH FROM (stored_at - event_time)) * 1000))   AS total_ms
FROM events;

-- 6) Per micro-batch (each batch has one ingested_at): size and latency
SELECT ingested_at                                                       AS batch_time,
       COUNT(*)                                                          AS rows,
       ROUND(AVG(EXTRACT(EPOCH FROM (stored_at - event_time)) * 1000))  AS avg_latency_ms
FROM events
GROUP BY ingested_at
ORDER BY ingested_at;

-- 7) Throughput: rows stored per minute
SELECT DATE_TRUNC('minute', stored_at) AS minute,
       COUNT(*)                        AS rows_stored
FROM events
GROUP BY 1
ORDER BY 1;