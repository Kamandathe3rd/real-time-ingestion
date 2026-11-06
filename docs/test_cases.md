# Test Cases: Real-Time Ingestion Pipeline

Manual test plan with expected and actual results.
Status: ✅ pass · ❌ fail · ⬜ not yet recorded

## Test environment

| Item | Value |
|---|---|
| OS | Windows 10/11, 64-bit |
| Python / PySpark | 3.14.8 / 4.3.0 |
| Java | Eclipse Temurin 25.0.5 |
| PostgreSQL | 18.6 |
| Spark trigger | 5 seconds (default) |
| Reset before each run | `Remove-Item -Recurse data, checkpoints` and `TRUNCATE events, events_staging` |

## Summary

| ID | Test | Status |
|---|---|---|
| TC-01 | CSV file has header and correct row count | ✅ |
| TC-02 | Event type mix is about 70 / 20 / 10 | ✅ |
| TC-03 | Bad rows are injected and counted | ✅ |
| TC-04 | Invalid generator settings are rejected | ✅ |
| TC-05 | A failed write never reaches the watched folder | ✅ |
| TC-06 | Spark detects every new file | ✅ |
| TC-07 | Restart does not reprocess files | ✅ |
| TC-08 | `--max-files` limits batch size | ✅ |
| TC-09 | Invalid rows are dropped (exact count) | ✅ |
| TC-10 | Messy event types are fixed, not dropped | ✅ |
| TC-11 | Duplicates are removed | ✅ |
| TC-12 | A bad value does not crash the stream (`try_cast`) | ✅ |
| TC-13 | Spark can write to PostgreSQL | ✅ |
| TC-14 | Database rules reject bad data | ✅ |
| TC-15 | End-to-end reconciliation (generator = Spark = PostgreSQL) | ✅ |
| TC-16 | Replayed data is not stored twice (idempotent write) | ✅ |
| TC-17 | Stored data passes quality checks | ✅ |
| TC-18 | Latency stays within trigger + processing time | ✅ |
| TC-19 | Processing time per batch after warm-up | ✅ |
| TC-20 | Time-zone-safe latency (`TIMESTAMPTZ`) | ✅ |

---

## A. CSV generation

### TC-01: CSV file has header and correct row count
| | |
|---|---|
| **Steps** | `python src\data_generator.py --events 5 --interval 1 --batches 3`, then `(Get-ChildItem data\input).Count` and `(Get-Content data\input\*.csv).Count` |
| **Expected** | 3 files; 18 lines (3 × (1 header + 5 events)); first line is `event_id,event_time,user_id,event_type,product_id,product_name,category,price,quantity` |
| **Actual** | 3 files; 18 lines; first line is `event_id,event_time,user_id,event_type,product_id,product_name,category,price,quantity` |
| **Status** | ✅ |

### TC-02: Event type mix is about 70 / 20 / 10
| | |
|---|---|
| **Steps** | Count `event_type` over 1,000 calls of `make_event()` (`Counter` one-liner from the build notes) |
| **Expected** | About 700 view, 200 add_to_cart, 100 purchase (random, so not exact) |
| **Actual** | view 705, add_to_cart 199, purchase 96 (out of 1,000) |
| **Status** | ✅ |

### TC-03: Bad rows are injected and counted
| | |
|---|---|
| **Steps** | `python src\data_generator.py --events 50 --interval 0 --batches 4 --bad-rate 0.2` |
| **Expected** | All 7 problem types appear in the summary; data rows = 200 + `duplicate` |
| **Actual** | 200 events; 50 bad rows: bad_price 7, bad_timestamp 5, duplicate 10, messy_event_type 9, missing_user_id 11, negative_price 2, unknown_event_type 6. Data rows: 210 (= 200 + 10) |
| **Status** | ✅ |

### TC-04: Invalid generator settings are rejected
| | |
|---|---|
| **Steps** | `--events 0`, `--events abc`, `--bad-rate 2` |
| **Expected** | `error: --events must be at least 1`; `error: argument --events: invalid int value: 'abc'`; `error: --bad-rate must be between 0 and 1`; no files written |
| **Actual** | All three errors shown as expected |
| **Status** | ✅ |

### TC-05: A failed write never reaches the watched folder
| | |
|---|---|
| **Steps** | Temporarily remove `"quantity"` from `FIELDNAMES` and run the generator |
| **Expected** | `ValueError: dict contains fields not in fieldnames: 'quantity'`; the broken file stays in `data\tmp\`; `data\input\` is unchanged |
| **Actual** | Error shown; broken file left in `data\tmp\` only |
| **Status** | ✅ |

## B. Spark file detection

### TC-06: Spark detects every new file
| | |
|---|---|
| **Steps** | Start Spark, then `python src\data_generator.py --events 40 --interval 8 --batches 5 --bad-rate 0.2` |
| **Expected** | 5 batches with data (one per file, since 8 s > 5 s trigger) |
| **Actual** | 5 batches (0–4), rows in: 41, 43, 41, 41, 40 = 206 (= 200 events + 6 duplicates) |
| **Status** | ✅ |

### TC-07: Restart does not reprocess files
| | |
|---|---|
| **Steps** | After TC-06, stop Spark (Ctrl + C), start it again, wait 15 s |
| **Expected** | No new `Batch` lines (checkpoint remembers the 5 files) |
| **Actual** | After restart, no new `Batch` lines within 15 s; the checkpoint remembered all 5 files |
| **Status** | ✅ |

### TC-08: `--max-files` limits batch size
| | |
|---|---|
| **Steps** | Generate 5 files first, then `python src\spark_streaming_to_postgres.py --max-files 2` |
| **Expected** | Batches of 2, 2 and 1 files (about 100, 100, 50 rows in) |
| **Actual** | 3 batches from a 5-file backlog: rows in 101, 103, 51 (2 + 2 + 1 files) = 255 (= 250 events + 5 duplicates); inserted 91 + 94 + 46 = 231 (= 250 − 19 invalid) |
| **Status** | ✅ |

## C. Transformations

### TC-09: Invalid rows are dropped (exact count)
| | |
|---|---|
| **Steps** | Same run as TC-06; add up `invalid dropped` from the Spark log |
| **Expected** | bad_price + bad_timestamp + missing_user_id + negative_price + unknown_event_type = 4 + 9 + 3 + 5 + 8 = **29** |
| **Actual** | 5 + 7 + 5 + 6 + 6 = **29** invalid dropped |
| **Status** | ✅ |

### TC-10: Messy event types are fixed, not dropped
| | |
|---|---|
| **Steps** | `python scratch\check_cleaning.py` after a run with `--bad-rate 0.2` |
| **Expected** | Only `add_to_cart`, `purchase`, `view` (lowercase); messy rows are counted in "Clean rows" |
| **Actual** | Raw 207, Clean 183, Dropped 24; event types: add_to_cart 41, purchase 20, view 122 |
| **Status** | ✅ |

### TC-11: Duplicates are removed
| | |
|---|---|
| **Steps** | Same run as TC-06; add up `duplicates removed` |
| **Expected** | Equal to `duplicate` in the generator summary; rows in = 200 + duplicates |
| **Actual** | 1 + 3 + 1 + 1 + 0 = **6** duplicates removed (generator: duplicate 6); rows in 206 = 200 + 6; inserted 171 = 200 − 29 |
| **Status** | ✅ |

### TC-12: A bad value does not crash the stream (`try_cast`)
| | |
|---|---|
| **Steps** | Temporarily replace `try_cast("decimal(10,2)")` with `cast(...)` and run `check_cleaning.py` on data with a `bad_price` row |
| **Expected** | With `cast`: `[CAST_INVALID_INPUT] The value 'abc' ... cannot be cast to "DECIMAL(10,2)"`. With `try_cast`: no error, row dropped |
| **Actual** | Behaved as expected (crash with `cast`, clean run with `try_cast`) |
| **Status** | ✅ |

## D. PostgreSQL writes

### TC-13: Spark can write to PostgreSQL
| | |
|---|---|
| **Steps** | `python scratch\test_postgres_connection.py` |
| **Expected** | `Write OK`, the row is read back, `stored_at` filled in by the database |
| **Actual** | `Write OK`; row `spark-test-0001` returned with `stored_at` set |
| **Status** | ✅ |

### TC-14: Database rules reject bad data
| | |
|---|---|
| **Steps** | As `spark_user`, insert: a valid row, the same `event_id` again, `event_type = 'refund'`, `price = -5` |
| **Expected** | `INSERT 0 1`; then `duplicate key value violates unique constraint "events_pkey"`; `violates check constraint "events_event_type_check"`; `violates check constraint "events_price_check"` |
| **Actual** | Exactly as expected |
| **Status** | ✅ |

### TC-15: End-to-end reconciliation
| | |
|---|---|
| **Steps** | Reset; start Spark; `python src\data_generator.py --events 100 --interval 8 --batches 5 --bad-rate 0.1`; run `sql\verification_queries.sql` |
| **Expected** | Rows in `events` = 500 − 33 dropping bad rows = **467** = sum of `inserted`; `total_rows` = `unique_event_ids` |
| **Actual** | Spark: rows in 509 (= 500 + 9 duplicates), invalid 33, duplicates 9, inserted 99 + 91 + 94 + 91 + 92 = **467**. PostgreSQL query 1: `total_rows` 467 = `unique_event_ids` 467 |
| **Status** | ✅ |

### TC-16: Replayed data is not stored twice
| | |
|---|---|
| **Steps** | While Spark runs, copy a processed file to a new name: `Copy-Item (Get-ChildItem data\input\*.csv \| Select-Object -First 1).FullName data\input\events_replay_copy.csv` |
| **Expected** | Log shows `0 inserted \| N already in DB`; no error; row count unchanged |
| **Actual** | Replayed copy of the first file → `Batch 5: 102 rows in \| 1 invalid dropped \| 2 duplicates removed \| 0 inserted \| 99 already in DB`; no error; `total_rows` still 467 |
| **Status** | ✅ |

### TC-17: Stored data passes quality checks
| | |
|---|---|
| **Steps** | Query 3 of `verification_queries.sql` after TC-15 |
| **Expected** | `negative_prices`, `bad_quantities`, `missing_users`, `stored_before_event` all `0` |
| **Actual** | `negative_prices` 0, `bad_quantities` 0, `missing_users` 0, `stored_before_event` 0 |
| **Status** | ✅ |

## E. Performance

### TC-18: Latency stays within trigger + processing time
| | |
|---|---|
| **Steps** | Spark log from the TC-06 run (`latency avg`) |
| **Expected** | After the first batch, average latency ≤ trigger (5 s) + processing time |
| **Actual** | Batch 0 (warm-up): 8,302 ms. Batches 1–4: 3,215 / 4,202 / 6,149 / 3,183 ms, all within trigger (5 s) + processing time |
| **Status** | ✅ |

### TC-19: Processing time per batch after warm-up
| | |
|---|---|
| **Steps** | Spark log from the TC-06 run (`processed in`, `trigger`) |
| **Expected** | Each batch finishes inside the 5 s trigger after warm-up |
| **Actual** | Batch 0: 4,534 ms processing, 6,208 ms trigger (one "falling behind" warning: 6,223 ms > 5,000 ms). Batches 1–4: 1,097–1,508 ms processing, 1,972–2,488 ms trigger, all well inside the 5 s trigger |
| **Status** | ✅ |

### TC-20: Time-zone-safe latency
| | |
|---|---|
| **Steps** | After TC-15, run query 4 after `SET TIME ZONE 'UTC'` and again after `SET TIME ZONE 'Africa/Johannesburg'` |
| **Expected** | Displayed times differ by 2 hours; latency values identical; no negative latencies |
| **Actual** | UTC: earliest `2026-11-06 09:52:20.923+00`, avg 6,812 ms. Africa/Johannesburg: earliest `2026-11-06 11:52:20.923+02`, avg 6,812 ms. Same moment, identical latency, no negative values |
| **Status** | ✅ |

## Defects found during testing

| ID | Defect | Fix | Verified by |
|---|---|---|---|
| D-01 | Plain `cast` crashed the whole stream on one bad value (Spark 4 ANSI mode) | Use `try_cast` / `try_to_timestamp` | TC-12 |
| D-02 | Plain JDBC `append` crashed on a