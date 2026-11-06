# Performance Metrics: Real-Time Ingestion Pipeline

## 1. Test environment

| Item | Value |
|---|---|
| CPU | Intel Core i7-10510U @ 1.80 GHz (4 cores, 8 threads) |
| Logical processors (used by `local[*]`) | 8 |
| RAM | 32 GB |
| OS | Windows 10/11, 64-bit |
| Software | Python 3.14.8, PySpark 4.3.0 (local mode), Java Temurin 25.0.5, PostgreSQL 18.6 |
| Spark settings | `local[*]`, `spark.sql.shuffle.partitions = 4`, trigger 5 s unless stated |

## 2. Metrics and how they are measured

| Metric | Definition | Source |
|---|---|---|
| End-to-end latency | `stored_at − event_time` | `verification_queries.sql`, query 4 |
| Wait for Spark | `ingested_at − event_time` (time in the folder until the next trigger) | Query 5 |
| Spark → DB | `stored_at − ingested_at` (cleaning + staging write + merge) | Query 5 |
| Processing time | Duration of `process_batch()` | Spark log `processed in` |
| Trigger time | Spark's total time for one micro-batch | Spark log `trigger` (listener) |
| Throughput | Rows processed per second | Spark log `processed … rows/s`; query 7 (rows per minute) |
| Data loss | Rows missing or duplicated in PostgreSQL | Reconciliation (`test_cases.md`, TC-15) |

## 3. Targets ("expected limits")

| Metric | Target | Reason |
|---|---|---|
| Data loss | 0 rows | Every valid event must be stored exactly once |
| Average end-to-end latency (after warm-up) | ≤ trigger interval + 3 s | An event waits at most one trigger, plus processing |
| Processing time per batch (after warm-up) | < trigger interval | Otherwise batches queue up ("falling behind") |
| First batch | May exceed the targets | Spark warm-up (JVM, code generation, JDBC connection) |

## 4. Results

### S0: Baseline without PostgreSQL (Phase 2, console preview)

Generator: 5 files × 40 events, 8 s apart, bad-rate 0.2. Trigger 5 s.

| Batch | Rows in | Kept | Avg latency (ms) | Processing (ms) | Trigger (ms) | Processed rows/s |
|---|---|---|---|---|---|---|
| 0 (warm-up) | 42 | 32 | 8,180 | 4,212 | 5,986 | 7.0 |
| 1 | 40 | 36 | 2,955 | 1,214 | 2,286 | 17.5 |
| 2 | 41 | 32 | 3,711 | 1,003 | 2,036 | 20.1 |
| 3 | 41 | 31 | 5,475 | 624 | 1,529 | 26.8 |
| 4 | 41 | 35 | 2,434 | 574 | 1,476 | 27.8 |

Batch 0 logged `Current batch is falling behind` (6.0 s > 5 s trigger); batches 1–4 finished well within the trigger.

### S1: Baseline with PostgreSQL

Generator: 5 files × 100 events, 8 s apart, bad-rate 0.1. Trigger 5 s.

| Batch | Rows in | Inserted | Avg latency (ms) | Processing (ms) | Trigger (ms) | Processed rows/s |
|---|---|---|---|---|---|---|
| 0 (warm-up) | 102 | 99 | 10,851 | 7,843 | 10,031 | 10.2 |
| 1 | 102 | 91 | 7,433 | 1,905 | 2,939 | 34.7 |
| 2 | 103 | 94 | 4,204 | 1,350 | 2,258 | 45.6 |
| 3 | 101 | 91 | 1,192 | 1,406 | 2,303 | 43.9 |
| 4 | 101 | 92 | 3,196 | 1,269 | 2,325 | 43.4 |

| End-to-end (SQL) | avg ms | min ms | p95 ms | max ms | wait for Spark ms | Spark → DB ms |
|---|---|---|---|---|---|---|
| S1 | 6,812 | 2,076 | 13,716 | 13,717 | 6,149 | 664 |

Batch 0 logged `Current batch is falling behind` (10.0 s > 5 s trigger). Total stored: 467 rows, all within one minute (query 7).

### S2: Shorter trigger (2 s)

Same generator settings as S1. Spark started with `--trigger-seconds 2`.

| End-to-end (SQL) | avg ms | min ms | p95 ms | max ms | wait for Spark ms | Spark → DB ms |
|---|---|---|---|---|---|---|
| S2 | 3,049 | 2,107 | 6,222 | 6,222 | 2,573 | 476 |

| Batch | Rows in | Inserted | Avg latency (ms) | Processing (ms) | Trigger (ms) | Processed rows/s |
|---|---|---|---|---|---|---|
| 0 (warm-up) | 102 | 92 | 4,531 | 4,468 | 6,031 | 16.9 |
| 1 | 100 | 93 | 1,447 | 1,027 | 1,924 | 52.0 |
| 2 | 102 | 97 | 1,364 | 1,031 | 1,922 | 53.1 |
| 3 | 100 | 95 | 1,564 | 1,434 | 2,679 | 37.3 |
| 4 | 104 | 94 | 1,521 | 1,212 | 2,188 | 47.5 |

Generator: 500 events, 29 dropping bad rows, 8 duplicates → 471 stored (query 1: 471 = 471 unique), 0 data loss.
Processing time per batch after warm-up: 1,027 – 1,434 ms. "Falling behind" warnings: 3 (batch 0 warm-up, batch 3 at 2,681 ms, batch 4 at 2,188 ms), because a full micro-batch on this laptop takes about 2 s, the same as the trigger.

### S3: Higher load (2,000 events per file)

Generator: 5 files × 2,000 events, 8 s apart, bad-rate 0.1. Trigger 5 s.

| Batch | Rows in | Inserted | Processing (ms) | Trigger (ms) | Processed rows/s |
|---|---|---|---|---|---|
| 0 (warm-up) | 2,021 | 1,853 | 7,424 | 9,481 | 213.2 |
| 1 | 2,028 | 1,848 | 1,946 | 2,937 | 690.5 |
| 2 | 2,037 | 1,849 | 1,103 | 1,971 | 1,033.5 |
| 3 | 2,038 | 1,869 | 997 | 1,887 | 1,080.0 |
| 4 | 2,027 | 1,838 | 1,076 | 2,010 | 1,008.5 |

| End-to-end (SQL) | avg ms | p95 ms | Rows stored | Rows per minute (query 7, peak) |
|---|---|---|---|---|
| S3 | 5,761 | 11,791 | 9,257 | 9,257 |

Generator: 10,000 events, 743 dropping bad rows, 151 duplicates → 9,257 stored (query 1: 9,257 = 9,257 unique), 0 data loss. Wait for Spark 5,021 ms, Spark → DB 739 ms. Batch 0 logged `Current batch is falling behind` (9.5 s > 5 s trigger).

## 5. Summary against targets

| Target | S1 (5 s, 100/file) | S2 (2 s, 100/file) | S3 (5 s, 2,000/file) |
|---|---|---|---|
| Data loss = 0 (reconciliation) | ✅ 467 = 500 − 33 | ✅ 471 = 500 − 29 | ✅ 9,257 = 10,000 − 743 |
| Avg latency after warm-up ≤ trigger + 3 s | ✅ 4,955 ms ≤ 8,000 | ✅ 2,280 ms ≤ 5,000 | ✅ 4,253 ms ≤ 8,000 |
| Processing < trigger after warm-up | ✅ max trigger 2,939 ms < 5,000 | ❌ 2 of 4 batches over 2,000 ms (2,679 and 2,188 ms) | ✅ max trigger 2,937 ms < 5,000 |

"After warm-up" averages are row-weighted from query 6, batches 1–4.

## 6. Analysis

1. **Warm-up.** The first micro-batch was always the slowest: 7,843 ms of processing in S1,
   4,468 ms in S2 and 7,424 ms in S3, against roughly 1.0–1.9 s for every later batch
   (about 4–7× slower). The first batch pays one-off start-up costs: the JVM compiling hot code,
   Spark generating code for the query plan, and the first JDBC and PostgreSQL connections.
   It is also the only batch that consistently triggered the "falling behind" warning at a 5 s trigger.

2. **Where latency goes.** In S1, 6,149 ms of the 6,812 ms average end-to-end latency (about 90%)
   was the time an event waited in the folder for the next trigger. Only 664 ms was spent between
   Spark picking up the batch and PostgreSQL storing it. The write path is not the bottleneck;
   the trigger interval is.

3. **Trigger effect (S1 vs S2).** Reducing the trigger from 5 s to 2 s cut the average latency from
   6,812 ms to 3,049 ms (−55%) and the p95 from 13,716 ms to 6,222 ms. The Spark → DB part stayed
   about the same (664 vs 476 ms), confirming that only the waiting time changed. However, a full
   micro-batch takes about 2 s on this laptop, so 2 of the 4 post-warm-up batches overran the 2 s
   trigger (2,679 and 2,188 ms). About 2–3 s is the lowest trigger this setup can sustain.

4. **Load effect (S1 vs S3).** With files 20× larger (2,000 instead of 100 events), processing per batch
   stayed in the same range (about 1.0–1.9 s after warm-up), while throughput rose from 35–46 rows/s
   to 690–1,080 rows/s (about 25×). Spark → DB grew only from 664 to 739 ms. Most of a batch's time is
   a fixed cost (starting tasks, opening connections, running the merge SQL), so larger batches spread
   that cost over more rows. All 9,257 expected rows were stored, with no data loss.

5. **Cost of PostgreSQL.** With the same 40-event files, batches 1–4 took 574–1,214 ms to process
   without a database write (S0) and 1,097–1,508 ms with the staging write and merge (test run TC-06),
   about 0.5 s more on average. This matches the 476–739 ms "Spark → DB" time measured in SQL.

6. **Waiting time, not processing time, also explains the extreme values.** When 5 files were already
   waiting before Spark started (TC-08), average latency reached 38–44 s even though each batch took
   only 1–4.5 s to process. Replayed events (TC-16) showed about 370 s of "latency" because they were
   6 minutes old; nothing was inserted, so this did not affect the stored latency figures.

Results come from a laptop CPU (Intel Core i7-10510U), so individual timings can vary slightly
between runs; the patterns above were consistent across all scenarios.

## 7. Bottlenecks and recommendations

| Observation | Recommendation |
|---|---|
| About 90% of end-to-end latency is waiting for the trigger (S1: 6,149 of 6,812 ms) | Use a **3 s trigger** as the balance point: 2 s cut latency by 55% but caused overruns. For sub-second latency, replace file polling with a message queue such as Kafka |
| The first batch is 4–7× slower (warm-up) and triggers "falling behind" | Keep the streaming job running continuously instead of restarting it; optionally send a small warm-up file at start-up; exclude batch 0 from latency targets |
| Batch time is mostly a fixed cost: 20× more rows gave about 25× higher throughput at similar processing time (S3) | Under high load, prefer fewer, larger batches. Use `--max-files` to keep batch sizes bounded when a backlog builds |
| A backlog makes latency grow even when processing is fast (TC-08: 38–44 s) | Monitor the number of waiting files; start the job before producers; use `--max-files` so a backlog is cleared in controlled steps |
| Each batch runs several Spark actions (counts, aggregation, staging write) | At higher volumes, combine the counts into a single aggregation; the batch is already cached with `persist()` |
| `spark.sql.shuffle.partitions = 4` while the CPU has 8 threads | Fine for these batch sizes; for much larger batches, raise it towards 8 so de-duplication uses all threads |