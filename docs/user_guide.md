# User Guide: Running the Real-Time Ingestion Pipeline

All commands are for **Windows PowerShell** (or the PyCharm terminal) and are run from the
**project root** (the folder that contains `requirements.txt`).

## 1. Prerequisites

| Tool | Tested version | Notes |
|---|---|---|
| Windows | 11, 64-bit | |
| Python | 3.14.8 | 3.10 or newer is required by PySpark 4 |
| Java JDK | Eclipse Temurin 25.0.5 | `JAVA_HOME` must point to it |
| Hadoop Windows helpers | `winutils.exe` + `hadoop.dll` | In `C:\hadoop\bin` |
| PostgreSQL | 18.6 (with pgAdmin 4 and command line tools) | Service running on port 5432 |
| Git | Any recent version | |
| Internet | First run only | Spark downloads the PostgreSQL JDBC driver (42.7.7) from Maven Central |

## 2. One-time setup

### 2.1 Get the code and create the virtual environment

```powershell
git clone https://github.com/<your-username>/real-time-ingestion.git
cd real-time-ingestion
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

**Success:** the prompt starts with `(.venv)` and pip ends with `Successfully installed ... pyspark-4.3.0 ...`.

If activation fails with *"running scripts is disabled"*, run
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, answer `Y`, and try again.

### 2.2 Check Java and the Hadoop helpers

```powershell
java -version
echo $env:JAVA_HOME
echo $env:HADOOP_HOME
dir C:\hadoop\bin
```

**Success:** Java prints a version, `JAVA_HOME` points to the JDK folder, `HADOOP_HOME` is `C:\hadoop`,
and `C:\hadoop\bin` contains `winutils.exe` and `hadoop.dll`. `C:\hadoop\bin` must also be on `PATH`.
Restart the terminal (or PyCharm) after changing environment variables.

### 2.3 Create the database

```powershell
$psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"
& $psql -U postgres -f sql\postgres_setup.sql
```

Enter the **postgres** (admin) password chosen during installation.

**Success:** the output ends with the description of the `events` table (11 columns, 3 indexes,
4 check constraints) and the `events_staging` table.

⚠️ Running this script again **deletes** the database and all stored events.

The script creates a lab-only user. Its connection details are in
[`postgres_connection_details.txt`](../postgres_connection_details.txt):

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `ecommerce_events` |
| User | `spark_user` |
| Password | `spark_lab_pass` |

## 3. Run the pipeline

You need **two terminals**, both with `(.venv)` active. Start Spark **first**.

**Terminal 1: streaming job**

```powershell
python src\spark_streaming_to_postgres.py
```

Wait for:

```
INFO    | PostgreSQL OK: 0 rows already in ecommerce_events.events
INFO    | Streaming started. Watching ...\data\input every 5s, max files per batch: no limit (Ctrl+C to stop)
```

The first run downloads the JDBC driver; later runs are faster.

**Terminal 2: event generator**

```powershell
python src\data_generator.py --events 100 --interval 8 --batches 5
```

Every 8 seconds a new CSV file appears in `data\input\`, and Terminal 1 logs one batch per file:

```
INFO    | Batch 0: 102 rows in | 8 invalid dropped | 2 duplicates removed | 92 inserted | 0 already in DB
INFO    | Batch 0: latency avg 11191 ms, max 11192 ms | processed in 9077 ms
INFO    | Spark metrics batch 0: 102 rows | input 0.0 rows/s | processed 9.8 rows/s | trigger 10630 ms
```

Run the generator without `--batches` to produce files until you press **Ctrl + C**.

## 4. Check the results

```powershell
& $psql -h localhost -U spark_user -d ecommerce_events -f sql\verification_queries.sql
```

Password: `spark_lab_pass`.

| Query | Expected |
|---|---|
| 1. Row count | `total_rows` = `unique_event_ids` = sum of `inserted` in the Spark log |
| 2. Rows per event type | Only `add_to_cart`, `purchase`, `view` |
| 3. Data quality | All four counters are `0` |
| 4. End-to-end latency | Positive values, a few seconds (see `performance_metrics.md`) |
| 5–7. Latency split, per batch, per minute | Used for the performance report |

**Reconciliation rule:** rows in `events` = generator events − (bad_price + bad_timestamp +
missing_user_id + negative_price + unknown_event_type) from the generator's summary.

You can also browse the table in **pgAdmin 4**: Servers → PostgreSQL 18 → Databases →
ecommerce_events → Schemas → public → Tables → events → right-click → View/Edit Data.

## 5. Options

### `data_generator.py`

| Option | Default | Meaning |
|---|---|---|
| `--events` | 20 | Events per CSV file (at least 1) |
| `--interval` | 5 | Seconds between files |
| `--batches` | 0 | Number of files; `0` = run until Ctrl + C |
| `--bad-rate` | 0.05 | Share of events given a data problem (0 to 1); `0` = clean data only |

### `spark_streaming_to_postgres.py`

| Option | Default | Meaning |
|---|---|---|
| `--trigger-seconds` | 5 | How often Spark checks for new files (at least 1) |
| `--max-files` | 0 | Max new files per micro-batch; `0` = no limit |

Both scripts show their options with `--help`.

## 6. Stop and reset

**Stop:** press **Ctrl + C** in each terminal. Messages such as `ERROR: The process "..." not found`,
`SUCCESS: The process with PID ... has been terminated` or `KeyboardInterrupt while sending command`
are normal Windows shutdown noise.

**Reset for a clean run:** delete the files **and** empty the tables, together:

```powershell
Remove-Item -Recurse data, checkpoints -ErrorAction SilentlyContinue
& $psql -h localhost -U spark_user -d ecommerce_events -c "TRUNCATE events, events_staging;"
```

| If you reset only... | Result |
|---|---|
| The tables | The checkpoint still marks old files as done, so they are not reloaded |
| `checkpoints\` | Old files are re-read, but already stored, so the log shows `0 inserted` |

## 7. Troubleshooting

| Error (look for) | Cause | Fix |
|---|---|---|
| `UnsatisfiedLinkError ... NativeIO$Windows` | `hadoop.dll` not found | Add `C:\hadoop\bin` to `PATH`, then restart the terminal |
|