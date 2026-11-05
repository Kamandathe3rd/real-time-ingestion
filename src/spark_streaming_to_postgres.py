"""
spark_streaming_to_postgres.py
Reads e-commerce event CSV files as a stream with Spark Structured Streaming,
cleans them, and writes them to PostgreSQL.
"""
import argparse
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2                                     # NEW
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import StructType, StructField, StringType

# Make Spark's Python workers use this project's .venv Python
os.environ["PYSPARK_PYTHON"] = sys.executable

# Folders (data/ and checkpoints/ are in .gitignore)
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "data" / "input"
CHECKPOINT_DIR = BASE_DIR / "checkpoints" / "events_to_postgres"
CONNECTION_FILE = BASE_DIR / "postgres_connection_details.txt"

# PostgreSQL JDBC driver, downloaded by Spark from Maven Central on first run
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.7"

logger = logging.getLogger("spark_streaming")

# Every column is read as text first; clean_events() converts and validates the types
RAW_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", StringType()),
    StructField("quantity", StringType()),
])

# PostgreSQL tables (created by sql/postgres_setup.sql)
EVENTS_TABLE = "events"
STAGING_TABLE = "events_staging"
EVENT_COLUMNS = [
    "event_id", "event_time", "user_id", "event_type", "product_id",
    "product_name", "category", "price", "quantity", "ingested_at",
]
COLUMN_LIST = ", ".join(EVENT_COLUMNS)

# Copy only NEW events from staging; an event_id already in events is skipped
MOVE_NEW_EVENTS_SQL = f"""
    INSERT INTO {EVENTS_TABLE} ({COLUMN_LIST})
    SELECT {COLUMN_LIST} FROM {STAGING_TABLE}
    ON CONFLICT (event_id) DO NOTHING
"""



# Cleaning rules
VALID_EVENT_TYPES = ["view", "add_to_cart", "purchase"]
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"


def create_spark_session():
    """Start a local Spark session."""
    spark = (SparkSession.builder
             .appName("EcommerceEventStreaming")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)  # NEW
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_connection_details(path=CONNECTION_FILE):
    """Read key=value lines from the connection details file into a dict."""
    details = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            details[key.strip()] = value.strip()

    missing = {"host", "port", "database", "user", "password"} - details.keys()
    if missing:
        raise ValueError(f"Missing in {path.name}: {', '.join(sorted(missing))}")
    return details


def jdbc_options(details):
    """Build the Spark JDBC options for PostgreSQL from the connection details."""
    return {
        "url": f"jdbc:postgresql://{details['host']}:{details['port']}/{details['database']}",
        "user": details["user"],
        "password": details["password"],
        "driver": "org.postgresql.Driver",
    }

def pg_connect(details):
    """Open a direct PostgreSQL connection (used for SQL that Spark's JDBC writer can't run)."""
    return psycopg2.connect(
        host=details["host"],
        port=details["port"],
        dbname=details["database"],
        user=details["user"],
        password=details["password"],
    )


def count_events(details):
    """Return the number of rows in the events table (also proves the connection works)."""
    conn = pg_connect(details)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {EVENTS_TABLE}")
            return cur.fetchone()[0]
    finally:
        conn.close()


def write_to_staging(final_df, details):
    """Replace the staging table's contents with this batch (Spark JDBC)."""
    (final_df.select(EVENT_COLUMNS)
        .write
        .format("jdbc")
        .options(**jdbc_options(details))
        .option("dbtable", STAGING_TABLE)
        .option("truncate", True)      # with overwrite: empty the table, keep its definition
        .mode("overwrite")
        .save())


def move_new_events(details):
    """Insert staging rows into events, skipping existing event_ids. Returns rows inserted."""
    conn = pg_connect(details)
    try:
        with conn, conn.cursor() as cur:   # commits on success, rolls back on error
            cur.execute(MOVE_NEW_EVENTS_SQL)
            return cur.rowcount
    finally:
        conn.close()



def read_event_stream(spark, max_files=0):
    """Watch INPUT_DIR and return a streaming DataFrame of raw CSV rows."""
    reader = (spark.readStream
              .schema(RAW_SCHEMA)
              .option("header", True))
    if max_files > 0:
        reader = reader.option("maxFilesPerTrigger", max_files)
    return reader.csv(str(INPUT_DIR))


def clean_events(raw_df):
    """Convert raw text columns to proper types and keep only valid rows."""
    typed_df = raw_df.select(
        F.trim("event_id").alias("event_id"),
        F.try_to_timestamp(F.trim("event_time"), F.lit(TIMESTAMP_FORMAT)).alias("event_time"),
        F.trim("user_id").try_cast("int").alias("user_id"),
        F.lower(F.trim("event_type")).alias("event_type"),
        F.trim("product_id").try_cast("int").alias("product_id"),
        F.trim("product_name").alias("product_name"),
        F.trim("category").alias("category"),
        F.trim("price").try_cast("decimal(10,2)").alias("price"),
        F.trim("quantity").try_cast("int").alias("quantity"),
    )

    is_valid = (
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("event_type").isin(VALID_EVENT_TYPES)
        & F.col("product_id").isNotNull()
        & (F.col("price") >= 0)
        & (F.col("quantity") > 0)
    )

    return (typed_df
            .filter(is_valid)
            .withColumn("ingested_at", F.current_timestamp()))


def process_batch(batch_df, batch_id, details):
    """Clean, de-duplicate and store one micro-batch in PostgreSQL."""
    start = time.perf_counter()
    batch_df.persist()   # the batch is used several times below: keep it in memory
    try:
        rows_in = batch_df.count()
        if rows_in == 0:
            return

        valid_df = clean_events(batch_df)
        final_df = valid_df.dropDuplicates(["event_id"])

        rows_valid = valid_df.count()
        stats = final_df.agg(
            F.count("*").alias("rows_out"),
            F.avg(F.unix_micros("ingested_at") - F.unix_micros("event_time")).alias("avg_us"),
            F.max(F.unix_micros("ingested_at") - F.unix_micros("event_time")).alias("max_us"),
        ).first()
        rows_out = stats["rows_out"]

        inserted = 0
        if rows_out > 0:
            write_to_staging(final_df, details)
            inserted = move_new_events(details)

        logger.info(
            "Batch %d: %d rows in | %d invalid dropped | %d duplicates removed | "
            "%d inserted | %d already in DB",
            batch_id, rows_in, rows_in - rows_valid, rows_valid - rows_out,
            inserted, rows_out - inserted,
        )
        if rows_out > 0:
            logger.info(
                "Batch %d: latency avg %.0f ms, max %.0f ms | processed in %.0f ms",
                batch_id, stats["avg_us"] / 1000, stats["max_us"] / 1000,
                (time.perf_counter() - start) * 1000,
            )
    finally:
        batch_df.unpersist()


class ProgressLogger(StreamingQueryListener):
    """Logs Spark's own metrics after every micro-batch that had data."""

    def onQueryStarted(self, event):
        logger.info("Query started (id %s)", event.id)

    def onQueryProgress(self, event):
        p = event.progress
        if p.numInputRows == 0:
            return
        logger.info(
            "Spark metrics batch %d: %d rows | input %.1f rows/s | processed %.1f rows/s | trigger %d ms",
            p.batchId, p.numInputRows, p.inputRowsPerSecond,
            p.processedRowsPerSecond, p.durationMs.get("triggerExecution", 0),
        )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        logger.info("Query terminated (id %s)", event.id)


def parse_args():
    """Read settings from the command line."""
    parser = argparse.ArgumentParser(
        description="Stream e-commerce event CSV files with Spark Structured Streaming."
    )
    parser.add_argument("--trigger-seconds", type=int, default=5,
                        help="how often Spark checks for new files (default: 5)")
    parser.add_argument("--max-files", type=int, default=0,
                        help="max new files per micro-batch; 0 = no limit (default: 0)")
    args = parser.parse_args()

    if args.trigger_seconds < 1:
        parser.error("--trigger-seconds must be at least 1")
    if args.max_files < 0:
        parser.error("--max-files cannot be negative")
    return args


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)   # hide py4j's chatty INFO messages
    args = parse_args()
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    details = load_connection_details()  # NEW
    logger.info("PostgreSQL OK: %d rows already in %s.%s",  # NEW
                count_events(details), details["database"], EVENTS_TABLE)  # NEW

    spark = create_spark_session()
    spark = create_spark_session()
    spark.streams.addListener(ProgressLogger())
    raw_df = read_event_stream(spark, args.max_files)

    query = (raw_df.writeStream
             .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, details))
             .option("checkpointLocation", str(CHECKPOINT_DIR))
             .trigger(processingTime=f"{args.trigger_seconds} seconds")
             .start())

    logger.info("Streaming started. Watching %s every %ds, max files per batch: %s (Ctrl+C to stop)",
                INPUT_DIR, args.trigger_seconds, args.max_files or "no limit")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.warning("Stopping stream (Ctrl+C)...")
        query.stop()
    finally:
        spark.stop()
        logger.info("Spark stopped.")


if __name__ == "__main__":
    main()