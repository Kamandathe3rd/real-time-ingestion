"""
data_generator.py
Generates fake e-commerce events and writes them as CSV files.
"""
import argparse                 # NEW
import csv                      # NEW
import logging                  # NEW
import random
import time                     # NEW
import uuid
from collections import Counter          # NEW
from datetime import datetime
from pathlib import Path        # NEW

# NEW - Folders (data/ is in .gitignore)
BASE_DIR = Path(__file__).resolve().parent.parent   # project root
INPUT_DIR = BASE_DIR / "data" / "input"             # Spark watches this folder
TMP_DIR = BASE_DIR / "data" / "tmp"                 # files are written here first

logger = logging.getLogger("data_generator")

# NEW - Column order in every CSV file
FIELDNAMES = [
    "event_id", "event_time", "user_id", "event_type",
    "product_id", "product_name", "category", "price", "quantity",
]

# Product catalogue: (product_id, product_name, category, price)
PRODUCTS = [
    (101, "Wireless Mouse", "Electronics", 25.99),
    (102, "USB-C Charger", "Electronics", 19.50),
    (103, "Bluetooth Speaker", "Electronics", 49.99),
    (201, "Running Shoes", "Fashion", 79.00),
    (202, "Denim Jacket", "Fashion", 65.00),
    (301, "Coffee Maker", "Home", 89.90),
    (302, "Desk Lamp", "Home", 22.75),
    (401, "Notebook Set", "Books", 12.99),
]

# What a user can do, and how often (out of 100)
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [70, 20, 10]

# Deliberate data-quality problems, so Phase 2 has real cleaning to do
BAD_ROW_TYPES = [
    "missing_user_id",     # user_id is empty
    "negative_price",      # price below zero
    "bad_price",           # price is text, not a number
    "unknown_event_type",  # an action we don't support
    "messy_event_type",    # right action, wrong case/spaces: "  VIEW "
    "bad_timestamp",       # event_time cannot be read as a date
    "duplicate",           # the same event written twice
]

def make_event():
    """Return one fake event as a dictionary."""
    product_id, product_name, category, price = random.choice(PRODUCTS)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    quantity = random.randint(1, 3) if event_type == "purchase" else 1

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now().isoformat(sep=" ", timespec="milliseconds"),
        "user_id": random.randint(1, 500),
        "event_type": event_type,
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "price": price,
        "quantity": quantity,
    }

def corrupt_event(event, problem):
    """Return a copy of event with one deliberate data-quality problem."""
    bad = dict(event)
    if problem == "missing_user_id":
        bad["user_id"] = ""
    elif problem == "negative_price":
        bad["price"] = -event["price"]
    elif problem == "bad_price":
        bad["price"] = "abc"
    elif problem == "unknown_event_type":
        bad["event_type"] = "refund"
    elif problem == "messy_event_type":
        bad["event_type"] = f"  {event['event_type'].upper()} "
    elif problem == "bad_timestamp":
        bad["event_time"] = "not-a-date"
    # "duplicate" keeps the event unchanged; write_batch writes it twice
    return bad

# NEW
def write_batch(num_events, bad_rate=0.0):
    """Write num_events fake events to a new CSV file.

    Returns the file path and a Counter of the bad rows that were added.
    """
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    file_name = f"events_{datetime.now():%Y%m%d_%H%M%S_%f}.csv"
    tmp_path = TMP_DIR / file_name
    final_path = INPUT_DIR / file_name
    problems = Counter()

    # 1) Write the whole file in the tmp folder
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for _ in range(num_events):
            event = make_event()
            if random.random() < bad_rate:
                problem = random.choice(BAD_ROW_TYPES)
                event = corrupt_event(event, problem)
                problems[problem] += 1
                if problem == "duplicate":
                    writer.writerow(event)   # first copy (second copy below)
            writer.writerow(event)

    # 2) Move the finished file into the folder Spark watches
    tmp_path.replace(final_path)
    return final_path, problems


def parse_args():
    """Read settings from the command line."""
    parser = argparse.ArgumentParser(
        description="Generate fake e-commerce event CSV files for Spark."
    )
    parser.add_argument("--events", type=int, default=20,
                        help="events per file (default: 20)")
    parser.add_argument("--interval", type=float, default=5,
                        help="seconds between files (default: 5)")
    parser.add_argument("--batches", type=int, default=0,
                        help="how many files to write; 0 = run until Ctrl+C (default: 0)")
    parser.add_argument("--bad-rate", type=float, default=0.05,  # NEW
                        help="share of events given a data problem, 0 to 1 (default: 0.05)")
    args = parser.parse_args()

    if args.events < 1:
        parser.error("--events must be at least 1")
    if args.interval < 0 or args.batches < 0:
        parser.error("--interval and --batches cannot be negative")
    if not 0 <= args.bad_rate <= 1:  # NEW
        parser.error("--bad-rate must be between 0 and 1")  # NEW
    return args

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    logger.info("Writing %d events every %ss to %s", args.events, args.interval, INPUT_DIR)
    logger.info("Bad-row rate: %.0f%%", args.bad_rate * 100)
    logger.info("Press Ctrl+C to stop.")

    batch = 0
    total_problems = Counter()
    try:
        while True:
            path, problems = write_batch(args.events, args.bad_rate)
            batch += 1
            total_problems += problems
            logger.info("[batch %d] %d events (%d bad) -> %s",
                        batch, args.events, sum(problems.values()), path.name)

            if args.batches and batch >= args.batches:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.warning("Stopped by user (Ctrl+C).")

    logger.info("Done: %d files, %d events in total.", batch, batch * args.events)
    logger.info("Bad rows added: %d", sum(total_problems.values()))
    for problem, count in sorted(total_problems.items()):
        logger.info("  %s: %d", problem, count)

if __name__ == "__main__":
    main()