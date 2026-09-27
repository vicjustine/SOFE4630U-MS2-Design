"""
SOFE 4630U Milestone 2 - Design part: producer for the MySQL storage stage.

Milestone 1 built a producer/consumer system that published the records of
Labels.csv to a Pub/Sub topic. This milestone adds a storage stage in the
middle of that system:

    Labels.csv -> this producer -> Pub/Sub -> Integration Connector -> MySQL

The Application Integration workflow is triggered by each Pub/Sub message and
the MySQL sink connector inserts it into the SmartMeter table, so no consumer
code is required to perform the write.

The CSV does not match the SmartMeter table exactly, so each record is
transformed before publishing:

    CSV column      SmartMeter column   transformation
    ------------------------------------------------------------------
    (none)          ID (int, PK)        generated sequentially
    time (float)    time (bigint)       truncated to whole seconds
    profileName     profile_name        renamed to match the column
    temperature     temperature         cast from string to float
    humidity        humidity            cast; blank becomes NULL
    pressure        pressure            cast from string to float
"""

import csv
import glob
import json
import os
import time

from google.cloud import pubsub_v1   # pip install google-cloud-pubsub

# ======= EDIT THIS =======
PROJECT_ID = "projectmilestone1-485800"
TOPIC_ID = "smartMeterReadings"   # the topic the mysql-integration listens on
CSV_FILE = "Labels.csv"
START_ID = 1000                   # first primary key; raise it to avoid collisions
DELAY_SECONDS = 0.2
# =========================


def load_credentials():
    """Locate the service account key so google.cloud can authenticate."""
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return

    keys = glob.glob("*.json")
    if not keys:
        raise SystemExit(
            "No service account key (*.json) found in this folder.\n"
            "Download the key from GCP (IAM & Admin > Service Accounts > Keys)\n"
            "and place it beside this script, or set\n"
            "GOOGLE_APPLICATION_CREDENTIALS to its path."
        )
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keys[0]


def to_float(value):
    """Convert a CSV field to a float, treating a blank field as NULL."""
    value = (value or "").strip()
    if value == "":
        return None
    return float(value)


def build_record(row, record_id):
    """Map one CSV row onto the columns of the SmartMeter table."""
    return {
        "ID": record_id,
        # the table stores time as a bigint, so the fractional part is dropped
        "time": int(float(row["time"])),
        "profile_name": row["profileName"],
        "temperature": to_float(row["temperature"]),
        "humidity": to_float(row["humidity"]),
        "pressure": to_float(row["pressure"]),
    }


def main():
    load_credentials()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Publishing CSV rows to: {topic_path}")
    print(f"Reading CSV file: {CSV_FILE}\n")

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        count = 0
        record_id = START_ID
        for row in reader:
            record = build_record(row, record_id)

            # serialize the dictionary: dict -> JSON string -> bytes
            message_bytes = json.dumps(record).encode("utf-8")

            future = publisher.publish(topic_path, message_bytes)
            message_id = future.result()   # wait for the publish to be confirmed

            count += 1
            record_id += 1
            print(f"[{count}] id={record['ID']} published={message_id} "
                  f"profile={record['profile_name']}")

            time.sleep(DELAY_SECONDS)

    print(f"\nDone. Published {count} records to {TOPIC_ID}.")
    print("The MySQL sink connector inserts each message into the SmartMeter table.")


if __name__ == "__main__":
    main()
