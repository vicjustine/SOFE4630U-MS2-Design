"""
SOFE 4630U Milestone 2 - Design part: producer for the Redis storage stage.

The same Milestone 1 records are also routed into a Redis key-value store:

    Labels.csv -> this producer -> Pub/Sub -> Integration Connector -> Redis

The Redis sink connector maps the Pub/Sub message's ordering key to the Redis
key and the message's data to the Redis value, so the producer must set both.
Each record is keyed by its profile name and row number, for example
"reading:denver:0", which makes an individual reading retrievable directly
rather than only by scanning the whole table.

This is the difference between the two storage stages: MySQL stores the
records as rows that can be queried and aggregated, while Redis stores them as
independent values addressed by key, for low latency lookup.
"""

import csv
import glob
import json
import os
import time

from google.cloud import pubsub_v1   # pip install google-cloud-pubsub

# ======= EDIT THIS =======
PROJECT_ID = "projectmilestone1-485800"
TOPIC_ID = "Image2Redis"   # the topic the redis-integration listens on
CSV_FILE = "Labels.csv"
DELAY_SECONDS = 0.2
# =========================


def load_credentials():
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


def main():
    load_credentials()

    # ordering keys must be enabled on the publisher for them to be sent
    options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    publisher = pubsub_v1.PublisherClient(publisher_options=options)
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Publishing CSV rows to: {topic_path}")
    print(f"Reading CSV file: {CSV_FILE}\n")

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        count = 0
        for index, row in enumerate(reader):
            record = dict(row)

            # the connector maps the ordering key to the Redis key
            redis_key = f"reading:{row['profileName']}:{index}"

            message_bytes = json.dumps(record).encode("utf-8")

            future = publisher.publish(topic_path, message_bytes,
                                       ordering_key=redis_key)
            message_id = future.result()

            count += 1
            print(f"[{count}] key={redis_key} published={message_id}")

            time.sleep(DELAY_SECONDS)

    print(f"\nDone. Published {count} records to {TOPIC_ID}.")
    print("The Redis sink connector stores each message under its ordering key.")


if __name__ == "__main__":
    main()
