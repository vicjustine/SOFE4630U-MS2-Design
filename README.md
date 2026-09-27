# SOFE 4630U Milestone 2 — Design Part

Scripts for the design section of Milestone 2 (Data Storage and Integration
Connectors).

Milestone 1 built a producer/consumer system that published the records of
`Labels.csv` to a Pub/Sub topic. This milestone adds a **storage stage** in the
middle of that system, so the records are persisted rather than only consumed:

```
Labels.csv -> producer -> Pub/Sub -> Integration Connector -> MySQL / Redis
```

The Application Integration workflow is triggered by each Pub/Sub message, and
the sink connector performs the write. No consumer code is needed to store the
data, which is the point of using a connector rather than writing integration
logic by hand.

## Files

| File | Description |
|---|---|
| `csv_to_mysql_producer.py` | Publishes the CSV records to the topic feeding the MySQL connector |
| `verify_mysql.py` | Connects to MySQL and reports what was stored (check step) |
| `csv_to_redis_producer.py` | Publishes the CSV records with ordering keys, for the Redis connector |
| `verify_redis.py` | Connects to Redis and reports the stored keys (check step) |
| `Labels.csv` | The 100 records carried over from Milestone 1 |

## Requirements

```
pip install google-cloud-pubsub pymysql redis
```

Also required:

- A GCP project with the `smartMeterReadings` and `Image2Redis` topics
- MySQL and Redis deployed on GKE (see the lab's `mySQL/` and `Redis/` manifests)
- `mysql-connector` and `redis-connector` created and **Active**
- `mysql-integration` and `redis-integration` **published**
- A service account key (`*.json`) in this folder — the scripts find it
  automatically and it is excluded from version control

## The two storage designs

### MySQL — structured records

The CSV does not match the `SmartMeter` table, so each row is transformed
before publishing:

| CSV column | SmartMeter column | Transformation |
|---|---|---|
| (none) | `ID` (int, primary key) | generated sequentially from `START_ID` |
| `time` (float) | `time` (bigint) | truncated to whole seconds |
| `profileName` | `profile_name` | renamed to match the column |
| `temperature` | `temperature` | cast to float |
| `humidity` | `humidity` | cast to float; a blank field becomes NULL |
| `pressure` | `pressure` | cast to float |

11 of the 100 records have a blank humidity field. These are published as JSON
`null` and stored as SQL NULL, rather than being dropped, so the gap in the
source data is preserved rather than hidden.

### Redis — key-value records

The Redis connector maps the Pub/Sub message's **ordering key** to the Redis
key and its **data** to the value. Each record is therefore published with an
ordering key of the form:

```
reading:<profileName>:<row-number>      e.g. reading:denver:0
```

This makes an individual reading retrievable directly by key, rather than only
by scanning a table.

## Running

Publish into the MySQL pipeline, then check what arrived:

```
python3 csv_to_mysql_producer.py
python3 verify_mysql.py <mysql-external-ip>
```

Publish into the Redis pipeline, then check:

```
python3 csv_to_redis_producer.py
python3 verify_redis.py <redis-external-ip>
```

Get the external IPs with:

```
kubectl get services
```

## Note on the ID column

`SmartMeter.ID` is the primary key and the connector uses a **Create**
operation, so publishing a record whose ID already exists will fail. Raise
`START_ID` in `csv_to_mysql_producer.py` before re-running against a table that
already holds the previous batch.

## Cleaning up

The integrations and connectors consume credit while they are running. After
demonstrating the pipeline, unpublish both integrations and suspend both
connectors.
