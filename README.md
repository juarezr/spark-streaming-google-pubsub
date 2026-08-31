# spark-streaming-google-pubsub

Apache Spark connector for **Google Cloud Pub/Sub** (standard). Read messages from a subscription into:

- **Structured Streaming** (primary) via `.format("google-pubsub")`
- **Classic Spark Streaming (DStreams)** via a thin Legacy-compatible Java API

Designed for Spark **3.5** (Scala 2.12) and Spark **4.0–4.2** (Scala 2.13; built against 4.1), including GCP Dataproc **2.3** (Spark 3.5) and **3.0** (Spark 4.1).
Authentication defaults to **Application Default Credentials (ADC)**.

## Why this connector

This project aims to deliver:

- Structured Streaming Data Source V2 (`MicroBatchStream`)
- Configurable ack semantics (`afterCommit` default, optional `early`)
- No subscription rewind on restart unless you set `seek`
- Retries/backoff and bounded outstanding bytes for 24×7 jobs
- A DStreams shim for gradual migration from Legacy style streaming
- Support for newer/modern Dataproc images

## Coordinates

| Spark   | Scala | Artifact                                                     |
|---------|-------|--------------------------------------------------------------|
| 3.5.x   | 2.12  | `io.github.juarezr:spark-streaming-google-pubsub_2.12:0.3.0` |
| 4.0–4.2 | 2.13  | `io.github.juarezr:spark-streaming-google-pubsub_2.13:0.3.0` |

Prefer `--packages` (or a Maven/Gradle dependency) so Google client libraries resolve as transitives.

Fat JAR (`*-all.jar`, Google client deps bundled): built locally with `mvn package`, and attached to [GitHub Releases](https://github.com/juarezr/spark-streaming-google-pubsub/releases) — **not** published to Maven Central.

## Schema (Structured Streaming)

| Column        | Type                 | Description                                 |
|---------------|:---------------------|:--------------------------------------------|
| `messageId`   | string               | Pub/Sub message id                          |
| `data`        | binary               | Payload bytes                               |
| `attributes`  | `map<string,string>` | Attributes                                  |
| `publishTime` | timestamp            | Publish time                                |
| `orderingKey` | string               | Ordering key (may be empty)                 |
| `ackId`       | string               | Ack id (useful when managing acks manually) |

## Options

| Option                | Default       | Description                                 |
|:----------------------|:--------------|:--------------------------------------------|
| `projectId`           | required      | GCP project id                              |
| `subscription`        | required      | Subscription id or full resource name       |
| `topic`               |               | Optional topic (reserved for admin helpers) |
| `ackMode`             | `afterCommit` | `afterCommit` or `early`                    |
| `maxMessagesPerPull`  | `1000`        | Max messages per pull                       |
| `maxBytesOutstanding` | `104857600`   | Soft cap on in-flight payload bytes         |
| `ackDeadlineSeconds`  | `60`          | Extend deadline while a batch is in flight  |
| `pullTimeoutSeconds`  | `20`          | RPC deadline (seconds) for each pull call   |
| `seek`                | `none`        | `none`, `beginning`, `timestamp`, `snapshot`|
| `seekTime`            |               | Epoch millis/RFC-3339 (if `seek=timestamp`) |
| `seekSnapshot`        |               | Snapshot resource (when `seek=snapshot`)    |
| `credentialsFile`     | ADC           | Path to service-account JSON (optional)     |
| `emulatorHost`        |               | e.g. `localhost:8085` for the emulator      |
| `returnImmediately`   | `false`       | Pub/Sub pull `returnImmediately`            |

**Restart behavior:** with `seek=none` (default), the subscription cursor is **not** rewound.
Unacked messages redeliver after a crash/watchdog restart — matching typical Dataproc workflow recovery.

## Structured Streaming (Java)

```java
Dataset<Row> messages = spark.readStream()
    .format("google-pubsub")
    .option("projectId", "my-project")
    .option("subscription", "my-subscription")
    .option("ackMode", "afterCommit")
    .load();

messages
    .selectExpr("messageId", "CAST(data AS STRING) AS payload", "publishTime")
    .writeStream()
    .format("parquet")
    .option("path", "gs://bucket/tables/event")
    .option("checkpointLocation", "gs://bucket/checkpoints/event")
    .start()
    .awaitTermination();
```

## Structured Streaming (Scala)

See [`examples/scala/StructuredStreamingExample.scala`](examples/scala/StructuredStreamingExample.scala).

## Structured Streaming (PySpark)

```python
messages = (
    spark.readStream.format("google-pubsub")
    .option("projectId", "my-project")
    .option("subscription", "my-subscription")
    .option("ackMode", "afterCommit")
    .load()
)
```

Full script: [`examples/python/structured_streaming_example.py`](examples/python/structured_streaming_example.py).

## DStreams shim (Legacy-compatible Java API)

```java
import io.github.juarezr.spark.pubsub.dstream.PubsubUtils;
import io.github.juarezr.spark.pubsub.dstream.SparkGCPCredentials;
import io.github.juarezr.spark.pubsub.dstream.SparkPubsubMessage;

SparkGCPCredentials credentials = SparkGCPCredentials.builder().build(); // ADC
JavaReceiverInputDStream<SparkPubsubMessage> stream = PubsubUtils.createStream(
    jssc, projectId, topic, subscription, credentials, StorageLevel.MEMORY_AND_DISK_SER());
```

Migration from Legacy: change the Maven/Gradle dependency and imports from
`org.apache.spark.streaming.pubsub.*` to `io.github.juarezr.spark.pubsub.dstream.*`.

> DStreams remain available on Spark 4.x but are **deprecated**. Prefer Structured Streaming for new work.

## Google Dataproc

1. Prefer `--packages` with the Maven coordinate so Google client dependencies resolve from Central.
   Alternatively, copy `*-all.jar` from a [GitHub Release](https://github.com/juarezr/spark-streaming-google-pubsub/releases) (or `mvn package`) to GCS and pass `--jars`.
2. Submit with Dataproc 2.3 (Spark 3.5 / Scala 2.12) or 3.0 (Spark 4.1 / Scala 2.13). Spark 4.2 is supported via the same `_2.13` coordinate on Apache Spark / other platforms until Dataproc ships it.
3. Grant the cluster service account `roles/pubsub.subscriber` (and publisher if needed).
4. Rely on the metadata server for ADC — do not ship JSON keys.

```bash
# Preferred: resolve the thin JAR and its Google client transitives
gcloud dataproc jobs submit spark \
  --cluster=my-cluster \
  --region=us-east4 \
  --packages=io.github.juarezr:spark-streaming-google-pubsub_2.12:0.3.0 \
  --class=com.example.MyApp \
  -- gs://my-bucket/apps/my-app.jar

# Alternative: single shaded JAR from GitHub Releases (or a local `mvn package`)
gcloud dataproc jobs submit spark \
  --cluster=my-cluster \
  --region=us-east4 \
  --jars=gs://my-bucket/jars/spark-streaming-google-pubsub_2.12-0.3.0-all.jar \
  --class=com.example.MyApp \
  -- gs://my-bucket/apps/my-app.jar
```

## Databricks

Add the Maven coordinate as a library on the cluster (`_2.12` or `_2.13` matching the runtime).
Configure a Databricks secret or instance profile / GCP service account so ADC works, then use the
same `.format("google-pubsub")` options as above. Use a durable `checkpointLocation` on cloud storage.

## Build and test locally

Requirements: JDK 11+ (17 recommended), Maven 3.9+.

```bash
# Spark 3.5 / Scala 2.12 (default)
mvn -Pspark35 clean verify

# Spark 4.1 / Scala 2.13 (published _2.13 baseline)
mvn -Pspark41 clean verify

# Spark 4.0 / 4.2 (same artifactId; CI-only profiles)
mvn -Pspark40 clean verify
mvn -Pspark42 clean verify

# Format check
mvn -Pspark35 spotless:check
```

### Unit tests

```bash
mvn -Pspark35 test
```

### Integration tests (Pub/Sub emulator)

```bash
docker run --rm -p 8085:8085 \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085

export PUBSUB_EMULATOR_HOST=localhost:8085
mvn -Pspark35 verify
```

### Manual test against real GCP (ADC)

```bash
gcloud auth application-default login
# or: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json

mvn -Pspark35 -DskipTests package

spark-submit \
  --class io.github.juarezr.spark.pubsub.examples.JavaStructuredStreamingExample \
  --jars target/spark-streaming-google-pubsub_2.12-0.3.0-SNAPSHOT-all.jar \
  examples/java/JavaStructuredStreamingExample.java \
  YOUR_PROJECT YOUR_SUBSCRIPTION /tmp/pubsub-cp /tmp/pubsub-out
```

(Use a compiled example JAR or paste the example into your application module.)

## Publishing

See [`docs/publishing-maven-central.md`](docs/publishing-maven-central.md).

## Reliability notes

- **`ackMode=afterCommit` (default):** messages are acknowledged after Spark commits the micro-batch.
  Failures before commit lead to redelivery (at-least-once).
- **`ackMode=early`:** ack soon after pull/store (Legacy-like). Faster ack release, higher loss risk on crash.
- Outstanding byte accounting prevents unbounded memory growth under backpressure.
  If Spark requests a new micro-batch without committing the previous one (`ackMode=afterCommit`),
  the connector nacks that pull and releases the byte charge so `maxBytesOutstanding` cannot stall
  empty pulls. Ack failures also release the charge (and nack best-effort) before Spark fails the batch.
- With `ackMode=afterCommit`, ack deadlines are extended periodically on the driver (about every
  `ackDeadlineSeconds / 3`) while the micro-batch is in flight, not only once at pull.
- Transient Pub/Sub errors are retried with exponential backoff.

You can monitor these with custom metrics on `StreamingQueryProgress` (Spark UI): last-pull size, payload bytes, outstanding payload bytes, batch ids, `pubsubRetryAttempts` (retries in this micro-batch), and `pubsubRetryAttemptsTotal` (retries since the stream started). They are **not** the Pub/Sub subscription backlog.

## Limitations

This connector is a **read-only Structured Streaming (micro-batch)** source. The following Spark features are not implemented:

- **Spark 4.1 Real-time Mode** — does not fit Pub/Sub’s lease/ack model (driver pull and payload-in-offset vs long-running executor `nextWithTimeout`). That API is built around log sources such as Kafka.
- **Trigger.AvailableNow** (“drain then stop”) — a subscription has no durable log-end offset.
- **Continuous Processing** — experimental; Spark recommends Real-time Mode instead. Same lease-model mismatch.
- **Streaming sink and batch `spark.read`** — a subscription is a queue, not a table. Rewind/replay uses explicit `seek` / snapshot **options**, not a batch scan.
- **SQL filter pushdown that seeks** — a `WHERE publishTime >= …` must not rewind a **shared** subscription. Filter on attributes with a GCP subscription filter; rewind with explicit `seek`.
- **Spark admission control (`ReadLimit`)** — not implemented; `maxMessagesPerPull` and `maxBytesOutstanding` already bound each pull.

## License

GPL-3.0 — see [`LICENSE`](LICENSE).
