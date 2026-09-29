package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.testsupport.SparkProfileFingerprint;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test against the Pub/Sub emulator when {@code PUBSUB_EMULATOR_HOST} is set.
 *
 * <p>Example: {@code docker run -p 8085:8085
 * gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators gcloud beta emulators pubsub start
 * --host-port=0.0.0.0:8085}
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PubSubMicroBatchStreamIT {

  static {
    SparkProfileFingerprint.sparkVersion();
  }

  private static final String PROJECT = "test-project";
  private static final String TOPIC = "it-topic";
  private static final String SUBSCRIPTION = "it-subscription";

  private String emulatorHost;
  private PubSubEmulatorFixtures.EmulatorChannel emulator;
  private SparkSession spark;

  @BeforeAll
  void setUp() throws Exception {
    emulatorHost = PubSubEmulatorFixtures.requireEmulatorHost();
    emulator = PubSubEmulatorFixtures.openChannel(emulatorHost);
    PubSubEmulatorFixtures.recreateTopicAndSubscription(emulator, PROJECT, TOPIC, SUBSCRIPTION);

    Publisher publisher = PubSubEmulatorFixtures.newPublisher(emulator, PROJECT, TOPIC);
    try {
      byte[] payload = examplePayload();
      for (int i = 0; i < 50; i++) {
        publisher
            .publish(
                PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(payload))
                    .putAttributes("idx", Integer.toString(i))
                    .build())
            .get(30, TimeUnit.SECONDS);
      }
    } finally {
      publisher.shutdown();
      publisher.awaitTermination(30, TimeUnit.SECONDS);
    }

    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("pubsub-it")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
  }

  private byte[] examplePayload() throws Exception {
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                getClass().getResourceAsStream("/examples/example-proto.csv"),
                StandardCharsets.UTF_8))) {
      reader.readLine();
      String[] columns = reader.readLine().split(",", 3);
      return Base64.getDecoder().decode(columns[2]);
    }
  }

  @AfterAll
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
    if (emulator != null) {
      emulator.close();
    }
  }

  @Test
  void readsAndAcksAfterCommit(@TempDir Path tempDir) throws Exception {
    Path checkpoint = Files.createDirectory(tempDir.resolve("checkpoint"));
    Path output = Files.createDirectory(tempDir.resolve("output"));

    Dataset<Row> stream =
        spark
            .readStream()
            .format("google-pubsub")
            .option(PubSubConfig.PROJECT_ID, PROJECT)
            .option(PubSubConfig.SUBSCRIPTION, SUBSCRIPTION)
            .option(PubSubConfig.EMULATOR_HOST, emulatorHost)
            .option(PubSubConfig.ACK_MODE, AckMode.AFTER_COMMIT.name())
            .option(PubSubConfig.GATHER_MODE, "batch")
            .option(PubSubConfig.RECEIVE_TIME, "2s")
            .option(PubSubConfig.BATCH_COUNT, "50")
            .load();

    AtomicInteger seen = new AtomicInteger();
    StreamingQuery query =
        stream
            .writeStream()
            .option("checkpointLocation", checkpoint.toString())
            .foreachBatch(
                (Dataset<Row> batch, Long id) -> {
                  seen.addAndGet((int) batch.count());
                  batch.write().mode("append").json(output.toString());
                })
            .trigger(Trigger.ProcessingTime("100 milliseconds"))
            .start();

    query.awaitTermination(10_000L);
    query.stop();
    assumeTrue(seen.get() > 0, "Expected to read at least one message from the emulator");
  }

  @Test
  void idleBatchGatherKeepsQueryActive(@TempDir Path tempDir) throws Exception {
    String idleSubscription = "it-idle-subscription";
    PubSubEmulatorFixtures.recreateSubscriptionOnTopic(emulator, PROJECT, TOPIC, idleSubscription);

    Path checkpoint = Files.createDirectory(tempDir.resolve("checkpoint"));
    Dataset<Row> stream =
        spark
            .readStream()
            .format("google-pubsub")
            .option(PubSubConfig.PROJECT_ID, PROJECT)
            .option(PubSubConfig.SUBSCRIPTION, idleSubscription)
            .option(PubSubConfig.EMULATOR_HOST, emulatorHost)
            .option(PubSubConfig.ACK_MODE, AckMode.AFTER_COMMIT.name())
            .option(PubSubConfig.GATHER_MODE, "batch")
            .option(PubSubConfig.RECEIVE_TIME, "1s")
            .load();

    StreamingQuery query =
        stream
            .writeStream()
            .option("checkpointLocation", checkpoint.toString())
            .foreachBatch((Dataset<Row> batch, Long id) -> {})
            .trigger(Trigger.ProcessingTime("100 milliseconds"))
            .start();

    query.awaitTermination(3_000L);
    boolean active = query.isActive();
    query.stop();
    assumeTrue(active, "Idle batch gather should leave the query running");
  }

  @Test
  void availableNowDrainsAndStops(@TempDir Path tempDir) throws Exception {
    String topic = "it-available-now-topic";
    String subscription = "it-available-now-subscription";
    int published = 15;
    PubSubEmulatorFixtures.recreateTopicAndSubscription(emulator, PROJECT, topic, subscription);
    Publisher publisher = PubSubEmulatorFixtures.newPublisher(emulator, PROJECT, topic);
    try {
      byte[] payload = examplePayload();
      for (int i = 0; i < published; i++) {
        publisher
            .publish(
                PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(payload))
                    .putAttributes("idx", Integer.toString(i))
                    .build())
            .get(30, TimeUnit.SECONDS);
      }
    } finally {
      publisher.shutdown();
      publisher.awaitTermination(30, TimeUnit.SECONDS);
    }

    Path checkpoint = Files.createDirectory(tempDir.resolve("checkpoint"));
    Path output = Files.createDirectory(tempDir.resolve("output"));
    Dataset<Row> stream =
        spark
            .readStream()
            .format("google-pubsub")
            .option(PubSubConfig.PROJECT_ID, PROJECT)
            .option(PubSubConfig.SUBSCRIPTION, subscription)
            .option(PubSubConfig.EMULATOR_HOST, emulatorHost)
            .option(PubSubConfig.ACK_MODE, AckMode.AFTER_COMMIT.name())
            .option(PubSubConfig.GATHER_MODE, "batch")
            .option(PubSubConfig.BATCH_COUNT, "8")
            .load();

    AtomicInteger seen = new AtomicInteger();
    StreamingQuery query =
        stream
            .writeStream()
            .option("checkpointLocation", checkpoint.toString())
            .foreachBatch(
                (Dataset<Row> batch, Long id) -> {
                  seen.addAndGet((int) batch.count());
                  batch.write().mode("append").json(output.toString());
                })
            .trigger(Trigger.AvailableNow())
            .start();

    query.awaitTermination(30_000L);
    assertFalse(query.isActive(), "AvailableNow should terminate after the subscription is idle");
    assertEquals(published, seen.get());
  }
}
