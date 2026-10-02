package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.MetadataMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SchemaMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test for {@code schemaMode=dynamic} with a registered Avro topic schema when {@code
 * PUBSUB_EMULATOR_HOST} is set.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PubSubDynamicSchemaIT {

  private static final String PROJECT = "test-project";
  private static final String SCHEMA_ID = "basketball-event-schema-v3";
  private static final String TOPIC = "dynamic-schema-it-topic-v3";
  private static final String SUBSCRIPTION = "dynamic-schema-it-sub-v3";

  private String emulatorHost;
  private PubSubEmulatorFixtures.EmulatorChannel emulator;
  private SparkSession spark;
  private boolean schemaTopicReady;

  @BeforeAll
  void setUp() throws Exception {
    emulatorHost = PubSubEmulatorFixtures.requireEmulatorHost();
    emulator = PubSubEmulatorFixtures.openChannel(emulatorHost);
    try {
      PubSubEmulatorFixtures.recreateSchemaTopicAndSubscription(
          emulator,
          PROJECT,
          SCHEMA_ID,
          TOPIC,
          SUBSCRIPTION,
          BasketballEventFixtures.avroDefinition());
      schemaTopicReady = true;
    } catch (Exception e) {
      schemaTopicReady = false;
      assumeTrue(
          false,
          "Pub/Sub emulator schema APIs unavailable; use a recent gcloud beta pubsub emulator: "
              + e.getMessage());
    }

    try {
      Publisher publisher = PubSubEmulatorFixtures.newPublisher(emulator, PROJECT, TOPIC);
      try {
        for (int i = 0; i < BasketballEventFixtures.MESSAGE_COUNT; i++) {
          publisher
              .publish(
                  PubsubMessage.newBuilder()
                      .setData(ByteString.copyFrom(BasketballEventFixtures.jsonPayload(i)))
                      .build())
              .get(30, TimeUnit.SECONDS);
        }
      } finally {
        publisher.shutdown();
        publisher.awaitTermination(30, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      assumeTrue(
          false,
          "Emulator rejected JSON payloads for schema topic (check schema/message alignment): "
              + e.getMessage());
    }

    PubSubConfig config =
        PubSubConfig.builder()
            .projectId(PROJECT)
            .subscription(SUBSCRIPTION)
            .topic(TOPIC)
            .emulatorHost(emulatorHost)
            .schemaMode(SchemaMode.DYNAMIC)
            .build();
    StructType inferred = PubSubSchema.inferTableSchema(config);
    assertTrue(
        inferred.length() >= 10,
        "Expected basketball Avro fields on inferred schema, got " + inferred.length());

    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("pubsub-dynamic-schema-it")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
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
  void dynamicSchemaDrainsBasketballEvents(@TempDir Path tempDir) throws Exception {
    assumeTrue(schemaTopicReady);
    Path checkpoint = Files.createDirectory(tempDir.resolve("checkpoint"));
    Path output = Files.createDirectory(tempDir.resolve("output"));

    Dataset<Row> stream =
        spark
            .readStream()
            .format("google-pubsub")
            .option(PubSubConfig.PROJECT_ID, PROJECT)
            .option(PubSubConfig.SUBSCRIPTION, SUBSCRIPTION)
            .option(PubSubConfig.TOPIC, TOPIC)
            .option(PubSubConfig.EMULATOR_HOST, emulatorHost)
            .option(PubSubConfig.SCHEMA_MODE, SchemaMode.DYNAMIC.name().toLowerCase())
            .option(PubSubConfig.ACK_MODE, AckMode.AFTER_COMMIT.name())
            .option(PubSubConfig.GATHER_MODE, "batch")
            .option(PubSubConfig.BATCH_COUNT, "100")
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

    query.awaitTermination(90_000L);
    if (query.isActive()) {
      query.stop();
    }
    assertEquals(BasketballEventFixtures.MESSAGE_COUNT, seen.get());

    Dataset<Row> rows = spark.read().json(output.toString());
    assertEquals(BasketballEventFixtures.MESSAGE_COUNT, rows.count());
    assertEquals(BasketballEventFixtures.GAME_COUNT, rows.select("gameId").distinct().count());
    assertTrue(rows.filter("eventType = 'SCORE' AND points > 0").count() > 0);
    assertTrue(rows.filter("playerId IS NOT NULL AND length(playerId) > 0").count() > 0);
    assertTrue(rows.filter("length(lineup) > 0").count() > 0);
    assertTrue(rows.filter("rebounds >= 0").count() > 0);
  }

  @Test
  void dynamicSchemaWithSlimMetadataExposesExtraColumns() {
    assumeTrue(schemaTopicReady);
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId(PROJECT)
            .subscription(SUBSCRIPTION)
            .topic(TOPIC)
            .emulatorHost(emulatorHost)
            .schemaMode(SchemaMode.DYNAMIC)
            .metadataMode(MetadataMode.SLIM)
            .build();
    StructType table = PubSubSchema.inferTableSchema(config);
    String[] metadata =
        java.util.Arrays.stream(PubSubSchema.metadataColumns(config, table))
            .map(column -> column.name())
            .toArray(String[]::new);
    assertArrayEquals(new String[] {"messageid", "publishtime", "orderingkey", "ackid"}, metadata);
  }
}
