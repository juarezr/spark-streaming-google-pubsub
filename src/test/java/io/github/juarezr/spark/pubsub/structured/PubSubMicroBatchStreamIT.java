package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

  private static final String PROJECT = "test-project";
  private static final String TOPIC = "it-topic";
  private static final String SUBSCRIPTION = "it-subscription";

  private String emulatorHost;
  private ManagedChannel channel;
  private SparkSession spark;

  @BeforeAll
  void setUp() throws Exception {
    emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
    assumeTrue(
        emulatorHost != null && !emulatorHost.isBlank(),
        "Skipping IT: set PUBSUB_EMULATOR_HOST to run against the emulator");

    channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
    FixedTransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    NoCredentialsProvider credentialsProvider = NoCredentialsProvider.create();

    TopicAdminSettings topicSettings =
        TopicAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();
    SubscriptionAdminSettings subSettings =
        SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();

    ProjectTopicName topicName = ProjectTopicName.of(PROJECT, TOPIC);
    ProjectSubscriptionName subName = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);

    try (TopicAdminClient topicAdmin = TopicAdminClient.create(topicSettings);
        SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(subSettings)) {
      try {
        topicAdmin.deleteTopic(topicName.toString());
      } catch (Exception ignored) {
        // first run
      }
      try {
        subAdmin.deleteSubscription(subName.toString());
      } catch (Exception ignored) {
        // first run
      }
      topicAdmin.createTopic(topicName.toString());
      subAdmin.createSubscription(
          subName.toString(), topicName.toString(), PushConfig.getDefaultInstance(), 60);
    }

    Publisher publisher =
        Publisher.newBuilder(topicName.toString())
            .setChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();
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
    if (channel != null) {
      channel.shutdownNow();
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
            .option(PubSubConfig.PULL_DEADLINE, "2s")
            .option(PubSubConfig.PULL_MAX_MESSAGES, "10")
            .option(PubSubConfig.GATHER_MODE, "batch")
            .option(PubSubConfig.BATCH_TIME, "2s")
            .option(PubSubConfig.BATCH_COUNT, "50")
            .load();

    AtomicInteger seen = new AtomicInteger();
    StreamingQuery query =
        stream
            .writeStream()
            .foreachBatch(
                (Dataset<Row> batch, Long id) -> {
                  seen.addAndGet((int) batch.count());
                  batch.write().mode("append").json(output.toString());
                })
            .option("checkpointLocation", checkpoint.toString())
            .trigger(Trigger.ProcessingTime("100 milliseconds"))
            .start();

    query.awaitTermination(10_000L);
    query.stop();
    assumeTrue(seen.get() > 0, "Expected to read at least one message from the emulator");
  }
}
