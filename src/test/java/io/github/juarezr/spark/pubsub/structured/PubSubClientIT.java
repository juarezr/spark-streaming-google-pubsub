package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SeekMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Integration tests for {@link PubSubClient} against the Pub/Sub emulator. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PubSubClientIT {

  private static final String PROJECT = "test-project";
  private static final String TOPIC = "pubsub-client-it-topic";
  private static final String SUBSCRIPTION = "pubsub-client-it-sub";

  private String emulatorHost;
  private PubSubEmulatorFixtures.EmulatorChannel emulator;

  @BeforeAll
  void setUp() throws Exception {
    emulatorHost = PubSubEmulatorFixtures.requireEmulatorHost();
    emulator = PubSubEmulatorFixtures.openChannel(emulatorHost);
    PubSubEmulatorFixtures.recreateTopicAndSubscription(emulator, PROJECT, TOPIC, SUBSCRIPTION);
  }

  @AfterAll
  void tearDown() {
    if (emulator != null) {
      emulator.close();
    }
  }

  @SuppressWarnings("null")
  @Test
  void pollReceivesPublishedMessagesAndAcknowledgeClearsConsumers() throws Exception {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId(PROJECT)
            .subscription(SUBSCRIPTION)
            .emulatorHost(emulatorHost)
            .batchCount(10)
            .build();
    PubSubClient client = new PubSubClient(config);
    try {
      client.start();
      publishMessages("alpha", "beta", "gamma");
      List<PulledMessage> polled = pollUntilCount(client, 3, Duration.ofSeconds(30));
      assertEquals(3, polled.size());
      List<String> bodies =
          polled.stream().map(m -> new String(m.data())).sorted().collect(Collectors.toList());
      assertEquals(List.of("alpha", "beta", "gamma"), bodies);

      client.acknowledge(polled.stream().map(PulledMessage::ackId).collect(Collectors.toList()));
      assertTrue(client.poll(Duration.ofMillis(200)).isEmpty());
    } finally {
      client.close();
    }
  }

  @SuppressWarnings("null")
  @Test
  void seekBeginningRewindsSubscription() throws Exception {
    publishMessages("before-seek");

    PubSubConfig seekConfig =
        PubSubConfig.builder()
            .projectId(PROJECT)
            .subscription(SUBSCRIPTION)
            .emulatorHost(emulatorHost)
            .seekMode(SeekMode.BEGINNING)
            .batchCount(10)
            .build();
    PubSubClient client = new PubSubClient(seekConfig);
    try {
      client.start();
      List<PulledMessage> polled = client.poll(Duration.ofSeconds(30));
      assertTrue(polled.size() >= 1);
      client.acknowledge(polled.stream().map(PulledMessage::ackId).collect(Collectors.toList()));
    } finally {
      client.close();
    }
  }

  /** Collect messages across multiple polls until {@code count} or {@code timeout}. */
  private static List<PulledMessage> pollUntilCount(
      PubSubClient client, int count, Duration timeout) {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    List<PulledMessage> collected = new ArrayList<>();
    while (System.nanoTime() < deadlineNanos && collected.size() < count) {
      collected.addAll(client.poll(Duration.ofSeconds(2)));
    }
    return collected;
  }

  private void publishMessages(String... payloads) throws Exception {
    Publisher publisher = PubSubEmulatorFixtures.newPublisher(emulator, PROJECT, TOPIC);
    try {
      for (String payload : payloads) {
        publisher
            .publish(PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(payload)).build())
            .get(30, TimeUnit.SECONDS);
      }
    } finally {
      publisher.shutdown();
      publisher.awaitTermination(30, TimeUnit.SECONDS);
    }
  }
}
