package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration tests for {@link PubSubClient.PubSubMessageReceiver} (via {@link PubSubClient}) when
 * {@code PUBSUB_EMULATOR_HOST} is set.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PubSubMessageReceiverIT {

  private static final String PROJECT = "test-project";
  private static final String TOPIC = "pubsub-receiver-it-topic";
  private static final String SUBSCRIPTION = "pubsub-receiver-it-sub";

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
  void receiveMessageMapsAttributesOrderingKeyAndBody() throws Exception {
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
      publishWithOrdering(
          PubsubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8("payload-with-meta"))
              .putAttributes("source", "receiver-it")
              .setOrderingKey("order-key-1")
              .build());

      List<PulledMessage> polled = pollUntilCount(client, 1, Duration.ofSeconds(30));
      assertEquals(1, polled.size());
      PulledMessage message = polled.get(0);
      assertEquals("payload-with-meta", new String(message.data()));
      assertEquals("receiver-it", message.attributes().get("source"));
      assertEquals("order-key-1", message.orderingKey());
      assertEquals(message.messageId(), message.ackId());
      assertTrue(message.publishTimeMillis() > 0L);

      client.acknowledge(List.of(message.ackId()));
    } finally {
      client.close();
    }
  }

  @SuppressWarnings("null")
  @Test
  void limitNextPollCapsDrainWithoutLosingMessages() throws Exception {
    PubSubEmulatorFixtures.recreateSubscriptionOnTopic(emulator, PROJECT, TOPIC, SUBSCRIPTION);

    PubSubConfig config =
        PubSubConfig.builder()
            .projectId(PROJECT)
            .subscription(SUBSCRIPTION)
            .emulatorHost(emulatorHost)
            .batchCount(100)
            .build();
    PubSubClient client = new PubSubClient(config);
    try {
      client.start();
      publish(
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("a")).build(),
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("b")).build(),
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("c")).build());

      client.limitNextPoll(2);
      List<PulledMessage> capped = client.poll(Duration.ofSeconds(30));
      assertEquals(2, capped.size());

      List<PulledMessage> remainder = client.poll(Duration.ofSeconds(30));
      assertEquals(1, remainder.size());

      client.acknowledge(PulledMessage.ackIds(capped));
      client.acknowledge(PulledMessage.ackIds(remainder));
    } finally {
      client.close();
    }
  }

  private static List<PulledMessage> pollUntilCount(
      PubSubClient client, int count, Duration timeout) {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    List<PulledMessage> collected = new ArrayList<>();
    while (System.nanoTime() < deadlineNanos && collected.size() < count) {
      collected.addAll(client.poll(Duration.ofSeconds(2)));
    }
    return collected;
  }

  private void publish(PubsubMessage... messages) throws Exception {
    publishWithPublisher(PubSubEmulatorFixtures.newPublisher(emulator, PROJECT, TOPIC), messages);
  }

  private void publishWithOrdering(PubsubMessage... messages) throws Exception {
    publishWithPublisher(
        PubSubEmulatorFixtures.newOrderingPublisher(emulator, PROJECT, TOPIC), messages);
  }

  private static void publishWithPublisher(Publisher publisher, PubsubMessage... messages)
      throws Exception {
    try {
      for (PubsubMessage message : messages) {
        publisher.publish(message).get(30, TimeUnit.SECONDS);
      }
    } finally {
      publisher.shutdown();
      publisher.awaitTermination(30, TimeUnit.SECONDS);
    }
  }
}
