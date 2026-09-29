package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PubSubClientTest {

  @Test
  void asStringFormatsDuration() {
    assertEquals("1m30s", PubSubClient.asString(Duration.ofSeconds(90)));
    assertEquals(
        "2h5m3s", PubSubClient.asString(Duration.ofHours(2).plusMinutes(5).plusSeconds(3)));
  }

  @Test
  void acknowledgeInvokesConsumerAckAndRemovesEntry() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    setConsumers(client, new ConcurrentHashMap<>(Collections.singletonMap("ack-1", consumer)));

    client.acknowledge(List.of("ack-1"));

    verify(consumer).ack();
    assertTrue(getConsumers(client).isEmpty());
  }

  @Test
  void nackInvokesConsumerNack() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    setConsumers(client, new ConcurrentHashMap<>(Collections.singletonMap("ack-2", consumer)));

    client.nack(List.of("ack-2"));

    verify(consumer).nack();
  }

  @Test
  void replyIgnoresNullEmptyAndUnknownAckIds() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    setConsumers(client, new ConcurrentHashMap<>(Collections.singletonMap("known", consumer)));

    client.acknowledge(null);
    client.acknowledge(Collections.emptyList());
    client.acknowledge(List.of("missing"));

    verify(consumer, never()).ack();
    verify(consumer, never()).nack();
  }

  @Test
  void pollReturnsEmptyWhenQueueTimesOut() throws Exception {
    PubSubClient client = newPubSubClientReadyForPoll();
    setField(client, "queue", new LinkedBlockingQueue<PubSubClient.HeldMessage>());

    List<PulledMessage> messages = client.poll(Duration.ofMillis(50));

    assertTrue(messages.isEmpty());
  }

  @Test
  void pollDrainsQueueUpToLimitNextPollCap() throws Exception {
    PubSubClient client = newPubSubClientReadyForPoll();
    LinkedBlockingQueue<PubSubClient.HeldMessage> queue = new LinkedBlockingQueue<>();
    queue.put(held("a", 1000L));
    queue.put(held("b", 2000L));
    queue.put(held("c", 3000L));
    setField(client, "queue", queue);
    setField(client, "queuedBytes", new AtomicLong(0));

    client.limitNextPoll(2);
    List<PulledMessage> messages = client.poll(Duration.ofSeconds(1));

    assertEquals(2, messages.size());
    assertEquals("a", messages.get(0).messageId());
    assertEquals("b", messages.get(1).messageId());
    assertEquals(1, queue.size());
    assertEquals(2000L, client.lastSeenNewestPublishMillis());
  }

  @Test
  void closeNacksQueuedMessagesWithoutStartingSubscriber() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    LinkedBlockingQueue<PubSubClient.HeldMessage> queue = new LinkedBlockingQueue<>();
    queue.put(held("m1", 0L, consumer));
    setField(client, "queue", queue);
    setField(client, "stopped", new AtomicBoolean(false));
    setField(client, "consumers", new ConcurrentHashMap<String, AckReplyConsumer>());
    setField(client, "queuedBytes", new AtomicLong(1));

    client.close();

    verify(consumer).nack();
    assertTrue(queue.isEmpty());
  }

  private static PubSubClient newPubSubClientReadyForPoll() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    setField(client, "subscriber", mock(Subscriber.class));
    setField(client, "stopped", new AtomicBoolean(false));
    setField(client, "queuedBytes", new AtomicLong(0));
    setField(client, "consumers", new ConcurrentHashMap<String, AckReplyConsumer>());
    return client;
  }

  private static PubSubClient.HeldMessage held(String id, long publishMillis) {
    return held(id, publishMillis, mock(AckReplyConsumer.class));
  }

  private static PubSubClient.HeldMessage held(
      String id, long publishMillis, AckReplyConsumer consumer) {
    PulledMessage message =
        new PulledMessage(id, new byte[] {1}, Collections.emptyMap(), publishMillis, "", id);
    return new PubSubClient.HeldMessage(message, consumer);
  }

  private static PubSubConfig minimalConfig() {
    return PubSubConfig.builder().projectId("p").subscription("s").build();
  }

  @SuppressWarnings("unchecked")
  private static ConcurrentHashMap<String, AckReplyConsumer> getConsumers(PubSubClient client)
      throws Exception {
    return (ConcurrentHashMap<String, AckReplyConsumer>) getField(client, "consumers");
  }

  private static void setConsumers(
      PubSubClient client, ConcurrentHashMap<String, AckReplyConsumer> consumers) throws Exception {
    setField(client, "consumers", consumers);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Object getField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }
}
