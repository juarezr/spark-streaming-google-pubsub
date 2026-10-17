package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
  void pollWaitsWithinTimeoutForLimitNextPollCap() throws Exception {
    PubSubClient client = newPubSubClientReadyForPoll();
    LinkedBlockingQueue<PubSubClient.HeldMessage> queue = new LinkedBlockingQueue<>();
    queue.put(held("a", 1000L));
    setField(client, "queue", queue);
    setField(client, "queuedBytes", new AtomicLong(0));

    Thread delayed =
        new Thread(
            () -> {
              try {
                TimeUnit.MILLISECONDS.sleep(100);
                queue.put(held("b", 2000L));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    delayed.start();

    client.limitNextPoll(2);
    List<PulledMessage> messages = client.poll(Duration.ofSeconds(2));

    delayed.join(TimeUnit.SECONDS.toMillis(5));
    assertEquals(2, messages.size());
    assertEquals("a", messages.get(0).messageId());
    assertEquals("b", messages.get(1).messageId());
  }

  @Test
  void pollThrowsWhenFatalConnectionRecorded() throws Exception {
    PubSubConfig config = minimalConfig();
    PubSubClient client = newPubSubClientReadyForPoll();
    setField(client, "queue", new LinkedBlockingQueue<PubSubClient.HeldMessage>());

    PubSubConnectionErrors connectionErrors = new PubSubConnectionErrors(config.subscriptionPath());
    setField(client, "connectionErrors", connectionErrors);

    StatusRuntimeException failure =
        new StatusRuntimeException(Status.NOT_FOUND.withDescription("subscription"));
    connectionErrors.failed(ApiService.State.FAILED, failure);

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> client.poll(Duration.ofMillis(10)));

    assertTrue(ex.getMessage().contains("CONNECTION"));
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

  @Test
  void requeueReturnsOverflowOnTheNextPollWithinOneBatch() throws Exception {
    int oneMib = 1024 * 1024;
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").batchSize(oneMib).build();
    PubSubClient client = new PubSubClient(config);
    setField(client, "subscriber", mock(Subscriber.class));
    setField(client, "stopped", new AtomicBoolean(false));
    ConcurrentHashMap<String, AckReplyConsumer> consumers = new ConcurrentHashMap<>();
    AckReplyConsumer firstConsumer = mock(AckReplyConsumer.class);
    AckReplyConsumer secondConsumer = mock(AckReplyConsumer.class);
    consumers.put("a", firstConsumer);
    consumers.put("b", secondConsumer);
    setField(client, "consumers", consumers);
    LinkedBlockingQueue<PubSubClient.HeldMessage> queue = new LinkedBlockingQueue<>();
    queue.put(heldBytes("a", 1_000L, firstConsumer, new byte[oneMib]));
    queue.put(heldBytes("b", 2_000L, secondConsumer, new byte[oneMib]));
    setField(client, "queue", queue);
    setField(client, "queuedBytes", new AtomicLong(2L * oneMib));

    List<PulledMessage> polled = client.poll(Duration.ofSeconds(1));

    assertEquals(2, polled.size());
    assertEquals(0L, client.queuedBytes());
    client.requeue(List.of(polled.get(1)));

    assertEquals(oneMib, client.queuedBytes());
    assertTrue(client.queuedBytes() <= config.batchSize());
    verify(firstConsumer, never()).nack();
    verify(secondConsumer, never()).nack();

    List<PulledMessage> next = client.poll(Duration.ofMillis(200));

    assertEquals(1, next.size());
    assertEquals("b", next.get(0).messageId());
    assertEquals(0L, client.queuedBytes());
  }

  @Test
  void climbPassesTheBaselineAndLeavesOutstandingAtTwiceTheBaseline() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    long baseline = 64L * 1024L * 1024L;
    long limit = baseline * 2L;
    setField(client, "memoryBudget", baseline);
    setField(client, "baselineBytes", baseline);
    setField(client, "flowSeedBytes", baseline);
    setField(client, "memoryLimitBytes", limit);
    setField(client, "calmStreak", DriverHeap.GROW_STREAK);

    client.noteGather(20L, DriverHeap.GROW_MIN_AVAILABLE, baseline);

    assertEquals(baseline + baseline / 8L, client.memoryBudget());
    assertEquals(baseline, client.memoryBaseline());
    assertEquals(limit, client.memoryLimit());
  }

  @Test
  void retargetFollowsAHigherCapWhenNothingIsHeld() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    long baseline = 64L * 1024L * 1024L;
    long raised = baseline + baseline / 8L;
    armFlow(client, baseline, baseline, raised, 0L, 0L);

    client.retargetFlow();

    assertEquals(raised * 2L, client.flowOutstandingBytes());
    assertEquals(baseline, client.memoryBaseline());
  }

  @Test
  void retargetWaitsWhileTheQueueHoldsBytes() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    Subscriber subscriber = mock(Subscriber.class);
    setField(client, "subscriber", subscriber);
    long baseline = 64L * 1024L * 1024L;
    armFlow(client, baseline, baseline, baseline + baseline / 8L, 1L, 0L);

    client.retargetFlow();

    verify(subscriber, never()).stopAsync();
    assertEquals(baseline * 2L, client.flowOutstandingBytes());
    assertEquals(baseline, client.memoryBaseline());
  }

  @Test
  void retargetWaitsWhileBytesAreStillOutstanding() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    Subscriber subscriber = mock(Subscriber.class);
    setField(client, "subscriber", subscriber);
    long baseline = 64L * 1024L * 1024L;
    armFlow(client, baseline, baseline, baseline + baseline / 8L, 0L, 1L);

    client.retargetFlow();

    verify(subscriber, never()).stopAsync();
    assertEquals(baseline * 2L, client.flowOutstandingBytes());
  }

  @Test
  void retargetFollowsAHalvedCap() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    long baseline = 64L * 1024L * 1024L;
    long live = baseline + baseline / 8L;
    long halved = live / 2L;
    armFlow(client, baseline, live, halved, 0L, 0L);

    client.retargetFlow();

    assertEquals(halved * 2L, client.flowOutstandingBytes());
    assertEquals(baseline, client.memoryBaseline());
  }

  @Test
  void retargetSkipsWhenTheCapHasNotMoved() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    long baseline = 64L * 1024L * 1024L;
    armFlow(client, baseline, baseline, baseline, 0L, 0L);

    client.retargetFlow();

    assertEquals(baseline * 2L, client.flowOutstandingBytes());
    assertEquals(baseline, client.memoryBaseline());
  }

  @Test
  void reserveDoesNotPassTheCap() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    setField(client, "queuedBytes", new AtomicLong(0));
    setField(client, "memoryBudget", 1000L);

    assertTrue(client.tryReserve(400));
    assertTrue(client.tryReserve(400));
    assertEquals(800L, client.queuedBytes());
    assertFalse(client.tryReserve(300));
    assertEquals(800L, client.queuedBytes());
    assertTrue(client.tryReserve(200));
    assertEquals(1000L, client.queuedBytes());
    assertFalse(client.tryReserve(1));
    assertEquals(1000L, client.queuedBytes());
  }

  @Test
  void emptyQueueTakesOneMessageLargerThanTheCap() throws Exception {
    PubSubClient client = new PubSubClient(minimalConfig());
    setField(client, "queuedBytes", new AtomicLong(0));
    setField(client, "memoryBudget", 1000L);

    assertTrue(client.tryReserve(1500));
    assertEquals(1500L, client.queuedBytes());
    assertFalse(client.tryReserve(1));
    assertEquals(1500L, client.queuedBytes());
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
    return heldBytes(id, publishMillis, consumer, new byte[] {1});
  }

  private static PubSubClient.HeldMessage heldBytes(
      String id, long publishMillis, AckReplyConsumer consumer, byte[] data) {
    PulledMessage message =
        new PulledMessage(id, data, Collections.emptyMap(), publishMillis, "", id);
    return new PubSubClient.HeldMessage(message, consumer);
  }

  private static void armFlow(
      PubSubClient client, long baseline, long flowSeed, long budget, long queued, long outstanding)
      throws Exception {
    setField(client, "baselineBytes", baseline);
    setField(client, "flowSeedBytes", flowSeed);
    setField(client, "memoryBudget", budget);
    setField(client, "queuedBytes", new AtomicLong(queued));
    setField(client, "outstandingBytes", new AtomicLong(outstanding));
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
