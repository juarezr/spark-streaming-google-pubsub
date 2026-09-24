package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.junit.jupiter.api.Test;

class PubSubGatherTest {

  @Test
  void emptyPullKeepsTheCurrentOffset() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset initial = stream.initialOffset();
    Offset latest = stream.latestOffset();

    assertEquals(initial, latest);
    assertEquals(
        "-", stream.metrics(Optional.empty()).get(PubSubSourceMetrics.LAST_PULL_MESSAGE_AGE_MS));
    assertFalse(stream.firstBatchLogged());
    assertNull(stream.lastGatheredWindow());
    verify(client, times(1)).poll(any(Duration.class));
  }

  @Test
  void firstNonEmptyGatherRecordsPublishTimeWindow() {
    PubSubClient client = mock(PubSubClient.class);
    PulledMessage older =
        new PulledMessage("old", new byte[] {1}, Collections.emptyMap(), 1_000L, "", "ack-old");
    PulledMessage newer =
        new PulledMessage("new", new byte[] {1}, Collections.emptyMap(), 4_000L, "", "ack-new");
    when(client.poll(any(Duration.class))).thenReturn(List.of(older, newer));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    stream.latestOffset();
    PublishTimeWindow window = stream.lastGatheredWindow();

    assertTrue(stream.firstBatchLogged());
    assertEquals(0L, stream.lastGatheredBatchId());
    assertEquals(1_000L, window.oldestMillis());
    assertEquals(4_000L, window.newestMillis());
    assertEquals(2, window.messageCount());

    stream.commit(stream.reportLatestOffset());
    assertEquals(1_000L, stream.lastGatheredWindow().oldestMillis());
    assertEquals(4_000L, stream.lastGatheredWindow().newestMillis());
  }

  @Test
  void pullReportsNewestMessageAge() {
    PubSubClient client = mock(PubSubClient.class);
    PulledMessage older =
        new PulledMessage("old", new byte[] {1}, Collections.emptyMap(), 1_000L, "", "ack-old");
    PulledMessage newer =
        new PulledMessage("new", new byte[] {1}, Collections.emptyMap(), 4_000L, "", "ack-new");
    when(client.poll(any(Duration.class))).thenReturn(List.of(older, newer));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    long before = System.currentTimeMillis();
    stream.latestOffset();
    long after = System.currentTimeMillis();
    long age =
        Long.parseLong(
            stream.metrics(Optional.empty()).get(PubSubSourceMetrics.LAST_PULL_MESSAGE_AGE_MS));

    assertTrue(age >= before - 4_000L);
    assertTrue(age <= after - 4_000L);
  }

  @Test
  void batchGatherUsesThreePullsToReachThreeThousandMessages() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class)))
        .thenReturn(messages(0, 1000))
        .thenReturn(messages(1000, 1000))
        .thenReturn(messages(2000, 1000));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackMode(AckMode.EARLY)
            .batchCount(3000)
            .receiveTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);

    assertEquals(1, partitions.length);
    assertEquals(3000, ((PubSubInputPartition) partitions[0]).messages().size());
    verify(client, times(3)).poll(any(Duration.class));
  }

  @Test
  void latestOffsetWithSameStartStaysIdempotent() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 2));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset first = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());
    Offset repeated = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());

    assertEquals(first, repeated);
    verify(client, times(1)).poll(any(Duration.class));
    verify(client, never()).acknowledge(anyList());
  }

  @Test
  void latestOffsetGathersNextBatchWhenStartConsumedPrevious() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 2)).thenReturn(messages(2, 3));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset first = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());
    Offset second = stream.latestOffset(first, ReadLimit.allAvailable());

    assertEquals(0L, ((PubSubOffset) first).batchId());
    assertEquals(1L, ((PubSubOffset) second).batchId());
    InputPartition[] partitions = stream.planInputPartitions(first, second);
    assertEquals(3, ((PubSubInputPartition) partitions[0]).messages().size());
    verify(client, times(2)).poll(any(Duration.class));
    verify(client).acknowledge(List.of("ack-0", "ack-1"));
  }

  @Test
  void emptyFollowUpAfterConsumedStartAcksPreviousBatch() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class)))
        .thenReturn(messages(0, 2))
        .thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset first = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());
    Offset next = stream.latestOffset(first, ReadLimit.allAvailable());

    assertNull(next);
    verify(client).acknowledge(List.of("ack-0", "ack-1"));
    verify(client).releaseMessages(any());
  }

  @Test
  void formatStatsFillsBatchTimes() {
    List<PulledMessage> pulled =
        List.of(
            new PulledMessage("old", new byte[] {1}, Collections.emptyMap(), 1_000L, "", "ack-old"),
            new PulledMessage(
                "new", new byte[] {1}, Collections.emptyMap(), 2_000L, "", "ack-new"));
    String line = PublishTimeWindow.of(pulled).formatStats(0L, "BATCH: First batch:");

    assertTrue(line.contains("BATCH: First batch:"));
    assertTrue(line.contains("batchId=0"));
    assertTrue(line.contains("messages=2"));
    assertTrue(line.contains("1970-01-01T00:00:01Z"));
    assertTrue(line.contains("1970-01-01T00:00:02Z"));
    assertFalse(line.contains("{}"));
  }

  @Test
  void latestOffsetIsIdempotentUntilCommit() {
    PubSubClient client = mock(PubSubClient.class);
    List<PulledMessage> first = messages(0, 2);
    when(client.poll(any(Duration.class))).thenReturn(first).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset firstOffset = stream.latestOffset();
    Offset repeatedOffset = stream.latestOffset();

    assertEquals(firstOffset, repeatedOffset);
    verify(client, times(1)).poll(any(Duration.class));
    verify(client, never()).nack(List.of("ack-0", "ack-1"));
  }

  @Test
  void commitAcknowledgesDriverHeldMessages() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 2));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();
    stream.commit(latest);

    verify(client).acknowledge(List.of("ack-0", "ack-1"));
    verify(client).releaseMessages(any());
  }

  @Test
  void stopNacksUncommittedAfterCommitBatch() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 2));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    stream.latestOffset();
    stream.stop();

    verify(client).nack(List.of("ack-0", "ack-1"));
    verify(client).close();
  }

  @Test
  void admissionControlEmptyPullReturnsNull() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());

    assertNull(latest);
    verify(client, times(1)).poll(any(Duration.class));
  }

  @Test
  void sparkMaxRowsCapsBelowBatchCount() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class)))
        .thenReturn(messages(0, 500))
        .thenReturn(messages(500, 500))
        .thenReturn(messages(1000, 500));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackMode(AckMode.EARLY)
            .batchCount(3000)
            .receiveTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(500));
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);

    assertEquals(500, ((PubSubInputPartition) partitions[0]).messages().size());
    verify(client, times(1)).poll(any(Duration.class));
  }

  @Test
  void pullModeRequestsOnlyRemainingRows() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 10));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(10));

    verify(client).poll(any(Duration.class));
  }

  @Test
  void minRowsKeepsPullingUntilDeadlineWhenIdle() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .receiveTime(Duration.ofMillis(80))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.minRows(100, 80));

    assertNull(latest);
    verify(client, atLeast(2)).poll(any(Duration.class));
  }

  @Test
  void idleBatchGatherReturnsAfterFirstEmptyPull() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .receiveTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset initial = stream.initialOffset();
    Offset latest = stream.latestOffset();

    assertEquals(initial, latest);
    verify(client, times(2)).poll(any(Duration.class));
  }

  @Test
  void unsetBatchTimeProbesWithoutPulling() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 1));
    PubSubConfig config = PubSubConfig.builder().projectId("p").subscription("s").build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset first = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());
    Offset second = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());

    assertNull(first);
    assertNull(second);
    verify(client, never()).poll(any(Duration.class));
  }

  @Test
  void interruptAbortsGatherAndNacks() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class)))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return messages(0, 2);
            });
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .receiveTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    try {
      Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());
      assertNull(latest);
      verify(client).nack(List.of("ack-0", "ack-1"));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void batchGatherKeepsMessagesWhenFollowUpIsEmpty() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class)))
        .thenReturn(messages(0, 2))
        .thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackMode(AckMode.EARLY)
            .batchCount(100)
            .receiveTime(Duration.ofMillis(80))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);

    assertEquals(2, ((PubSubInputPartition) partitions[0]).messages().size());
  }

  @Test
  void batchFollowUpPullUsesRemainingPullDeadline() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 1)).thenReturn(messages(1, 1));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackMode(AckMode.EARLY)
            .batchCount(2)
            .receiveTime(Duration.ofSeconds(3))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    stream.latestOffset();

    verify(client, times(2)).poll(any(Duration.class));
  }

  @Test
  void defaultReadLimitUsesBatchCount() {
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").batchCount(250).build();
    PubSubMicroBatchStream stream =
        new PubSubMicroBatchStream(config, 1, mock(PubSubClient.class), false);

    ReadLimit limit = stream.getDefaultReadLimit();

    assertEquals(ReadLimit.maxRows(250), limit);
  }

  @Test
  void toStringUsesSubscriptionPath() {
    PubSubConfig config = PubSubConfig.builder().projectId("p").subscription("s").build();
    PubSubMicroBatchStream stream =
        new PubSubMicroBatchStream(config, 1, mock(PubSubClient.class), false);

    assertEquals("google-pubsub:projects/p/subscriptions/s", stream.toString());
  }

  private static List<PulledMessage> messages(int start, int count) {
    List<PulledMessage> messages = new ArrayList<>(count);
    for (int i = start; i < start + count; i++) {
      messages.add(
          new PulledMessage(
              "message-" + i, new byte[] {1}, Collections.emptyMap(), 0L, "", "ack-" + i));
    }
    return messages;
  }
}
