package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
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
import org.mockito.ArgumentCaptor;

class PubSubGatherTest {

  @Test
  void emptyPullKeepsTheCurrentOffset() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt())).thenReturn(Collections.emptyList());
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
    verify(client, times(1)).pull(any(Duration.class), anyInt());
  }

  @Test
  void firstNonEmptyGatherRecordsPublishTimeWindow() {
    PubSubClient client = mock(PubSubClient.class);
    PulledMessage older =
        new PulledMessage("old", new byte[] {1}, Collections.emptyMap(), 1_000L, "", "ack-old");
    PulledMessage newer =
        new PulledMessage("new", new byte[] {1}, Collections.emptyMap(), 4_000L, "", "ack-new");
    when(client.pull(any(Duration.class), anyInt())).thenReturn(List.of(older, newer));
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
    when(client.pull(any(Duration.class), anyInt())).thenReturn(List.of(older, newer));
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
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(messages(0, 1000))
        .thenReturn(messages(1000, 1000))
        .thenReturn(messages(2000, 1000));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackMode(AckMode.EARLY)
            .batchCount(3000)
            .batchTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);

    assertEquals(1, partitions.length);
    assertEquals(3000, ((PubSubInputPartition) partitions[0]).messages().size());
    verify(client, times(3)).pull(any(Duration.class), anyInt());
  }

  @Test
  void latestOffsetIsIdempotentUntilCommit() {
    PubSubClient client = mock(PubSubClient.class);
    List<PulledMessage> first = messages(0, 2);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(first)
        .thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset firstOffset = stream.latestOffset();
    Offset repeatedOffset = stream.latestOffset();

    assertEquals(firstOffset, repeatedOffset);
    verify(client, times(1)).pull(any(Duration.class), anyInt());
    verify(client, never()).nack(List.of("ack-0", "ack-1"));
  }

  @Test
  void commitAcknowledgesDriverHeldMessages() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt())).thenReturn(messages(0, 2));
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
    when(client.pull(any(Duration.class), anyInt())).thenReturn(messages(0, 2));
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
    when(client.pull(any(Duration.class), anyInt())).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());

    assertNull(latest);
    verify(client, times(1)).pull(any(Duration.class), anyInt());
  }

  @Test
  void sparkMaxRowsCapsBelowBatchCount() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(messages(0, 500))
        .thenReturn(messages(500, 500))
        .thenReturn(messages(1000, 500));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackMode(AckMode.EARLY)
            .batchCount(3000)
            .batchTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(500));
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);

    assertEquals(500, ((PubSubInputPartition) partitions[0]).messages().size());
    verify(client, times(1)).pull(any(Duration.class), eq(500));
  }

  @Test
  void pullModeRequestsOnlyRemainingRows() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt())).thenReturn(messages(0, 10));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(10));

    verify(client).pull(any(Duration.class), eq(10));
  }

  @Test
  void minRowsKeepsPullingUntilDeadlineWhenIdle() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt())).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .batchTime(Duration.ofMillis(80))
            .pullDeadline(Duration.ofMillis(20))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.minRows(100, 80));

    assertNull(latest);
    verify(client, atLeast(2)).pull(any(Duration.class), anyInt());
  }

  @Test
  void idleBatchGatherKeepsTheCurrentOffset() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt())).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .batchTime(Duration.ofMillis(80))
            .pullDeadline(Duration.ofMillis(20))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset initial = stream.initialOffset();
    Offset latest = stream.latestOffset();

    assertEquals(initial, latest);
    verify(client, atLeast(1)).pull(any(Duration.class), anyInt());
  }

  @Test
  void batchGatherKeepsMessagesWhenFollowUpIsEmpty() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(messages(0, 2))
        .thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackMode(AckMode.EARLY)
            .batchCount(100)
            .batchTime(Duration.ofMillis(80))
            .pullDeadline(Duration.ofMillis(20))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);

    assertEquals(2, ((PubSubInputPartition) partitions[0]).messages().size());
  }

  @Test
  void batchFollowUpPullUsesRemainingPullDeadline() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(messages(0, 1))
        .thenReturn(messages(1, 1));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackMode(AckMode.EARLY)
            .batchCount(2)
            .batchTime(Duration.ofSeconds(3))
            .pullDeadline(Duration.ofSeconds(5))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    stream.latestOffset();

    ArgumentCaptor<Duration> deadlines = ArgumentCaptor.forClass(Duration.class);
    verify(client, times(2)).pull(deadlines.capture(), anyInt());
    Duration followUp = deadlines.getAllValues().get(1);
    assertTrue(followUp.compareTo(Duration.ofSeconds(1)) > 0);
    assertTrue(followUp.compareTo(Duration.ofSeconds(5)) <= 0);
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
