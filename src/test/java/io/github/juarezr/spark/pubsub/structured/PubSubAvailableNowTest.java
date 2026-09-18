package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SeekMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow;
import org.junit.jupiter.api.Test;

class PubSubAvailableNowTest {

  @Test
  void streamImplementsSupportsTriggerAvailableNow() {
    PubSubMicroBatchStream stream = stream(clientAlwaysEmpty(), batchConfig());
    assertInstanceOf(SupportsTriggerAvailableNow.class, stream);
    assertFalse(stream.availableNow());
    stream.prepareForTriggerAvailableNow();
    assertTrue(stream.availableNow());
  }

  @Test
  void alreadyIdleReturnsNullAfterConsecutiveEmptyPulls() {
    PubSubClient client = clientAlwaysEmpty();
    PubSubMicroBatchStream stream = stream(client, batchConfig());
    stream.prepareForTriggerAvailableNow();

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());

    assertNull(latest);
    verify(client, times(PubSubMicroBatchStream.AVAILABLE_NOW_IDLE_PULLS))
        .pull(any(Duration.class), anyInt());
  }

  @Test
  void firstEmptyPullDoesNotStopDrain() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(Collections.emptyList())
        .thenReturn(messages(0, 3))
        .thenReturn(Collections.emptyList())
        .thenReturn(Collections.emptyList())
        .thenReturn(Collections.emptyList());
    PubSubMicroBatchStream stream = stream(client, batchConfig());
    stream.prepareForTriggerAvailableNow();

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());

    assertNotNull(latest);
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);
    assertEquals(3, ((PubSubInputPartition) partitions[0]).messages().size());
    verify(client, times(2 + PubSubMicroBatchStream.AVAILABLE_NOW_IDLE_PULLS))
        .pull(any(Duration.class), anyInt());
  }

  @Test
  void processingTimeStillStopsOnDebouncedEmpty() {
    PubSubClient client = clientAlwaysEmpty();
    PubSubMicroBatchStream stream = stream(client, batchConfig());

    Offset latest = stream.latestOffset();

    assertEquals(stream.initialOffset(), latest);
    verify(client, times(2)).pull(any(Duration.class), anyInt());
  }

  @Test
  void skipsAutoBatchTimeProbe() {
    PubSubConfig unsetBatchTime =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.BATCH)
            .batchSize(0)
            .build();
    PubSubClient probeClient = clientAlwaysEmpty();
    PubSubMicroBatchStream probe = stream(probeClient, unsetBatchTime);
    Offset probed = probe.latestOffset();
    assertEquals(probe.initialOffset(), probed);
    verify(probeClient, never()).pull(any(Duration.class), anyInt());

    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(messages(0, 2))
        .thenReturn(Collections.emptyList())
        .thenReturn(Collections.emptyList())
        .thenReturn(Collections.emptyList());
    PubSubMicroBatchStream stream = stream(client, unsetBatchTime);
    stream.prepareForTriggerAvailableNow();
    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());

    assertNotNull(latest);
    verify(client, times(1 + PubSubMicroBatchStream.AVAILABLE_NOW_IDLE_PULLS))
        .pull(any(Duration.class), anyInt());
  }

  @Test
  void snapshotRecoveryDrainsThenReturnsNull() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(messages(0, 5))
        .thenReturn(messages(5, 5))
        .thenReturn(messages(10, 2))
        .thenReturn(Collections.emptyList())
        .thenReturn(Collections.emptyList())
        .thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("recovery-sub")
            .seekMode(SeekMode.SNAPSHOT)
            .seekSnapshot("projects/p/snapshots/recovery")
            .gatherMode(GatherMode.BATCH)
            .batchCount(5)
            .batchSize(0)
            .batchTime(Duration.ofSeconds(2))
            .build();
    PubSubMicroBatchStream stream = stream(client, config);
    stream.prepareForTriggerAvailableNow();

    Offset first = stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(5));
    assertEquals(0L, ((PubSubOffset) first).batchId());
    assertEquals(5, partitionSize(stream, stream.initialOffset(), first));
    stream.commit(first);

    Offset second = stream.latestOffset(first, ReadLimit.maxRows(5));
    assertEquals(1L, ((PubSubOffset) second).batchId());
    assertEquals(5, partitionSize(stream, first, second));
    stream.commit(second);

    Offset third = stream.latestOffset(second, ReadLimit.maxRows(5));
    assertEquals(2L, ((PubSubOffset) third).batchId());
    assertEquals(2, partitionSize(stream, second, third));
    stream.commit(third);

    Offset done = stream.latestOffset(third, ReadLimit.maxRows(5));
    assertNull(done);
    // 5 + 5 + (2 then 3 idle) + 3 idle to return null
    verify(client, times(9)).pull(any(Duration.class), anyInt());
    verify(client).acknowledge(List.of("ack-0", "ack-1", "ack-2", "ack-3", "ack-4"));
    verify(client).acknowledge(List.of("ack-5", "ack-6", "ack-7", "ack-8", "ack-9"));
    verify(client).acknowledge(List.of("ack-10", "ack-11"));
    verify(client, never()).nack(anyList());
  }

  @Test
  void batchCountSplitsHotDrainWithoutStopping() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(messages(0, 4))
        .thenReturn(messages(4, 4));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.BATCH)
            .batchCount(4)
            .batchSize(0)
            .build();
    PubSubMicroBatchStream stream = stream(client, config);
    stream.prepareForTriggerAvailableNow();

    Offset first = stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(4));
    assertEquals(4, partitionSize(stream, stream.initialOffset(), first));
    stream.commit(first);
    Offset second = stream.latestOffset(first, ReadLimit.maxRows(4));
    assertEquals(4, partitionSize(stream, first, second));
    verify(client, times(2)).pull(any(Duration.class), anyInt());
  }

  @Test
  void limitTimeWithoutAvailableNowThrows() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.BATCH)
            .batchSize(0)
            .limitTime("2000")
            .build();
    PubSubClient client = clientAlwaysEmpty();
    PubSubMicroBatchStream stream = stream(client, config);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () -> stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable()));
    assertTrue(ex.getMessage().contains("AvailableNow"));
    verify(client, never()).pull(any(Duration.class), anyInt());
  }

  @Test
  void limitTimeKeepsInRangeNacksOverLimitThenStops() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(List.of(messageAt(1000, 0), messageAt(1500, 1), messageAt(2000, 2)));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.BATCH)
            .batchSize(0)
            .limitTime("2000")
            .build();
    PubSubMicroBatchStream stream = stream(client, config);
    stream.prepareForTriggerAvailableNow();

    Offset first = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());
    assertEquals(2, partitionSize(stream, stream.initialOffset(), first));
    verify(client).nack(List.of("ack-2"));
    stream.commit(first);

    Offset done = stream.latestOffset(first, ReadLimit.allAvailable());
    assertNull(done);
    assertTrue(stream.limitReached());
    verify(client, times(1)).pull(any(Duration.class), anyInt());
    verify(client).acknowledge(List.of("ack-0", "ack-1"));
  }

  @Test
  void allOverLimitNacksAndReturnsNull() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt())).thenReturn(List.of(messageAt(3000, 0)));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.BATCH)
            .batchSize(0)
            .limitTime("2000")
            .build();
    PubSubMicroBatchStream stream = stream(client, config);
    stream.prepareForTriggerAvailableNow();

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());

    assertNull(latest);
    assertTrue(stream.limitReached());
    verify(client).nack(List.of("ack-0"));
    verify(client, never()).acknowledge(anyList());
  }

  @Test
  void batchCountSplitsBeforeLimitBound() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt()))
        .thenReturn(
            List.of(messageAt(1000, 0), messageAt(1100, 1), messageAt(1200, 2), messageAt(1300, 3)))
        .thenReturn(List.of(messageAt(1400, 4), messageAt(2000, 5)));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.BATCH)
            .batchCount(4)
            .batchSize(0)
            .limitTime("2000")
            .build();
    PubSubMicroBatchStream stream = stream(client, config);
    stream.prepareForTriggerAvailableNow();

    Offset first = stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(4));
    assertEquals(4, partitionSize(stream, stream.initialOffset(), first));
    stream.commit(first);

    Offset second = stream.latestOffset(first, ReadLimit.maxRows(4));
    assertEquals(1, partitionSize(stream, first, second));
    verify(client).nack(List.of("ack-5"));
    stream.commit(second);

    Offset done = stream.latestOffset(second, ReadLimit.maxRows(4));
    assertNull(done);
    verify(client, times(2)).pull(any(Duration.class), anyInt());
  }

  private static int partitionSize(PubSubMicroBatchStream stream, Offset start, Offset end) {
    InputPartition[] partitions = stream.planInputPartitions(start, end);
    return ((PubSubInputPartition) partitions[0]).messages().size();
  }

  private static PubSubMicroBatchStream stream(PubSubClient client, PubSubConfig config) {
    return new PubSubMicroBatchStream(config, 1, client, false);
  }

  private static PubSubConfig batchConfig() {
    return PubSubConfig.builder()
        .projectId("p")
        .subscription("s")
        .gatherMode(GatherMode.BATCH)
        .batchSize(0)
        .batchTime(Duration.ofSeconds(2))
        .build();
  }

  private static PubSubClient clientAlwaysEmpty() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class), anyInt())).thenReturn(Collections.emptyList());
    return client;
  }

  private static List<PulledMessage> messages(int start, int count) {
    List<PulledMessage> messages = new ArrayList<>(count);
    for (int i = start; i < start + count; i++) {
      messages.add(messageAt(0L, i));
    }
    return messages;
  }

  private static PulledMessage messageAt(long publishTimeMillis, int index) {
    return new PulledMessage(
        "message-" + index,
        new byte[] {1},
        Collections.emptyMap(),
        publishTimeMillis,
        "",
        "ack-" + index);
  }
}
