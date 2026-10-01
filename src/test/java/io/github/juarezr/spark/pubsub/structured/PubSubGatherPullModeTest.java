package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.atLeastOnce;
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
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.junit.jupiter.api.Test;

/**
 * Simulates {@link GatherMode#PULL} gather behavior on the driver (mocked {@link PubSubClient}).
 */
class PubSubGatherPullModeTest {

  @Test
  void singlePollReturnsAvailableMessagesWithoutFillingBatchCount() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 2));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .batchCount(3000)
            .receiveTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);

    assertEquals(2, ((PubSubInputPartition) partitions[0]).messages().size());
    verify(client, times(1)).poll(any(Duration.class));
  }

  @Test
  void emptyQueueUsesOnePollUnlikeBatchDebounce() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(Collections.emptyList());
    PubSubConfig pull =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .receiveTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream pullStream = new PubSubMicroBatchStream(pull, 1, client, false);

    PubSubConfig batch =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.BATCH)
            .receiveTime(Duration.ofSeconds(10))
            .build();
    PubSubClient batchClient = mock(PubSubClient.class);
    when(batchClient.poll(any(Duration.class))).thenReturn(Collections.emptyList());
    PubSubMicroBatchStream batchStream = new PubSubMicroBatchStream(batch, 1, batchClient, false);

    pullStream.latestOffset();
    batchStream.latestOffset();

    verify(client, times(1)).poll(any(Duration.class));
    verify(batchClient, times(2)).poll(any(Duration.class));
  }

  @Test
  void capsNextPollToSparkMaxRows() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 25));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(10));

    verify(client).limitNextPoll(10);
    verify(client).poll(any(Duration.class));
  }

  @Test
  void minRowsPullsUntilThresholdThenStops() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class)))
        .thenReturn(messages(0, 1))
        .thenReturn(messages(1, 1))
        .thenReturn(messages(2, 1))
        .thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .receiveTime(Duration.ofSeconds(10))
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest =
        stream.latestOffset(
            stream.initialOffset(), ReadLimit.minRows(3, Duration.ofSeconds(10).toMillis()));

    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);
    assertEquals(3, ((PubSubInputPartition) partitions[0]).messages().size());
    verify(client, times(3)).poll(any(Duration.class));
  }

  @Test
  void earlyAckWithPullAcknowledgesOnGatherBeforeCommit() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 2));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .ackMode(AckMode.EARLY)
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();

    verify(client, atLeastOnce()).acknowledge(List.of("ack-0", "ack-1"));
    stream.commit(latest);
    verify(client, times(2)).acknowledge(List.of("ack-0", "ack-1"));
  }

  @Test
  void afterCommitWithPullDefersAckUntilCommit() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 2));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .ackMode(AckMode.AFTER_COMMIT)
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset();
    verify(client, never()).acknowledge(anyList());

    stream.commit(latest);
    verify(client).acknowledge(List.of("ack-0", "ack-1"));
  }

  @Test
  void pullWithAllAvailableReadLimitStillSinglePollWhenNoMinRows() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(messages(0, 5));
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .batchCount(100)
            .build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset latest = stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable());
    assertEquals(5, ((PubSubInputPartition) partitionsFor(stream, latest)).messages().size());
    verify(client, times(1)).poll(any(Duration.class));
  }

  @Test
  void pullEmptyWithAllAvailableReturnsNullOffset() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.poll(any(Duration.class))).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    assertNull(stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable()));
  }

  private static PubSubInputPartition partitionsFor(PubSubMicroBatchStream stream, Offset latest) {
    InputPartition[] partitions = stream.planInputPartitions(stream.initialOffset(), latest);
    return (PubSubInputPartition) partitions[0];
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
