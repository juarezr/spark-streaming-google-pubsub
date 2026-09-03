package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
import org.junit.jupiter.api.Test;

class PubSubGatherTest {

  @Test
  void emptyPullKeepsTheCurrentOffset() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class))).thenReturn(Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset initial = stream.initialOffset();
    Offset latest = stream.latestOffset();

    assertEquals(initial, latest);
    assertEquals(
        "-", stream.metrics(Optional.empty()).get(PubSubSourceMetrics.LAST_PULL_MESSAGE_AGE_MS));
    verify(client, times(1)).pull(any(Duration.class));
  }

  @Test
  void pullReportsNewestMessageAge() {
    PubSubClient client = mock(PubSubClient.class);
    PulledMessage older =
        new PulledMessage("old", new byte[] {1}, Collections.emptyMap(), 1_000L, "", "ack-old");
    PulledMessage newer =
        new PulledMessage("new", new byte[] {1}, Collections.emptyMap(), 4_000L, "", "ack-new");
    when(client.pull(any(Duration.class))).thenReturn(List.of(older, newer));
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
    when(client.pull(any(Duration.class)))
        .thenReturn(messages(0, 1000), messages(1000, 1000), messages(2000, 1000));
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
    verify(client, times(3)).pull(any(Duration.class));
  }

  @Test
  void latestOffsetIsIdempotentUntilCommit() {
    PubSubClient client = mock(PubSubClient.class);
    List<PulledMessage> first = messages(0, 2);
    when(client.pull(any(Duration.class))).thenReturn(first, Collections.emptyList());
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    Offset firstOffset = stream.latestOffset();
    Offset repeatedOffset = stream.latestOffset();

    assertEquals(firstOffset, repeatedOffset);
    verify(client, times(1)).pull(any(Duration.class));
    verify(client, never()).nack(List.of("ack-0", "ack-1"));
  }

  @Test
  void commitAcknowledgesDriverHeldMessages() {
    PubSubClient client = mock(PubSubClient.class);
    when(client.pull(any(Duration.class))).thenReturn(messages(0, 2));
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
    when(client.pull(any(Duration.class))).thenReturn(messages(0, 2));
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    PubSubMicroBatchStream stream = new PubSubMicroBatchStream(config, 1, client, false);

    stream.latestOffset();
    stream.stop();

    verify(client).nack(List.of("ack-0", "ack-1"));
    verify(client).close();
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
