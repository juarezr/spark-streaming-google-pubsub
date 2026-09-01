package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.juarezr.spark.pubsub.client.PulledMessage;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.junit.jupiter.api.Test;

class PubSubSourceMetricsTest {

  @Test
  void emptyPullBeforeAnyOffset() {
    Map<String, String> metrics =
        PubSubSourceMetrics.snapshot(0, 0L, 0L, null, Optional.empty(), 0L, 0L);
    assertEquals("0", metrics.get(PubSubSourceMetrics.LAST_PULL_MESSAGE_COUNT));
    assertEquals("0", metrics.get(PubSubSourceMetrics.LAST_PULL_PAYLOAD_BYTES));
    assertEquals("0", metrics.get(PubSubSourceMetrics.OUTSTANDING_PAYLOAD_BYTES));
    assertEquals("-1", metrics.get(PubSubSourceMetrics.LAST_PRODUCED_BATCH_ID));
    assertEquals("-", metrics.get(PubSubSourceMetrics.LAST_CONSUMED_BATCH_ID));
    assertEquals("0", metrics.get(PubSubSourceMetrics.PUBSUB_RETRY_ATTEMPTS));
    assertEquals("0", metrics.get(PubSubSourceMetrics.PUBSUB_RETRY_ATTEMPTS_TOTAL));
  }

  @Test
  void pullWithMessagesAndConsumedOffset() {
    PulledMessage message =
        new PulledMessage("m1", new byte[] {1, 2, 3}, Collections.emptyMap(), 0L, "", "ack-1");
    PubSubOffset consumed = new PubSubOffset(7L, List.of(message));
    Map<String, String> metrics =
        PubSubSourceMetrics.snapshot(1, 3L, 3L, 7L, Optional.of(consumed), 1L, 4L);
    assertEquals("1", metrics.get(PubSubSourceMetrics.LAST_PULL_MESSAGE_COUNT));
    assertEquals("3", metrics.get(PubSubSourceMetrics.LAST_PULL_PAYLOAD_BYTES));
    assertEquals("3", metrics.get(PubSubSourceMetrics.OUTSTANDING_PAYLOAD_BYTES));
    assertEquals("7", metrics.get(PubSubSourceMetrics.LAST_PRODUCED_BATCH_ID));
    assertEquals("7", metrics.get(PubSubSourceMetrics.LAST_CONSUMED_BATCH_ID));
    assertEquals("1", metrics.get(PubSubSourceMetrics.PUBSUB_RETRY_ATTEMPTS));
    assertEquals("4", metrics.get(PubSubSourceMetrics.PUBSUB_RETRY_ATTEMPTS_TOTAL));
  }

  @Test
  void retryAttemptsThisBatchIsDeltaFromLastReport() {
    assertEquals(0L, PubSubSourceMetrics.retryAttemptsThisBatch(0L, 0L));
    assertEquals(2L, PubSubSourceMetrics.retryAttemptsThisBatch(2L, 0L));
    assertEquals(1L, PubSubSourceMetrics.retryAttemptsThisBatch(4L, 3L));
    assertEquals(0L, PubSubSourceMetrics.retryAttemptsThisBatch(4L, 4L));
    assertEquals(0L, PubSubSourceMetrics.retryAttemptsThisBatch(3L, 5L));
  }

  @Test
  void unknownOffsetTypeLeavesConsumedBatchAbsent() {
    Offset other =
        new Offset() {
          @Override
          public String json() {
            return "{}";
          }
        };
    Map<String, String> metrics =
        PubSubSourceMetrics.snapshot(2, 10L, 4L, 1L, Optional.of(other), 0L, 0L);
    assertEquals("-", metrics.get(PubSubSourceMetrics.LAST_CONSUMED_BATCH_ID));
    assertEquals("1", metrics.get(PubSubSourceMetrics.LAST_PRODUCED_BATCH_ID));
  }
}
