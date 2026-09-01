package io.github.juarezr.spark.pubsub.structured;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.connector.read.streaming.Offset;

/**
 * Cheap in-process Structured Streaming source metrics. Values are strings for {@code
 * StreamingQueryProgress} / Spark UI. Does not call Pub/Sub admin or Cloud Monitoring.
 */
final class PubSubSourceMetrics {
  static final String LAST_PULL_MESSAGE_COUNT = "lastPullMessageCount";
  static final String LAST_PULL_PAYLOAD_BYTES = "lastPullPayloadBytes";
  static final String OUTSTANDING_PAYLOAD_BYTES = "outstandingPayloadBytes";
  static final String LAST_PRODUCED_BATCH_ID = "lastProducedBatchId";
  static final String LAST_CONSUMED_BATCH_ID = "lastConsumedBatchId";
  static final String PUBSUB_RETRY_ATTEMPTS = "pubsubRetryAttempts";
  static final String PUBSUB_RETRY_ATTEMPTS_TOTAL = "pubsubRetryAttemptsTotal";
  static final String ABSENT_BATCH_ID = "-";

  private PubSubSourceMetrics() {}

  static Map<String, String> snapshot(
      int lastPullMessageCount,
      long lastPullPayloadBytes,
      long outstandingPayloadBytes,
      Long lastProducedBatchId,
      Optional<Offset> latestConsumedOffset,
      long pubsubRetryAttempts,
      long pubsubRetryAttemptsTotal) {
    Map<String, String> metrics = new LinkedHashMap<>();
    metrics.put(LAST_PULL_MESSAGE_COUNT, Integer.toString(lastPullMessageCount));
    metrics.put(LAST_PULL_PAYLOAD_BYTES, Long.toString(lastPullPayloadBytes));
    metrics.put(OUTSTANDING_PAYLOAD_BYTES, Long.toString(outstandingPayloadBytes));
    metrics.put(
        LAST_PRODUCED_BATCH_ID,
        lastProducedBatchId == null ? "-1" : Long.toString(lastProducedBatchId));
    metrics.put(LAST_CONSUMED_BATCH_ID, consumedBatchId(latestConsumedOffset));
    metrics.put(PUBSUB_RETRY_ATTEMPTS, Long.toString(pubsubRetryAttempts));
    metrics.put(PUBSUB_RETRY_ATTEMPTS_TOTAL, Long.toString(pubsubRetryAttemptsTotal));
    return metrics;
  }

  /** Retries since the last progress report. {@code total} is the lifetime counter. */
  static long retryAttemptsThisBatch(long total, long lastReportedTotal) {
    return Math.max(0L, total - lastReportedTotal);
  }

  private static String consumedBatchId(Optional<Offset> latestConsumedOffset) {
    if (latestConsumedOffset == null || !latestConsumedOffset.isPresent()) {
      return ABSENT_BATCH_ID;
    }
    Offset offset = latestConsumedOffset.get();
    if (offset instanceof PubSubOffset) {
      return Long.toString(((PubSubOffset) offset).batchId());
    }
    return ABSENT_BATCH_ID;
  }
}
