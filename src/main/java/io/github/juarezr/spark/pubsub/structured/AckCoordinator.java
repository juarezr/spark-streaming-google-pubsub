package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.AckMode;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks in-flight ack ids per batch and acknowledges or nacks according to {@link AckMode}.
 *
 * <p>Memory is bounded by removing entries on commit/abort; it does not grow unbounded over time.
 */
final class AckCoordinator implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AckCoordinator.class);

  private final AckMode ackMode;
  private final transient Map<String, List<PulledMessage>> pendingByBatch =
      new ConcurrentHashMap<>();

  AckCoordinator(AckMode ackMode) {
    this.ackMode = ackMode;
  }

  void registerBatch(String batchId, List<PulledMessage> messages) {
    pendingByBatch.put(batchId, new ArrayList<>(messages));
  }

  List<PulledMessage> messagesForBatch(String batchId) {
    return pendingByBatch.getOrDefault(batchId, Collections.emptyList());
  }

  /**
   * For {@link AckMode#EARLY}, acknowledge immediately after registration. For {@link
   * AckMode#AFTER_COMMIT}, this is a no-op until {@link #commit(PubSubClient, String)}.
   */
  void onPulled(PubSubClient client, String batchId) {
    if (ackMode != AckMode.EARLY) {
      return;
    }
    final List<PulledMessage> messages = pendingByBatch.get(batchId);
    if (messages == null || messages.isEmpty()) {
      return;
    }
    final List<String> ackIds = PulledMessage.ackIds(messages);
    client.acknowledge(ackIds);
    release(client, batchId);
    LOG.debug("Early-acked {} messages for batch {}", ackIds.size(), batchId);
  }

  void commit(PubSubClient client, String batchId) {
    final List<PulledMessage> messages = pendingByBatch.get(batchId);
    if (messages == null) {
      return;
    }
    if (ackMode == AckMode.AFTER_COMMIT && !messages.isEmpty()) {
      final List<String> ackIds = PulledMessage.ackIds(messages);
      client.acknowledge(ackIds);
      LOG.debug("Committed (acked) {} messages for batch {}", ackIds.size(), batchId);
    }
    release(client, batchId);
  }

  void abort(PubSubClient client, String batchId) {
    final List<PulledMessage> messages = pendingByBatch.get(batchId);
    if (messages == null) {
      return;
    }
    if (ackMode == AckMode.AFTER_COMMIT && !messages.isEmpty()) {
      final List<String> ackIds = PulledMessage.ackIds(messages);
      try {
        client.nack(ackIds);
        LOG.warn("Aborted batch {}; nacked {} messages", batchId, ackIds.size());
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to nack {} messages for aborted batch {}; they will redeliver on deadline",
            ackIds.size(),
            batchId,
            e);
      }
    }
    release(client, batchId);
  }

  private void release(PubSubClient client, String batchId) {
    List<PulledMessage> removed = pendingByBatch.remove(batchId);
    if (removed == null) {
      return;
    }
    client.releaseMessages(removed);
  }

  int pendingBatchCount() {
    return pendingByBatch.size();
  }

  void clear() {
    pendingByBatch.clear();
  }
}
