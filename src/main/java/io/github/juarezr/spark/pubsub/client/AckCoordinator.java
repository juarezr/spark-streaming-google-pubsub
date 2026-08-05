package io.github.juarezr.spark.pubsub.client;

import io.github.juarezr.spark.pubsub.config.AckMode;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks in-flight ack ids per batch and acknowledges or nacks according to {@link AckMode}.
 *
 * <p>Memory is bounded by removing entries on commit/abort; it does not grow unbounded over time.
 */
public final class AckCoordinator implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AckCoordinator.class);

  private final AckMode ackMode;
  private final transient Map<String, List<PulledMessage>> pendingByBatch =
      new ConcurrentHashMap<>();

  public AckCoordinator(AckMode ackMode) {
    this.ackMode = ackMode;
  }

  public AckMode ackMode() {
    return ackMode;
  }

  public void registerBatch(String batchId, List<PulledMessage> messages) {
    pendingByBatch.put(batchId, new ArrayList<>(messages));
  }

  public List<PulledMessage> messagesForBatch(String batchId) {
    return pendingByBatch.getOrDefault(batchId, Collections.emptyList());
  }

  /**
   * For {@link AckMode#EARLY}, acknowledge immediately after registration. For {@link
   * AckMode#AFTER_COMMIT}, this is a no-op until {@link #commit(PubSubClient, String)}.
   */
  public void onPulled(PubSubClient client, String batchId) {
    if (ackMode != AckMode.EARLY) {
      return;
    }
    List<PulledMessage> messages = pendingByBatch.get(batchId);
    if (messages == null || messages.isEmpty()) {
      return;
    }
    List<String> ackIds = messages.stream().map(PulledMessage::ackId).collect(Collectors.toList());
    client.acknowledge(ackIds);
    release(client, batchId);
    LOG.debug("Early-acked {} messages for batch {}", ackIds.size(), batchId);
  }

  public void commit(PubSubClient client, String batchId) {
    List<PulledMessage> messages = pendingByBatch.get(batchId);
    if (messages == null) {
      return;
    }
    if (ackMode == AckMode.AFTER_COMMIT && !messages.isEmpty()) {
      List<String> ackIds =
          messages.stream().map(PulledMessage::ackId).collect(Collectors.toList());
      client.acknowledge(ackIds);
      LOG.debug("Committed (acked) {} messages for batch {}", ackIds.size(), batchId);
    }
    release(client, batchId);
  }

  public void abort(PubSubClient client, String batchId) {
    List<PulledMessage> messages = pendingByBatch.get(batchId);
    if (messages == null) {
      return;
    }
    if (ackMode == AckMode.AFTER_COMMIT && !messages.isEmpty()) {
      List<String> ackIds =
          messages.stream().map(PulledMessage::ackId).collect(Collectors.toList());
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
    long bytes = 0L;
    for (PulledMessage m : removed) {
      bytes += m.data().length;
    }
    client.releaseBytes(bytes);
  }

  public int pendingBatchCount() {
    return pendingByBatch.size();
  }

  public void clear() {
    pendingByBatch.clear();
  }
}
