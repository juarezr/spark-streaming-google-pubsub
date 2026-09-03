package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReportsSourceMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pull-based micro-batch stream. Progress is tracked with synthetic offsets; the Pub/Sub
 * subscription cursor remains the durable source of truth across process restarts (no rewind unless
 * configured).
 */
final class PubSubMicroBatchStream implements MicroBatchStream, ReportsSourceMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubMicroBatchStream.class);

  private final PubSubConfig config;
  private final PubSubClient client;
  private final AckCoordinator ackCoordinator;
  private final AckLeaseWatchdog leaseWatchdog = new AckLeaseWatchdog();
  private final AtomicLong nextBatchId = new AtomicLong(0);
  private final AtomicInteger lastPullMessageCount = new AtomicInteger(0);
  private final AtomicLong lastPullPayloadBytes = new AtomicLong(0);
  private final AtomicLong lastReportedRetryAttempts = new AtomicLong(0);
  private final int numPartitions;
  private volatile PubSubOffset lastProduced;
  private volatile PubSubOffset currentOffset = PubSubOffset.empty(-1L);
  private final ConcurrentMap<Long, List<PulledMessage>> messagesByBatch =
      new ConcurrentHashMap<>();

  PubSubMicroBatchStream(PubSubConfig config, int numPartitions) {
    this(config, numPartitions, new PubSubClient(config), true);
  }

  PubSubMicroBatchStream(
      PubSubConfig config, int numPartitions, PubSubClient client, boolean startClient) {
    this.config = config;
    this.client = client;
    this.ackCoordinator = new AckCoordinator(config.ackMode());
    this.numPartitions = Math.max(1, numPartitions);
    if (!startClient) {
      return;
    }
    try {
      this.client.start();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to start Pub/Sub client", e);
    }
  }

  @Override
  public Offset latestOffset() {
    if (lastProduced != null) {
      return lastProduced;
    }
    List<PulledMessage> pulled = gatherMessages();
    if (pulled.isEmpty()) {
      lastPullMessageCount.set(0);
      lastPullPayloadBytes.set(0);
      return currentOffset;
    }
    long batchId = nextBatchId.getAndIncrement();
    PubSubOffset offset = new PubSubOffset(batchId);
    String batchKey = Long.toString(batchId);
    messagesByBatch.put(batchId, pulled);
    try {
      if (config.ackMode() == AckMode.AFTER_COMMIT) {
        ackCoordinator.registerBatch(batchKey, pulled);
      } else if (config.gatherMode() == GatherMode.PULL) {
        ackCoordinator.registerBatch(batchKey, pulled);
        ackCoordinator.onPulled(client, batchKey);
      }
    } catch (RuntimeException e) {
      ackCoordinator.abort(client, batchKey);
      messagesByBatch.remove(batchId);
      throw e;
    }
    if (config.ackMode() == AckMode.AFTER_COMMIT && config.gatherMode() == GatherMode.PULL) {
      List<String> ids = PulledMessage.ackIds(pulled);
      try {
        client.extendAckDeadline(ids, ackDeadlineSeconds());
      } catch (RuntimeException e) {
        LOG.warn("Failed to extend ack deadline for batch {}", batchId, e);
      }
      leaseWatchdog.start(client, ids, ackDeadlineSeconds());
    }
    lastProduced = offset;
    final int pulledSize = pulled.size();
    final long pulledBytes = PulledMessage.payloadBytes(pulled);
    lastPullMessageCount.set(pulledSize);
    lastPullPayloadBytes.set(pulledBytes);
    LOG.debug("latestOffset batchId={} messages={} bytes={}", batchId, pulledSize, pulledBytes);
    return offset;
  }

  private List<PulledMessage> gatherMessages() {
    List<PulledMessage> messages = new ArrayList<>();
    if (config.gatherMode() == GatherMode.PULL) {
      messages.addAll(client.pull(config.pullDeadline()));
      return messages;
    }

    long deadlineNanos = System.nanoTime() + config.batchTime().toNanos();
    long payloadBytes = 0L;
    boolean firstPull = true;
    try {
      while (System.nanoTime() < deadlineNanos) {
        Duration remaining = Duration.ofNanos(Math.max(1L, deadlineNanos - System.nanoTime()));
        Duration rpcDeadline =
            firstPull
                ? min(config.pullDeadline(), remaining)
                : min(Duration.ofSeconds(1), remaining);
        List<PulledMessage> pulled = client.pull(rpcDeadline);
        firstPull = false;
        if (!pulled.isEmpty()) {
          messages.addAll(pulled);
          payloadBytes += PulledMessage.payloadBytes(pulled);
          if (config.ackMode() == AckMode.EARLY) {
            client.acknowledge(PulledMessage.ackIds(pulled));
            client.releaseMessages(pulled);
          } else {
            try {
              client.extendAckDeadline(PulledMessage.ackIds(pulled), ackDeadlineSeconds());
            } catch (RuntimeException e) {
              LOG.warn("Failed to extend initial ack deadline while gathering", e);
            }
            List<String> ids = PulledMessage.ackIds(messages);
            if (messages.size() == pulled.size()) {
              leaseWatchdog.start(client, ids, ackDeadlineSeconds());
            } else {
              leaseWatchdog.update(ids);
            }
          }
        }
        if ((config.batchSize() > 0 && payloadBytes >= config.batchSize())
            || (config.batchCount() > 0 && messages.size() >= config.batchCount())) {
          break;
        }
      }
    } catch (RuntimeException e) {
      leaseWatchdog.stop();
      if (config.ackMode() == AckMode.AFTER_COMMIT && !messages.isEmpty()) {
        try {
          client.nack(PulledMessage.ackIds(messages));
        } catch (RuntimeException nackError) {
          LOG.warn("Failed to nack messages after gather failure", nackError);
        } finally {
          client.releaseMessages(messages);
        }
      }
      throw e;
    }
    return messages;
  }

  private static Duration min(Duration left, Duration right) {
    return left.compareTo(right) <= 0 ? left : right;
  }

  private int ackDeadlineSeconds() {
    return Math.toIntExact(config.ackDeadline().getSeconds());
  }

  @Override
  public Map<String, String> metrics(final Optional<Offset> latestConsumedOffset) {
    PubSubOffset produced = lastProduced;
    final Long producedBatchId = produced == null ? null : produced.batchId();
    final long retryTotal = client.retryAttempts();
    final long reportedTotal = lastReportedRetryAttempts.getAndSet(retryTotal);
    final long retryThisBatch =
        PubSubSourceMetrics.retryAttemptsThisBatch(retryTotal, reportedTotal);
    return PubSubSourceMetrics.snapshot(
        lastPullMessageCount.get(),
        lastPullPayloadBytes.get(),
        client.outstandingBytes(),
        producedBatchId,
        latestConsumedOffset,
        retryThisBatch,
        retryTotal);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    PubSubOffset endOffset = (PubSubOffset) end;
    List<PulledMessage> messages = messagesByBatch.getOrDefault(endOffset.batchId(), List.of());
    if (messages.isEmpty()) {
      return new InputPartition[] {new PubSubInputPartition(messages)};
    }
    int parts = Math.min(numPartitions, messages.size());
    List<List<PulledMessage>> slices = new ArrayList<>(parts);
    for (int i = 0; i < parts; i++) {
      slices.add(new ArrayList<>());
    }
    for (int i = 0; i < messages.size(); i++) {
      slices.get(i % parts).add(messages.get(i));
    }
    InputPartition[] partitions = new InputPartition[parts];
    for (int i = 0; i < parts; i++) {
      partitions[i] = new PubSubInputPartition(slices.get(i));
    }
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new PubSubPartitionReaderFactory();
  }

  @Override
  public Offset initialOffset() {
    return currentOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    PubSubOffset offset = PubSubOffset.fromJson(json);
    nextBatchId.updateAndGet(v -> Math.max(v, offset.batchId() + 1));
    currentOffset = offset;
    return offset;
  }

  @Override
  public void commit(Offset end) {
    PubSubOffset endOffset = (PubSubOffset) end;
    String batchKey = Long.toString(endOffset.batchId());
    leaseWatchdog.stop();
    if (config.ackMode() == AckMode.AFTER_COMMIT) {
      List<PulledMessage> messages = messagesByBatch.getOrDefault(endOffset.batchId(), List.of());
      List<String> ackIds = PulledMessage.ackIds(messages);
      try {
        if (!ackIds.isEmpty()) {
          try {
            client.acknowledge(ackIds);
          } catch (RuntimeException e) {
            try {
              client.nack(ackIds);
            } catch (RuntimeException nackError) {
              LOG.warn(
                  "Failed to nack {} messages after ack failure for batch {}",
                  ackIds.size(),
                  endOffset.batchId(),
                  nackError);
            }
            throw e;
          }
        }
      } finally {
        if (!ackIds.isEmpty()) {
          client.releaseMessages(messages);
        }
        ackCoordinator.clear();
        finishBatch(endOffset.batchId());
      }
      currentOffset = endOffset;
    } else {
      ackCoordinator.commit(client, batchKey);
      finishBatch(endOffset.batchId());
      currentOffset = endOffset;
    }
    LOG.debug("Committed offset batchId={}", endOffset.batchId());
  }

  @Override
  public void stop() {
    try {
      leaseWatchdog.stop();
      if (lastProduced != null && config.ackMode() == AckMode.AFTER_COMMIT) {
        String batchKey = Long.toString(lastProduced.batchId());
        List<PulledMessage> messages = ackCoordinator.messagesForBatch(batchKey);
        if (!messages.isEmpty()) {
          LOG.info(
              "Stopping stream; {} uncommitted messages will redeliver if not committed",
              messages.size());
          ackCoordinator.abort(client, batchKey);
          messagesByBatch.remove(lastProduced.batchId());
        }
      }
    } finally {
      ackCoordinator.clear();
      messagesByBatch.clear();
      client.resetOutstandingBytes();
      client.close();
    }
  }

  private void finishBatch(long batchId) {
    messagesByBatch.remove(batchId);
    if (lastProduced != null && lastProduced.batchId() == batchId) {
      lastProduced = null;
    }
  }
}
