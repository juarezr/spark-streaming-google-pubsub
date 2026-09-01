package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.client.AckCoordinator;
import io.github.juarezr.spark.pubsub.client.AckLeaseWatchdog;
import io.github.juarezr.spark.pubsub.client.PubSubClient;
import io.github.juarezr.spark.pubsub.client.PulledMessage;
import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
public final class PubSubMicroBatchStream implements MicroBatchStream, ReportsSourceMetrics {
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

  public PubSubMicroBatchStream(PubSubConfig config, int numPartitions) {
    this.config = config;
    this.client = new PubSubClient(config);
    this.ackCoordinator = new AckCoordinator(config.ackMode());
    this.numPartitions = Math.max(1, numPartitions);
    try {
      this.client.start();
    } catch (IOException e) {
      throw new IllegalStateException("Unable to start Pub/Sub client", e);
    }
  }

  @Override
  public Offset latestOffset() {
    if (config.ackMode() == AckMode.AFTER_COMMIT) {
      leaseWatchdog.stop();
      abandon(client, lastProduced);
      lastProduced = null;
    }
    List<PulledMessage> pulled = client.pull();
    long batchId = nextBatchId.getAndIncrement();
    PubSubOffset offset = new PubSubOffset(batchId, pulled);
    String batchKey = Long.toString(batchId);
    ackCoordinator.registerBatch(batchKey, pulled);
    if (config.ackMode() == AckMode.EARLY) {
      ackCoordinator.onPulled(client, batchKey);
    } else if (!pulled.isEmpty()) {
      try {
        client.extendAckDeadline(offset.ackIds(), config.ackDeadlineSeconds());
      } catch (RuntimeException e) {
        LOG.warn("Failed to extend ack deadline for batch {}", batchId, e);
      }
      leaseWatchdog.start(client, offset.ackIds(), config.ackDeadlineSeconds());
    }
    lastProduced = offset;
    final int pulledSize = pulled.size();
    final long pulledBytes = pulled.stream().mapToLong(m -> m.data().length).sum();
    lastPullMessageCount.set(pulledSize);
    lastPullPayloadBytes.set(pulledBytes);
    LOG.debug("latestOffset batchId={} messages={} bytes={}", batchId, pulledSize, pulledBytes);
    return offset;
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
    List<PulledMessage> messages = endOffset.messages();
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
    return PubSubOffset.empty(-1L);
  }

  @Override
  public Offset deserializeOffset(String json) {
    PubSubOffset offset = PubSubOffset.fromJson(json);
    nextBatchId.updateAndGet(v -> Math.max(v, offset.batchId() + 1));
    return offset;
  }

  @Override
  public void commit(Offset end) {
    PubSubOffset endOffset = (PubSubOffset) end;
    String batchKey = Long.toString(endOffset.batchId());
    leaseWatchdog.stop();
    if (config.ackMode() == AckMode.AFTER_COMMIT) {
      List<String> ackIds = endOffset.ackIds();
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
          client.releaseMessages(endOffset.messages());
        }
        ackCoordinator.clear();
        if (lastProduced != null && lastProduced.batchId() == endOffset.batchId()) {
          lastProduced = null;
        }
      }
    } else {
      ackCoordinator.commit(client, batchKey);
    }
    LOG.debug("Committed offset batchId={}", endOffset.batchId());
  }

  static void abandon(final PubSubClient client, final PubSubOffset offset) {
    if (client == null || offset == null || offset.ackIds().isEmpty()) {
      return;
    }
    List<String> ackIds = offset.ackIds();
    try {
      client.nack(ackIds);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to nack {} messages for abandoned batch {}; they will redeliver on deadline",
          ackIds.size(),
          offset.batchId(),
          e);
    }
    client.releaseMessages(offset.messages());
  }

  @Override
  public void stop() {
    try {
      leaseWatchdog.stop();
      if (lastProduced != null
          && config.ackMode() == AckMode.AFTER_COMMIT
          && !lastProduced.ackIds().isEmpty()) {
        LOG.info(
            "Stopping stream; {} uncommitted messages will redeliver if not committed",
            lastProduced.ackIds().size());
      }
    } finally {
      ackCoordinator.clear();
      client.resetOutstandingBytes();
      client.close();
    }
  }
}
