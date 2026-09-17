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
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.ReportsSourceMetrics;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pull-based micro-batch stream. Progress is tracked with synthetic offsets; the Pub/Sub
 * subscription cursor remains the durable source of truth across process restarts (no rewind unless
 * configured).
 */
final class PubSubMicroBatchStream
    implements MicroBatchStream, ReportsSourceMetrics, SupportsAdmissionControl {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubMicroBatchStream.class);

  private final PubSubConfig config;
  private final StructType readSchema;
  private final PubSubClient client;
  private final AckCoordinator ackCoordinator;
  private final AckLeaseWatchdog leaseWatchdog = new AckLeaseWatchdog();
  private final AtomicLong nextBatchId = new AtomicLong(0);
  private final AtomicInteger lastPullMessageCount = new AtomicInteger(0);
  private final AtomicLong lastPullPayloadBytes = new AtomicLong(0);
  private volatile Long lastPullMessageAgeMs;
  private final AtomicLong lastReportedRetryAttempts = new AtomicLong(0);
  private final int numPartitions;
  private volatile PubSubOffset lastProduced;
  private volatile PubSubOffset currentOffset = PubSubOffset.empty(-1L);
  private volatile boolean firstBatchLogged;
  private volatile Long lastGatheredBatchId;
  private volatile PublishTimeWindow lastGatheredWindow;
  private final ConcurrentMap<Long, List<PulledMessage>> messagesByBatch =
      new ConcurrentHashMap<>();
  private final AutoBatchTime autoBatchTime;
  private volatile boolean cancelled;

  PubSubMicroBatchStream(PubSubConfig config, int numPartitions) {
    this(config, numPartitions, PubSubSchema.tableSchema(config, null));
  }

  PubSubMicroBatchStream(PubSubConfig config, int numPartitions, StructType readSchema) {
    this(config, numPartitions, new PubSubClient(config), true, readSchema);
  }

  PubSubMicroBatchStream(
      PubSubConfig config, int numPartitions, PubSubClient client, boolean startClient) {
    this(config, numPartitions, client, startClient, PubSubSchema.tableSchema(config, null));
  }

  PubSubMicroBatchStream(
      PubSubConfig config,
      int numPartitions,
      PubSubClient client,
      boolean startClient,
      StructType readSchema) {
    this.config = config;
    this.readSchema = readSchema == null ? PubSubSchema.tableSchema(config, null) : readSchema;
    this.client = client;
    this.ackCoordinator = new AckCoordinator(config.ackMode());
    this.autoBatchTime = AutoBatchTime.create(config);
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
  public ReadLimit getDefaultReadLimit() {
    if (config.batchCount() > 0) {
      return ReadLimit.maxRows(config.batchCount());
    }
    return ReadLimit.allAvailable();
  }

  @Override
  public Offset reportLatestOffset() {
    return lastProduced != null ? lastProduced : currentOffset;
  }

  @Override
  public Offset latestOffset() {
    return latestOffset(null, AdmissionLimits.from(config, null), false);
  }

  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    return latestOffset(startOffset, AdmissionLimits.from(config, limit), true);
  }

  /**
   * Spark 3.5 commits batch N-1 only when constructing batch N. Returning the same {@code
   * lastProduced} after Spark has already accepted it as {@code startOffset} makes {@code
   * constructNextBatch} see no new data, so {@link #commit} never runs and the query idles while
   * the watchdog renews the first batch.
   */
  private Offset latestOffset(Offset startOffset, AdmissionLimits limits, boolean nullIfEmpty) {
    if (lastProduced != null && !startConsumed(startOffset, lastProduced)) {
      return lastProduced;
    }
    if (autoBatchTime.shouldProbe()) {
      lastPullMessageCount.set(0);
      lastPullPayloadBytes.set(0);
      lastPullMessageAgeMs = null;
      return nullIfEmpty ? null : currentOffset;
    }
    AdmissionLimits gatherLimits =
        limits.withWaitTime(autoBatchTime.gatherWindow(limits.waitTime()));
    long gatherStarted = System.nanoTime();
    List<PulledMessage> pulled = gatherMessages(gatherLimits);
    autoBatchTime.onGatherFinished(System.nanoTime() - gatherStarted, !pulled.isEmpty());
    if (pulled.isEmpty()) {
      lastPullMessageCount.set(0);
      lastPullPayloadBytes.set(0);
      lastPullMessageAgeMs = null;
      if (lastProduced != null && startConsumed(startOffset, lastProduced)) {
        commit(lastProduced);
      }
      return nullIfEmpty ? null : currentOffset;
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
        LOG.warn("BATCH: Failed to extend ack deadline for batch {}: {}", batchId, e.getMessage());
      }
      leaseWatchdog.start(client, ids, ackDeadlineSeconds());
    }
    lastProduced = offset;
    final int pulledSize = pulled.size();
    final long pulledBytes = PulledMessage.payloadBytes(pulled);
    lastPullMessageCount.set(pulledSize);
    lastPullPayloadBytes.set(pulledBytes);
    lastPullMessageAgeMs =
        PubSubSourceMetrics.newestMessageAgeMs(pulled, System.currentTimeMillis());
    retainGatheredWindow(batchId, pulled);
    LOG.debug(
        "BATCH: latestOffset batchId={} messages={} bytes={}", batchId, pulledSize, pulledBytes);
    return offset;
  }

  private List<PulledMessage> gatherMessages(AdmissionLimits limits) {
    List<PulledMessage> messages = new ArrayList<>();
    if (limits.singlePull()) {
      if (gatherAborted()) {
        return messages;
      }
      int maxMessages = limits.messagesForNextPull(0, config.pullMaxMessages());
      if (maxMessages > 0) {
        messages.addAll(client.pull(config.pullDeadline(), maxMessages));
      }
      return messages;
    }

    Duration wait = limits.waitTime();
    final long waitNanos = wait == null ? config.pullDeadline().toNanos() : wait.toNanos();
    final long deadlineNanos = System.nanoTime() + waitNanos;
    long payloadBytes = 0L;
    boolean debounced = false;
    try {
      while (System.nanoTime() < deadlineNanos) {
        if (gatherAborted()) {
          abortGather(messages);
          return List.of();
        }
        if (limits.reachedMax(messages.size(), payloadBytes)) {
          break;
        }
        Duration remaining = Duration.ofNanos(Math.max(1L, deadlineNanos - System.nanoTime()));
        final Duration rpcDeadline = min(config.pullDeadline(), remaining);
        int maxMessages = limits.messagesForNextPull(messages.size(), config.pullMaxMessages());
        if (maxMessages <= 0) {
          break;
        }
        List<PulledMessage> pulled = client.pull(rpcDeadline, maxMessages);
        if (gatherAborted()) {
          messages.addAll(pulled);
          abortGather(messages);
          return List.of();
        }
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
              final String amount = PubSubClient.asString(rpcDeadline);
              LOG.warn(
                  "BATCH: Failed to extend initial ack deadline ({} remaining) while gathering: {}",
                  amount,
                  e.getMessage());
            }
            List<String> ids = PulledMessage.ackIds(messages);
            if (messages.size() == pulled.size()) {
              leaseWatchdog.start(client, ids, ackDeadlineSeconds());
            } else {
              leaseWatchdog.update(ids);
            }
          }
        } else if (config.gatherMode() == GatherMode.BATCH && messages.isEmpty() && !debounced) {
          debounced = true;
          Duration remainingDebounce =
              Duration.ofNanos(Math.max(1L, deadlineNanos - System.nanoTime()));
          Duration debounceDeadline = min(Duration.ofSeconds(1), remainingDebounce);
          pulled = client.pull(debounceDeadline, maxMessages);
          if (gatherAborted()) {
            messages.addAll(pulled);
            abortGather(messages);
            return List.of();
          }
          if (pulled.isEmpty()) {
            break;
          }
          messages.addAll(pulled);
          payloadBytes += PulledMessage.payloadBytes(pulled);
          if (config.ackMode() == AckMode.EARLY) {
            client.acknowledge(PulledMessage.ackIds(pulled));
            client.releaseMessages(pulled);
          } else {
            try {
              client.extendAckDeadline(PulledMessage.ackIds(pulled), ackDeadlineSeconds());
            } catch (RuntimeException e) {
              LOG.warn(
                  "BATCH: Failed to extend initial ack deadline while gathering: {}",
                  e.getMessage());
            }
            leaseWatchdog.start(client, PulledMessage.ackIds(messages), ackDeadlineSeconds());
          }
        } else if (config.gatherMode() == GatherMode.BATCH || limits.minRowsMet(messages.size())) {
          break;
        }
        if (limits.reachedMax(messages.size(), payloadBytes)) {
          break;
        }
        if (config.gatherMode() == GatherMode.PULL && limits.minRowsMet(messages.size())) {
          break;
        }
      }
    } catch (RuntimeException e) {
      abortGather(messages);
      throw e;
    }
    return messages;
  }

  private boolean gatherAborted() {
    return cancelled || Thread.currentThread().isInterrupted();
  }

  private void abortGather(List<PulledMessage> messages) {
    leaseWatchdog.stop();
    if (config.ackMode() == AckMode.AFTER_COMMIT && !messages.isEmpty()) {
      try {
        client.nack(PulledMessage.ackIds(messages));
      } catch (RuntimeException nackError) {
        LOG.warn("BATCH: Failed to nack messages after gather abort: {}", nackError.getMessage());
      } finally {
        client.releaseMessages(messages);
      }
    }
  }

  private static Duration min(Duration left, Duration right) {
    return left.compareTo(right) <= 0 ? left : right;
  }

  /** True when Spark already persisted {@code produced} as the start of the next range. */
  static boolean startConsumed(Offset startOffset, PubSubOffset produced) {
    if (startOffset == null || produced == null) {
      return false;
    }
    final long startBatchId =
        startOffset instanceof PubSubOffset
            ? ((PubSubOffset) startOffset).batchId()
            : PubSubOffset.fromJson(startOffset.json()).batchId();
    return startBatchId >= produced.batchId();
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
        lastPullMessageAgeMs,
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
    return new PubSubPartitionReaderFactory(readSchema);
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
            LOG.debug(
                "BATCH: Committed (acked) {} messages for batch {}",
                ackIds.size(),
                endOffset.batchId());
          } catch (RuntimeException e) {
            try {
              client.nack(ackIds);
            } catch (RuntimeException nackError) {
              LOG.warn(
                  "BATCH: Failed to nack {} messages after ack failure for batch {}",
                  ackIds.size(),
                  endOffset.batchId(),
                  nackError.getMessage());
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
    LOG.debug("BATCH: Committed offset batchId={}", endOffset.batchId());
    autoBatchTime.onCommit();
  }

  @Override
  public void stop() {
    cancelled = true;
    try {
      leaseWatchdog.stop();
      int uncommitted = 0;
      if (lastProduced != null && config.ackMode() == AckMode.AFTER_COMMIT) {
        String batchKey = Long.toString(lastProduced.batchId());
        List<PulledMessage> messages = ackCoordinator.messagesForBatch(batchKey);
        if (!messages.isEmpty()) {
          uncommitted = messages.size();
          ackCoordinator.abort(client, batchKey);
          messagesByBatch.remove(lastProduced.batchId());
        }
      }
      logStop(uncommitted);
    } finally {
      ackCoordinator.clear();
      messagesByBatch.clear();
      client.resetOutstandingBytes();
      client.close();
    }
  }

  private void retainGatheredWindow(long batchId, List<PulledMessage> pulled) {
    PublishTimeWindow window = PublishTimeWindow.of(pulled);
    lastGatheredBatchId = batchId;
    lastGatheredWindow = window;
    if (firstBatchLogged || window == null) {
      return;
    }
    this.firstBatchLogged = true;
    LOG.info(window.formatStats(batchId, "BATCH: First batch:"));
  }

  private void logStop(int uncommitted) {
    PublishTimeWindow window = lastGatheredWindow;
    if (window == null) {
      LOG.info("BATCH: Stopping stream; no messages gathered");
      return;
    }
    final long batchId = this.lastGatheredBatchId == null ? -1L : this.lastGatheredBatchId;
    final String prefix =
        uncommitted <= 0
            ? "BATCH: Stopping stream; 0 uncommitted messages found; Last batch is:"
            : String.format(
                "BATCH: Stopping stream; %d uncommitted messages will redeliver if not committed; Last batch is:",
                uncommitted);
    LOG.info(window.formatStats(batchId, prefix));
  }

  PublishTimeWindow lastGatheredWindow() {
    return lastGatheredWindow;
  }

  boolean firstBatchLogged() {
    return firstBatchLogged;
  }

  Long lastGatheredBatchId() {
    return lastGatheredBatchId;
  }

  @Override
  public String toString() {
    return PubSubConfig.SHORT_NAME + ":" + config.subscriptionPath();
  }

  private void finishBatch(long batchId) {
    messagesByBatch.remove(batchId);
    if (lastProduced != null && lastProduced.batchId() == batchId) {
      lastProduced = null;
    }
  }
}
