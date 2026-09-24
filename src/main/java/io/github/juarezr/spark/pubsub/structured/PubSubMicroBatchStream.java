package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pull-based micro-batch stream. Progress is tracked with synthetic offsets; the Pub/Sub
 * subscription cursor remains the durable source of truth across process restarts (no rewind unless
 * configured).
 */
final class PubSubMicroBatchStream
    implements MicroBatchStream, ReportsSourceMetrics, SupportsTriggerAvailableNow {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubMicroBatchStream.class);

  /**
   * Pub/Sub has no log-end offset. AvailableNow treats this many consecutive empty polls as idle
   * and returns {@code null} so Spark stops.
   */
  static final int AVAILABLE_NOW_IDLE_POLLS = 3;

  static final Duration RECEIVE_IDLE = Duration.ofSeconds(1);

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
  private volatile boolean availableNow;
  private volatile boolean limitReached;

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
  public void prepareForTriggerAvailableNow() {
    this.availableNow = true;
    if (config.limitTime().isPresent()) {
      LOG.info(
          "BATCH: AvailableNow recovery drain; limitTime={} exclusive; stop at bound or {}"
              + " consecutive empty polls",
          config.limitTime().get(),
          AVAILABLE_NOW_IDLE_POLLS);
    } else {
      LOG.info(
          "BATCH: AvailableNow drain enabled; stop after {} consecutive empty polls"
              + " (Pub/Sub has no log-end offset; use seek=snapshot for recovery)",
          AVAILABLE_NOW_IDLE_POLLS);
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

    if (config.limitTime().isPresent() && !this.availableNow) {
      throw new IllegalStateException(
          "limitTime requires Trigger.AvailableNow(); ProcessingTime cannot stop at the bound");
    }
    if (lastProduced != null && !startConsumed(startOffset, lastProduced)) {
      return lastProduced;
    }
    if (this.limitReached) {
      resetMetrics();
      if (lastProduced != null && startConsumed(startOffset, lastProduced)) {
        commit(lastProduced);
      }
      return nullIfEmpty ? null : this.currentOffset;
    }
    if (!availableNow && autoBatchTime.shouldProbe()) {
      resetMetrics();
      return nullIfEmpty ? null : currentOffset;
    }
    // Streaming pull flow control holds at most one batch. Spark has finished that batch
    // (start offset caught up) but calls source commit only after the next offset is built.
    // Ack first so the subscriber can deliver the rest; otherwise the following empty polls
    // look like the end of an AvailableNow drain and the query stops with a partial backlog.
    if (lastProduced != null && startConsumed(startOffset, lastProduced)) {
      commit(lastProduced);
    }
    List<PulledMessage> pulled = gatherMessages(limits);
    if (pulled.isEmpty()) {
      resetMetrics();
      if (lastProduced != null && startConsumed(startOffset, lastProduced)) {
        commit(lastProduced);
      }
      return nullIfEmpty ? null : currentOffset;
    }
    long batchId = nextBatchId.getAndIncrement();
    PubSubOffset offset = produceNextOffset(pulled, batchId);
    this.lastProduced = offset;
    updateMetrics(pulled, batchId);
    retainGatheredWindow(batchId, pulled);
    return offset;
  }

  private PubSubOffset produceNextOffset(List<PulledMessage> pulled, long batchId) {
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
    return offset;
  }

  private void resetMetrics() {
    updateMetrics(null, -1L);
  }

  private void updateMetrics(List<PulledMessage> pulled, long batchId) {
    if (pulled == null) {
      this.lastPullMessageCount.set(0);
      this.lastPullPayloadBytes.set(0);
      this.lastPullMessageAgeMs = null;
    } else {
      final int pulledSize = pulled.size();
      final long pulledBytes = PulledMessage.payloadBytes(pulled);
      this.lastPullMessageCount.set(pulledSize);
      this.lastPullPayloadBytes.set(pulledBytes);
      long now = System.currentTimeMillis();
      this.lastPullMessageAgeMs = PubSubSourceMetrics.newestMessageAgeMs(pulled, now);
      LOG.debug(
          "BATCH: latestOffset batchId={} messages={} bytes={}", batchId, pulledSize, pulledBytes);
    }
  }

  private List<PulledMessage> gatherMessages(AdmissionLimits gatherLimits) {
    AdmissionLimits limits =
        availableNow
            ? gatherLimits.withDrainUntilIdle()
            : gatherLimits.withWaitTime(this.autoBatchTime.receiveWindow(gatherLimits.waitTime()));

    long gatherStarted = System.nanoTime();
    List<PulledMessage> pulled =
        limits.drainUntilIdle()
            ? gatherUntilIdleOrMax(limits)
            : limits.singlePull()
                ? gatherMessagesFromSinglePull(limits)
                : gatherMessagesUntilDeadline(limits);
    long gatherFinished = System.nanoTime();

    this.autoBatchTime.onGatherFinished(gatherFinished - gatherStarted, !pulled.isEmpty());
    return pulled;
  }

  private List<PulledMessage> gatherMessagesFromSinglePull(AdmissionLimits limits) {

    List<PulledMessage> messages = new ArrayList<>();
    if (gatherAborted()) {
      return messages;
    }
    if (limits.remainingRows(0) <= 0) {
      return messages;
    }
    List<PulledMessage> pulled = poll(limits, messages.size(), RECEIVE_IDLE);
    return retainPolled(messages, pulled, limits, 0L).messages;
  }

  private List<PulledMessage> gatherMessagesUntilDeadline(AdmissionLimits limits) {

    List<PulledMessage> messages = new ArrayList<>();
    Duration wait = limits.waitTime();
    final long waitNanos = wait == null ? RECEIVE_IDLE.toNanos() : wait.toNanos();
    final long deadlineNanos = System.nanoTime() + waitNanos;
    long payloadBytes = 0L;
    boolean debounced = false;
    try {
      while (System.nanoTime() < deadlineNanos) {
        if (gatherAborted()) {
          return stopLeaseNackAndRelease(messages, null);
        }
        if (limits.reachedMax(messages.size(), payloadBytes)
            || limits.remainingRows(messages.size()) <= 0) {
          break;
        }
        Duration slice = remainingWait(deadlineNanos);
        List<PulledMessage> pulled = poll(limits, messages.size(), slice);
        if (gatherAborted()) {
          return stopLeaseNackAndRelease(messages, pulled);
        }
        if (!pulled.isEmpty()) {
          RetainResult retained = retainPolled(messages, pulled, limits, payloadBytes);
          payloadBytes = retained.payloadBytes;
          messages = retained.messages;
        } else if (config.gatherMode() == GatherMode.BATCH && messages.isEmpty() && !debounced) {
          debounced = true;
          List<PulledMessage> pulled2 =
              poll(limits, messages.size(), min(RECEIVE_IDLE, remainingWait(deadlineNanos)));
          if (gatherAborted()) {
            return stopLeaseNackAndRelease(messages, pulled2);
          }
          if (pulled2.isEmpty()) {
            break;
          }
          RetainResult retained = retainPolled(messages, pulled2, limits, payloadBytes);
          payloadBytes = retained.payloadBytes;
          messages = retained.messages;
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
      stopLeaseNackAndRelease(messages, null);
      throw e;
    }
    return messages;
  }

  private List<PulledMessage> poll(AdmissionLimits limits, int already, Duration timeout) {
    client.limitNextPoll(limits.remainingRows(already));
    return client.poll(timeout);
  }

  private Duration remainingWait(long deadlineNanos) {
    return Duration.ofNanos(Math.max(1L, deadlineNanos - System.nanoTime()));
  }

  private static final class RetainResult {
    final List<PulledMessage> messages;
    final long payloadBytes;

    RetainResult(List<PulledMessage> messages, long payloadBytes) {
      this.messages = messages;
      this.payloadBytes = payloadBytes;
    }
  }

  private RetainResult retainPolled(
      List<PulledMessage> messages,
      List<PulledMessage> pulled,
      AdmissionLimits limits,
      long payloadBytes) {

    List<PulledMessage> overflow = new ArrayList<>();
    for (PulledMessage message : pulled) {
      if (limits.reachedMax(messages.size(), payloadBytes)
          || limits.remainingRows(messages.size()) <= 0) {
        overflow.add(message);
        continue;
      }
      messages.add(message);
      payloadBytes += message.data().length;
    }
    if (!overflow.isEmpty()) {
      if (config.ackMode() == AckMode.AFTER_COMMIT) {
        try {
          client.nack(PulledMessage.ackIds(overflow));
        } catch (RuntimeException e) {
          LOG.warn("BATCH: Failed to nack overflow messages: {}", e.getMessage());
        } finally {
          client.releaseMessages(overflow);
        }
      }
    }
    List<PulledMessage> kept =
        overflow.isEmpty() ? pulled : pulled.subList(0, pulled.size() - overflow.size());
    if (config.ackMode() == AckMode.EARLY && !kept.isEmpty()) {
      client.acknowledge(PulledMessage.ackIds(kept));
      client.releaseMessages(kept);
    }
    return new RetainResult(messages, payloadBytes);
  }

  /**
   * AvailableNow receive: keep polling until {@code batchCount}/{@code batchSize} or {@link
   * #AVAILABLE_NOW_IDLE_POLLS} consecutive empty polls.
   */
  private List<PulledMessage> gatherUntilIdleOrMax(AdmissionLimits limits) {
    List<PulledMessage> messages = new ArrayList<>();
    long payloadBytes = 0L;
    int emptyStreak = 0;
    try {
      while (true) {
        if (gatherAborted()) {
          return stopLeaseNackAndRelease(messages, null);
        }
        if (limits.reachedMax(messages.size(), payloadBytes)
            || limits.remainingRows(messages.size()) <= 0) {
          break;
        }
        List<PulledMessage> pulled = poll(limits, messages.size(), RECEIVE_IDLE);
        if (gatherAborted()) {
          return stopLeaseNackAndRelease(messages, pulled);
        }
        if (pulled.isEmpty()) {
          emptyStreak++;
          if (emptyStreak >= AVAILABLE_NOW_IDLE_POLLS) {
            LOG.info(
                "BATCH: AvailableNow idle after {} consecutive empty polls; received={}",
                emptyStreak,
                messages.size());
            break;
          }
          continue;
        }
        BoundSplit split = splitAtLimit(pulled);
        if (!split.overLimit.isEmpty()) {
          this.limitReached = true;
          nackOverLimit(split.overLimit);
          LOG.info(
              "BATCH: AvailableNow reached limitTime={}; inRange={} overLimit={}",
              config.limitTime().orElse("-"),
              split.inRange.size(),
              split.overLimit.size());

          if (!split.inRange.isEmpty()) {
            RetainResult retained = retainPolled(messages, split.inRange, limits, payloadBytes);
            payloadBytes = retained.payloadBytes;
            messages = retained.messages;
          }
          break;
        }
        emptyStreak = 0;
        RetainResult retained = retainPolled(messages, split.inRange, limits, payloadBytes);
        payloadBytes = retained.payloadBytes;
        messages = retained.messages;
      }
    } catch (RuntimeException e) {
      stopLeaseNackAndRelease(messages, null);
      throw e;
    }
    return messages;
  }

  private static final class BoundSplit {
    final List<PulledMessage> inRange;
    final List<PulledMessage> overLimit;

    BoundSplit(List<PulledMessage> inRange, List<PulledMessage> overLimit) {
      this.inRange = inRange;
      this.overLimit = overLimit;
    }
  }

  private BoundSplit splitAtLimit(List<PulledMessage> pulled) {
    Optional<Instant> limit = config.limitTimeAsInstant();
    if (limit.isEmpty()) {
      return new BoundSplit(pulled, List.of());
    }
    long boundMillis = limit.get().toEpochMilli();
    List<PulledMessage> inRange = new ArrayList<>();
    List<PulledMessage> overLimit = new ArrayList<>();
    for (PulledMessage message : pulled) {
      if (message.publishTimeMillis() >= boundMillis) {
        overLimit.add(message);
      } else {
        inRange.add(message);
      }
    }
    return new BoundSplit(inRange, overLimit);
  }

  private void nackOverLimit(List<PulledMessage> overLimit) {
    if (overLimit.isEmpty()) {
      return;
    }
    try {
      client.nack(PulledMessage.ackIds(overLimit));
    } catch (RuntimeException e) {
      LOG.warn(
          "BATCH: Failed to nack {} over-limit messages: {}", overLimit.size(), e.getMessage());
    } finally {
      client.releaseMessages(overLimit);
    }
  }

  private boolean gatherAborted() {
    return cancelled || Thread.currentThread().isInterrupted();
  }

  private List<PulledMessage> stopLeaseNackAndRelease(
      List<PulledMessage> messages, List<PulledMessage> pulled) {

    if (pulled != null) {
      messages.addAll(pulled);
    }
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
    return List.of();
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
        ackCoordinator.discard(batchKey);
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
                "BATCH: Stopping stream; %d uncommitted messages will redeliver if not committed;"
                    + " Last batch is:",
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

  boolean availableNow() {
    return availableNow;
  }

  boolean limitReached() {
    return limitReached;
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
