package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Infers {@code receiveTime} when omitted: probe Spark batch interval, one idle raise of the
 * interval, then {@code receiveTime = batchInterval − estimatedWrite − margin}. Write is smoothed
 * (max of recent batches); idle leftover does not raise receive. No receive freeze — only rate
 * limiting toward the write-first target.
 *
 * <pre>
 * @startuml
 * !theme vibrant
 * title Auto receiveTime: PROBE then write-first receive
 *
 * participant Spark
 * participant Auto as AutoBatchTime
 *
 * == PROBE ==
 * Spark -> Auto : shouldProbe() empty micro-batch
 * Auto -> Auto : batchInterval from idle gap\nfirst receive = interval / 2
 *
 * == AUTO ==
 * loop each micro-batch
 *   Auto -> Auto : one idle raise of batchInterval if Spark slept
 *   Auto -> Auto : onCommit: write sample\nreceive = interval − max(write, reserve) − margin
 *   note right of Auto
 *     margin = max(1s, 1.6% interval)
 *     write reserve = max(1s, 5/60 interval)
 *     rate limit receive step per batch
 *     sanity reset if write estimate biased
 *     INFO log 20 batches after stable
 *   end note
 * end
 * @enduml
 * </pre>
 */
final class AutoBatchTime {
  private static final Logger LOG = LoggerFactory.getLogger(AutoBatchTime.class);

  static final long PROBE_NOISE_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final long SEED_MIN_NANOS = TimeUnit.SECONDS.toNanos(10);
  static final int ZERO_TRIGGER_NOISE_CYCLES = 3;
  static final int MIN_TUNE_ADJUSTS = 5;
  static final int STABLE_RECEIVE_CYCLES = 5;
  static final long STABLE_RECEIVE_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
  static final int WRITE_SMOOTH_SAMPLES = 5;
  static final int POST_STABLE_LOG_BATCHES = 20;
  static final int SANITY_STREAK_THRESHOLD = 3;
  static final long MAX_RECEIVE_STEP_NANOS = TimeUnit.SECONDS.toNanos(2);
  static final long RECEIVE_FLOOR_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final double RECEIVE_WRITE_MARGIN_PERCENT = 1.0 / 60.0;
  static final double RECEIVE_WRITE_RESERVE_PERCENT = 5.0 / 60.0;
  static final long RECEIVE_WRITE_MARGIN_MIN_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final Duration ZERO_TRIGGER_RECEIVE = Duration.ofSeconds(1);
  static final Duration ACK_DEADLINE_SEED = Duration.ofSeconds(180);
  static final Duration ACK_DEADLINE_MIN = Duration.ofSeconds(60);
  static final Duration ACK_DEADLINE_MAX = Duration.ofSeconds(600);

  enum Mode {
    FIXED,
    PROBE,
    AUTO,
    ZERO
  }

  private final LongSupplier nanoTime;
  private final Duration fixedReceiveTime;
  private Mode mode;
  private Long probeStartedNanos;
  private Duration batchInterval;
  private Duration currentReceive;
  private long lastGatherNanos;
  private long lastWriteNanos;
  private long lastReturnNanos;
  private long lastOffsetStartNanos;
  private long lastMicroBatchStartNanos;
  private long lastCycleNanos;
  private long estimatedWriteNanos;
  private boolean lastGatherEmpty;
  private boolean pendingWrite;
  private int probeNoiseCount;
  private int probeAttempts;
  private boolean zeroTriggerWarned;
  private boolean batchIntervalFrozen;
  private int autoCycles;
  private int adjustCount;
  private int stableStreak;
  private boolean stabilizedForLogging;
  private int postStableLogRemaining;
  private int chronicBehindStreak;
  private int writeSpikeStreak;
  private final long[] recentWriteNanos = new long[WRITE_SMOOTH_SAMPLES];
  private int recentWriteCount;
  private boolean excludeNextWriteSample;

  static AutoBatchTime create(PubSubConfig config) {
    return create(config, System::nanoTime);
  }

  static AutoBatchTime create(PubSubConfig config, LongSupplier nanoTime) {
    Duration configured = config.receiveTime();
    if (config.gatherMode() == GatherMode.BATCH && configured == null) {
      return new AutoBatchTime(null, nanoTime);
    }
    Duration fixed = configured != null ? configured : Duration.ofSeconds(1);
    return new AutoBatchTime(fixed, nanoTime);
  }

  AutoBatchTime(Duration fixedReceiveTime, LongSupplier nanoTime) {
    this.fixedReceiveTime = fixedReceiveTime;
    this.nanoTime = nanoTime;
    this.mode = fixedReceiveTime == null ? Mode.PROBE : Mode.FIXED;
    if (fixedReceiveTime != null) {
      this.currentReceive = fixedReceiveTime;
    }
  }

  Mode mode() {
    return mode;
  }

  Duration batchInterval() {
    return batchInterval;
  }

  Duration currentReceive() {
    return currentReceive;
  }

  /**
   * @return {@code true} after write-first estimates have stabilized (logging only, not frozen)
   */
  boolean receiveFrozen() {
    return stabilizedForLogging;
  }

  /**
   * @deprecated write-first does not use max tune cap freeze
   */
  boolean frozenAtMaxTuneCap() {
    return false;
  }

  long lastGatherNanos() {
    return lastGatherNanos;
  }

  long autoCycles() {
    return autoCycles;
  }

  long adjustCount() {
    return adjustCount;
  }

  int stableStreak() {
    return stableStreak;
  }

  long estimatedWriteNanos() {
    return estimatedWriteNanos;
  }

  /**
   * @return {@code true} when this micro-batch must return empty without receiving
   */
  boolean shouldProbe() {
    long now = nanoTime.getAsLong();
    if (mode == Mode.AUTO) {
      maybeIdleRaiseBatchInterval(now);
      autoCycles++;
      lastMicroBatchStartNanos = now;
      lastOffsetStartNanos = now;
      return false;
    }
    if (mode != Mode.PROBE) {
      lastOffsetStartNanos = now;
      return false;
    }
    if (probeStartedNanos == null) {
      probeStartedNanos = now;
      lastOffsetStartNanos = now;
      probeAttempts = 1;
      LOG.warn(
          "AUTO: receiveTime is unset; inferring Spark batch interval from idle gaps (empty"
              + " micro-batch, no receive) until one gap is at least 10s. In a 3 sub-second gap"
              + " sequence assume Trigger.ProcessingTime(0). probeAttempt=1");
      return true;
    }
    probeAttempts++;
    long gapFromStart = now - probeStartedNanos;
    lastOffsetStartNanos = now;
    if (gapFromStart < PROBE_NOISE_NANOS) {
      probeNoiseCount++;
      LOG.info(
          "AUTO: probeAttempt={} gap {} is under 1s ({}/{} noise); keep probing",
          probeAttempts,
          format(Duration.ofNanos(gapFromStart)),
          probeNoiseCount,
          ZERO_TRIGGER_NOISE_CYCLES);
      if (probeNoiseCount >= ZERO_TRIGGER_NOISE_CYCLES) {
        enterZeroTrigger();
        return false;
      }
      return true;
    }
    if (gapFromStart < SEED_MIN_NANOS) {
      probeStartedNanos = now;
      lastOffsetStartNanos = now;
      LOG.warn(
          "AUTO: probeAttempt={} inferred batch interval {} is under 10s; not seeding (keep"
              + " probing)",
          probeAttempts,
          format(Duration.ofNanos(gapFromStart)));
      return true;
    }
    probeNoiseCount = 0;
    batchInterval = Duration.ofNanos(gapFromStart);
    currentReceive = firstReceive(batchInterval);
    estimatedWriteNanos = receiveWriteReserveNanos(batchInterval.toNanos());
    mode = Mode.AUTO;
    LOG.info(
        "AUTO: inferred batch interval={} first receiveTime={} (seed probeAttempt={})",
        format(batchInterval),
        format(currentReceive),
        probeAttempts);
    return false;
  }

  Duration receiveWindow(Duration admissionWait) {
    Duration base;
    if (mode == Mode.FIXED) {
      base = fixedReceiveTime;
    } else {
      base = currentReceive;
    }
    if (base == null) {
      base = Duration.ofSeconds(1);
    }
    if (admissionWait != null && admissionWait.compareTo(base) < 0) {
      return admissionWait;
    }
    return base;
  }

  void onGatherFinished(long gatherNanos, boolean nonEmpty) {
    lastGatherNanos = gatherNanos;
    lastGatherEmpty = !nonEmpty;
    lastReturnNanos = nanoTime.getAsLong();
    pendingWrite = nonEmpty;
  }

  void onCommit() {
    if (!pendingWrite || batchInterval == null || mode != Mode.AUTO) {
      return;
    }
    long now = nanoTime.getAsLong();
    long write = measuredWriteNanos(now);
    if (write < 0) {
      return;
    }
    lastWriteNanos = write;
    lastCycleNanos = lastGatherNanos + write;
    if (!excludeNextWriteSample) {
      recordWriteSample(write);
    } else {
      excludeNextWriteSample = false;
    }
    pendingWrite = false;
    recomputeReceiveFromWrite(now);
  }

  /**
   * Lease used when {@code ackDeadline} is omitted: {@code 3 ×} batch interval, seed 180s, clamp
   * 60s–600s.
   */
  Duration inferredAckDeadline() {
    if (batchInterval == null) {
      return ACK_DEADLINE_SEED;
    }
    long seconds = Math.max(1L, batchInterval.getSeconds() * 3L);
    Duration inferred = Duration.ofSeconds(seconds);
    if (inferred.compareTo(ACK_DEADLINE_MIN) < 0) {
      return ACK_DEADLINE_MIN;
    }
    if (inferred.compareTo(ACK_DEADLINE_MAX) > 0) {
      return ACK_DEADLINE_MAX;
    }
    return inferred;
  }

  private void maybeIdleRaiseBatchInterval(long now) {
    if (batchIntervalFrozen || lastOffsetStartNanos == 0L || batchInterval == null) {
      return;
    }
    long gap = now - lastOffsetStartNanos;
    if (gap <= batchInterval.toNanos()) {
      return;
    }
    long write = calcWriteTime(now);
    long cycle = lastGatherNanos + write;
    long margin = receiveWriteMarginNanos(batchInterval.toNanos());
    boolean sparkSlept = lastGatherEmpty || gap > cycle + margin;
    if (!sparkSlept) {
      return;
    }
    if (autoCycles < 1 && !lastGatherEmpty) {
      return;
    }
    batchInterval = Duration.ofNanos(gap);
    batchIntervalFrozen = true;
    adjustCount = 0;
    stableStreak = 0;
    stabilizedForLogging = false;
    postStableLogRemaining = 0;
    resetWriteSamples();
    recomputeReceiveFromWrite(now);
    LOG.info(
        "AUTO: inferred batch interval={} receiveTime={} (idle leftover after a micro-batch;"
            + " batch interval is done)",
        format(batchInterval),
        format(currentReceive));
  }

  long calcWriteTime(long now) {
    long write = lastWriteNanos;
    if (pendingWrite && lastReturnNanos != 0L) {
      write = Math.max(write, now - lastReturnNanos);
    }
    return write;
  }

  boolean waitingToAdjustReceive() {
    if (mode != Mode.AUTO || batchInterval == null || currentReceive == null) {
      return true;
    }
    if (lastGatherNanos <= 0L && lastWriteNanos <= 0L) {
      return true;
    }
    return false;
  }

  private long measuredWriteNanos(long now) {
    long write = now - lastReturnNanos;
    if (lastMicroBatchStartNanos > 0L && lastGatherNanos > 0L) {
      long fromBatchStart = now - lastMicroBatchStartNanos;
      write = Math.max(write, fromBatchStart - lastGatherNanos);
    }
    return write;
  }

  private void recomputeReceiveFromWrite(long now) {
    long intervalNanos = batchInterval.toNanos();
    long margin = receiveWriteMarginNanos(intervalNanos);
    runSanityChecks(intervalNanos, margin);

    long writeEst = computeEstimatedWrite(intervalNanos);
    estimatedWriteNanos = writeEst;
    Duration priorReceive = currentReceive;
    Duration target = receiveFromWriteEstimate(batchInterval, writeEst);

    long priorNanos = priorReceive == null ? 0L : priorReceive.toNanos();
    long targetNanos = target.toNanos();
    long delta = targetNanos - priorNanos;
    if (delta > MAX_RECEIVE_STEP_NANOS) {
      targetNanos = priorNanos + MAX_RECEIVE_STEP_NANOS;
    } else if (delta < -MAX_RECEIVE_STEP_NANOS) {
      targetNanos = priorNanos - MAX_RECEIVE_STEP_NANOS;
    }
    target = Duration.ofNanos(targetNanos);
    target = clampReceive(target, batchInterval, writeEst);

    if (priorReceive != null
        && receiveDeltaNanos(priorReceive, target) <= STABLE_RECEIVE_DELTA_NANOS) {
      stableStreak++;
    } else {
      stableStreak = 0;
    }
    currentReceive = target;
    adjustCount++;

    if (!stabilizedForLogging
        && adjustCount >= MIN_TUNE_ADJUSTS
        && stableStreak >= STABLE_RECEIVE_CYCLES) {
      stabilizedForLogging = true;
      postStableLogRemaining = POST_STABLE_LOG_BATCHES;
      LOG.info(
          "AUTO: write-first estimates stable after {} adjust(s); post-stable logging for {}"
              + " batch(es)",
          adjustCount,
          postStableLogRemaining);
    }

    long cycle = lastCycleNanos > 0 ? lastCycleNanos : lastGatherNanos + calcWriteTime(now);
    boolean behind = cycle > intervalNanos + margin;
    if (!stabilizedForLogging || postStableLogRemaining > 0) {
      LOG.info(
          "AUTO: write-first gather={} write={} writeEst={} receive {} -> {} cycle={} behind={}",
          format(Duration.ofNanos(lastGatherNanos)),
          format(Duration.ofNanos(lastWriteNanos)),
          format(Duration.ofNanos(writeEst)),
          format(priorReceive),
          format(currentReceive),
          format(Duration.ofNanos(cycle)),
          behind);
    }
    if (postStableLogRemaining > 0) {
      postStableLogRemaining--;
      if (postStableLogRemaining == 0) {
        LOG.info(
            "AUTO: post-stable logging complete; writeEst={} receive={}",
            format(Duration.ofNanos(writeEst)),
            format(currentReceive));
      }
    }
  }

  private void runSanityChecks(long intervalNanos, long margin) {
    if (lastWriteNanos > 0 && estimatedWriteNanos > 0) {
      if (lastWriteNanos > estimatedWriteNanos * 2L) {
        writeSpikeStreak++;
      } else {
        writeSpikeStreak = 0;
      }
      if (writeSpikeStreak >= SANITY_STREAK_THRESHOLD) {
        LOG.warn(
            "AUTO: write {} exceeds 2× writeEst {} for {} batch(es); resetting write samples",
            format(Duration.ofNanos(lastWriteNanos)),
            format(Duration.ofNanos(estimatedWriteNanos)),
            writeSpikeStreak);
        resetWriteSamples();
        writeSpikeStreak = 0;
      }
    }

    if (lastCycleNanos > intervalNanos + margin) {
      chronicBehindStreak++;
    } else {
      chronicBehindStreak = 0;
    }

    long writeEst = computeEstimatedWrite(intervalNanos);
    long maxReceive = intervalNanos - writeEst - margin;
    if (chronicBehindStreak >= SANITY_STREAK_THRESHOLD
        && currentReceive != null
        && currentReceive.toNanos() >= maxReceive - STABLE_RECEIVE_DELTA_NANOS) {
      LOG.warn(
          "AUTO: chronically behind interval with receive at clamp; bumping write estimate"
              + " (streak={})",
          chronicBehindStreak);
      resetWriteSamples();
      if (lastWriteNanos > 0) {
        recordWriteSample(lastWriteNanos);
      }
      chronicBehindStreak = 0;
    }

    long floor = receiveFloorNanos(intervalNanos);
    if (currentReceive != null
        && currentReceive.toNanos() <= floor + STABLE_RECEIVE_DELTA_NANOS
        && chronicBehindStreak > 0) {
      LOG.warn("AUTO: receive at floor while behind; resetting write samples to reserve");
      resetWriteSamples();
      chronicBehindStreak = 0;
    }
  }

  private long computeEstimatedWrite(long intervalNanos) {
    long reserve = receiveWriteReserveNanos(intervalNanos);
    return Math.max(reserve, smoothedWriteNanos());
  }

  private void enterZeroTrigger() {
    mode = Mode.ZERO;
    batchInterval = null;
    currentReceive = ZERO_TRIGGER_RECEIVE;
    if (!zeroTriggerWarned) {
      zeroTriggerWarned = true;
      LOG.warn(
          "AUTO: receiveTime is unset and Spark trigger looks like Trigger.ProcessingTime(0);"
              + " using receiveTime={}. Set receiveTime explicitly to override.",
          format(currentReceive));
    }
  }

  static Duration firstReceive(Duration batchInterval) {
    return batchInterval.dividedBy(2);
  }

  static long receiveFloorNanos(long batchIntervalNanos) {
    long quarter = batchIntervalNanos / 4L;
    return Math.max(RECEIVE_FLOOR_NANOS, quarter);
  }

  static long receiveWriteMarginNanos(long batchIntervalNanos) {
    long scaled = (long) (batchIntervalNanos * RECEIVE_WRITE_MARGIN_PERCENT);
    return Math.max(RECEIVE_WRITE_MARGIN_MIN_NANOS, scaled);
  }

  static long receiveWriteReserveNanos(long batchIntervalNanos) {
    long scaled = (long) (batchIntervalNanos * RECEIVE_WRITE_RESERVE_PERCENT);
    return Math.max(RECEIVE_WRITE_MARGIN_MIN_NANOS, scaled);
  }

  static Duration receiveFromWriteEstimate(Duration batchInterval, long writeEstNanos) {
    long intervalNanos = batchInterval.toNanos();
    long margin = receiveWriteMarginNanos(intervalNanos);
    long reserve = receiveWriteReserveNanos(intervalNanos);
    long writeForBudget = Math.max(Math.max(writeEstNanos, 0L), reserve);
    long floor = receiveFloorNanos(intervalNanos);
    long v = intervalNanos - writeForBudget - margin;
    if (v < floor) {
      return Duration.ofNanos(floor);
    }
    return Duration.ofNanos(v);
  }

  static Duration clampReceive(Duration value, Duration batchInterval, long writeEstNanos) {
    Duration target = receiveFromWriteEstimate(batchInterval, writeEstNanos);
    long intervalNanos = batchInterval.toNanos();
    long floor = receiveFloorNanos(intervalNanos);
    long max = target.toNanos();
    if (max < floor) {
      max = Math.max(floor, intervalNanos / 2);
    }
    long v = value.toNanos();
    if (v < floor) {
      return Duration.ofNanos(floor);
    }
    if (v > max) {
      return Duration.ofNanos(max);
    }
    return value;
  }

  private void recordWriteSample(long writeNanos) {
    if (recentWriteCount < WRITE_SMOOTH_SAMPLES) {
      recentWriteNanos[recentWriteCount++] = writeNanos;
      return;
    }
    System.arraycopy(recentWriteNanos, 1, recentWriteNanos, 0, WRITE_SMOOTH_SAMPLES - 1);
    recentWriteNanos[WRITE_SMOOTH_SAMPLES - 1] = writeNanos;
  }

  private long smoothedWriteNanos() {
    long max = 0L;
    for (int i = 0; i < recentWriteCount; i++) {
      max = Math.max(max, recentWriteNanos[i]);
    }
    if (max == 0L && lastWriteNanos > 0) {
      max = lastWriteNanos;
    }
    return max;
  }

  private void resetWriteSamples() {
    recentWriteCount = 0;
    Arrays.fill(recentWriteNanos, 0L);
  }

  private static long receiveDeltaNanos(Duration a, Duration b) {
    return Math.abs(a.toNanos() - b.toNanos());
  }

  static String format(Duration duration) {
    if (duration == null) {
      return "-";
    }
    long ms = duration.toMillis();
    if (ms % 1000L == 0L) {
      return (ms / 1000L) + "s";
    }
    return ms + "ms";
  }
}
