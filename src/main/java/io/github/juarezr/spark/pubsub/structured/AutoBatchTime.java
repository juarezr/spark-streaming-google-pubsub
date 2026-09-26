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
 * Infers a sane {@code receiveTime} when the option is omitted so receive + write fits the Spark
 * batch interval ({@code Trigger.ProcessingTime}). Probe idle micro-batch gaps for the batch
 * interval, then one start-to-start leftover raise if Spark slept, then approximate receive until
 * stable: overrun shrinks by half the excess, leftover idle raises by half the gap. Never sets the
 * batch interval from gather + write.
 *
 * <pre>
 * @startuml
 * !theme vibrant
 * title Auto receiveTime: PROBE then idle leftover / half overrun-shrink
 *
 * participant App
 * participant Spark
 * participant Stream as PubSubMicroBatchStream
 * participant Auto as AutoBatchTime
 *
 * App -> Spark : format("google-pubsub")\noption(gatherMode=batch)\nreceiveTime omitted\ntrigger(ProcessingTime)
 * Spark -> Stream : new(config)
 * Stream -> Auto : create(config)
 *
 * == PROBE: empty micro-batch; finite ==
 *
 * Spark -> Stream : next micro-batch
 * Stream -> Auto : shouldProbe()
 * alt gap >= 10s
 *   Auto -> Auto : batchInterval = gap\nreceive = batchInterval / 2\nmode = AUTO
 * else gap < 1s three times
 *   Auto -> Auto : Mode.ZERO receive = 1s
 * end
 *
 * == AUTO: one leftover raise; never grow from busy ==
 *
 * loop each later micro-batch
 *   Spark -> Stream : next micro-batch
 *   Stream -> Auto : shouldProbe()
 *   note right of Auto
 *     After the first AUTO micro-batch,
 *     if start-to-start > seed and Spark
 *     slept (gap > gather + write + margin):
 *     one re-measure of batch interval,
 *     then batch interval is done.
 *     Busy cycles (cycle ≈ gap) never
 *     change it.
 *     margin = max(1s, 1.6% of interval).
 *     Overrun shrink uses half excess;
 *     ignore overrun within margin.
 *     Freeze when receive stable for 5
 *     adjusts (min 5), unless still
 *     chronically over interval; max 32.
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
  static final int MAX_TUNE_ADJUSTS = 32;
  static final long STABLE_RECEIVE_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
  static final int WRITE_SMOOTH_SAMPLES = 5;
  static final long RECEIVE_FLOOR_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final double RECEIVE_WRITE_MARGIN_PERCENT = 1.0 / 60.0;
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
  private boolean lastGatherEmpty;
  private boolean pendingWrite;
  private int probeNoiseCount;
  private int probeAttempts;
  private boolean zeroTriggerWarned;
  private boolean batchIntervalFrozen;
  private int autoCycles;
  private int adjustCount;
  private int stableStreak;
  private boolean receiveFrozen;
  private boolean frozenAtMaxTuneCap;
  private final long[] recentWriteNanos = new long[WRITE_SMOOTH_SAMPLES];
  private int recentWriteCount;
  private boolean recentOverrun0;
  private boolean recentOverrun1;
  private boolean recentOverrun2;

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

  boolean receiveFrozen() {
    return receiveFrozen;
  }

  boolean frozenAtMaxTuneCap() {
    return frozenAtMaxTuneCap;
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

  /**
   * @return {@code true} when this micro-batch must return empty without receiving
   */
  boolean shouldProbe() {
    long now = nanoTime.getAsLong();
    if (mode == Mode.AUTO) {
      maybeIdleRaiseBatchInterval(now);
      autoCycles++;
      if (autoCycles >= 2) {
        maybeAdjustReceive(now);
      }
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
    long write = nanoTime.getAsLong() - lastReturnNanos;
    if (write < 0) {
      return;
    }
    lastWriteNanos = write;
    recordWriteSample(write);
    pendingWrite = false;
    if (batchIntervalFrozen) {
      maybeAdjustReceive(nanoTime.getAsLong());
    }
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

  /**
   * One raise of the batch interval from micro-batch start-to-start leftover (Spark slept). Never
   * sets the interval from {@code cycle = gather + write}. Skips the first AUTO micro-batch when
   * that cycle is a busy overrun (startup).
   */
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
    receiveFrozen = false;
    frozenAtMaxTuneCap = false;
    adjustCount = 0;
    stableStreak = 0;
    resetTuneHistory();
    if (currentReceive == null) {
      currentReceive = firstReceive(batchInterval);
    } else {
      currentReceive = clampReceive(currentReceive, batchInterval, smoothedWriteNanos());
    }
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
    if (receiveFrozen || mode != Mode.AUTO || batchInterval == null || currentReceive == null) {
      return true;
    }
    if (lastGatherNanos <= 0L && lastWriteNanos <= 0L) {
      return true;
    }
    return false;
  }

  private void maybeAdjustReceive(long now) {
    boolean waiting = waitingToAdjustReceive();
    if (waiting) {
      return;
    }
    long intervalNanos = batchInterval.toNanos();
    long margin = receiveWriteMarginNanos(intervalNanos);
    long write = calcWriteTime(now);
    long cycle = lastGatherNanos + write;
    long floor = receiveFloorNanos(intervalNanos);
    Duration priorReceive = currentReceive;
    Duration next = currentReceive;
    if (cycle > intervalNanos) {
      long overrun = cycle - intervalNanos;
      if (overrun > margin) {
        long shrink = overrun / 2L;
        next = Duration.ofNanos(Math.max(floor, currentReceive.toNanos() - shrink));
        LOG.info(
            "AUTO: overrun cycle={} > batch interval={}; shrink receiveTime {} -> {} (half"
                + " excess {})",
            format(Duration.ofNanos(cycle)),
            format(batchInterval),
            format(currentReceive),
            format(next),
            format(Duration.ofNanos(overrun)));
      }
    } else if (lastGatherEmpty || cycle + margin < intervalNanos) {
      long leftover = intervalNanos - cycle;
      if (leftover > margin) {
        next = Duration.ofNanos(currentReceive.toNanos() + leftover / 2L);
      }
    }
    next = clampReceive(next, batchInterval, smoothedWriteNanos());
    if (receiveDeltaNanos(priorReceive, next) <= STABLE_RECEIVE_DELTA_NANOS) {
      stableStreak++;
    } else {
      stableStreak = 0;
    }
    currentReceive = next;
    adjustCount++;
    pushRecentOverrun(cycle > intervalNanos + margin);
    maybeFreezeReceive();
  }

  private void maybeFreezeReceive() {
    if (receiveFrozen) {
      return;
    }
    if (adjustCount >= MAX_TUNE_ADJUSTS) {
      receiveFrozen = true;
      frozenAtMaxTuneCap = true;
      LOG.warn(
          "AUTO: receiveTime={} frozen at max tune cap after {} adjust(s)",
          format(currentReceive),
          adjustCount);
      return;
    }
    if (adjustCount < MIN_TUNE_ADJUSTS || stableStreak < STABLE_RECEIVE_CYCLES) {
      return;
    }
    if (recentChronicOverrun()) {
      return;
    }
    receiveFrozen = true;
    frozenAtMaxTuneCap = false;
    LOG.info(
        "AUTO: receiveTime={} frozen after {} adjust(s), stable streak {}",
        format(currentReceive),
        adjustCount,
        stableStreak);
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

  static Duration clampReceive(Duration value, Duration batchInterval, long writeNanos) {
    long intervalNanos = batchInterval.toNanos();
    long floor = receiveFloorNanos(intervalNanos);
    long margin = receiveWriteMarginNanos(intervalNanos);
    long max = intervalNanos - Math.max(writeNanos, 0L) - margin;
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
    long max = lastWriteNanos;
    for (int i = 0; i < recentWriteCount; i++) {
      max = Math.max(max, recentWriteNanos[i]);
    }
    return max;
  }

  private void resetTuneHistory() {
    recentWriteCount = 0;
    Arrays.fill(recentWriteNanos, 0L);
    recentOverrun0 = false;
    recentOverrun1 = false;
    recentOverrun2 = false;
  }

  private void pushRecentOverrun(boolean chronic) {
    recentOverrun2 = recentOverrun1;
    recentOverrun1 = recentOverrun0;
    recentOverrun0 = chronic;
  }

  private boolean recentChronicOverrun() {
    return recentOverrun0 || recentOverrun1 || recentOverrun2;
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
