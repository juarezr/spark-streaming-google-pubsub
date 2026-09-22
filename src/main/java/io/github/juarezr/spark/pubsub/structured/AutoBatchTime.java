package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sizes {@code batchTime} when the option is omitted: empty-gap probe for Spark {@code
 * Trigger.ProcessingTime} until one gap is at least 10s (or 3 sub-second gaps assume 0), then
 * {@code T/2}, then {@code T - writeAvg - writeStdev - safety}. Start-to-start gaps may raise
 * {@code T} when Spark waited (idle 1s slack; busy {@code max(5s, 10% T)}). A raise clears stale
 * overrun. After T is at least 30s or after any raise, five matching no-raise batches freeze
 * further raises. Never shrinks {@code T}. Never re-enters PROBE.
 *
 * <pre>
 * @startuml
 * !theme vibrant
 * title Auto batchTime: PROBE then AUTO then freeze raises
 *
 * participant App
 * participant Spark
 * participant Stream as PubSubMicroBatchStream
 * participant Auto as AutoBatchTime
 *
 * App -> Spark : format("google-pubsub")\noption(gatherMode=batch)\nbatchTime omitted\ntrigger(ProcessingTime)
 * Spark -> Stream : new(config)
 * Stream -> Auto : create(config)
 * note right of Auto
 *   gatherMode=batch and batchTime unset
 *   starts Mode.PROBE, not AUTO
 * end note
 *
 * == PROBE: empty latestOffset, no Pull; finite ==
 *
 * Spark -> Stream : latestOffset()
 * Stream -> Auto : shouldProbe()
 * note right of Auto
 *   probeAttempt=1 starts the gap timer
 *   returns true (empty, no Pull)
 * end note
 * Auto --> Stream : true
 * Stream --> Spark : empty Offset
 *
 * Spark -> Stream : latestOffset()
 * Stream -> Auto : shouldProbe()
 * alt gap < 1s
 *   note right of Auto
 *     Trigger noise. After 3 consecutive
 *     cycles: Mode.ZERO, gather =
 *     min(pullDeadline, 5s). Leave PROBE.
 *   end note
 *   Auto --> Stream : true (stay PROBE)
 * else 1s <= gap < 10s
 *   note right of Auto
 *     Startup noise: do not seed T.
 *     Reset the probe start; stay PROBE
 *   end note
 *   Auto --> Stream : true (stay PROBE)
 * else gap >= 10s
 *   Auto -> Auto : T = gap\ngather = T / 2\nmode = AUTO
 *   note right of Auto
 *     PROBE ends. Later latestOffset
 *     always Pulls. Never re-enter PROBE.
 *   end note
 *   Auto --> Stream : false
 * end
 *
 * == AUTO: Pull; raise T only until 5 matching batches ==
 *
 * loop each later latestOffset until raiseFrozen
 *   Spark -> Stream : latestOffset()
 *   Stream -> Auto : shouldProbe()
 *   note right of Auto
 *     Always false (does Pull).
 *     If raiseFrozen: skip raise.
 *     Else slack = 1s if last gather
 *     empty, else max(5s, 10% of T).
 *     Raise when gap > T + slack
 *     AND Spark waited:
 *     last gather empty, or
 *     gap > gather + write + slack.
 *     On raise: T = gap, clear
 *     lastOverrunNanos, reset
 *     matching count. Never shrink T.
 *   end note
 *   alt no raise
 *     note right of Auto
 *       If armed (T >= 30s or
 *       raiseCount >= 1): matching++.
 *       After 5 matching: raiseFrozen.
 *       A 16s seed is not armed.
 *     end note
 *   end
 *   Auto --> Stream : false
 *   Stream -> Auto : gatherWindow(admissionWait)
 *   note right of Auto
 *     No write samples: gather = T / 2
 *     Else: T - writeAvg - writeStdev - safety
 *     safety = 0.5% * T + overrun
 *     (500ms overrun seed after first write)
 *     clamp [min(pullDeadline, 5s) .. T - 1s]
 *     window = min(admissionWait, gather)
 *   end note
 *   Stream -> Stream : Pull until gather window
 *   Stream -> Auto : onGatherFinished(nanos, nonEmpty)
 *   Stream --> Spark : Offset
 *
 *   Spark -> Stream : commit()
 *   Stream -> Auto : onCommit()
 *   note right of Auto
 *     Non-empty gather only:
 *     record write sample, store overrun
 *     if gather + write > T, then
 *     refreshGather()
 *   end note
 * end
 * @enduml
 * </pre>
 */
final class AutoBatchTime {
  private static final Logger LOG = LoggerFactory.getLogger(AutoBatchTime.class);

  static final long PROBE_NOISE_NANOS = TimeUnit.SECONDS.toNanos(1);

  /** Busy start-to-start must exceed work and T by this floor so wal/planning is not a wait. */
  static final long BUSY_WAIT_MIN_NANOS = TimeUnit.SECONDS.toNanos(5);

  static final double BUSY_WAIT_FRACTION = 0.10;

  /**
   * First inferred Trigger.ProcessingTime below this is startup noise; keep probing instead of
   * seeding.
   */
  static final long SEED_MIN_NANOS = TimeUnit.SECONDS.toNanos(10);

  static final int ZERO_TRIGGER_NOISE_CYCLES = 3;
  static final int WRITE_WINDOW = 8;
  static final double SAFETY_FRACTION = 0.005;
  static final long OVERRUN_SEED_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
  static final Duration ZERO_TRIGGER_MAX_GATHER = Duration.ofSeconds(5);

  /** Consecutive no-raise AUTO batches after T is armed, then freeze raises. */
  static final int STABLE_NO_RAISE_BATCHES = 5;

  /** Arm the stability counter once T is at least this, or after any raise. */
  static final long STABLE_ARM_MIN_NANOS = TimeUnit.SECONDS.toNanos(30);

  enum Mode {
    FIXED,
    PROBE,
    AUTO,
    ZERO
  }

  private final LongSupplier nanoTime;
  private final Duration pullDeadline;
  private final Duration fixedBatchTime;
  private Mode mode;
  private Long probeStartedNanos;
  private Duration processingTime;
  private Duration currentGather;
  private final long[] writeNanos = new long[WRITE_WINDOW];
  private int writeCount;
  private int writeIndex;
  private long lastGatherNanos;
  private long lastWriteNanos;
  private long lastReturnNanos;
  private long lastOffsetStartNanos;
  private boolean lastGatherEmpty;
  private boolean lastNonEmpty;
  private boolean pendingWrite;
  private int finishedNonEmpty;
  private int probeNoiseCount;
  private int probeAttempts;
  private boolean zeroTriggerWarned;
  private Long lastOverrunNanos;
  private int raiseCount;
  private int stableNoRaiseCount;
  private boolean raiseFrozen;

  static AutoBatchTime create(PubSubConfig config) {
    return create(config, System::nanoTime);
  }

  static AutoBatchTime create(PubSubConfig config, LongSupplier nanoTime) {
    Duration configured = config.batchTime();
    if (config.gatherMode() == GatherMode.BATCH && configured == null) {
      return new AutoBatchTime(null, config.pullDeadline(), nanoTime);
    }
    Duration fixed = configured != null ? configured : Duration.ofSeconds(10);
    return new AutoBatchTime(fixed, config.pullDeadline(), nanoTime);
  }

  AutoBatchTime(Duration fixedBatchTime, Duration pullDeadline, LongSupplier nanoTime) {
    this.fixedBatchTime = fixedBatchTime;
    this.pullDeadline = pullDeadline == null ? PubSubConfig.DEFAULT_PULL_DEADLINE : pullDeadline;
    this.nanoTime = nanoTime;
    this.mode = fixedBatchTime == null ? Mode.PROBE : Mode.FIXED;
  }

  Mode mode() {
    return mode;
  }

  Duration processingTime() {
    return processingTime;
  }

  Duration currentGather() {
    return currentGather;
  }

  /**
   * @return {@code true} when this {@code latestOffset} must return empty without Pulling
   */
  boolean shouldProbe() {
    long now = nanoTime.getAsLong();
    if (mode == Mode.AUTO) {
      maybeRaiseFromStart(now);
      lastOffsetStartNanos = now;
      observeCycle(now);
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
          "batchTime is unset; inferring Spark ProcessingTime from idle gaps (empty latestOffset,"
              + " no Pull) until one gap is at least 10s. In a 3 sub-second gap sequence assume"
              + " ProcessingTime(0). probeAttempt=1");
      return true;
    }
    probeAttempts++;
    long gapFromStart = now - probeStartedNanos;
    lastOffsetStartNanos = now;
    if (gapFromStart < PROBE_NOISE_NANOS) {
      probeNoiseCount++;
      LOG.info(
          "BATCH: probeAttempt={} gap {} is under 1s ({}/{} noise); keep probing",
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
          "BATCH: probeAttempt={} inferred processingTime={} is under 10s; not seeding (keep"
              + " probing)",
          probeAttempts,
          format(Duration.ofNanos(gapFromStart)));
      return true;
    }
    probeNoiseCount = 0;
    processingTime = Duration.ofNanos(gapFromStart);
    currentGather = firstGather(processingTime);
    mode = Mode.AUTO;
    LOG.info(
        "BATCH: inferred processingTime={} first gather={} (seed probeAttempt={}; start-to-start"
            + " wait may raise Trigger.ProcessingTime)",
        format(processingTime),
        format(currentGather),
        probeAttempts);
    return false;
  }

  Duration gatherWindow(Duration admissionWait) {
    Duration base;
    if (mode == Mode.FIXED) {
      base = fixedBatchTime;
    } else {
      base = currentGather;
    }
    if (base == null) {
      base = pullDeadline;
    }
    if (admissionWait != null && admissionWait.compareTo(base) < 0) {
      return admissionWait;
    }
    return base;
  }

  void onGatherFinished(long gatherNanos, boolean nonEmpty) {
    lastGatherNanos = gatherNanos;
    lastNonEmpty = nonEmpty;
    lastGatherEmpty = !nonEmpty;
    lastReturnNanos = nanoTime.getAsLong();
    pendingWrite = nonEmpty;
  }

  void onCommit() {
    if (!pendingWrite || processingTime == null || mode != Mode.AUTO) {
      return;
    }
    long write = nanoTime.getAsLong() - lastReturnNanos;
    if (write < 0) {
      return;
    }
    long cycle = lastGatherNanos + write;
    long t = processingTime.toNanos();
    if (cycle > t) {
      lastOverrunNanos = cycle - t;
    }
    lastWriteNanos = write;
    recordWrite(write);
    pendingWrite = false;
    finishedNonEmpty++;
    refreshGather();
  }

  private void maybeRaiseFromStart(long now) {
    if (raiseFrozen || lastOffsetStartNanos == 0L || processingTime == null) {
      return;
    }
    long gap = now - lastOffsetStartNanos;
    long slack = lastGatherEmpty ? PROBE_NOISE_NANOS : busyWaitSlackNanos();
    long processingSlack = processingTime.toNanos() + slack;
    boolean waited = sparkWaited(now, gap, slack);
    if (gap <= processingSlack || !waited) {
      noteStableNoRaise();
      return;
    }
    processingTime = Duration.ofNanos(gap);
    lastOverrunNanos = null;
    raiseCount++;
    stableNoRaiseCount = 0;
    refreshGather();
    LOG.info(
        "BATCH: inferred processingTime={} gather={} (raise #{}; start-to-start wait may raise"
            + " Trigger.ProcessingTime again until {} matching batches)",
        format(processingTime),
        format(currentGather),
        raiseCount,
        STABLE_NO_RAISE_BATCHES);
  }

  private void noteStableNoRaise() {
    if (!raiseArmed()) {
      return;
    }
    stableNoRaiseCount++;
    if (stableNoRaiseCount < STABLE_NO_RAISE_BATCHES) {
      return;
    }
    raiseFrozen = true;
    LOG.info(
        "BATCH: processingTime={} stable after {} matching batches; stop raising T",
        format(processingTime),
        stableNoRaiseCount);
  }

  private boolean raiseArmed() {
    if (raiseCount >= 1) {
      return true;
    }
    return processingTime != null && processingTime.toNanos() >= STABLE_ARM_MIN_NANOS;
  }

  boolean raiseFrozen() {
    return raiseFrozen;
  }

  private boolean sparkWaited(long now, long gap, long slack) {
    if (lastGatherEmpty) {
      return true;
    }
    long write =
        pendingWrite && lastReturnNanos != 0L
            ? Math.max(0L, now - lastReturnNanos)
            : lastWriteNanos;
    return gap > lastGatherNanos + write + slack;
  }

  private long busyWaitSlackNanos() {
    if (processingTime == null) {
      return BUSY_WAIT_MIN_NANOS;
    }
    return Math.max(BUSY_WAIT_MIN_NANOS, (long) (BUSY_WAIT_FRACTION * processingTime.toNanos()));
  }

  private void enterZeroTrigger() {
    mode = Mode.ZERO;
    processingTime = null;
    currentGather = min(pullDeadline, ZERO_TRIGGER_MAX_GATHER);
    if (!zeroTriggerWarned) {
      zeroTriggerWarned = true;
      LOG.warn(
          "batchTime is unset and Spark trigger looks like ProcessingTime(0); using gather={}."
              + " Set batchTime explicitly to override.",
          format(currentGather));
    }
  }

  private void observeCycle(long now) {
    if (!lastNonEmpty || lastReturnNanos == 0 || processingTime == null) {
      return;
    }
    long gap = now - lastReturnNanos;
    long t = processingTime.toNanos();
    long cycle = lastGatherNanos + gap;
    if (cycle > t) {
      lastOverrunNanos = cycle - t;
      if (pendingWrite) {
        recordWrite(gap);
        pendingWrite = false;
        finishedNonEmpty++;
      }
    }
    lastNonEmpty = false;
    refreshGather();
  }

  void recordWrite(long sampleNanos) {
    if (sampleNanos < 0) {
      return;
    }
    writeNanos[writeIndex] = sampleNanos;
    writeIndex = (writeIndex + 1) % WRITE_WINDOW;
    writeCount++;
  }

  void refreshGather() {
    if (mode != Mode.AUTO || processingTime == null) {
      return;
    }
    if (writeCount == 0) {
      currentGather = firstGather(processingTime);
      return;
    }
    long t = processingTime.toNanos();
    long overrun = lastOverrunNanos != null ? lastOverrunNanos : overrunSeed();
    long safety = (long) (SAFETY_FRACTION * t) + overrun;
    long next = t - (long) writeAvg() - (long) writeStdev() - safety;
    currentGather = clamp(Duration.ofNanos(Math.max(0L, next)), processingTime, pullDeadline);
  }

  private long overrunSeed() {
    return finishedNonEmpty >= 1 ? OVERRUN_SEED_NANOS : 0L;
  }

  static Duration firstGather(Duration processingTime) {
    return processingTime.dividedBy(2);
  }

  static Duration clamp(Duration value, Duration processingTime, Duration pullDeadline) {
    Duration minGather = min(pullDeadline, Duration.ofSeconds(5));
    Duration maxGather = processingTime.minusSeconds(1);
    if (maxGather.isNegative() || maxGather.isZero()) {
      maxGather = firstGather(processingTime);
    }
    if (minGather.compareTo(maxGather) > 0) {
      minGather = maxGather.compareTo(Duration.ofMillis(1)) < 0 ? Duration.ofMillis(1) : maxGather;
    }
    if (value.compareTo(minGather) < 0) {
      return minGather;
    }
    if (value.compareTo(maxGather) > 0) {
      return maxGather;
    }
    return value;
  }

  double writeAvg() {
    int n = sampleCount();
    if (n == 0) {
      return 0;
    }
    long sum = 0L;
    for (int i = 0; i < n; i++) {
      sum += writeNanos[i];
    }
    return (double) sum / n;
  }

  double writeStdev() {
    int n = sampleCount();
    if (n < 2) {
      return 0;
    }
    double avg = writeAvg();
    double var = 0;
    for (int i = 0; i < n; i++) {
      double d = writeNanos[i] - avg;
      var += d * d;
    }
    return Math.sqrt(var / (n - 1));
  }

  private int sampleCount() {
    return Math.min(writeCount, WRITE_WINDOW);
  }

  private static Duration min(Duration left, Duration right) {
    return left.compareTo(right) <= 0 ? left : right;
  }

  private static String format(Duration duration) {
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
