package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.common.Into;
import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fills {@code receiveTime} when the app omits it.
 *
 * <p>{@code cycleTimeEst} is Spark's trigger, stored as a whole second. {@code receiveBudget} for
 * the next cycle is {@code cycleTimeEst − writeBudget − marginTimeEst}. {@code writeBudget} is the
 * forecast of this cycle's {@code writeDuration}: it rises to a heavier measurement immediately and
 * otherwise moves halfway toward it.
 */
final class AutoReceiveTime {
  private static final Logger LOG = LoggerFactory.getLogger(AutoReceiveTime.class);

  static final long PROBE_NOISE_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final long SEED_MIN_NANOS = TimeUnit.SECONDS.toNanos(10);
  static final long MARGIN_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final long RECEIVE_FLOOR_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final int PROBE_GAP_SAMPLES = 3;
  static final double PROBE_GAP_STABLE_RATIO = 0.10;
  static final double PROBE_FULL_PERIOD_RATIO = 1.5;
  static final int ZERO_TRIGGER_NOISE_CYCLES = 3;
  static final long SHORT_ESTIMATE_NANOS = TimeUnit.SECONDS.toNanos(45);
  static final double POST_FIRST_BATCH_CYCLE_RATIO = 1.25;
  static final Duration ZERO_TRIGGER_RECEIVE = Duration.ofSeconds(1);
  static final Duration ACK_DEADLINE_SEED = Duration.ofSeconds(180);
  static final Duration ACK_DEADLINE_MIN = Duration.ofSeconds(60);
  static final Duration ACK_DEADLINE_MAX = Duration.ofSeconds(600);

  enum Phase {
    FIXED,
    PROBE,
    WARMUP,
    CRUISE,
    ZERO
  }

  static final class CycleSnapshot {
    final Phase phase;
    final long cycleTimeEstNanos;
    final long marginTimeEstNanos;
    final long receiveBudgetNanos;
    final long receiveDurationNanos;
    final long commitDurationNanos;
    final long writeDurationNanos;
    final long writeBudgetNanos;
    final long sleepDurationNanos;
    final long cycleDurationNanos;
    final long behindDurationNanos;

    CycleSnapshot(
        Phase phase,
        long cycleTimeEstNanos,
        long marginTimeEstNanos,
        long receiveBudgetNanos,
        long receiveDurationNanos,
        long commitDurationNanos,
        long writeDurationNanos,
        long writeBudgetNanos,
        long sleepDurationNanos,
        long cycleDurationNanos,
        long behindDurationNanos) {
      this.phase = phase;
      this.cycleTimeEstNanos = cycleTimeEstNanos;
      this.marginTimeEstNanos = marginTimeEstNanos;
      this.receiveBudgetNanos = receiveBudgetNanos;
      this.receiveDurationNanos = receiveDurationNanos;
      this.commitDurationNanos = commitDurationNanos;
      this.writeDurationNanos = writeDurationNanos;
      this.writeBudgetNanos = writeBudgetNanos;
      this.sleepDurationNanos = sleepDurationNanos;
      this.cycleDurationNanos = cycleDurationNanos;
      this.behindDurationNanos = behindDurationNanos;
    }

    @Override
    public String toString() {
      return "AUTO phase="
          + phase.name().toLowerCase()
          + " cycleTimeEst="
          + elapsed(cycleTimeEstNanos)
          + " marginTimeEst="
          + elapsed(marginTimeEstNanos)
          + " receiveBudget="
          + elapsed(receiveBudgetNanos)
          + " receiveDuration="
          + elapsed(receiveDurationNanos)
          + " commitDuration="
          + elapsed(commitDurationNanos)
          + " writeDuration="
          + elapsed(writeDurationNanos)
          + " writeBudget="
          + elapsed(writeBudgetNanos)
          + " sleepDuration="
          + elapsed(sleepDurationNanos)
          + " cycleDuration="
          + elapsed(cycleDurationNanos)
          + " behindDuration="
          + elapsed(behindDurationNanos);
    }

    private static String elapsed(long nanos) {
      return nanos <= 0L ? "0s" : Into.elapsed(nanos);
    }
  }

  private final LongSupplier nanoTime;
  private final Duration fixedReceiveTime;
  private final Duration processingTimeHint;
  private Phase phase;
  private Long probeStartedNanos;
  private long cycleTimeEstNanos;
  private long receiveBudgetNanos;
  private long writeBudgetNanos;
  private long receiveDurationNanos;
  private long commitDurationNanos;
  private long writeDurationNanos;
  private long sleepDurationNanos;
  private long cycleDurationNanos;
  private int probeNoiseCount;
  private int probeAttempts;
  private boolean zeroTriggerWarned;
  private boolean intervalRaised;
  private boolean postFirstHeavyChecked;
  private long returnNanos;
  private boolean commitRecorded;
  private boolean gatherEmpty;
  private int gatherCount;
  private long lastMicroBatchStartNanos;
  private final long[] probeGapNanos = new long[PROBE_GAP_SAMPLES];
  private int probeGapCount;
  private CycleSnapshot lastSnapshot;

  static AutoReceiveTime create(PubSubConfig config) {
    return create(config, System::nanoTime);
  }

  static AutoReceiveTime create(PubSubConfig config, LongSupplier nanoTime) {
    Duration configured = config.receiveTime();
    if (config.gatherMode() == GatherMode.BATCH && configured == null) {
      return new AutoReceiveTime(null, nanoTime, config.processingTime());
    }
    Duration fixed = configured != null ? configured : Duration.ofSeconds(1);
    return new AutoReceiveTime(fixed, nanoTime);
  }

  AutoReceiveTime(Duration fixedReceiveTime, LongSupplier nanoTime) {
    this(fixedReceiveTime, nanoTime, null);
  }

  AutoReceiveTime(Duration fixedReceiveTime, LongSupplier nanoTime, Duration processingTimeHint) {
    this.fixedReceiveTime = fixedReceiveTime;
    this.nanoTime = nanoTime;
    this.processingTimeHint = processingTimeHint;
    this.phase = fixedReceiveTime == null ? Phase.PROBE : Phase.FIXED;
    if (fixedReceiveTime != null) {
      this.receiveBudgetNanos = fixedReceiveTime.toNanos();
    }
  }

  Phase phase() {
    return phase;
  }

  Duration cycleTimeEst() {
    return cycleTimeEstNanos <= 0L ? null : Duration.ofNanos(cycleTimeEstNanos);
  }

  Duration currentReceive() {
    return receiveBudgetNanos <= 0L ? null : Duration.ofNanos(receiveBudgetNanos);
  }

  long writeBudgetNanos() {
    return writeBudgetNanos;
  }

  long writeDurationNanos() {
    return writeDurationNanos;
  }

  CycleSnapshot lastSnapshot() {
    return lastSnapshot;
  }

  /** The greatest whole second the gap has reached. 60.6s stays 60s. 61.882s stays 61s. */
  static long wholeSecondNanos(long gapNanos) {
    if (gapNanos <= 0L) {
      return 0L;
    }
    long second = TimeUnit.SECONDS.toNanos(1);
    return (gapNanos / second) * second;
  }

  static boolean probeGapConsistent(long max, long second) {
    if (max <= 0L || second <= 0L) {
      return false;
    }
    long delta = Math.abs(max - second);
    if (delta <= (long) (max * PROBE_GAP_STABLE_RATIO)) {
      return true;
    }
    return max >= (long) (second * PROBE_FULL_PERIOD_RATIO);
  }

  boolean shouldProbe() {
    long now = nanoTime.getAsLong();
    if (phase == Phase.WARMUP || phase == Phase.CRUISE) {
      observeCycle(now);
      if (phase == Phase.WARMUP || phase == Phase.CRUISE) {
        lastMicroBatchStartNanos = now;
      }
      return false;
    }
    if (phase != Phase.PROBE) {
      lastMicroBatchStartNanos = now;
      return false;
    }
    if (probeStartedNanos == null) {
      probeStartedNanos = now;
      probeAttempts = 1;
      LOG.info(
          "AUTO phase=probe cycleTimeEst=unset receiveBudget=unset"
              + " (empty micro-batch until the trigger gap is consistent)");
      return true;
    }
    probeAttempts++;
    long gap = now - probeStartedNanos;
    if (gap < PROBE_NOISE_NANOS) {
      probeNoiseCount++;
      if (probeNoiseCount >= ZERO_TRIGGER_NOISE_CYCLES) {
        enterZeroTrigger();
        return false;
      }
      return true;
    }
    if (gap < SEED_MIN_NANOS) {
      probeStartedNanos = now;
      return true;
    }
    probeNoiseCount = 0;
    recordProbeGap(gap);
    probeStartedNanos = now;
    if (processingTimeHint != null) {
      finishProbe(wholeSecondNanos(processingTimeHint.toNanos()));
      return false;
    }
    if (probeGapCount < 2) {
      return true;
    }
    long max = maxProbeGapNanos();
    long second = secondLargestProbeGapNanos();
    if (!probeGapConsistent(max, second)) {
      return true;
    }
    long chosen = max;
    if (Math.abs(max - second) <= (long) (max * PROBE_GAP_STABLE_RATIO)) {
      chosen = Math.min(max, second);
    }
    finishProbe(wholeSecondNanos(chosen));
    return false;
  }

  Duration receiveWindow(Duration admissionWait) {
    long base = phase == Phase.FIXED ? fixedReceiveTime.toNanos() : receiveBudgetNanos;
    if (base <= 0L) {
      base = RECEIVE_FLOOR_NANOS;
    }
    if (admissionWait != null && admissionWait.toNanos() < base) {
      return admissionWait;
    }
    return Duration.ofNanos(base);
  }

  void onGatherFinished(long gatherNanos, int messagesCount) {
    receiveDurationNanos = gatherNanos;
    gatherEmpty = messagesCount <= 0;
    gatherCount = messagesCount;
    commitRecorded = false;
    commitDurationNanos = 0L;
    returnNanos = nanoTime.getAsLong();
  }

  void onCommit() {
    if (gatherEmpty || gatherCount <= 0 || cycleTimeEstNanos <= 0L || returnNanos <= 0L) {
      return;
    }
    if (phase != Phase.WARMUP && phase != Phase.CRUISE) {
      return;
    }
    long now = nanoTime.getAsLong();
    commitDurationNanos = Math.max(0L, now - returnNanos);
    commitRecorded = true;
  }

  /**
   * Lease when {@code ackDeadline} is omitted: {@code 3 ×} the trigger estimate, seed 180s, clamp
   * 60s–600s. Not updated after the probe.
   */
  Duration inferredAckDeadline() {
    if (cycleTimeEstNanos <= 0L) {
      return ACK_DEADLINE_SEED;
    }
    long seconds = Math.max(1L, TimeUnit.NANOSECONDS.toSeconds(cycleTimeEstNanos) * 3L);
    Duration inferred = Duration.ofSeconds(seconds);
    if (inferred.compareTo(ACK_DEADLINE_MIN) < 0) {
      return ACK_DEADLINE_MIN;
    }
    if (inferred.compareTo(ACK_DEADLINE_MAX) > 0) {
      return ACK_DEADLINE_MAX;
    }
    return inferred;
  }

  private void finishProbe(long wholeSecond) {
    cycleTimeEstNanos = wholeSecond;
    receiveBudgetNanos = cycleTimeEstNanos / 2;
    writeBudgetNanos = cycleTimeEstNanos - receiveBudgetNanos - MARGIN_NANOS;
    phase = Phase.WARMUP;
    LOG.info(
        "AUTO phase=warmup cycleTimeEst={} receiveBudget={} writeBudget={} marginTimeEst=1s",
        Into.elapsed(cycleTimeEstNanos),
        Into.elapsed(receiveBudgetNanos),
        Into.elapsed(writeBudgetNanos));
  }

  private void observeCycle(long now) {
    if (lastMicroBatchStartNanos <= 0L) {
      return;
    }
    cycleDurationNanos = now - lastMicroBatchStartNanos;
    if (maybeCorrectShortEstimate()) {
      return;
    }
    maybeRaiseWholeSecond();
    if (commitRecorded && receiveDurationNanos > 0L) {
      applyWrite(cycleTimeEstNanos);
    }
    logSnapshot();
  }

  private void applyWrite(long estimate) {
    long expectedSleep = Math.max(0L, estimate - receiveDurationNanos - commitDurationNanos);
    long actualExtra = cycleDurationNanos - receiveDurationNanos - commitDurationNanos;
    boolean slept = actualExtra <= expectedSleep + MARGIN_NANOS;
    if (slept || cycleDurationNanos <= estimate) {
      writeDurationNanos = commitDurationNanos;
    } else {
      writeDurationNanos = Math.max(commitDurationNanos, cycleDurationNanos - receiveDurationNanos);
    }
    sleepDurationNanos =
        Math.max(0L, cycleDurationNanos - receiveDurationNanos - writeDurationNanos);
    if (phase == Phase.WARMUP) {
      writeBudgetNanos = writeDurationNanos;
      phase = Phase.CRUISE;
      return;
    }
    if (writeDurationNanos > writeBudgetNanos) {
      writeBudgetNanos = writeDurationNanos;
    } else if (slept || cycleDurationNanos <= estimate) {
      writeBudgetNanos = halfway(writeBudgetNanos, writeDurationNanos);
    }
    long target = estimate - writeBudgetNanos - MARGIN_NANOS;
    long floor = Math.max(RECEIVE_FLOOR_NANOS, estimate / 4L);
    long next = halfway(receiveBudgetNanos, target);
    if (next < floor) {
      next = floor;
    }
    receiveBudgetNanos = next;
  }

  private void maybeRaiseWholeSecond() {
    if (intervalRaised || cycleDurationNanos <= 0L || cycleTimeEstNanos <= 0L) {
      return;
    }
    boolean slept = false;
    if (commitRecorded && receiveDurationNanos > 0L) {
      long expectedSleep =
          Math.max(0L, cycleTimeEstNanos - receiveDurationNanos - commitDurationNanos);
      long actualExtra = cycleDurationNanos - receiveDurationNanos - commitDurationNanos;
      slept = actualExtra <= expectedSleep + MARGIN_NANOS;
    }
    boolean quiet = slept || gatherCount <= 0;
    if (!quiet) {
      return;
    }
    long oneSecond = TimeUnit.SECONDS.toNanos(1);
    long halfSecond = oneSecond / 2;
    boolean upperHalf =
        cycleDurationNanos > cycleTimeEstNanos + halfSecond
            && cycleDurationNanos <= cycleTimeEstNanos + oneSecond;
    long raised = wholeSecondNanos(cycleDurationNanos);
    boolean doublePeriod = raised >= cycleTimeEstNanos * 2;
    if (upperHalf) {
      raised = cycleTimeEstNanos + oneSecond;
    } else if (!doublePeriod) {
      return;
    }
    cycleTimeEstNanos = raised;
    intervalRaised = true;
    long target = cycleTimeEstNanos - writeBudgetNanos - MARGIN_NANOS;
    if (receiveBudgetNanos > target && target > 0L) {
      receiveBudgetNanos = halfway(receiveBudgetNanos, target);
    }
  }

  private boolean maybeCorrectShortEstimate() {
    if (postFirstHeavyChecked || gatherCount <= 0 || cycleTimeEstNanos <= 0L) {
      return false;
    }
    postFirstHeavyChecked = true;
    if (cycleTimeEstNanos >= SHORT_ESTIMATE_NANOS) {
      return false;
    }
    if (cycleDurationNanos <= (long) (cycleTimeEstNanos * POST_FIRST_BATCH_CYCLE_RATIO)) {
      return false;
    }
    LOG.warn(
        "AUTO phase=probe cycleTimeEst reset; cycleDuration={} exceeds short estimate {}",
        Into.elapsed(cycleDurationNanos),
        Into.elapsed(cycleTimeEstNanos));
    phase = Phase.PROBE;
    cycleTimeEstNanos = 0L;
    receiveBudgetNanos = 0L;
    writeBudgetNanos = 0L;
    probeStartedNanos = null;
    probeGapCount = 0;
    probeAttempts = 0;
    probeNoiseCount = 0;
    intervalRaised = false;
    lastMicroBatchStartNanos = 0L;
    return true;
  }

  private void logSnapshot() {
    long behind =
        Math.max(0L, cycleDurationNanos - (cycleTimeEstNanos > 0L ? cycleTimeEstNanos : 0L));
    lastSnapshot =
        new CycleSnapshot(
            phase,
            cycleTimeEstNanos,
            MARGIN_NANOS,
            receiveBudgetNanos,
            receiveDurationNanos,
            commitDurationNanos,
            writeDurationNanos,
            writeBudgetNanos,
            sleepDurationNanos,
            cycleDurationNanos,
            behind);
    LOG.info(lastSnapshot.toString());
  }

  private static long halfway(long current, long target) {
    return current + (target - current) / 2;
  }

  private void enterZeroTrigger() {
    phase = Phase.ZERO;
    cycleTimeEstNanos = 0L;
    receiveBudgetNanos = ZERO_TRIGGER_RECEIVE.toNanos();
    if (!zeroTriggerWarned) {
      zeroTriggerWarned = true;
      LOG.warn(
          "AUTO phase=zero receiveBudget={} (trigger looks like ProcessingTime(0))",
          Into.elapsed(receiveBudgetNanos));
    }
  }

  private void recordProbeGap(long gapNanos) {
    if (probeGapCount < PROBE_GAP_SAMPLES) {
      probeGapNanos[probeGapCount++] = gapNanos;
      return;
    }
    System.arraycopy(probeGapNanos, 1, probeGapNanos, 0, PROBE_GAP_SAMPLES - 1);
    probeGapNanos[PROBE_GAP_SAMPLES - 1] = gapNanos;
  }

  private long maxProbeGapNanos() {
    long max = 0L;
    for (int i = 0; i < probeGapCount; i++) {
      max = Math.max(max, probeGapNanos[i]);
    }
    return max;
  }

  private long secondLargestProbeGapNanos() {
    long max = 0L;
    long second = 0L;
    for (int i = 0; i < probeGapCount; i++) {
      long g = probeGapNanos[i];
      if (g >= max) {
        second = max;
        max = g;
      } else if (g > second) {
        second = g;
      }
    }
    return second;
  }
}
