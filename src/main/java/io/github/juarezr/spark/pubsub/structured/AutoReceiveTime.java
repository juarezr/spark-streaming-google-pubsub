package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.common.Into;
import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fills {@code receiveTime} when the app omits it.
 *
 * <p>{@code cycleBudget} is Spark's trigger, stored as a whole second. {@code receiveBudget} for
 * the next cycle is {@code cycleBudget − writeBudget}. {@code writeBudget} walks down from the
 * probe seed, by a quarter of the idle below the top quarter of the trigger and then by 3% of
 * {@code cycleBudget}, while that idle is above an eighth of the trigger and the cycle finished on
 * time. In the top quarter a late cycle records its post-receive time. While fewer than eight
 * samples exist, {@code writeBudget} steps toward that reserve by at most 3% of {@code cycleBudget}
 * only on the cycle that recorded one. A quiet cycle does not step it up. An on-time cycle whose
 * idle is spare steps below that reserve, one careful step, and not below the commit. A quiet cycle
 * that is not spare still stops at the reserve. Sleep and a quiet commit are not samples.
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
  static final int TAIL_WINDOW = 8;
  static final int CYCLE_CHECK_WINDOW = 8;
  static final long WHOLE_SECOND_SNAP_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
  static final long TAIL_SPREAD_CAP_NANOS = TimeUnit.SECONDS.toNanos(2);
  static final int RAPID_IDLE_DIVISOR = 4;
  static final int CAREFUL_STEP_PERCENT = 3;
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
    final long cycleBudgetNanos;
    final long marginTimeEstNanos;
    final long receiveBudgetNanos;
    final long receiveDurationNanos;
    final long commitDurationNanos;
    final long writeDurationNanos;
    final long writeBudgetNanos;
    final long sleepDurationNanos;
    final long idleDurationNanos;
    final long cycleDurationNanos;
    final long behindDurationNanos;
    final long tailMedianNanos;
    final long tailStdevNanos;
    final int messages;
    final long receiveRatePerSecond;
    final long syncRatePerSecond;
    final long cycleRatePerSecond;
    final int tailSamples;

    CycleSnapshot(
        Phase phase,
        long cycleBudgetNanos,
        long marginTimeEstNanos,
        long receiveBudgetNanos,
        long receiveDurationNanos,
        long commitDurationNanos,
        long writeDurationNanos,
        long writeBudgetNanos,
        long sleepDurationNanos,
        long idleDurationNanos,
        long cycleDurationNanos,
        long behindDurationNanos,
        long tailMedianNanos,
        long tailStdevNanos,
        int messages,
        long receiveRatePerSecond,
        long syncRatePerSecond,
        long cycleRatePerSecond,
        int tailSamples) {
      this.phase = phase;
      this.cycleBudgetNanos = cycleBudgetNanos;
      this.marginTimeEstNanos = marginTimeEstNanos;
      this.receiveBudgetNanos = receiveBudgetNanos;
      this.receiveDurationNanos = receiveDurationNanos;
      this.commitDurationNanos = commitDurationNanos;
      this.writeDurationNanos = writeDurationNanos;
      this.writeBudgetNanos = writeBudgetNanos;
      this.sleepDurationNanos = sleepDurationNanos;
      this.idleDurationNanos = idleDurationNanos;
      this.cycleDurationNanos = cycleDurationNanos;
      this.behindDurationNanos = behindDurationNanos;
      this.tailMedianNanos = tailMedianNanos;
      this.tailStdevNanos = tailStdevNanos;
      this.messages = messages;
      this.receiveRatePerSecond = receiveRatePerSecond;
      this.syncRatePerSecond = syncRatePerSecond;
      this.cycleRatePerSecond = cycleRatePerSecond;
      this.tailSamples = tailSamples;
    }

    @Override
    public String toString() {
      return "AUTO phase="
          + phaseLabel()
          + " cycleBudget="
          + elapsed(cycleBudgetNanos)
          + " cycleDuration="
          + elapsed(cycleDurationNanos)
          + " receiveBudget="
          + elapsed(receiveBudgetNanos)
          + " receiveDuration="
          + elapsed(receiveDurationNanos)
          + " writeBudget="
          + elapsed(writeBudgetNanos)
          + " writeDuration="
          + elapsed(writeDurationNanos)
          + " commitDuration="
          + elapsed(commitDurationNanos)
          + " sleepDuration="
          + elapsed(sleepDurationNanos)
          + " idleDuration="
          + millis(idleDurationNanos)
          + " behindDuration="
          + elapsed(behindDurationNanos)
          + " tailMedian="
          + tailMillis(tailMedianNanos)
          + " tailStdev="
          + tailMillis(tailStdevNanos)
          + " msgs="
          + messages
          + " receiveRate="
          + receiveRatePerSecond
          + "/s"
          + " syncRate="
          + syncRatePerSecond
          + "/s"
          + " cycleRate="
          + cycleRatePerSecond
          + "/s";
    }

    private String phaseLabel() {
      if (phase == Phase.CRUISE
          && cycleBudgetNanos > 0L
          && tailSamples > 0
          && tailSamples < TAIL_WINDOW) {
        return "sample";
      }
      return phase.name().toLowerCase();
    }

    private static String elapsed(long nanos) {
      return nanos <= 0L ? "0s" : Into.elapsed(nanos);
    }

    private static String tailMillis(long nanos) {
      return nanos < 0L ? "unset" : millis(nanos);
    }

    private static String millis(long nanos) {
      return Long.toString(TimeUnit.NANOSECONDS.toMillis(nanos)) + "ms";
    }
  }

  private final LongSupplier nanoTime;
  private final Duration fixedReceiveTime;
  private final Duration processingTimeHint;
  private Phase phase;
  private Long probeStartedNanos;
  private long cycleBudgetNanos;
  private long receiveBudgetNanos;
  private long writeBudgetNanos;
  private long receiveDurationNanos;
  private long commitDurationNanos;
  private long writeDurationNanos;
  private long sleepDurationNanos;
  private long cycleDurationNanos;
  private int probeNoiseCount;
  private boolean zeroTriggerWarned;
  private boolean intervalRaised;
  private boolean cycleCheckStopped;
  private boolean cycleSlept;
  private boolean postFirstHeavyChecked;
  private long returnNanos;
  private boolean commitRecorded;
  private boolean gatherEmpty;
  private int gatherCount;
  private long lastMicroBatchStartNanos;
  private final long[] probeGapNanos = new long[PROBE_GAP_SAMPLES];
  private int probeGapCount;
  private final long[] tailWindow = new long[TAIL_WINDOW];
  private int tailCount;
  private int tailCursor;
  private final long[] cycleWindow = new long[CYCLE_CHECK_WINDOW];
  private int cycleCount;
  private Long pendingCycleSecondNanos;
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

  Duration cycleBudget() {
    return cycleBudgetNanos <= 0L ? null : Duration.ofNanos(cycleBudgetNanos);
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

  /**
   * The greatest whole second the gap has reached. 60.6s stays 60s. A gap within {@link
   * #WHOLE_SECOND_SNAP_NANOS} under the next second is that second, so 59999ms is 60s.
   */
  static long wholeSecondNanos(long gapNanos) {
    if (gapNanos <= 0L) {
      return 0L;
    }
    long second = TimeUnit.SECONDS.toNanos(1);
    long floor = (gapNanos / second) * second;
    long next = floor + second;
    if (gapNanos != floor && next - gapNanos <= WHOLE_SECOND_SNAP_NANOS) {
      return next;
    }
    return floor;
  }

  /** The whole second a cycle sits on, or empty when it is more than the snap away from both. */
  static OptionalLong namedWholeSecond(long nanos) {
    if (nanos <= 0L) {
      return OptionalLong.empty();
    }
    long second = TimeUnit.SECONDS.toNanos(1);
    long floor = (nanos / second) * second;
    if (nanos - floor <= WHOLE_SECOND_SNAP_NANOS) {
      return OptionalLong.of(floor);
    }
    long next = floor + second;
    if (next - nanos <= WHOLE_SECOND_SNAP_NANOS) {
      return OptionalLong.of(next);
    }
    return OptionalLong.empty();
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
      LOG.info(
          "AUTO phase=probe cycleBudget=unset receiveBudget=unset"
              + " (empty micro-batch until the trigger gap is consistent)");
      return true;
    }
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
    // TODO: soak-only: remove or lower it to debug level before a provider release
    LOG.info("AUTO phase=probe gap={}ms", TimeUnit.NANOSECONDS.toMillis(gap));
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
    if (gatherEmpty || gatherCount <= 0 || cycleBudgetNanos <= 0L || returnNanos <= 0L) {
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
    if (cycleBudgetNanos <= 0L) {
      return ACK_DEADLINE_SEED;
    }
    long seconds = Math.max(1L, TimeUnit.NANOSECONDS.toSeconds(cycleBudgetNanos) * 3L);
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
    cycleBudgetNanos = wholeSecond;
    receiveBudgetNanos = cycleBudgetNanos / 2;
    writeBudgetNanos = cycleBudgetNanos - receiveBudgetNanos - MARGIN_NANOS;
    phase = Phase.WARMUP;
    LOG.info(
        "AUTO phase=warmup cycleBudget={} receiveBudget={} writeBudget={} marginTimeEst=1s",
        Into.elapsed(cycleBudgetNanos),
        Into.elapsed(receiveBudgetNanos),
        Into.elapsed(writeBudgetNanos));
  }

  private void observeCycle(long now) {
    if (lastMicroBatchStartNanos <= 0L) {
      return;
    }
    cycleSlept = false;
    cycleDurationNanos = now - lastMicroBatchStartNanos;
    if (maybeCorrectShortEstimate()) {
      return;
    }
    maybeCorrectCycleTime();
    if (commitRecorded && receiveDurationNanos > 0L) {
      applyWrite(cycleBudgetNanos);
    }
    logSnapshot();
  }

  private boolean sleptAgainstEstimate(long estimate) {
    if (!commitRecorded || receiveDurationNanos <= 0L) {
      return false;
    }
    long expectedSleep = Math.max(0L, estimate - receiveDurationNanos - commitDurationNanos);
    long actualExtra = cycleDurationNanos - receiveDurationNanos - commitDurationNanos;
    if (actualExtra <= expectedSleep + MARGIN_NANOS) {
      return true;
    }
    // A cycle that lands within the snap of this second or the next one is the trigger, not a late
    // write. A 1ms-short estimate otherwise labels the whole wait as writeDuration.
    long oneSecond = TimeUnit.SECONDS.toNanos(1);
    return withinSnap(cycleDurationNanos, estimate)
        || withinSnap(cycleDurationNanos, estimate + oneSecond);
  }

  private static boolean withinSnap(long value, long second) {
    return Math.abs(value - second) <= WHOLE_SECOND_SNAP_NANOS;
  }

  private void applyWrite(long estimate) {
    boolean slept = sleptAgainstEstimate(estimate);
    cycleSlept = slept;
    if (slept || cycleDurationNanos <= estimate) {
      writeDurationNanos = commitDurationNanos;
    } else {
      writeDurationNanos = Math.max(commitDurationNanos, cycleDurationNanos - receiveDurationNanos);
    }
    sleepDurationNanos =
        Math.max(0L, cycleDurationNanos - receiveDurationNanos - writeDurationNanos);
    boolean quiet = slept || cycleDurationNanos <= estimate;
    boolean trulyLate = !slept && cycleDurationNanos > estimate + MARGIN_NANOS;
    boolean topQuarter = receiveBudgetNanos >= estimate - estimate / 4L;
    // Idle above an eighth of the trigger is spare from the climb, including after a spike.
    // Idle at or below that is the edge: another step is what made the next batch late.
    long idle = slept ? sleepDurationNanos : 0L;
    long overrun = Math.max(0L, cycleDurationNanos - estimate);
    // The 1s pad can still call a cycle slept when it is already past the trigger. That idle is
    // not spare. A gap inside the 100ms snap is the whole second and still is.
    boolean pastTrigger = overrun > WHOLE_SECOND_SNAP_NANOS;
    boolean spare = idle > estimate / 8L && !pastTrigger;
    // Below the top quarter a short receive inflates writeDuration, so the first sample still
    // waits for the top quarter. After one exists, a late cycle above half the trigger still
    // records. A floor cycle does not.
    boolean record =
        trulyLate && (topQuarter || (tailCount > 0 && receiveBudgetNanos >= estimate / 2L));
    if (record) {
      // The commit plus the overrun is about 2s and sits under the climb's budget, so the window
      // never moved. The time that missed the trigger is the post-receive work.
      pushTail(writeDurationNanos);
    }
    boolean windowFull = tailCount >= TAIL_WINDOW;
    // Idle minutes must not chase the reserve up while the window is still filling. Once it is
    // full, a late cycle still tracks it. An on-time cycle with spare idle is not that late write,
    // so it may step below the reserve. A quiet cycle that is not spare still stops there.
    boolean reserveOpen = tailCount > 0 && (windowFull || record);
    long step = carefulStep(estimate);
    if (tailCount > 0 && quiet && spare) {
      long stepped = writeBudgetNanos - step;
      writeBudgetNanos = Math.max(commitDurationNanos, stepped);
    } else if (reserveOpen) {
      long reserve = tailReserve();
      if (reserve > writeBudgetNanos) {
        writeBudgetNanos = Math.min(writeBudgetNanos + step, reserve);
      } else if (reserve < writeBudgetNanos) {
        writeBudgetNanos = Math.max(writeBudgetNanos - step, reserve);
      }
    } else if (tailCount > 0 && quiet) {
      long reserve = tailReserve();
      if (reserve < writeBudgetNanos) {
        writeBudgetNanos = Math.max(writeBudgetNanos - step, reserve);
      }
    } else if (trulyLate && !topQuarter && tailCount == 0) {
      // The leftover still holds the climb's slack. Only the time past the trigger is new cost,
      // so one 62s cycle cannot freeze the reserve at sleep+write. Once a sample exists this add
      // is closed: one 88s cycle pinned receive at the floor.
      if (overrun > 0L) {
        writeBudgetNanos += overrun;
      }
    } else if (quiet && spare && tailCount == 0) {
      long growth = growthStep(estimate, topQuarter);
      if (growth > 0L && sleepDurationNanos > growth) {
        long stepped = writeBudgetNanos - growth;
        writeBudgetNanos = Math.max(commitDurationNanos, stepped);
      }
    }
    if (phase == Phase.WARMUP) {
      phase = Phase.CRUISE;
    }
    long target = Math.max(0L, estimate - writeBudgetNanos);
    long floor = Math.max(RECEIVE_FLOOR_NANOS, estimate / 4L);
    long next;
    if (target >= receiveBudgetNanos) {
      long rise =
          tailCount > 0 && (reserveOpen || quiet)
              ? step
              : quiet && spare && tailCount == 0 ? growthStep(estimate, topQuarter) : 0L;
      next = rise > 0L ? Math.min(receiveBudgetNanos + rise, target) : receiveBudgetNanos;
    } else {
      next = target;
    }
    if (next < floor) {
      next = floor;
    }
    receiveBudgetNanos = next;
  }

  private long growthStep(long estimate, boolean topQuarter) {
    if (topQuarter) {
      return carefulStep(estimate);
    }
    return sleepDurationNanos / RAPID_IDLE_DIVISOR;
  }

  private static long carefulStep(long estimate) {
    return estimate * CAREFUL_STEP_PERCENT / 100L;
  }

  private void pushTail(long sample) {
    tailWindow[tailCursor] = sample;
    tailCursor = (tailCursor + 1) % TAIL_WINDOW;
    if (tailCount < TAIL_WINDOW) {
      tailCount++;
    }
  }

  /**
   * Median plus one standard deviation. The added spread stops at {@link #TAIL_SPREAD_CAP_NANOS}.
   */
  private long tailReserve() {
    long median = tailMedianNanos();
    if (tailCount < 2) {
      return median;
    }
    long spread = tailStdevNanos();
    if (spread > TAIL_SPREAD_CAP_NANOS) {
      spread = TAIL_SPREAD_CAP_NANOS;
    }
    return median + spread;
  }

  private long tailMedianNanos() {
    if (tailCount <= 0) {
      return -1L;
    }
    long[] sorted = sortedTail();
    int n = sorted.length;
    return (n & 1) == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2L;
  }

  private long tailStdevNanos() {
    if (tailCount <= 0) {
      return -1L;
    }
    if (tailCount < 2) {
      return 0L;
    }
    long[] sorted = sortedTail();
    double mean = 0.0d;
    for (long value : sorted) {
      mean += value;
    }
    mean /= sorted.length;
    double variance = 0.0d;
    for (long value : sorted) {
      double delta = value - mean;
      variance += delta * delta;
    }
    return (long) Math.sqrt(variance / sorted.length);
  }

  private long[] sortedTail() {
    long[] sorted = Arrays.copyOf(tailWindow, tailCount);
    Arrays.sort(sorted);
    return sorted;
  }

  private void maybeCorrectCycleTime() {
    if (cycleDurationNanos <= 0L || cycleBudgetNanos <= 0L || cycleCheckStopped) {
      return;
    }
    if (!intervalRaised && quietDoublePeriod()) {
      adoptEstimate(wholeSecondNanos(cycleDurationNanos));
      intervalRaised = true;
      clearCycleCheck();
      return;
    }
    noteCycleDuration(cycleDurationNanos);
  }

  private boolean quietDoublePeriod() {
    long raised = wholeSecondNanos(cycleDurationNanos);
    if (raised < cycleBudgetNanos * 2L) {
      return false;
    }
    return gatherCount <= 0 || sleptAgainstEstimate(cycleBudgetNanos);
  }

  private void noteCycleDuration(long cycle) {
    cycleWindow[cycleCount] = cycle;
    cycleCount++;
    if (cycleCount < CYCLE_CHECK_WINDOW) {
      return;
    }
    OptionalLong tight = tightSecond(cycleWindow);
    cycleCount = 0;
    // TODO: soak-only: remove or lower it to debug level before a provider release
    LOG.info(
        "AUTO phase={} cycleCheck round={} pending={}",
        phase.name().toLowerCase(),
        tight.isPresent() ? Into.elapsed(tight.getAsLong()) : "unset",
        pendingCycleSecondNanos == null ? "unset" : Into.elapsed(pendingCycleSecondNanos));
    if (tight.isEmpty()) {
      pendingCycleSecondNanos = null;
      return;
    }
    long second = tight.getAsLong();
    if (pendingCycleSecondNanos == null || pendingCycleSecondNanos.longValue() != second) {
      pendingCycleSecondNanos = second;
      return;
    }
    if (second != cycleBudgetNanos) {
      adoptEstimate(second);
    }
    cycleCheckStopped = true;
    pendingCycleSecondNanos = null;
  }

  private static OptionalLong tightSecond(long[] samples) {
    OptionalLong named = namedWholeSecond(samples[0]);
    if (named.isEmpty()) {
      return OptionalLong.empty();
    }
    long second = named.getAsLong();
    for (int i = 1; i < samples.length; i++) {
      if (!withinSnap(samples[i], second)) {
        return OptionalLong.empty();
      }
    }
    return named;
  }

  private void adoptEstimate(long next) {
    // TODO: soak-only: remove or lower it to debug level before a provider release
    LOG.info(
        "AUTO phase={} cycleBudget set {} -> {}",
        phase.name().toLowerCase(),
        Into.elapsed(cycleBudgetNanos),
        Into.elapsed(next));
    cycleBudgetNanos = next;
    long target = Math.max(0L, cycleBudgetNanos - writeBudgetNanos);
    if (receiveBudgetNanos > target) {
      receiveBudgetNanos = target;
    }
  }

  private void clearCycleCheck() {
    cycleCount = 0;
    pendingCycleSecondNanos = null;
  }

  private boolean maybeCorrectShortEstimate() {
    if (postFirstHeavyChecked || gatherCount <= 0 || cycleBudgetNanos <= 0L) {
      return false;
    }
    postFirstHeavyChecked = true;
    if (cycleBudgetNanos >= SHORT_ESTIMATE_NANOS) {
      return false;
    }
    if (cycleDurationNanos <= (long) (cycleBudgetNanos * POST_FIRST_BATCH_CYCLE_RATIO)) {
      return false;
    }
    LOG.warn(
        "AUTO phase=probe cycleBudget reset; cycleDuration={} exceeds short estimate {}",
        Into.elapsed(cycleDurationNanos),
        Into.elapsed(cycleBudgetNanos));
    phase = Phase.PROBE;
    cycleBudgetNanos = 0L;
    receiveBudgetNanos = 0L;
    writeBudgetNanos = 0L;
    probeStartedNanos = null;
    probeGapCount = 0;
    probeNoiseCount = 0;
    intervalRaised = false;
    cycleCheckStopped = false;
    tailCount = 0;
    tailCursor = 0;
    clearCycleCheck();
    lastMicroBatchStartNanos = 0L;
    return true;
  }

  /** Messages per second. A zero message count or a zero duration is 0. */
  static long perSecond(int messages, long durationNanos) {
    if (messages <= 0 || durationNanos <= 0L) {
      return 0L;
    }
    return (messages * 1_000_000_000L) / durationNanos;
  }

  private void logSnapshot() {
    long behind =
        Math.max(0L, cycleDurationNanos - (cycleBudgetNanos > 0L ? cycleBudgetNanos : 0L));
    lastSnapshot =
        new CycleSnapshot(
            phase,
            cycleBudgetNanos,
            MARGIN_NANOS,
            receiveBudgetNanos,
            receiveDurationNanos,
            commitDurationNanos,
            writeDurationNanos,
            writeBudgetNanos,
            sleepDurationNanos,
            cycleSlept ? sleepDurationNanos : 0L,
            cycleDurationNanos,
            behind,
            tailMedianNanos(),
            tailStdevNanos(),
            gatherCount,
            perSecond(gatherCount, receiveDurationNanos),
            perSecond(gatherCount, writeDurationNanos),
            perSecond(gatherCount, cycleDurationNanos),
            tailCount);
    // TODO: soak-only: tailMedian, tailStdev, msgs, receiveRate, syncRate, cycleRate, and
    // idleDuration — remove or lower this line to debug before a provider release
    LOG.info(lastSnapshot.toString());
  }

  private void enterZeroTrigger() {
    phase = Phase.ZERO;
    cycleBudgetNanos = 0L;
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
