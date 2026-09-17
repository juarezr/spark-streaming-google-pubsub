package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sizes {@code batchTime} when the option is omitted: one empty-gap probe for Spark {@code
 * Trigger.ProcessingTime}, then {@code T/2}, then {@code T - writeAvg - writeStdev - safety}.
 */
final class AutoBatchTime {
  private static final Logger LOG = LoggerFactory.getLogger(AutoBatchTime.class);

  static final long PROBE_NOISE_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final int WRITE_WINDOW = 8;
  static final double SAFETY_FRACTION = 0.005;
  static final long OVERRUN_SEED_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

  enum Mode {
    FIXED,
    PROBE,
    AUTO
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
  private long lastReturnNanos;
  private boolean lastNonEmpty;
  private boolean pendingWrite;
  private int finishedNonEmpty;
  private Long lastOverrunNanos;

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
    if (mode != Mode.PROBE) {
      observeCycle(now);
      return false;
    }
    if (probeStartedNanos == null) {
      probeStartedNanos = now;
      LOG.warn(
          "batchTime is unset; inferring Spark ProcessingTime from the next idle gap (one empty"
              + " cycle, no Pull). Trigger.Once() requires an explicit batchTime or the probe"
              + " consumes the only micro-batch.");
      return true;
    }
    long gap = now - probeStartedNanos;
    if (gap < PROBE_NOISE_NANOS) {
      return true;
    }
    processingTime = Duration.ofNanos(gap);
    currentGather = firstGather(processingTime);
    mode = Mode.AUTO;
    LOG.info(
        "BATCH: inferred processingTime={} first gather={}",
        format(processingTime),
        format(currentGather));
    return false;
  }

  Duration gatherWindow(Duration admissionWait) {
    Duration base = mode == Mode.FIXED ? fixedBatchTime : currentGather;
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
    lastReturnNanos = nanoTime.getAsLong();
    pendingWrite = nonEmpty;
  }

  void onCommit() {
    if (!pendingWrite || processingTime == null) {
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
    recordWrite(write);
    pendingWrite = false;
    finishedNonEmpty++;
    refreshGather();
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
