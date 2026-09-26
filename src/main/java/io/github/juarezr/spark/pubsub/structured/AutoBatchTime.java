package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Infers a sane {@code receiveTime} when the option is omitted so receive + write fits the Spark
 * batch interval ({@code Trigger.ProcessingTime}). Probe idle micro-batch gaps for the batch
 * interval, then one start-to-start leftover raise if Spark slept, then keep approximating
 * receive for 15 cycles: overrun shrinks it, leftover idle raises it by half the gap. Never
 * sets the batch interval from gather + write.
 *
 * <pre>
 * @startuml
 * !theme vibrant
 * title Auto receiveTime: PROBE then idle leftover / overrun-shrink
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
 *     slept (gap > gather + write + 1s):
 *     one re-measure of batch interval,
 *     then batch interval is done.
 *     Busy cycles (cycle ≈ gap) never
 *     change it.
 *     After it is done: overrun shrinks
 *     receive; leftover idle raises it
 *     by half the gap (no one jump).
 *     After 15 receive adjusts: freeze.
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
  static final int ADJUST_STEPS = 15;
  static final long RECEIVE_FLOOR_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final long RECEIVE_WRITE_MARGIN_NANOS = TimeUnit.SECONDS.toNanos(1);
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
  private boolean receiveFrozen;

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

  long lastGatherNanos() {
    return lastGatherNanos;
  }

  long autoCycles() {
    return autoCycles;
  }

  long adjustCount() {
    return adjustCount;
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
    boolean sparkSlept = lastGatherEmpty || gap > cycle + RECEIVE_WRITE_MARGIN_NANOS;
    if (!sparkSlept) {
      return;
    }
    if (autoCycles < 1 && !lastGatherEmpty) {
      return;
    }
    batchInterval = Duration.ofNanos(gap);
    batchIntervalFrozen = true;
    receiveFrozen = false;
    adjustCount = 0;
    if (currentReceive == null) {
      currentReceive = firstReceive(batchInterval);
    } else {
      currentReceive = clampReceive(currentReceive, batchInterval, lastWriteNanos);
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
    long write = calcWriteTime(now);
    long cycle = lastGatherNanos + write;
    long floor = receiveFloorNanos(intervalNanos);
    Duration next = currentReceive;
    if (cycle > intervalNanos) {
      long shrink = cycle - intervalNanos;
      next = Duration.ofNanos(Math.max(floor, currentReceive.toNanos() - shrink));
      LOG.info(
          "AUTO: overrun cycle={} > batch interval={}; shrink receiveTime {} -> {}",
          format(Duration.ofNanos(cycle)),
          format(batchInterval),
          format(currentReceive),
          format(next));
    } else if (lastGatherEmpty || cycle + RECEIVE_WRITE_MARGIN_NANOS < intervalNanos) {
      long leftover = intervalNanos - cycle;
      if (leftover > RECEIVE_WRITE_MARGIN_NANOS) {
        next = Duration.ofNanos(currentReceive.toNanos() + leftover / 2L);
      }
    }
    next = clampReceive(next, batchInterval, write);
    currentReceive = next;
    adjustCount++;
    if (adjustCount >= ADJUST_STEPS) {
      receiveFrozen = true;
      LOG.info(
          "AUTO: receiveTime={} frozen after {} adjust(s)", format(currentReceive), adjustCount);
    }
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

  static Duration clampReceive(Duration value, Duration batchInterval, long writeNanos) {
    long intervalNanos = batchInterval.toNanos();
    long floor = receiveFloorNanos(intervalNanos);
    long max = intervalNanos - Math.max(writeNanos, 0L) - RECEIVE_WRITE_MARGIN_NANOS;
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
