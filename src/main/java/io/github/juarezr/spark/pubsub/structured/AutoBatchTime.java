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
 * batch interval ({@code Trigger.ProcessingTime}). Probe idle {@code latestOffset} gaps for the
 * interval T, then adjust receive time only: overrun shrinks it, leftover idle raises it, for a
 * fixed number of steps. Never grows T from a busy cycle.
 *
 * <pre>
 * @startuml
 * !theme vibrant
 * title Auto receiveTime: PROBE then idle-raise / overrun-shrink
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
 * == PROBE: empty latestOffset; finite ==
 *
 * Spark -> Stream : latestOffset()
 * Stream -> Auto : shouldProbe()
 * alt gap >= 10s
 *   Auto -> Auto : T = gap\nreceive = T / 2\nmode = AUTO
 * else gap < 1s three times
 *   Auto -> Auto : Mode.ZERO receive = 1s
 * end
 *
 * == AUTO: receive; never grow T from busy ==
 *
 * loop each later latestOffset
 *   Spark -> Stream : latestOffset()
 *   Stream -> Auto : shouldProbe()
 *   note right of Auto
 *     Idle empty gather and a larger gap:
 *     one re-measure of T, then T is done.
 *     Busy cycles never change T.
 *     Overrun: shrink receive.
 *     Leftover idle: raise receive.
 *     After N steps or within 1s of
 *     T - write - margin: freeze receive.
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
  static final int ADJUST_STEPS = 8;
  static final long RECEIVE_FLOOR_NANOS = TimeUnit.SECONDS.toNanos(1);
  static final long MARGIN_NANOS = TimeUnit.SECONDS.toNanos(1);
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
  private Duration processingTime;
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
  private boolean tFrozen;
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

  Duration processingTime() {
    return processingTime;
  }

  Duration currentReceive() {
    return currentReceive;
  }

  boolean receiveFrozen() {
    return receiveFrozen;
  }

  /**
   * @return {@code true} when this {@code latestOffset} must return empty without receiving
   */
  boolean shouldProbe() {
    long now = nanoTime.getAsLong();
    if (mode == Mode.AUTO) {
      maybeIdleRaiseT(now);
      maybeAdjustReceive(now);
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
          "BATCH: receiveTime is unset; inferring Spark batch interval from idle gaps (empty"
              + " latestOffset, no receive) until one gap is at least 10s. In a 3 sub-second gap"
              + " sequence assume ProcessingTime(0). probeAttempt=1");
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
          "BATCH: probeAttempt={} inferred batch interval {} is under 10s; not seeding (keep"
              + " probing)",
          probeAttempts,
          format(Duration.ofNanos(gapFromStart)));
      return true;
    }
    probeNoiseCount = 0;
    processingTime = Duration.ofNanos(gapFromStart);
    currentReceive = firstReceive(processingTime);
    mode = Mode.AUTO;
    LOG.info(
        "BATCH: inferred batch interval={} first receiveTime={} (seed probeAttempt={})",
        format(processingTime),
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
    if (!pendingWrite || processingTime == null || mode != Mode.AUTO) {
      return;
    }
    long write = nanoTime.getAsLong() - lastReturnNanos;
    if (write < 0) {
      return;
    }
    lastWriteNanos = write;
    pendingWrite = false;
    maybeAdjustReceive(nanoTime.getAsLong());
  }

  /**
   * Lease used when {@code ackDeadline} is omitted: {@code 3 ×} batch interval, seed 180s, clamp
   * 60s–600s.
   */
  Duration inferredAckDeadline() {
    if (processingTime == null) {
      return ACK_DEADLINE_SEED;
    }
    long seconds = Math.max(1L, processingTime.getSeconds() * 3L);
    Duration inferred = Duration.ofSeconds(seconds);
    if (inferred.compareTo(ACK_DEADLINE_MIN) < 0) {
      return ACK_DEADLINE_MIN;
    }
    if (inferred.compareTo(ACK_DEADLINE_MAX) > 0) {
      return ACK_DEADLINE_MAX;
    }
    return inferred;
  }

  private void maybeIdleRaiseT(long now) {
    if (tFrozen || lastOffsetStartNanos == 0L || processingTime == null || !lastGatherEmpty) {
      return;
    }
    long gap = now - lastOffsetStartNanos;
    if (gap <= processingTime.toNanos()) {
      return;
    }
    processingTime = Duration.ofNanos(gap);
    tFrozen = true;
    if (currentReceive == null) {
      currentReceive = firstReceive(processingTime);
    } else {
      currentReceive = clampReceive(currentReceive, processingTime, lastWriteNanos);
    }
    LOG.info(
        "BATCH: inferred batch interval={} receiveTime={} (idle re-measure; T is done)",
        format(processingTime),
        format(currentReceive));
  }

  private void maybeAdjustReceive(long now) {
    if (receiveFrozen || mode != Mode.AUTO || processingTime == null || currentReceive == null) {
      return;
    }
    if (lastGatherNanos <= 0L && lastWriteNanos <= 0L) {
      return;
    }
    long t = processingTime.toNanos();
    long write = lastWriteNanos;
    if (pendingWrite && lastReturnNanos != 0L) {
      write = Math.max(write, now - lastReturnNanos);
    }
    long cycle = lastGatherNanos + write;
    Duration next = currentReceive;
    if (cycle > t) {
      long shrink = cycle - t;
      next = Duration.ofNanos(Math.max(RECEIVE_FLOOR_NANOS, currentReceive.toNanos() - shrink));
      LOG.info(
          "BATCH: overrun cycle={} > interval={}; shrink receiveTime {} -> {}",
          format(Duration.ofNanos(cycle)),
          format(processingTime),
          format(currentReceive),
          format(next));
    } else if (lastGatherEmpty || cycle + MARGIN_NANOS < t) {
      long leftover = t - cycle;
      if (leftover > MARGIN_NANOS) {
        next = Duration.ofNanos(currentReceive.toNanos() + leftover);
      }
    }
    next = clampReceive(next, processingTime, write);
    currentReceive = next;
    adjustCount++;
    long target = Math.max(RECEIVE_FLOOR_NANOS, t - write - MARGIN_NANOS);
    if (Math.abs(next.toNanos() - target) <= MARGIN_NANOS || adjustCount >= ADJUST_STEPS) {
      receiveFrozen = true;
      LOG.info(
          "BATCH: receiveTime={} frozen after {} adjust(s)", format(currentReceive), adjustCount);
    }
  }

  private void enterZeroTrigger() {
    mode = Mode.ZERO;
    processingTime = null;
    currentReceive = ZERO_TRIGGER_RECEIVE;
    if (!zeroTriggerWarned) {
      zeroTriggerWarned = true;
      LOG.warn(
          "BATCH: receiveTime is unset and Spark trigger looks like ProcessingTime(0); using"
              + " receiveTime={}. Set receiveTime explicitly to override.",
          format(currentReceive));
    }
  }

  static Duration firstReceive(Duration processingTime) {
    return processingTime.dividedBy(2);
  }

  static Duration clampReceive(Duration value, Duration processingTime, long writeNanos) {
    long t = processingTime.toNanos();
    long max = t - Math.max(writeNanos, 0L) - MARGIN_NANOS;
    if (max < RECEIVE_FLOOR_NANOS) {
      max = Math.max(RECEIVE_FLOOR_NANOS, t / 2);
    }
    long v = value.toNanos();
    if (v < RECEIVE_FLOOR_NANOS) {
      return Duration.ofNanos(RECEIVE_FLOOR_NANOS);
    }
    if (v > max) {
      return Duration.ofNanos(max);
    }
    return value;
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
