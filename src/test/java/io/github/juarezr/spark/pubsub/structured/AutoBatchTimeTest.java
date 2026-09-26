package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AutoBatchTimeTest {

  private static final int SIM_INTERVAL_SAMPLES = AutoBatchTime.MAX_TUNE_ADJUSTS;

  @Test
  void unsetReceiveTimeInBatchModeStartsAsProbe() {
    PubSubConfig config = PubSubConfig.builder().projectId("p").subscription("s").build();
    AutoBatchTime auto = AutoBatchTime.create(config, new AtomicLong()::get);

    assertEquals(AutoBatchTime.Mode.PROBE, auto.mode());
    assertTrue(auto.shouldProbe());
  }

  @Test
  void configuredReceiveTimeIsFixed() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .receiveTime(Duration.ofSeconds(10))
            .build();
    AutoBatchTime auto = AutoBatchTime.create(config);

    assertEquals(AutoBatchTime.Mode.FIXED, auto.mode());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(10), auto.receiveWindow(null));
  }

  @Test
  void pullModeDoesNotProbeWhenReceiveTimeOmitted() {
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    AutoBatchTime auto = AutoBatchTime.create(config);

    assertEquals(AutoBatchTime.Mode.FIXED, auto.mode());
    assertFalse(auto.shouldProbe());
  }

  @Test
  void oneCleanGapInfersIntervalThenUsesHalf() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(100_000_000L);
    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(AutoBatchTime.Mode.AUTO, auto.mode());
    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertEquals(Duration.ofSeconds(30), auto.receiveWindow(null));
    assertEquals(Duration.ofSeconds(180), auto.inferredAckDeadline());
  }

  @Test
  void firstInferredGapUnderTenSecondsIsProbeNoise() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(4).toNanos());
    assertTrue(auto.shouldProbe());
    assertEquals(AutoBatchTime.Mode.PROBE, auto.mode());

    now.set(Duration.ofSeconds(4).toNanos() + Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(AutoBatchTime.Mode.AUTO, auto.mode());
    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertEquals(Duration.ofSeconds(30), auto.receiveWindow(null));
  }

  @Test
  void idleStartToStartRaisesSeededIntervalOnce() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(16).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(16), auto.batchInterval());
    assertEquals(Duration.ofSeconds(8), auto.receiveWindow(null));

    auto.onGatherFinished(Duration.ofSeconds(5).toNanos(), false);
    now.addAndGet(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
  }

  @Test
  void busyOverrunShrinksReceiveDoesNotGrowInterval() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);
    printAutoValuesIfDebug(auto, now);

    assertTrue(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    assertEquals(Duration.ofSeconds(30), auto.currentReceive());

    auto.onGatherFinished(Duration.ofSeconds(50).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(20).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(30)) <= 0);
  }

  @Test
  void busyStartToStartNearCycleDoesNotBecomeInterval() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);
    printAutoValuesIfDebug(auto, now);

    assertTrue(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    now.set(Duration.ofSeconds(50).toNanos());
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    assertEquals(Duration.ofSeconds(50), auto.batchInterval());

    now.addAndGet(Duration.ofSeconds(6).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(6).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(136).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);

    assertEquals(Duration.ofSeconds(50), auto.batchInterval());
  }

  @Test
  void leftoverAfterFirstAutoRaisesSeededIntervalOnce() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(50).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(50), auto.batchInterval());
    assertEquals(Duration.ofSeconds(25), auto.currentReceive());

    now.addAndGet(Duration.ofSeconds(6).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(6).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(136).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(50), auto.batchInterval());

    now.addAndGet(Duration.ofSeconds(48).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(48).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(5).toNanos());
    auto.onCommit();
    now.addAndGet(Duration.ofSeconds(7).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
  }

  @Test
  void threeNoiseGapsAssumeZeroTrigger() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());

    long of100ms = TimeUnit.MILLISECONDS.toNanos(100);
    now.addAndGet(of100ms);
    assertTrue(auto.shouldProbe());
    now.addAndGet(of100ms);
    assertTrue(auto.shouldProbe());
    now.addAndGet(of100ms);
    assertFalse(auto.shouldProbe());

    assertEquals(AutoBatchTime.Mode.ZERO, auto.mode());
    assertEquals(Duration.ofSeconds(1), auto.receiveWindow(null));
    assertEquals(Duration.ofSeconds(180), auto.inferredAckDeadline());
  }

  @Test
  void receiveWriteMarginNanosUsesPercentAndFloor() {
    long margin60s = AutoBatchTime.receiveWriteMarginNanos(Duration.ofSeconds(60).toNanos());
    assertEquals(Duration.ofSeconds(1).toNanos(), margin60s);
    long margin10s = AutoBatchTime.receiveWriteMarginNanos(Duration.ofSeconds(10).toNanos());
    assertEquals(Duration.ofSeconds(1).toNanos(), margin10s);
    long margin120s = AutoBatchTime.receiveWriteMarginNanos(Duration.ofSeconds(120).toNanos());
    assertTrue(margin120s >= Duration.ofMillis(1920).toNanos());
  }

  @Test
  void clampReceiveKeepsFloorAndMargin() {
    Duration clamped =
        AutoBatchTime.clampReceive(
            Duration.ofSeconds(80), Duration.ofSeconds(60), Duration.ofSeconds(10).toNanos());
    assertEquals(Duration.ofSeconds(49), clamped);
    Duration floor =
        AutoBatchTime.clampReceive(
            Duration.ofMillis(10), Duration.ofSeconds(60), Duration.ofSeconds(10).toNanos());
    assertEquals(Duration.ofSeconds(15), floor);
  }

  @Test
  void clampReceiveUsesScaledMarginForLongInterval() {
    long write = Duration.ofSeconds(10).toNanos();
    Duration clamped =
        AutoBatchTime.clampReceive(Duration.ofSeconds(110), Duration.ofSeconds(120), write);
    long margin = AutoBatchTime.receiveWriteMarginNanos(Duration.ofSeconds(120).toNanos());
    long expectedMax = Duration.ofSeconds(120).toNanos() - write - margin;
    assertEquals(Duration.ofNanos(expectedMax), clamped);
    assertTrue(margin > Duration.ofSeconds(1).toNanos());
  }

  @Test
  void leftoverThenTwoSmallWritesDoesNotFreezeReceive() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(50).toNanos());
    assertFalse(auto.shouldProbe());

    now.addAndGet(Duration.ofSeconds(6).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(6).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(136).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());

    now.addAndGet(Duration.ofSeconds(48).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(48).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(5).toNanos());
    auto.onCommit();
    now.addAndGet(Duration.ofSeconds(7).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    Duration afterLeftover = auto.currentReceive();

    now.addAndGet(Duration.ofSeconds(30).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(30).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());
    now.addAndGet(Duration.ofSeconds(30).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(30).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());

    assertFalse(auto.receiveFrozen());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(59)) < 0);
    assertTrue(auto.currentReceive().compareTo(afterLeftover) >= 0);
  }

  @Test
  void severalOverrunsShrinkReceiveWithoutFreezingEarly() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);
    printAutoValuesIfDebug(auto, now);

    assertTrue(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);

    Duration seeded = auto.currentReceive();
    for (int i = 0; i < 4; i++) {
      auto.onGatherFinished(Duration.ofSeconds(58).toNanos(), true);
      now.addAndGet(Duration.ofSeconds(6).toNanos());
      auto.onCommit();
      assertFalse(auto.shouldProbe());
      printAutoValuesIfDebug(auto, now);
    }

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertTrue(auto.currentReceive().compareTo(seeded) < 0);
    assertFalse(auto.receiveFrozen());
  }

  @Test
  void constantReceiveFreezesAfterStablePlateau() {
    final long[] gather = genRandomIntervals(45, 0, 0);
    final long[] write = genRandomIntervals(5, 0, 0);

    AutoBatchTime auto = runUntilFrozen(60, gather, write);

    assertTrue(auto.receiveFrozen());
    assertFalse(auto.frozenAtMaxTuneCap());
    assertTrue(auto.adjustCount() >= AutoBatchTime.MIN_TUNE_ADJUSTS);
    int maxAdjust = AutoBatchTime.MIN_TUNE_ADJUSTS + AutoBatchTime.STABLE_RECEIVE_CYCLES + 2;
    assertTrue(auto.adjustCount() <= maxAdjust);
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(52)) >= 0);
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(54)) <= 0);
  }

  @Test
  void autoReceiveFreezesWhenStableConstantInterval() {
    final long[] gather = genRandomIntervals(45, 0, 0);
    final long[] write = genRandomIntervals(5, 0, 0);

    AutoBatchTime auto = runUntilFrozen(60, gather, write);
    assertTrue(auto.receiveFrozen());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(52)) >= 0);
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(54)) <= 0);
  }

  @Test
  void autoReceiveFreezesWhenStableVariableInterval() {
    final long[] gather = genRandomIntervals(55, 10, 0);
    final long[] write = genRandomIntervals(5, 1, 0);

    AutoBatchTime auto = runUntilFrozen(60, gather, write);
    assertTrue(auto.receiveFrozen());
    long floor = AutoBatchTime.receiveFloorNanos(Duration.ofSeconds(60).toNanos());
    assertTrue(
        auto.currentReceive().toNanos() > floor,
        () -> "receive=" + AutoBatchTime.format(auto.currentReceive()));
    if (!auto.frozenAtMaxTuneCap()) {
      assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(48)) >= 0);
      assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(55)) <= 0);
    }
  }

  @Test
  void autoReceiveFreezesWhenStableDecreasingInterval() {
    final long[] gather = genRandomIntervals(55, 10, -3);
    final long[] write = genRandomIntervals(5, 2, 0);

    AutoBatchTime auto = runUntilFrozen(60, gather, write);
    assertTrue(auto.receiveFrozen());
    long floor = AutoBatchTime.receiveFloorNanos(Duration.ofSeconds(60).toNanos());
    assertTrue(
        auto.currentReceive().toNanos() > floor,
        () -> "receive=" + AutoBatchTime.format(auto.currentReceive()));
    if (!auto.frozenAtMaxTuneCap()) {
      assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(48)) >= 0);
      assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(55)) <= 0);
    }
  }

  @Test
  void chronicOverrunDefersStableFreezeUntilMaxCapOnVariableJitter() {
    final long[] gather = genRandomIntervals(55, 10, 0);
    final long[] write = genRandomIntervals(5, 1, 0);

    AutoBatchTime auto = runUntilFrozen(60, gather, write);

    assertTrue(auto.receiveFrozen());
    assertTrue(auto.frozenAtMaxTuneCap());
    long floor = AutoBatchTime.receiveFloorNanos(Duration.ofSeconds(60).toNanos());
    assertTrue(auto.currentReceive().toNanos() > floor);
  }

  @Test
  void autoReceiveFreezesWhenStableGrowingInterval() {
    final long[] gather = genRandomIntervals(10, 10, 3);
    final long[] write = genRandomIntervals(5, 2, 0);

    AutoBatchTime auto = runUntilFrozen(60, gather, write);
    assertTrue(auto.receiveFrozen());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(54)) <= 0);
    long medianWrite = medianNanos(write);
    long margin = AutoBatchTime.receiveWriteMarginNanos(Duration.ofSeconds(60).toNanos());
    assertTrue(
        auto.currentReceive().toNanos() + medianWrite <= Duration.ofSeconds(60).toNanos() - margin);
  }

  @Test
  void singleLargeOverrunAfterConvergeDoesNotDropReceiveBelowFortySeconds() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);
    printAutoValuesIfDebug(auto, now);

    assertTrue(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);

    for (int i = 0; i < 12; i++) {
      auto.onGatherFinished(Duration.ofSeconds(45).toNanos(), true);
      now.addAndGet(Duration.ofSeconds(5).toNanos());
      auto.onCommit();
      assertFalse(auto.shouldProbe());
      printAutoValuesIfDebug(auto, now);
      if (auto.receiveFrozen()) {
        break;
      }
    }
    assertTrue(auto.receiveFrozen());
    Duration beforeSpike = auto.currentReceive();

    auto.onGatherFinished(Duration.ofSeconds(60).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(11).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);

    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(40)) >= 0);
    assertTrue(auto.currentReceive().compareTo(beforeSpike) <= 0);
  }

  @Test
  void firstBusyCycleAfterShortSeedDoesNotShrinkReceiveToOneSecond() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    now.set(Duration.ofSeconds(40).toNanos());
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);
    assertEquals(Duration.ofSeconds(40), auto.batchInterval());
    assertEquals(Duration.ofSeconds(20), auto.currentReceive());

    auto.onGatherFinished(Duration.ofSeconds(20).toNanos(), true);
    now.set(Duration.ofSeconds(100).toNanos());
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);

    assertEquals(Duration.ofSeconds(40), auto.batchInterval());
    assertEquals(Duration.ofSeconds(20), auto.currentReceive());
    assertFalse(auto.receiveFrozen());
  }

  private long[] genRandomIntervals(
      final int minSecs, final int randomSecs, final int addedMillis) {
    final long[] intervals = new long[SIM_INTERVAL_SAMPLES];
    Random generator = new Random(42);

    long nextNanos = Duration.ofSeconds(minSecs).toNanos();
    final long randomNanos = Duration.ofSeconds(randomSecs).toNanos();
    final long addedNanos = Duration.ofMillis(addedMillis).toNanos();

    for (int i = 0; i < SIM_INTERVAL_SAMPLES; i++) {
      if (randomSecs == 0) {
        intervals[i] = nextNanos + 0;
      } else {
        final double nextSeed = generator.nextDouble() * 2.0;
        final long generatedNanos = (long) (nextSeed * randomNanos - randomNanos);
        intervals[i] = nextNanos + generatedNanos;
      }
      nextNanos += addedNanos;
    }
    return intervals;
  }

  private AutoBatchTime runUntilFrozen(
      final int probeSecs, final long[] gatherIntervals, final long[] writeIntervals) {

    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);
    printAutoValuesIfDebug(auto, now);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(probeSecs).toNanos());
    assertFalse(auto.shouldProbe());
    printAutoValuesIfDebug(auto, now);

    final long gather0 = gatherIntervals[0];
    final long write0 = writeIntervals[0];
    do {
      auto.onGatherFinished(gather0, true);
      now.addAndGet(write0);
      auto.onCommit();
      assertFalse(auto.shouldProbe());
      printAutoValuesIfDebug(auto, now);
    } while (auto.waitingToAdjustReceive());

    int maxSteps = AutoBatchTime.MAX_TUNE_ADJUSTS + 10;
    for (int i = 0; i < maxSteps && !auto.receiveFrozen(); i++) {
      final long gather = gatherIntervals[i % gatherIntervals.length];
      final long write = writeIntervals[i % writeIntervals.length];
      auto.onGatherFinished(gather, true);
      now.addAndGet(write);
      auto.onCommit();
      assertFalse(auto.shouldProbe());
      printAutoValuesIfDebug(auto, now);
    }
    return auto;
  }

  private static long medianNanos(long[] values) {
    long[] copy = values.clone();
    java.util.Arrays.sort(copy);
    return copy[copy.length / 2];
  }

  private void printAutoValuesIfDebug(AutoBatchTime auto, AtomicLong now) {
    if (!Boolean.getBoolean("autoBatchTime.debug")) {
      return;
    }
    long write = auto.calcWriteTime(now.get());
    System.out.println(
        "AutoCycle="
            + auto.autoCycles()
            + " adjustCount="
            + auto.adjustCount()
            + " stableStreak="
            + auto.stableStreak()
            + " lastGather: "
            + AutoBatchTime.format(Duration.ofNanos(auto.lastGatherNanos()))
            + " lastWrite: "
            + AutoBatchTime.format(Duration.ofNanos(write))
            + " cycle: "
            + AutoBatchTime.format(Duration.ofNanos(auto.lastGatherNanos() + write))
            + " currentReceive: "
            + AutoBatchTime.format(auto.currentReceive()));
  }
}
