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

  private static final int SIM_INTERVAL_SAMPLES = 32;

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

    auto.onGatherFinished(Duration.ofSeconds(5).toNanos(), 0);
    now.addAndGet(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
  }

  @Test
  void busyOverrunShrinksReceiveDoesNotGrowInterval() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(30), auto.currentReceive());

    auto.onGatherFinished(Duration.ofSeconds(50).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(20).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(40)) <= 0);
  }

  @Test
  void busyStartToStartNearCycleDoesNotBecomeInterval() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(50).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(50), auto.batchInterval());

    now.addAndGet(Duration.ofSeconds(6).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(6).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(136).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());

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

    now.addAndGet(Duration.ofSeconds(6).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(6).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(136).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(50), auto.batchInterval());

    now.addAndGet(Duration.ofSeconds(48).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(48).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(5).toNanos());
    auto.onCommit();
    now.addAndGet(Duration.ofSeconds(7).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(54)) <= 0);
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
  void receiveWriteMarginAndReserveUsePercentAndFloor() {
    assertEquals(
        Duration.ofSeconds(1).toNanos(),
        AutoBatchTime.receiveWriteMarginNanos(Duration.ofSeconds(60).toNanos()));
    assertEquals(
        Duration.ofSeconds(5).toNanos(),
        AutoBatchTime.receiveWriteReserveNanos(Duration.ofSeconds(60).toNanos()));
    assertEquals(
        Duration.ofSeconds(1).toNanos(),
        AutoBatchTime.receiveWriteReserveNanos(Duration.ofSeconds(10).toNanos()));
  }

  @Test
  void clampReceiveUsesWriteReserveWhenMeasuredWriteIsTiny() {
    Duration target =
        AutoBatchTime.receiveFromWriteEstimate(
            Duration.ofSeconds(60), Duration.ofSeconds(1).toNanos());
    assertEquals(Duration.ofSeconds(54), target);
    Duration clamped =
        AutoBatchTime.clampReceive(
            Duration.ofSeconds(58), Duration.ofSeconds(60), Duration.ofSeconds(1).toNanos());
    assertEquals(Duration.ofSeconds(54), clamped);
  }

  @Test
  void clampReceiveKeepsFloorAndMargin() {
    long writeEst = Duration.ofSeconds(10).toNanos();
    Duration clamped =
        AutoBatchTime.clampReceive(Duration.ofSeconds(80), Duration.ofSeconds(60), writeEst);
    assertEquals(Duration.ofSeconds(49), clamped);
    Duration floor =
        AutoBatchTime.clampReceive(Duration.ofMillis(10), Duration.ofSeconds(60), writeEst);
    assertEquals(Duration.ofSeconds(15), floor);
  }

  @Test
  void clampReceiveUsesScaledMarginForLongInterval() {
    long writeEst = Duration.ofSeconds(10).toNanos();
    Duration clamped =
        AutoBatchTime.clampReceive(Duration.ofSeconds(110), Duration.ofSeconds(120), writeEst);
    long margin = AutoBatchTime.receiveWriteMarginNanos(Duration.ofSeconds(120).toNanos());
    long reserve = AutoBatchTime.receiveWriteReserveNanos(Duration.ofSeconds(120).toNanos());
    long writeForMax = Math.max(writeEst, reserve);
    long expectedMax = Duration.ofSeconds(120).toNanos() - writeForMax - margin;
    assertEquals(Duration.ofNanos(expectedMax), clamped);
  }

  @Test
  void leftoverThenTwoSmallWritesDoesNotStabilizeReceiveTooEarly() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(50).toNanos());
    assertFalse(auto.shouldProbe());

    now.addAndGet(Duration.ofSeconds(6).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(6).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(136).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());

    now.addAndGet(Duration.ofSeconds(48).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(48).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(5).toNanos());
    auto.onCommit();
    now.addAndGet(Duration.ofSeconds(7).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(60), auto.batchInterval());

    now.addAndGet(Duration.ofSeconds(30).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(30).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());
    now.addAndGet(Duration.ofSeconds(30).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(30).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());

    assertFalse(auto.receiveFrozen());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(59)) < 0);
  }

  @Test
  void severalOverrunsIncreaseWriteEstimateAndLowerReceive() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    for (int i = 0; i < 4; i++) {
      auto.onGatherFinished(Duration.ofSeconds(58).toNanos(), 1);
      now.addAndGet(Duration.ofSeconds(6).toNanos());
      auto.onCommit();
      assertFalse(auto.shouldProbe());
    }

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(55)) < 0);
  }

  @Test
  void constantReceiveStabilizesNearFiftyFourSeconds() {
    final long[] gather = genRandomIntervals(45, 0, 0);
    final long[] write = genRandomIntervals(5, 0, 0);

    AutoBatchTime auto = runUntilStabilized(60, gather, write);

    assertTrue(auto.receiveFrozen());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(52)) >= 0);
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(54)) <= 0);
  }

  @Test
  void autoReceiveStabilizesWhenStableConstantInterval() {
    final long[] gather = genRandomIntervals(45, 0, 0);
    final long[] write = genRandomIntervals(5, 0, 0);

    AutoBatchTime auto = runUntilStabilized(60, gather, write);
    assertTrue(auto.receiveFrozen());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(52)) >= 0);
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(54)) <= 0);
  }

  @Test
  void autoReceiveWriteFirstVariableIntervalDoesNotClampToFiftyEight() {
    final long[] gather = genRandomIntervals(55, 10, 0);
    final long[] write = genRandomIntervals(5, 1, 0);

    AutoBatchTime auto = runSimulation(60, gather, write, 25);
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(58)) < 0);
    long floor = AutoBatchTime.receiveFloorNanos(Duration.ofSeconds(60).toNanos());
    assertTrue(auto.currentReceive().toNanos() > floor);
  }

  @Test
  void autoReceiveWriteFirstGrowingIntervalDoesNotInflateReceive() {
    final long[] gather = genRandomIntervals(10, 10, 3);
    final long[] write = genRandomIntervals(5, 2, 0);

    AutoBatchTime auto = runSimulation(60, gather, write, 25);
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(54)) <= 0);
  }

  @Test
  void singleLargeWriteSpikeLowersReceiveWithoutFloorHunt() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    for (int i = 0; i < 20; i++) {
      auto.onGatherFinished(Duration.ofSeconds(45).toNanos(), 1);
      now.addAndGet(Duration.ofSeconds(5).toNanos());
      auto.onCommit();
      assertFalse(auto.shouldProbe());
      if (auto.currentReceive().compareTo(Duration.ofSeconds(52)) >= 0) {
        break;
      }
    }
    Duration beforeSpike = auto.currentReceive();
    assertTrue(beforeSpike.compareTo(Duration.ofSeconds(50)) >= 0);

    auto.onGatherFinished(Duration.ofSeconds(60).toNanos(), 1);
    now.addAndGet(Duration.ofSeconds(11).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());

    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(40)) >= 0);
    assertTrue(auto.currentReceive().compareTo(beforeSpike) <= 0);
  }

  @Test
  void firstBusyCycleAfterShortSeedDoesNotShrinkReceiveToOneSecond() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(40).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(40), auto.batchInterval());
    assertEquals(Duration.ofSeconds(20), auto.currentReceive());

    auto.onGatherFinished(Duration.ofSeconds(20).toNanos(), 1);
    now.set(Duration.ofSeconds(100).toNanos());
    assertFalse(auto.shouldProbe());

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
        intervals[i] = nextNanos;
      } else {
        final double nextSeed = generator.nextDouble() * 2.0;
        final long generatedNanos = (long) (nextSeed * randomNanos - randomNanos);
        intervals[i] = nextNanos + generatedNanos;
      }
      nextNanos += addedNanos;
    }
    return intervals;
  }

  private AutoBatchTime runUntilStabilized(
      final int probeSecs, final long[] gatherIntervals, final long[] writeIntervals) {
    return runSimulation(probeSecs, gatherIntervals, writeIntervals, SIM_INTERVAL_SAMPLES);
  }

  private AutoBatchTime runSimulation(
      final int probeSecs,
      final long[] gatherIntervals,
      final long[] writeIntervals,
      final int maxSteps) {

    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(probeSecs).toNanos());
    assertFalse(auto.shouldProbe());

    final long gather0 = gatherIntervals[0];
    final long write0 = writeIntervals[0];
    do {
      auto.onGatherFinished(gather0, 1);
      now.addAndGet(write0);
      auto.onCommit();
      assertFalse(auto.shouldProbe());
    } while (auto.waitingToAdjustReceive());

    for (int i = 0; i < maxSteps && !auto.receiveFrozen(); i++) {
      final long gather = gatherIntervals[i % gatherIntervals.length];
      final long write = writeIntervals[i % writeIntervals.length];
      auto.onGatherFinished(gather, 1);
      now.addAndGet(write);
      auto.onCommit();
      assertFalse(auto.shouldProbe());
    }
    return auto;
  }
}
