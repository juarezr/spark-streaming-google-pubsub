package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AutoBatchTimeTest {

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

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(30), auto.currentReceive());

    auto.onGatherFinished(Duration.ofSeconds(50).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(20).toNanos());
    auto.onCommit();
    assertFalse(auto.shouldProbe());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(30)) <= 0);
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
    auto.onGatherFinished(Duration.ofSeconds(6).toNanos(), true);
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
    now.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
    assertTrue(auto.shouldProbe());
    now.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
    assertTrue(auto.shouldProbe());
    now.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
    assertFalse(auto.shouldProbe());

    assertEquals(AutoBatchTime.Mode.ZERO, auto.mode());
    assertEquals(Duration.ofSeconds(1), auto.receiveWindow(null));
    assertEquals(Duration.ofSeconds(180), auto.inferredAckDeadline());
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

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    assertProbe(now, auto, "severalOverrunsShrinkReceiveWithoutFreezingEarly");

    Duration seeded = auto.currentReceive();
    for (int i = 0; i < 4; i++) {
      auto.onGatherFinished(Duration.ofSeconds(58).toNanos(), true);
      now.addAndGet(Duration.ofSeconds(6).toNanos());
      auto.onCommit();
      assertProbe(now, auto);
    }

    assertEquals(Duration.ofSeconds(60), auto.batchInterval());
    assertTrue(auto.currentReceive().compareTo(seeded) < 0);
    assertFalse(auto.receiveFrozen());
  }

  @Test
  void fifteenthReceiveAdjustFreezesAllIntervals() {
    System.out.println("\nfifteenthReceiveAdjustFreezesAllIntervals:");
    fifteenthReceiveAdjustFreezesConstantInterval();
    fifteenthReceiveAdjustFreezesVariableInterval();
    fifteenthReceiveAdjustFreezesDecreasingInterval();
    fifteenthReceiveAdjustFreezesGrowingInterval();
  }

  @Test
  void fifteenthReceiveAdjustFreezesConstantInterval() {
    final long[] gather = genRandomIntervals(45, 0, 0);
    final long[] write = genRandomIntervals(5, 0, 0);

    fifteenthReceiveAdjustFreezes("Constant", 60, gather, write);
  }

  @Test
  void fifteenthReceiveAdjustFreezesVariableInterval() {
    final long[] gather = genRandomIntervals(55, 10, 0);
    final long[] write = genRandomIntervals(5, 1, 0);

    fifteenthReceiveAdjustFreezes("Variable", 60, gather, write);
  }

  @Test
  void fifteenthReceiveAdjustFreezesDecreasingInterval() {
    final long[] gather = genRandomIntervals(55, 10, -3);
    final long[] write = genRandomIntervals(5, 2, 0);

    fifteenthReceiveAdjustFreezes("Decreasing", 60, gather, write);
  }

  @Test
  void fifteenthReceiveAdjustFreezesGrowingInterval() {
    final long[] gather = genRandomIntervals(10, 10, 3);
    final long[] write = genRandomIntervals(5, 2, 0);

    fifteenthReceiveAdjustFreezes("Growing", 60, gather, write);
  }

  private long[] genRandomIntervals(
      final int minSecs, final int randomSecs, final int addedMillis) {
    final long[] intervals = new long[AutoBatchTime.ADJUST_STEPS];

    final ThreadLocalRandom generator = ThreadLocalRandom.current();
    long nextNanos = Duration.ofSeconds(minSecs).toNanos();
    final long randomNanos = Duration.ofSeconds(randomSecs).toNanos();
    final long addedNanos = Duration.ofMillis(addedMillis).toNanos();

    for (int i = 0; i < AutoBatchTime.ADJUST_STEPS; i++) {
      final long generatedNanos =
          randomSecs == 0 ? 0 : generator.nextLong(-randomNanos, randomNanos);
      intervals[i] = nextNanos + generatedNanos;
      nextNanos += addedNanos;
    }
    return intervals;
  }

  void fifteenthReceiveAdjustFreezes(
      final String caption,
      final int probeSecs,
      final long[] gatherIntervals,
      final long[] writeIntervals) {

    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);
    System.out.println("\nfifteenthReceiveAdjustFreezes" + caption + "Interval:");
    printAutoValues(auto, now);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(probeSecs).toNanos());
    assertProbe(now, auto);

    final long gather0 = gatherIntervals[0];
    final long write0 = writeIntervals[0];
    do {
      auto.onGatherFinished(gather0, true);
      now.addAndGet(write0);
      auto.onCommit();
      assertProbe(now, auto);
    } while (auto.waitingToAdjustReceive());

    for (int i = 0; i < AutoBatchTime.ADJUST_STEPS; i++) {
      final long gather = gatherIntervals[i];
      final long write = writeIntervals[i];
      auto.onGatherFinished(gather, true);
      now.addAndGet(write);
      auto.onCommit();
      assertProbe(now, auto);
    }
    assertTrue(auto.receiveFrozen());
  }

  private void assertProbe(final AtomicLong now, final AutoBatchTime auto, final String caption) {
    System.out.println("\n" + caption + ":");
    assertProbe(now, auto);
  }

  private void assertProbe(final AtomicLong now, final AutoBatchTime auto) {
    assertFalse(auto.shouldProbe());
    printAutoValues(auto, now);
  }

  private void printAutoValues(AutoBatchTime auto, AtomicLong now) {
    long write = auto.calcWriteTime(now.get());
    System.out.println(
        "AutoCycle="
            + auto.autoCycles()
            + " adjustCount="
            + auto.adjustCount()
            + " lastGather: "
            + AutoBatchTime.format(Duration.ofNanos(auto.lastGatherNanos()))
            + " lastWrite: "
            + AutoBatchTime.format(Duration.ofNanos(write))
            + " cycle: "
            + AutoBatchTime.format(Duration.ofNanos(auto.lastGatherNanos() + write))
            + " currentReceive: "
            + AutoBatchTime.format(auto.currentReceive()));
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

    auto.onGatherFinished(Duration.ofSeconds(20).toNanos(), true);
    now.set(Duration.ofSeconds(40).toNanos() + Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(40), auto.batchInterval());
    assertEquals(Duration.ofSeconds(20), auto.currentReceive());
    assertFalse(auto.receiveFrozen());
  }
}
