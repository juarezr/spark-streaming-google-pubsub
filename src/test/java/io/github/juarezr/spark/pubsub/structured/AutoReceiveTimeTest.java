package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.common.Into;
import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AutoReceiveTimeTest {

  private static void finishProbe(AutoReceiveTime auto, AtomicLong now, long gapSeconds) {
    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(gapSeconds).toNanos());
    assertTrue(auto.shouldProbe());
    now.addAndGet(Duration.ofSeconds(gapSeconds).toNanos());
    assertFalse(auto.shouldProbe());
  }

  private static void batch(
      AutoReceiveTime auto,
      AtomicLong now,
      long gatherSeconds,
      long commitMillis,
      long wallMillis,
      int count) {
    now.addAndGet(Duration.ofSeconds(gatherSeconds).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(gatherSeconds).toNanos(), count);
    now.addAndGet(Duration.ofMillis(commitMillis).toNanos());
    auto.onCommit();
    long start =
        now.get()
            - Duration.ofSeconds(gatherSeconds).toNanos()
            - Duration.ofMillis(commitMillis).toNanos();
    now.set(start + Duration.ofMillis(wallMillis).toNanos());
    assertFalse(auto.shouldProbe());
  }

  private static void batches(
      AutoReceiveTime auto, AtomicLong now, int times, long gatherSeconds, long wallMillis) {
    for (int i = 0; i < times; i++) {
      batch(auto, now, gatherSeconds, 1_000, wallMillis, 1_000);
    }
  }

  @Test
  void wholeSecondIsTheSecondTheGapHasReached() {
    assertEquals(
        Duration.ofSeconds(59).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofMillis(59_400).toNanos()));
    assertEquals(
        Duration.ofSeconds(60).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofMillis(60_109).toNanos()));
    assertEquals(
        Duration.ofSeconds(60).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofMillis(60_600).toNanos()));
    assertEquals(
        Duration.ofSeconds(61).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofMillis(61_882).toNanos()));
    assertEquals(
        Duration.ofSeconds(61).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofSeconds(61).toNanos()));
    assertEquals(
        Duration.ofSeconds(120).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofSeconds(120).toNanos()));
    assertEquals(
        Duration.ofSeconds(59).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofSeconds(59).toNanos()));
    assertEquals(
        Duration.ofSeconds(60).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofMillis(60_500).toNanos()));
    assertEquals(
        Duration.ofSeconds(60).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofMillis(59_999).toNanos()));
    assertEquals(
        Duration.ofSeconds(60).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofMillis(59_900).toNanos()));
    assertEquals(
        Duration.ofSeconds(59).toNanos(),
        AutoReceiveTime.wholeSecondNanos(Duration.ofMillis(59_899).toNanos()));
  }

  @Test
  void configuredReceiveTimeIsFixed() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .receiveTime(Duration.ofSeconds(10))
            .build();
    AutoReceiveTime auto = AutoReceiveTime.create(config);
    assertEquals(AutoReceiveTime.Phase.FIXED, auto.phase());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(10), auto.receiveWindow(null));
  }

  @Test
  void unsetReceiveTimeInBatchModeStartsAsProbe() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.BATCH)
            .build();
    AutoReceiveTime auto = AutoReceiveTime.create(config, new AtomicLong()::get);
    assertEquals(AutoReceiveTime.Phase.PROBE, auto.phase());
    assertTrue(auto.shouldProbe());
  }

  @Test
  void twoSixtySecondGapsFinishAtSixtyWithHalfReceive() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertEquals(AutoReceiveTime.Phase.WARMUP, auto.phase());
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    assertEquals(Duration.ofSeconds(30), auto.currentReceive());
    assertEquals(Duration.ofSeconds(29).toNanos(), auto.writeBudgetNanos());
  }

  @Test
  void shortWallAfterReceiveClimbsStaysAtFiftyNine() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 59);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 20, 1_000, 45_000, 1_000);
    batch(auto, now, 20, 1_000, 45_000, 1_000);
    assertTrue(auto.currentReceive().toNanos() > Duration.ofMillis(29_500).toNanos());
    assertEquals(Duration.ofSeconds(59), auto.cycleBudget());
    batch(auto, now, 20, 1_000, 60_000, 1_000);
    assertEquals(Duration.ofSeconds(59), auto.cycleBudget());
    batch(auto, now, 20, 1_000, 60_000, 1_000);
    assertEquals(Duration.ofSeconds(59), auto.cycleBudget());
    assertEquals(Duration.ofSeconds(1).toNanos(), auto.writeDurationNanos());
  }

  @Test
  void fiftyNineSecondProbeRisesToSixtyAfterTwoRounds() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 59);
    assertEquals(Duration.ofSeconds(59), auto.cycleBudget());
    assertFalse(auto.shouldProbe());
    batches(auto, now, 8, 30, 60_000);
    assertEquals(Duration.ofSeconds(59), auto.cycleBudget());
    batches(auto, now, 8, 30, 60_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    batches(auto, now, 16, 30, 70_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
  }

  @Test
  void sixtyPointOneDoesNotChangeASixtySecondEstimate() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 50, 1_000, 60_109, 10_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    assertEquals(Duration.ofSeconds(1).toNanos(), auto.writeDurationNanos());
  }

  @Test
  void shortCycleSleepIsNotALongWrite() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 38, 1_000, 60_100, 10_000);
    assertEquals(Duration.ofSeconds(1).toNanos(), auto.writeDurationNanos());
    assertEquals(Duration.ofMillis(23_725).toNanos(), auto.writeBudgetNanos());
    String line = auto.lastSnapshot().toString();
    assertTrue(line.contains("idleDuration=21100ms"));
    assertTrue(line.contains("tailMedian="));
    assertTrue(line.contains("tailStdev="));
    assertTrue(line.contains("msgs=10000"));
    assertTrue(line.contains("receiveRate="));
    assertTrue(line.contains("syncRate="));
    assertTrue(line.contains("cycleRate="));
    assertFalse(line.contains(" rate="));
  }

  @Test
  void ratesUseReceiveWriteAndCycle() {
    assertEquals(0L, AutoReceiveTime.perSecond(10, 0L));
    assertEquals(0L, AutoReceiveTime.perSecond(0, Duration.ofSeconds(1).toNanos()));
    long receiveNanos = Duration.ofMillis(234).toNanos();
    assertEquals(
        28_817L * 1_000_000_000L / receiveNanos, AutoReceiveTime.perSecond(28_817, receiveNanos));
    long writeNanos = Duration.ofMillis(57).toNanos();
    assertEquals(
        28_817L * 1_000_000_000L / writeNanos, AutoReceiveTime.perSecond(28_817, writeNanos));
    assertEquals(480L, AutoReceiveTime.perSecond(28_817, Duration.ofSeconds(60).toNanos()));
  }

  @Test
  void cruiseWithAnEmptyTailStaysCruise() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 38, 1_000, 60_100, 10_000);
    String line = auto.lastSnapshot().toString();
    assertTrue(line.contains("phase=cruise"));
    assertTrue(line.contains("tailMedian=unset"));
  }

  @Test
  void heavyWriteRaisesTheReserveWithoutJumpingReceive() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 60_000, 1_000);
    long before = auto.currentReceive().toNanos();
    long seed = auto.writeBudgetNanos();
    batch(auto, now, 48, 1_000, 70_000, 10_000);
    assertTrue(auto.writeDurationNanos() >= Duration.ofSeconds(20).toNanos());
    assertTrue(auto.currentReceive().toNanos() <= before);
    assertEquals(seed + Duration.ofSeconds(10).toNanos(), auto.writeBudgetNanos());
  }

  @Test
  void quietCycleKeepsTheWindowReserve() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 48, 1_000, 70_000, 10_000);
    long bumped = auto.writeBudgetNanos();
    batch(auto, now, 40, 1_000, 50_000, 1_000);
    assertEquals(bumped - Duration.ofMillis(2_250).toNanos(), auto.writeBudgetNanos());
    assertTrue(auto.lastSnapshot().toString().contains("tailMedian=unset"));
  }

  @Test
  void longerGapStillRaisesOnce() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 50, 1_000, 51_000, 1_000);
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(1).toNanos(), 0);
    long start = now.get() - Duration.ofSeconds(1).toNanos();
    now.set(start + Duration.ofSeconds(120).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(120), auto.cycleBudget());
  }

  @Test
  void threeSubSecondGapsAreZeroTrigger() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    assertTrue(auto.shouldProbe());
    now.addAndGet(Duration.ofMillis(200).toNanos());
    assertTrue(auto.shouldProbe());
    now.addAndGet(Duration.ofMillis(200).toNanos());
    assertTrue(auto.shouldProbe());
    now.addAndGet(Duration.ofMillis(200).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(AutoReceiveTime.Phase.ZERO, auto.phase());
    assertEquals(Duration.ofSeconds(1), auto.currentReceive());
  }

  @Test
  void processingTimeHintFinishesOnTheFirstGap() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get, Duration.ofSeconds(60));
    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    assertEquals(Duration.ofSeconds(180), auto.inferredAckDeadline());
  }

  @Test
  void twoSlightlyLongGapsFinishAtSixty() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    assertTrue(auto.shouldProbe());
    now.set(Duration.ofMillis(60_600).toNanos());
    assertTrue(auto.shouldProbe());
    now.addAndGet(Duration.ofMillis(60_800).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    assertEquals(Duration.ofSeconds(30), auto.currentReceive());
  }

  @Test
  void partialGapPlusFullPeriodKeepsTheLargerGap() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(27).toNanos());
    assertTrue(auto.shouldProbe());
    now.addAndGet(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
  }

  @Test
  void pairJustUnderTheNextSecondStaysAtSixty() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 60_736, 1_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    batch(auto, now, 30, 1_000, 60_957, 1_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
  }

  @Test
  void singleLateCycleArmsAndTheNextClearsIt() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 60_977, 1_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    batch(auto, now, 30, 1_000, 66_000, 1_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
  }

  @Test
  void lateCycleAfterReceiveClimbsDoesNotRaiseTheEstimate() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 60_000, 1_000);
    batch(auto, now, 30, 1_000, 60_000, 1_000);
    batch(auto, now, 40, 1_000, 61_882, 10_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
  }

  @Test
  void busyWallDoesNotRaiseTheEstimate() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 48, 1_000, 70_000, 10_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
  }

  @Test
  void gapOneMillisecondUnderSixtyFinishesAtSixty() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    assertTrue(auto.shouldProbe());
    now.set(Duration.ofMillis(59_999).toNanos());
    assertTrue(auto.shouldProbe());
    now.addAndGet(Duration.ofMillis(59_999).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    assertEquals(Duration.ofSeconds(30), auto.currentReceive());
  }

  @Test
  void oneTightRoundDoesNotAdoptAndALateRoundResetsThePair() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 59);
    assertFalse(auto.shouldProbe());
    batches(auto, now, 8, 30, 60_000);
    assertEquals(Duration.ofSeconds(59), auto.cycleBudget());
    batches(auto, now, 8, 30, 60_736);
    assertEquals(Duration.ofSeconds(59), auto.cycleBudget());
    batches(auto, now, 8, 30, 60_000);
    assertEquals(Duration.ofSeconds(59), auto.cycleBudget());
    batches(auto, now, 8, 30, 60_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
  }

  @Test
  void doublePeriodRaiseIsUndoneByTwoRoundsThenStops() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 50, 1_000, 51_000, 1_000);
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(1).toNanos(), 0);
    long start = now.get() - Duration.ofSeconds(1).toNanos();
    now.set(start + Duration.ofSeconds(120).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(120), auto.cycleBudget());
    batches(auto, now, 8, 30, 60_000);
    assertEquals(Duration.ofSeconds(120), auto.cycleBudget());
    batches(auto, now, 8, 30, 60_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    auto.onGatherFinished(Duration.ofSeconds(1).toNanos(), 0);
    start = now.get() - Duration.ofSeconds(1).toNanos();
    now.set(start + Duration.ofSeconds(120).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(60), auto.cycleBudget());
  }

  @Test
  void lateCycleAddsOnlyTheOverrun() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 48, 1_000, 62_301, 10_000);
    assertEquals(Duration.ofMillis(31_301).toNanos(), auto.writeBudgetNanos());
    assertTrue(auto.lastSnapshot().toString().contains("idleDuration=0ms"));
    assertTrue(auto.lastSnapshot().toString().contains("tailMedian=unset"));
    batch(auto, now, 30, 1_000, 60_000, 1_000);
    assertEquals(Duration.ofMillis(24_051).toNanos(), auto.writeBudgetNanos());
    assertTrue(auto.lastSnapshot().toString().contains("tailMedian=unset"));
  }

  @Test
  void eightLateCyclesStepTowardThePostReceiveTime() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    while (auto.currentReceive().compareTo(Duration.ofSeconds(45)) < 0) {
      batch(auto, now, 30, 1_000, 60_000, 1_000);
    }
    String crossed = auto.lastSnapshot().toString();
    assertTrue(crossed.contains("phase=cruise"));
    assertTrue(crossed.contains("tailMedian=unset"));
    batch(auto, now, 30, 1_000, 45_000, 1_000);
    String idle = auto.lastSnapshot().toString();
    assertTrue(idle.contains("phase=cruise"));
    assertTrue(idle.contains("idleDuration=14000ms"));
    assertTrue(idle.contains("tailMedian=unset"));
    long climbed = auto.writeBudgetNanos();
    for (int i = 0; i < 7; i++) {
      batch(auto, now, 52, 1_000, 60_000, 1_000);
      assertTrue(auto.lastSnapshot().toString().contains("phase=cruise"));
      assertTrue(auto.lastSnapshot().toString().contains("tailMedian=unset"));
    }
    assertEquals(climbed, auto.writeBudgetNanos());
    batch(auto, now, 52, 1_000, 60_905, 1_000);
    assertEquals(climbed, auto.writeBudgetNanos());
    assertTrue(auto.lastSnapshot().toString().contains("tailMedian=unset"));
    long careful = Duration.ofSeconds(60).toNanos() * 3L / 100L;
    long beforeLate = auto.writeBudgetNanos();
    long receiveBeforeLate = auto.currentReceive().toNanos();
    batch(auto, now, 52, 1_000, 62_000, 10_000);
    String first = auto.lastSnapshot().toString();
    assertTrue(first.contains("phase=sample"));
    assertTrue(first.contains("tailMedian=10000ms"));
    assertFalse(first.contains("tailMedian=14000ms"));
    long firstStep = auto.writeBudgetNanos();
    assertTrue(firstStep > beforeLate);
    assertTrue(firstStep - beforeLate <= careful);
    assertTrue(firstStep < beforeLate + Duration.ofSeconds(2).toNanos());
    assertTrue(auto.currentReceive().toNanos() < receiveBeforeLate);
    long receiveAfterLate = auto.currentReceive().toNanos();
    batch(auto, now, 30, 1_000, 45_000, 1_000);
    String quiet = auto.lastSnapshot().toString();
    assertTrue(quiet.contains("phase=sample"));
    assertTrue(quiet.contains("idleDuration=14000ms"));
    assertTrue(quiet.contains("tailMedian=10000ms"));
    assertFalse(quiet.contains("tailMedian=14000ms"));
    assertEquals(firstStep - careful, auto.writeBudgetNanos());
    assertTrue(auto.writeBudgetNanos() > Duration.ofSeconds(1).toNanos());
    long receiveAfterQuiet = auto.currentReceive().toNanos();
    assertTrue(receiveAfterQuiet > receiveAfterLate);
    assertTrue(receiveAfterQuiet - receiveAfterLate <= careful);
    for (int i = 0; i < 6; i++) {
      batch(auto, now, 52, 1_000, 62_000, 10_000);
      String line = auto.lastSnapshot().toString();
      assertTrue(line.contains("phase=sample"));
      assertTrue(line.contains("tailMedian=10000ms"));
      assertFalse(line.contains("tailMedian=14000ms"));
    }
    batch(auto, now, 52, 1_000, 62_000, 10_000);
    assertTrue(auto.lastSnapshot().toString().contains("phase=cruise"));
    long high = auto.writeBudgetNanos();
    assertTrue(high > climbed);
    long previous = high;
    for (int i = 0; i < 8; i++) {
      batch(auto, now, 58, 1_000, 62_000, 1_000);
      long current = auto.writeBudgetNanos();
      assertTrue(Math.abs(current - previous) <= careful);
      previous = current;
    }
    assertTrue(auto.writeBudgetNanos() > Duration.ofSeconds(1).toNanos());
    assertTrue(auto.writeBudgetNanos() < high);
  }

  @Test
  void lateCycleWithinThePadDoesNotStep() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    long seed = auto.writeBudgetNanos();
    batch(auto, now, 52, 1_000, 60_905, 1_000);
    assertEquals(seed, auto.writeBudgetNanos());
  }

  @Test
  void rapidClimbStepsByAQuarterOfTheIdle() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 47_000, 1_000);
    assertEquals(Duration.ofSeconds(25).toNanos(), auto.writeBudgetNanos());
    String line = auto.lastSnapshot().toString();
    assertTrue(line.contains("idleDuration=16000ms"));
    assertTrue(line.contains("phase=cruise"));
  }

  @Test
  void smallIdleDoesNotStep() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    long seed = auto.writeBudgetNanos();
    batch(auto, now, 30, 1_000, 34_600, 1_000);
    assertEquals(seed, auto.writeBudgetNanos());
  }

  @Test
  void topQuarterStepsByThreePercentAndASmallIdleDoesNot() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    while (auto.currentReceive().compareTo(Duration.ofSeconds(45)) < 0) {
      batch(auto, now, 30, 1_000, 60_000, 1_000);
    }
    long before = auto.writeBudgetNanos();
    batch(auto, now, 30, 1_000, 60_000, 1_000);
    long careful = Duration.ofSeconds(60).toNanos() * 3L / 100L;
    assertEquals(before - careful, auto.writeBudgetNanos());
    assertTrue(auto.lastSnapshot().toString().contains("phase=cruise"));
    while (auto.currentReceive().compareTo(Duration.ofSeconds(52)) < 0) {
      batch(auto, now, 30, 1_000, 60_000, 1_000);
    }
    long atEdge = auto.writeBudgetNanos();
    long receiveAtEdge = auto.currentReceive().toNanos();
    batch(auto, now, 52, 1_000, 62_000, 10_000);
    long afterLate = auto.writeBudgetNanos();
    assertTrue(afterLate > atEdge);
    assertTrue(afterLate - atEdge <= careful);
    assertTrue(afterLate < atEdge + Duration.ofSeconds(2).toNanos());
    assertTrue(auto.lastSnapshot().toString().contains("idleDuration=0ms"));
    assertTrue(auto.lastSnapshot().toString().contains("phase=sample"));
    assertTrue(auto.currentReceive().toNanos() < receiveAtEdge);
    long receiveAfterLate = auto.currentReceive().toNanos();
    batch(auto, now, 30, 1_000, 45_000, 1_000);
    assertEquals(afterLate - careful, auto.writeBudgetNanos());
    assertTrue(auto.writeBudgetNanos() > Duration.ofSeconds(1).toNanos());
    long receiveAfterQuiet = auto.currentReceive().toNanos();
    assertTrue(receiveAfterQuiet > receiveAfterLate);
    assertTrue(receiveAfterQuiet - receiveAfterLate <= careful);
    assertTrue(auto.lastSnapshot().toString().contains("idleDuration=14000ms"));
    assertFalse(auto.lastSnapshot().toString().contains("tailMedian=14000ms"));
  }

  @Test
  void lateCycleBelowTheTopQuarterStillSteps() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 55_000, 1_000);
    assertEquals(Duration.ofSeconds(36), auto.currentReceive());
    batch(auto, now, 36, 1_000, 62_000, 10_000);
    long afterLate = auto.writeBudgetNanos();
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(36)) < 0);
    batch(auto, now, 30, 1_000, 55_000, 1_000);
    long idle = Duration.ofSeconds(24).toNanos();
    assertEquals(afterLate - idle / 4L, auto.writeBudgetNanos());
    assertTrue(auto.lastSnapshot().toString().contains("phase=cruise"));
  }

  @Test
  void quietSpareCycleStepsBelowTheReserve() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    while (auto.currentReceive().compareTo(Duration.ofSeconds(56)) < 0) {
      batch(auto, now, 30, 1_000, 60_000, 1_000);
    }
    long careful = Duration.ofSeconds(60).toNanos() * 3L / 100L;
    for (int i = 0; i < 4; i++) {
      batch(auto, now, 54, 1_000, 62_000, 10_000);
    }
    for (int i = 0; i < 3; i++) {
      batch(auto, now, 57, 1_000, 62_000, 1_000);
    }
    long above = auto.writeBudgetNanos();
    batch(auto, now, 57, 1_000, 62_000, 1_000);
    long afterPush = auto.writeBudgetNanos();
    assertTrue(above > afterPush);
    assertTrue(above - afterPush <= careful);
    assertTrue(afterPush >= Duration.ofSeconds(8).toNanos());
    assertTrue(auto.lastSnapshot().toString().contains("phase=cruise"));
    batch(auto, now, 30, 1_000, 45_000, 1_000);
    assertEquals(afterPush - careful, auto.writeBudgetNanos());
    assertTrue(auto.writeBudgetNanos() > Duration.ofSeconds(1).toNanos());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(45)) > 0);
  }

  @Test
  void lateCycleBelowTheTopQuarterDoesNotAddTheWholeOverrunOnceASampleExists() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    while (auto.currentReceive().compareTo(Duration.ofSeconds(45)) < 0) {
      batch(auto, now, 30, 1_000, 60_000, 1_000);
    }
    long careful = Duration.ofSeconds(60).toNanos() * 3L / 100L;
    while (auto.currentReceive().compareTo(Duration.ofSeconds(45)) >= 0) {
      long before = auto.writeBudgetNanos();
      batch(auto, now, 40, 1_000, 70_000, 10_000);
      assertTrue(auto.writeBudgetNanos() - before <= careful);
    }
    long under = auto.writeBudgetNanos();
    batch(auto, now, 40, 1_000, 80_000, 10_000);
    long stepped = auto.writeBudgetNanos() - under;
    assertTrue(stepped > 0L);
    assertTrue(stepped <= careful);
    assertTrue(stepped < Duration.ofSeconds(20).toNanos());
    assertTrue(auto.lastSnapshot().toString().contains("phase=sample"));
  }

  @Test
  void eightSamplesSayCruiseEvenWhenReceiveIsUnderFortyFive() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    while (auto.currentReceive().compareTo(Duration.ofSeconds(45)) < 0) {
      batch(auto, now, 30, 1_000, 60_000, 1_000);
    }
    for (int i = 0; i < 8; i++) {
      batch(auto, now, 40, 1_000, 70_000, 10_000);
    }
    String line = auto.lastSnapshot().toString();
    assertTrue(line.contains("phase=cruise"));
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(45)) < 0);
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(15)) > 0);
  }

  @Test
  void probeGapConsistentDetectsStableAndFullPeriod() {
    assertTrue(AutoReceiveTime.probeGapConsistent(60, 58));
    assertTrue(AutoReceiveTime.probeGapConsistent(60, 27));
    assertFalse(AutoReceiveTime.probeGapConsistent(30, 22));
  }

  @Test
  void elapsedNanosFormatsSeconds() {
    assertEquals("45000ms", Into.elapsed(Duration.ofSeconds(45).toNanos()));
    assertEquals("15268ms", Into.elapsed(15_268_921_265L));
    assertEquals("56ms", Into.elapsed(Duration.ofMillis(56).toNanos()));
    assertEquals("1200ms", Into.elapsed(Duration.ofMillis(1200).toNanos()));
    assertEquals("30:00", Into.elapsed(Duration.ofSeconds(1800).toNanos()));
    assertEquals("5.3m", Into.elapsed(Duration.ofSeconds(315).toNanos()));
  }
}
