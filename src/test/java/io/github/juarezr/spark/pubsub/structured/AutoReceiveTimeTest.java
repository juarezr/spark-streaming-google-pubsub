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
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
    assertEquals(Duration.ofSeconds(30), auto.currentReceive());
    assertEquals(Duration.ofSeconds(29).toNanos(), auto.writeBudgetNanos());
  }

  @Test
  void shortWallAfterReceiveClimbsRaisesFiftyNineToSixty() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 59);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 20, 1_000, 45_000, 1_000);
    batch(auto, now, 20, 1_000, 45_000, 1_000);
    assertTrue(auto.currentReceive().toNanos() > Duration.ofMillis(29_500).toNanos());
    assertEquals(Duration.ofSeconds(59), auto.cycleTimeEst());
    batch(auto, now, 20, 1_000, 59_998, 1_000);
    assertEquals(Duration.ofSeconds(59), auto.cycleTimeEst());
    batch(auto, now, 20, 1_000, 59_998, 1_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
    assertEquals(Duration.ofSeconds(1).toNanos(), auto.writeDurationNanos());
  }

  @Test
  void fiftyNineSecondProbeRisesToSixtyOnTheNextFire() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 59);
    assertEquals(Duration.ofSeconds(59), auto.cycleTimeEst());
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 60_000, 10_000);
    assertEquals(Duration.ofSeconds(59), auto.cycleTimeEst());
    batch(auto, now, 30, 1_000, 60_000, 10_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
  }

  @Test
  void sixtyPointOneDoesNotChangeASixtySecondEstimate() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 50, 1_000, 60_109, 10_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
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
    assertTrue(auto.writeBudgetNanos() < Duration.ofSeconds(5).toNanos());
  }

  @Test
  void heavyWriteMovesReceiveHalfwayInsteadOfSnapping() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 60_000, 1_000);
    long before = auto.currentReceive().toNanos();
    batch(auto, now, 48, 1_000, 70_000, 10_000);
    long write = auto.writeDurationNanos();
    assertTrue(write >= Duration.ofSeconds(20).toNanos());
    long target =
        Duration.ofSeconds(60).toNanos()
            - auto.writeBudgetNanos()
            - Duration.ofSeconds(1).toNanos();
    long halfway = before + (target - before) / 2;
    assertEquals(halfway, auto.currentReceive().toNanos());
    assertTrue(auto.currentReceive().toNanos() != target);
  }

  @Test
  void quietStretchHalvesTheBudget() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 48, 1_000, 70_000, 10_000);
    long peak = auto.writeBudgetNanos();
    assertTrue(peak >= Duration.ofSeconds(20).toNanos());
    batch(auto, now, 40, 1_000, 50_000, 1_000);
    assertEquals(peak + (Duration.ofSeconds(1).toNanos() - peak) / 2, auto.writeBudgetNanos());
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
    assertEquals(Duration.ofSeconds(120), auto.cycleTimeEst());
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
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
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
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
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
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
  }

  @Test
  void singleLateCycleArmsAndTheNextClearsIt() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 60_977, 1_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
    batch(auto, now, 30, 1_000, 66_000, 1_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
  }

  @Test
  void lateCycleAfterReceiveClimbsDoesNotRaiseTheEstimate() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 30, 1_000, 60_000, 1_000);
    batch(auto, now, 30, 1_000, 60_000, 1_000);
    assertTrue(auto.currentReceive().toNanos() > Duration.ofSeconds(30).toNanos());
    batch(auto, now, 40, 1_000, 61_882, 10_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
  }

  @Test
  void busyWallDoesNotRaiseTheEstimate() {
    AtomicLong now = new AtomicLong(0);
    AutoReceiveTime auto = new AutoReceiveTime(null, now::get);
    finishProbe(auto, now, 60);
    assertFalse(auto.shouldProbe());
    batch(auto, now, 48, 1_000, 70_000, 10_000);
    assertEquals(Duration.ofSeconds(60), auto.cycleTimeEst());
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
