package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AutoBatchTimeTest {

  @Test
  void unsetBatchTimeInBatchModeStartsAsProbe() {
    PubSubConfig config = PubSubConfig.builder().projectId("p").subscription("s").build();
    AutoBatchTime auto = AutoBatchTime.create(config, new AtomicLong()::get);

    assertEquals(AutoBatchTime.Mode.PROBE, auto.mode());
    assertTrue(auto.shouldProbe());
  }

  @Test
  void configuredBatchTimeIsFixed() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .batchTime(Duration.ofSeconds(10))
            .build();
    AutoBatchTime auto = AutoBatchTime.create(config);

    assertEquals(AutoBatchTime.Mode.FIXED, auto.mode());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(10), auto.gatherWindow(null));
  }

  @Test
  void pullModeDoesNotProbeWhenBatchTimeOmitted() {
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").gatherMode(GatherMode.PULL).build();
    AutoBatchTime auto = AutoBatchTime.create(config);

    assertEquals(AutoBatchTime.Mode.FIXED, auto.mode());
    assertFalse(auto.shouldProbe());
  }

  @Test
  void oneCleanGapInfersProcessingTimeThenUsesHalf() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);

    assertTrue(auto.shouldProbe());
    now.set(100_000_000L);
    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(AutoBatchTime.Mode.AUTO, auto.mode());
    assertEquals(Duration.ofSeconds(60), auto.processingTime());
    assertEquals(Duration.ofSeconds(30), auto.gatherWindow(null));
  }

  @Test
  void formulaUsesWriteAvgStdevAndSafety() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);
    auto.shouldProbe();
    now.addAndGet(Duration.ofSeconds(60).toNanos());
    auto.shouldProbe();

    auto.recordWrite(Duration.ofSeconds(20).toNanos());
    auto.recordWrite(Duration.ofSeconds(22).toNanos());
    auto.recordWrite(Duration.ofSeconds(21).toNanos());
    auto.refreshGather();

    double avg = auto.writeAvg();
    double stdev = auto.writeStdev();
    long safety = (long) (0.005 * Duration.ofSeconds(60).toNanos());
    long expected = Duration.ofSeconds(60).toNanos() - (long) avg - (long) stdev - safety;
    assertEquals(Duration.ofNanos(expected), auto.currentGather());
  }

  @Test
  void clampKeepsGatherUnderTriggerForTypicalT() {
    Duration clamped =
        AutoBatchTime.clamp(Duration.ofSeconds(80), Duration.ofSeconds(60), Duration.ofSeconds(20));
    assertEquals(Duration.ofSeconds(59), clamped);
    Duration floor =
        AutoBatchTime.clamp(Duration.ofSeconds(1), Duration.ofSeconds(60), Duration.ofSeconds(20));
    assertEquals(Duration.ofSeconds(5), floor);
  }

  @Test
  void clampFitsShortTrigger() {
    Duration clamped =
        AutoBatchTime.clamp(Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofSeconds(20));
    assertEquals(Duration.ofMillis(500), clamped);
  }

  @Test
  void firstInferredGapUnderTenSecondsIsProbeNoise() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(4).toNanos());
    assertTrue(auto.shouldProbe());
    assertEquals(AutoBatchTime.Mode.PROBE, auto.mode());

    now.set(Duration.ofSeconds(4).toNanos() + Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(AutoBatchTime.Mode.AUTO, auto.mode());
    assertEquals(Duration.ofSeconds(60), auto.processingTime());
    assertEquals(Duration.ofSeconds(30), auto.gatherWindow(null));
  }

  @Test
  void idleStartToStartRaisesSeededProcessingTime() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(16).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(16), auto.processingTime());
    assertEquals(Duration.ofSeconds(8), auto.gatherWindow(null));

    auto.onGatherFinished(Duration.ofSeconds(5).toNanos(), false);
    now.addAndGet(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.processingTime());
    assertEquals(Duration.ofSeconds(30), auto.gatherWindow(null));
  }

  @Test
  void busyWaitRaisesSeededProcessingTime() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(16).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(16), auto.processingTime());

    auto.onGatherFinished(Duration.ofSeconds(8).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(2).toNanos());
    auto.onCommit();

    now.set(Duration.ofSeconds(16).toNanos() + Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.processingTime());
  }

  @Test
  void busyOverrunDoesNotRaiseProcessingTime() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(60), auto.processingTime());

    auto.onGatherFinished(Duration.ofSeconds(50).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(20).toNanos());
    auto.onCommit();

    now.set(Duration.ofSeconds(60).toNanos() + Duration.ofSeconds(70).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.processingTime());
  }

  @Test
  void busyMatchingTriggerDoesNotRaiseProcessingTime() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    auto.onGatherFinished(Duration.ofSeconds(20).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(5).toNanos());
    auto.onCommit();

    now.set(Duration.ofSeconds(60).toNanos() + Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.processingTime());
  }

  @Test
  void busyShortGapDoesNotShrinkProcessingTime() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);
    auto.shouldProbe();
    now.set(Duration.ofSeconds(60).toNanos());
    auto.shouldProbe();
    auto.onGatherFinished(Duration.ofSeconds(8).toNanos(), true);

    now.addAndGet(Duration.ofSeconds(2).toNanos());
    auto.shouldProbe();

    assertEquals(Duration.ofSeconds(60), auto.processingTime());
  }

  @Test
  void threeNoiseGapsAssumeZeroTrigger() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, Duration.ofSeconds(20), now::get);

    assertTrue(auto.shouldProbe());
    now.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
    assertTrue(auto.shouldProbe());
    now.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
    assertTrue(auto.shouldProbe());
    now.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
    assertFalse(auto.shouldProbe());

    assertEquals(AutoBatchTime.Mode.ZERO, auto.mode());
    assertEquals(Duration.ofSeconds(5), auto.gatherWindow(null));
  }
}
