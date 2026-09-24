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
    assertEquals(Duration.ofSeconds(60), auto.processingTime());
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
    assertEquals(Duration.ofSeconds(60), auto.processingTime());
    assertEquals(Duration.ofSeconds(30), auto.receiveWindow(null));
  }

  @Test
  void idleStartToStartRaisesSeededIntervalOnce() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(16).toNanos());
    assertFalse(auto.shouldProbe());
    assertEquals(Duration.ofSeconds(16), auto.processingTime());
    assertEquals(Duration.ofSeconds(8), auto.receiveWindow(null));

    auto.onGatherFinished(Duration.ofSeconds(5).toNanos(), false);
    now.addAndGet(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.processingTime());
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

    assertEquals(Duration.ofSeconds(60), auto.processingTime());
    assertTrue(auto.currentReceive().compareTo(Duration.ofSeconds(30)) <= 0);
  }

  @Test
  void busyCycleDoesNotRaiseInterval() {
    AtomicLong now = new AtomicLong(0);
    AutoBatchTime auto = new AutoBatchTime(null, now::get);

    assertTrue(auto.shouldProbe());
    now.set(Duration.ofSeconds(60).toNanos());
    assertFalse(auto.shouldProbe());

    auto.onGatherFinished(Duration.ofSeconds(20).toNanos(), true);
    now.addAndGet(Duration.ofSeconds(5).toNanos());
    auto.onCommit();

    now.set(Duration.ofSeconds(60).toNanos() + Duration.ofSeconds(70).toNanos());
    assertFalse(auto.shouldProbe());

    assertEquals(Duration.ofSeconds(60), auto.processingTime());
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
    assertEquals(Duration.ofSeconds(1), floor);
  }
}
