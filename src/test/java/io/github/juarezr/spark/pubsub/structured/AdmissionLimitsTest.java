package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.junit.jupiter.api.Test;

class AdmissionLimitsTest {

  @Test
  void minPositivePicksTheSmallerPositiveValue() {
    assertEquals(10L, AdmissionLimits.minPositive(10L, 20L));
    assertEquals(10L, AdmissionLimits.minPositive(0L, 10L));
    assertEquals(10L, AdmissionLimits.minPositive(10L, 0L));
    assertEquals(AdmissionLimits.UNLIMITED, AdmissionLimits.minPositive(0L, 0L));
  }

  @Test
  void sparkMaxRowsWinsWhenTighterThanBatchCount() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .batchCount(3000)
            .batchSize(0)
            .build();
    AdmissionLimits limits = AdmissionLimits.from(config, ReadLimit.maxRows(500));

    assertEquals(500L, limits.maxRows());
    assertEquals(0, limits.messagesForNextPull(500, 1000));
    assertEquals(500, limits.messagesForNextPull(0, 1000));
    assertTrue(limits.reachedMax(500, 0));
    assertFalse(limits.reachedMax(499, 0));
  }

  @Test
  void batchCountWinsWhenTighterThanSparkMaxRows() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .batchCount(100)
            .batchSize(0)
            .build();
    AdmissionLimits limits = AdmissionLimits.from(config, ReadLimit.maxRows(500));

    assertEquals(100L, limits.maxRows());
  }

  @Test
  void compositeMaxAndMinRows() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .batchCount(0)
            .batchSize(0)
            .batchTime(Duration.ofSeconds(10))
            .build();
    ReadLimit composite =
        ReadLimit.compositeLimit(
            new ReadLimit[] {ReadLimit.maxRows(200), ReadLimit.minRows(50, 2_000)});
    AdmissionLimits limits = AdmissionLimits.from(config, composite);

    assertEquals(200L, limits.maxRows());
    assertEquals(50L, limits.minRows());
    assertEquals(Duration.ofMillis(2_000), limits.waitTime());
    assertFalse(limits.singlePull());
    assertTrue(limits.minRowsMet(50));
    assertFalse(limits.minRowsMet(49));
  }

  @Test
  void pullModeWithoutMinRowsIsASinglePull() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .gatherMode(GatherMode.PULL)
            .batchSize(0)
            .build();
    AdmissionLimits limits = AdmissionLimits.from(config, ReadLimit.maxRows(10));

    assertTrue(limits.singlePull());
    assertEquals(10, limits.messagesForNextPull(0, 1000));
  }
}
