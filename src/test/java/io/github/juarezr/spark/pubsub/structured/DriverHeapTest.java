package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DriverHeapTest {

  @Test
  void usedIsTotalMinusFree() {
    assertEquals(300L, DriverHeap.used(1_000L, 700L));
    assertEquals(0L, DriverHeap.used(100L, 150L));
  }

  @Test
  void lineIncludesHeapMaxAndUsed() {
    String line = DriverHeap.line("gather", 4_096L, DriverHeap.used(1_000L, 400L), 96L, 8L);

    assertTrue(line.contains("heapMax=4096"));
    assertTrue(line.contains("heapUsed=600"));
    assertTrue(line.contains("at=gather"));
    assertTrue(line.contains("gatherBytes=96"));
    assertTrue(line.contains("queuedBytes=8"));
  }
}
