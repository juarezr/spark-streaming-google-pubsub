package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    String line =
        DriverHeap.line(
            "gather", 4_096L, DriverHeap.used(1_000L, 400L), 2_048L, 8_192L, 16_384L, 96L, 8L);

    assertTrue(line.contains("heapMax=4096"));
    assertTrue(line.contains("heapUsed=600"));
    assertTrue(line.contains("rss=2048"));
    assertTrue(line.contains("memAvailable=8192"));
    assertTrue(line.contains("memTotal=16384"));
    assertTrue(line.contains("at=gather"));
    assertTrue(line.contains("gatherBytes=96"));
    assertTrue(line.contains("queuedBytes=8"));
  }

  @Test
  void ceilingIsNotHotAtFortyFourPercent() {
    long max = 1_000L;
    assertFalse(DriverHeap.aboveCeiling(max * 44L / 100L, max));
  }

  @Test
  void ceilingIsHotAtSixtyPercent() {
    long max = 1_000L;
    assertTrue(DriverHeap.aboveCeiling(max * 60L / 100L, max));
    assertFalse(DriverHeap.aboveCeiling(max * 60L / 100L - 1L, max));
  }

  @Test
  void heapWarningIsDueOnceAnHour() {
    assertTrue(DriverHeap.warnDue(Long.MIN_VALUE, 0L));
    assertFalse(DriverHeap.warnDue(0L, DriverHeap.HOUR_MILLIS - 1L));
    assertTrue(DriverHeap.warnDue(0L, DriverHeap.HOUR_MILLIS));
  }

  @Test
  void residentBytesReadsVmRss() {
    assertEquals(2048L, DriverHeap.residentBytes("Name:\tjava\nVmRSS:\t     2 kB\n"));
    assertEquals(-1L, DriverHeap.residentBytes("Name:\tjava\n"));
  }

  @Test
  void hostReservePausesOnlyUnder512MiB() {
    long reserve = DriverHeap.HOST_RESERVE_BYTES;

    assertTrue(DriverHeap.belowHostReserve(reserve - 1L));
    assertFalse(DriverHeap.belowHostReserve(reserve));
    assertFalse(DriverHeap.belowHostReserve(-1L));
  }

  @Test
  void collectWaitsOneCycleBudget() {
    long budget = 60_000L;

    assertFalse(DriverHeap.collectDue(0L, 1_000L, budget, false));
    assertFalse(DriverHeap.collectDue(0L, budget - 1L, budget, false));
    assertTrue(DriverHeap.collectDue(0L, budget, budget, false));
    assertFalse(DriverHeap.collectDue(0L, budget, 0L, false));
    assertFalse(DriverHeap.collectDue(0L, budget, budget, true));
    assertFalse(DriverHeap.collectDue(Long.MIN_VALUE, budget, budget, false));
  }

  @Test
  void meminfoReadsAvailableAndTotal() {
    String meminfo = "MemTotal:\t     4 kB\nMemFree:\t     1 kB\nMemAvailable:\t     2 kB\n";

    assertEquals(4096L, DriverHeap.meminfoBytes(meminfo, "MemTotal:"));
    assertEquals(2048L, DriverHeap.meminfoBytes(meminfo, "MemAvailable:"));
    assertEquals(-1L, DriverHeap.meminfoBytes("MemFree:\t     1 kB\n", "MemAvailable:"));
    assertEquals(-1L, DriverHeap.meminfoBytes(null, "MemTotal:"));
  }
}
