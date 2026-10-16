package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
            "gather",
            4_096L,
            DriverHeap.used(1_000L, 400L),
            DriverHeap.percent(600L, 4_096L),
            10L,
            2_048L,
            -20L,
            8_192L,
            30L,
            16_384L,
            DriverHeap.percent(8_192L, 16_384L),
            96L,
            4,
            8L,
            2,
            32L);

    assertTrue(line.contains("heapMax=4096"));
    assertTrue(line.contains("heapUsed=600"));
    assertTrue(line.contains("heapFraction=14"));
    assertTrue(line.contains("heapChange=10"));
    assertTrue(line.contains("rss=2048"));
    assertTrue(line.contains("rssChange=-20"));
    assertTrue(line.contains("memAvailable=8192"));
    assertTrue(line.contains("memChange=30"));
    assertTrue(line.contains("memTotal=16384"));
    assertTrue(line.contains("memFraction=50"));
    assertTrue(line.contains("at=gather"));
    assertTrue(line.contains("gatherBytes=96"));
    assertTrue(line.contains("gatherMsgs=4"));
    assertTrue(line.contains("queuedBytes=8"));
    assertTrue(line.contains("queuedMsgs=2"));
    assertTrue(line.contains("memoryBudget=32"));
  }

  @Test
  void startupNamesTheTwoConstants() {
    assertEquals("HEAP: startup heapMax=4096 memTotal=16384", DriverHeap.startup(4_096L, 16_384L));
  }

  @Test
  void percentAndSignedChange() {
    assertEquals(14L, DriverHeap.percent(600L, 4_096L));
    assertEquals(50L, DriverHeap.percent(8_192L, 16_384L));
    assertEquals(-1L, DriverHeap.percent(1L, 0L));
    assertEquals(-1L, DriverHeap.percent(-1L, 100L));
    assertEquals(0L, DriverHeap.change(Long.MIN_VALUE, 100L));
    assertEquals(0L, DriverHeap.change(-1L, 100L));
    assertEquals(50L, DriverHeap.change(100L, 150L));
    assertEquals(-50L, DriverHeap.change(150L, 100L));
  }

  @Test
  void pauseReasonNamesHeapHostOrBoth() {
    assertEquals("heap", DriverHeap.pauseReason(true, false));
    assertEquals("host", DriverHeap.pauseReason(false, true));
    assertEquals("heap and host", DriverHeap.pauseReason(true, true));
    assertNull(DriverHeap.pauseReason(false, false));
  }

  @Test
  void seedIsTheSmallerOfBatchSizeAndHeapFraction() {
    long heapMax = 4_096L * 1024L * 1024L;
    long batch = 160L * 1024L * 1024L;

    assertEquals(64L * 1024L * 1024L, DriverHeap.seed(batch, heapMax));
    assertEquals(16L * 1024L * 1024L, DriverHeap.seed(16L * 1024L * 1024L, heapMax));
    assertEquals(64L * 1024L * 1024L, DriverHeap.seed(0L, heapMax));
    assertEquals(batch, DriverHeap.seed(batch, 0L));
  }

  @Test
  void budgetHalvesAtFiftyPercentAndDoesNotGrow() {
    long current = 32L * 1024L * 1024L;

    assertEquals(current, DriverHeap.nextBudget(current, 49L));
    assertEquals(current / 2L, DriverHeap.nextBudget(current, 50L));
    assertEquals(DriverHeap.BUDGET_FLOOR_BYTES, DriverHeap.nextBudget(6L * 1024L * 1024L, 80L));
    assertEquals(
        DriverHeap.BUDGET_FLOOR_BYTES, DriverHeap.nextBudget(DriverHeap.BUDGET_FLOOR_BYTES, 90L));
    assertEquals(current, DriverHeap.nextBudget(current, -1L));
  }

  @Test
  void heapStopNamesUsedBytesNextToTheSixtyPercentLimit() {
    String line =
        DriverHeap.heapStop(
            2_500_000_000L, 61L, "heapUsed=2500000000 heapFraction=61 rss=1 memAvailable=2");

    assertTrue(line.contains("heapFraction=61"));
    assertTrue(line.contains("at or above the 60% limit"));
    assertFalse(line.contains("% above"));
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
  void hostWarningStaysDueAfterAHeapWarning() {
    long heapWarnedAt = 0L;

    assertFalse(DriverHeap.warnDue(heapWarnedAt, 1_000L));
    assertTrue(DriverHeap.warnDue(Long.MIN_VALUE, 1_000L));
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
