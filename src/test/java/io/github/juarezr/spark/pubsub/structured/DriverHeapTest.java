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
    assertEquals(
        "HEAP: startup heapMax=4096 memTotal=16384 memoryBaseline=2048 memoryLimit=4096",
        DriverHeap.startup(4_096L, 16_384L, 2_048L, 4_096L));
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
  void budgetStepsDownByHalfTheLastMoveAtFiftyPercent() {
    long current = 32L * 1024L * 1024L;
    long lastMove = 8L * 1024L * 1024L;
    long aboveFloor = DriverHeap.BUDGET_FLOOR_BYTES + 1024L * 1024L;

    assertEquals(current, DriverHeap.nextBudget(current, 49L, lastMove));
    assertEquals(current - lastMove / 2L, DriverHeap.nextBudget(current, 50L, lastMove));
    assertEquals(DriverHeap.BUDGET_FLOOR_BYTES, DriverHeap.nextBudget(aboveFloor, 80L, lastMove));
    assertEquals(
        DriverHeap.BUDGET_FLOOR_BYTES,
        DriverHeap.nextBudget(DriverHeap.BUDGET_FLOOR_BYTES, 90L, lastMove));
    assertEquals(current, DriverHeap.nextBudget(current, -1L, lastMove));
  }

  @Test
  void limitFollowsTheDriverHeapAndStopsAtBatchSize() {
    long heapMax = 4_096L * 1024L * 1024L;
    long batch = 160L * 1024L * 1024L;

    assertEquals(64L * 1024L * 1024L, DriverHeap.seed(batch, heapMax));
    assertEquals(128L * 1024L * 1024L, DriverHeap.memoryLimit(batch, heapMax));
    assertEquals(128L * 1024L * 1024L, DriverHeap.seed(batch, 8_192L * 1024L * 1024L));
    assertEquals(batch, DriverHeap.memoryLimit(batch, 8_192L * 1024L * 1024L));
    assertEquals(16L * 1024L * 1024L, DriverHeap.seed(16L * 1024L * 1024L, heapMax));
    assertEquals(16L * 1024L * 1024L, DriverHeap.memoryLimit(16L * 1024L * 1024L, heapMax));
  }

  @Test
  void climbPassesTheBaselineAndStopsAtTheLimit() {
    long baseline = 64L * 1024L * 1024L;
    long limit = 128L * 1024L * 1024L;
    long free = DriverHeap.GROW_MIN_AVAILABLE;
    long lastMove = baseline / 8L;

    DriverHeap.BudgetStep grown =
        DriverHeap.adjust(
            baseline, limit, DriverHeap.GROW_STREAK, 0, 20L, free, baseline, lastMove, false);
    assertEquals(baseline + lastMove, grown.budget);
    assertEquals(lastMove, grown.lastMove);
    assertFalse(grown.lastShrank);
    assertTrue(grown.budget > baseline);
    assertTrue(grown.budget < limit);

    DriverHeap.BudgetStep stayed =
        DriverHeap.adjust(
            limit, limit, DriverHeap.GROW_STREAK, 0, 20L, free, limit, lastMove, false);
    assertEquals(limit, stayed.budget);
    assertEquals(limit, DriverHeap.growStep(limit - 1L, limit, lastMove, 20L, false));
  }

  @Test
  void climbStopsAtTheSeed() {
    long seed = 64L * 1024L * 1024L;
    long free = DriverHeap.GROW_MIN_AVAILABLE;
    long lastMove = seed / 8L;

    assertEquals(seed, DriverHeap.growStep(seed, seed, lastMove, 20L, false));
    assertEquals(seed, DriverHeap.growStep(seed - 1L, seed, lastMove, 20L, false));
    DriverHeap.BudgetStep stayed =
        DriverHeap.adjust(seed, seed, DriverHeap.GROW_STREAK, 0, 20L, free, seed, lastMove, false);
    assertEquals(seed, stayed.budget);
  }

  @Test
  void growStepRepeatsTheLastMoveAndHalvesNearTheBand() {
    long current = 32L * 1024L * 1024L;
    long ceiling = 128L * 1024L * 1024L;
    long lastMove = 8L * 1024L * 1024L;
    long near = ceiling - lastMove;

    assertEquals(current + lastMove, DriverHeap.growStep(current, ceiling, lastMove, 20L, false));
    assertEquals(
        current + lastMove / 2L, DriverHeap.growStep(current, ceiling, lastMove, 35L, false));
    assertEquals(
        current + lastMove / 2L, DriverHeap.growStep(current, ceiling, lastMove, 20L, true));
    assertEquals(near + lastMove / 2L, DriverHeap.growStep(near, ceiling, lastMove, 20L, false));
    assertEquals(ceiling, DriverHeap.growStep(ceiling - 1L, ceiling, lastMove, 20L, false));
    assertEquals(ceiling, DriverHeap.growStep(ceiling, ceiling, lastMove, 20L, false));
  }

  @Test
  void budgetGrowsOnlyAfterEightCalmFullGathers() {
    long current = 32L * 1024L * 1024L;
    long seed = 64L * 1024L * 1024L;
    long free = DriverHeap.GROW_MIN_AVAILABLE;
    long lastMove = current / 8L;
    int streak = 0;

    for (int i = 0; i < DriverHeap.GROW_STREAK - 1; i++) {
      DriverHeap.BudgetStep step =
          DriverHeap.adjust(current, seed, streak, 0, 20L, free, current, lastMove, false);
      assertEquals(current, step.budget);
      streak = step.calmStreak;
    }
    assertEquals(7, streak);

    DriverHeap.BudgetStep light =
        DriverHeap.adjust(current, seed, streak, 0, 20L, free, current / 2L, lastMove, false);
    assertEquals(current, light.budget);
    assertEquals(8, light.calmStreak);

    DriverHeap.BudgetStep grown =
        DriverHeap.adjust(current, seed, light.calmStreak, 0, 20L, free, current, lastMove, false);
    assertEquals(current + lastMove, grown.budget);
    assertEquals(0, grown.calmStreak);
  }

  @Test
  void twoHotGathersStepDownByHalfTheLastMoveAndOneDoesNot() {
    long current = 64L * 1024L * 1024L;
    long seed = 64L * 1024L * 1024L;
    long free = DriverHeap.GROW_MIN_AVAILABLE;
    long lastMove = 8L * 1024L * 1024L;

    DriverHeap.BudgetStep first =
        DriverHeap.adjust(current, seed, 7, 0, 50L, free, current, lastMove, false);
    assertEquals(current, first.budget);
    assertEquals(0, first.calmStreak);
    assertEquals(1, first.hotStreak);

    DriverHeap.BudgetStep empty =
        DriverHeap.adjust(current, seed, 7, first.hotStreak, 51L, free, 0L, lastMove, false);
    assertEquals(current, empty.budget);
    assertEquals(0, empty.calmStreak);
    assertEquals(1, empty.hotStreak);
    assertEquals(lastMove, empty.lastMove);

    DriverHeap.BudgetStep stepped =
        DriverHeap.adjust(current, seed, 0, empty.hotStreak, 50L, free, current, lastMove, false);
    assertEquals(current - lastMove / 2L, stepped.budget);
    assertEquals(lastMove / 2L, stepped.lastMove);
    assertTrue(stepped.lastShrank);
    assertEquals(0, stepped.calmStreak);
    assertEquals(0, stepped.hotStreak);

    DriverHeap.BudgetStep cooled =
        DriverHeap.adjust(current, seed, 0, 1, 20L, free, current, lastMove, false);
    assertEquals(current, cooled.budget);
    assertEquals(0, cooled.hotStreak);

    DriverHeap.BudgetStep warm =
        DriverHeap.adjust(
            current, seed, 7, 0, DriverHeap.GROW_PERCENT, free, current, lastMove, false);
    assertEquals(current, warm.budget);
    assertEquals(0, warm.calmStreak);

    DriverHeap.BudgetStep unread =
        DriverHeap.adjust(current, seed, 7, 0, 20L, -1L, current, lastMove, false);
    assertEquals(current, unread.budget);
    assertEquals(0, unread.calmStreak);

    DriverHeap.BudgetStep againFirst =
        DriverHeap.adjust(
            stepped.budget, seed, 0, 0, 50L, free, stepped.budget, stepped.lastMove, true);
    assertEquals(stepped.budget, againFirst.budget);
    DriverHeap.BudgetStep again =
        DriverHeap.adjust(
            stepped.budget,
            seed,
            0,
            againFirst.hotStreak,
            50L,
            free,
            stepped.budget,
            stepped.lastMove,
            true);
    assertEquals(stepped.budget - stepped.lastMove / 2L, again.budget);
    assertEquals(stepped.lastMove / 2L, again.lastMove);
  }

  @Test
  void calmGrowsRepeatAndRecoveryAddsHalfTheCut() {
    long current = 64L * 1024L * 1024L;
    long limit = 128L * 1024L * 1024L;
    long free = DriverHeap.GROW_MIN_AVAILABLE;
    long lastMove = DriverHeap.openingMove(current);

    assertEquals(8L * 1024L * 1024L, lastMove);
    assertEquals(1024L * 1024L * 1024L, free);
    assertEquals(DriverHeap.STEP_FLOOR_BYTES, DriverHeap.halfMove(DriverHeap.STEP_FLOOR_BYTES));

    DriverHeap.BudgetStep first =
        DriverHeap.adjust(
            current, limit, DriverHeap.GROW_STREAK, 0, 20L, free, current, lastMove, false);
    assertEquals(current + lastMove, first.budget);

    DriverHeap.BudgetStep second =
        DriverHeap.adjust(
            first.budget,
            limit,
            DriverHeap.GROW_STREAK,
            0,
            32L,
            free,
            first.budget,
            first.lastMove,
            false);
    assertEquals(first.budget + lastMove, second.budget);
    assertEquals(lastMove, second.lastMove);

    DriverHeap.BudgetStep nearBand =
        DriverHeap.adjust(
            second.budget,
            limit,
            DriverHeap.GROW_STREAK,
            0,
            35L,
            free,
            second.budget,
            second.lastMove,
            false);
    assertEquals(second.budget + lastMove / 2L, nearBand.budget);
    assertEquals(lastMove / 2L, nearBand.lastMove);

    DriverHeap.BudgetStep tight =
        DriverHeap.adjust(
            current, limit, DriverHeap.GROW_STREAK, 0, 20L, free - 1L, current, lastMove, false);
    assertEquals(current, tight.budget);

    DriverHeap.BudgetStep shrank =
        DriverHeap.adjust(second.budget, limit, 0, 1, 51L, free, second.budget, lastMove, false);
    assertEquals(second.budget - lastMove / 2L, shrank.budget);
    assertTrue(shrank.lastShrank);

    DriverHeap.BudgetStep recovered =
        DriverHeap.adjust(
            shrank.budget,
            limit,
            DriverHeap.GROW_STREAK,
            0,
            20L,
            free,
            shrank.budget,
            shrank.lastMove,
            true);
    assertEquals(shrank.budget + shrank.lastMove / 2L, recovered.budget);
    assertFalse(recovered.lastShrank);
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
