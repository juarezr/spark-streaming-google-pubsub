package io.github.juarezr.spark.pubsub.structured;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Formats the soak reading of driver heap against bytes the connector is holding. */
final class DriverHeap {
  static final int CEILING_PERCENT = 60;
  static final long HOUR_MILLIS = 3_600_000L;
  static final long HOST_RESERVE_BYTES = 512L * 1024L * 1024L;
  static final long HOST_CACHE_MILLIS = 1_000L;
  static final int SHRINK_PERCENT = 50;
  static final int GROW_PERCENT = 40;
  static final int GROW_STREAK = 8;
  static final int SHRINK_STREAK = 2;
  static final long BUDGET_FLOOR_BYTES = 4L * 1024L * 1024L;
  static final long STEP_FLOOR_BYTES = 1024L * 1024L;
  static final long GROW_MIN_AVAILABLE = 1024L * 1024L * 1024L;
  static final long RESTORE_MIN_AVAILABLE = 3L * 512L * 1024L * 1024L;
  static final int APPROACH_PERCENT = 35;
  static final int APPROACH_STREAK = 2;
  static final int RESTORE_PERCENT = 30;
  static final int BUDGET_DIVISOR = 64;
  static final int LIMIT_DIVISOR = 32;

  private DriverHeap() {}

  static long used(long totalMemory, long freeMemory) {
    return Math.max(0L, totalMemory - freeMemory);
  }

  /** True when used heap is at or above {@link #CEILING_PERCENT} of {@code heapMax}. */
  static boolean aboveCeiling(long heapUsed, long heapMax) {
    if (heapMax <= 0L || heapUsed <= 0L) {
      return false;
    }
    return heapUsed * 5L >= heapMax * 3L;
  }

  static boolean warnDue(long previousMillis, long nowMillis) {
    return previousMillis == Long.MIN_VALUE || nowMillis - previousMillis >= HOUR_MILLIS;
  }

  /** {@code -1} when {@code whole} is missing or {@code part} was not read. */
  static long percent(long part, long whole) {
    if (whole <= 0L || part < 0L) {
      return -1L;
    }
    return part * 100L / whole;
  }

  /** Signed byte delta. The first sample, and a missing reading, are 0. */
  static long change(long previous, long current) {
    if (previous == Long.MIN_VALUE || previous < 0L || current < 0L) {
      return 0L;
    }
    return current - previous;
  }

  /**
   * Startup cap. {@code batchSize} stays the user's maximum. When it is unset, the cap is a
   * fraction of the driver heap so the first gather cannot commit the whole heap. Outstanding bytes
   * are twice this, set once. The climb may pass this and stops at {@link #memoryLimit}.
   */
  static long seed(long batchSize, long heapMax) {
    return cap(batchSize, heapMax, BUDGET_DIVISOR);
  }

  /**
   * Top of the walk. {@code batchSize} still caps it. The caller also keeps it within outstanding.
   */
  static long memoryLimit(long batchSize, long heapMax) {
    return cap(batchSize, heapMax, LIMIT_DIVISOR);
  }

  private static long cap(long batchSize, long heapMax, int divisor) {
    long fromHeap = heapMax > 0L ? heapMax / divisor : 0L;
    if (batchSize > 0L && fromHeap > 0L) {
      return Math.min(batchSize, fromHeap);
    }
    if (fromHeap > 0L) {
      return fromHeap;
    }
    return batchSize > 0L ? batchSize : 0L;
  }

  /** First step. One eighth of the seed, and not under one mebibyte. */
  static long openingMove(long seed) {
    if (seed <= 0L) {
      return STEP_FLOOR_BYTES;
    }
    return Math.max(seed / 8L, STEP_FLOOR_BYTES);
  }

  /** Half of {@code lastMove}, and not under one mebibyte. */
  static long halfMove(long lastMove) {
    long move = lastMove > 0L ? lastMove : STEP_FLOOR_BYTES;
    return Math.max(move / 2L, STEP_FLOOR_BYTES);
  }

  /**
   * Two gathers in this streak were at {@link #APPROACH_PERCENT}, or the limit is within two steps.
   * One sample in that band does not halve. Available is not a second line.
   */
  static boolean approaching(long current, long ceiling, long lastMove, int approachStreak) {
    if (approachStreak >= APPROACH_STREAK) {
      return true;
    }
    long move = lastMove > 0L ? lastMove : STEP_FLOOR_BYTES;
    return ceiling > current && ceiling - current <= move * 2L;
  }

  /**
   * Repeats {@code lastMove} while the minute is calm. Halves it after two high gathers, near the
   * limit, or on the grow that follows a shrink. Doubles it, up to {@code opening}, when the gather
   * is under {@link #RESTORE_PERCENT} and at least {@link #RESTORE_MIN_AVAILABLE} is free.
   */
  static long growDelta(
      long current,
      long ceiling,
      long lastMove,
      long heapFraction,
      boolean lastShrank,
      int approachStreak,
      long memAvailable,
      long opening) {
    long move = lastMove > 0L ? lastMove : STEP_FLOOR_BYTES;
    if (lastShrank || approaching(current, ceiling, move, approachStreak)) {
      return halfMove(move);
    }
    if (restoring(heapFraction, memAvailable, move, opening)) {
      return doubledMove(move, opening);
    }
    return move;
  }

  /** Half the last move, and not under the floor. Fraction under 50 does not move. */
  static long nextBudget(long current, long heapFraction, long lastMove) {
    if (current <= BUDGET_FLOOR_BYTES || heapFraction < SHRINK_PERCENT) {
      return current;
    }
    return Math.max(current - halfMove(lastMove), BUDGET_FLOOR_BYTES);
  }

  /** Adds the remembered step, and not past {@code ceiling}. A lone high gather does not halve. */
  static long growStep(
      long current, long ceiling, long lastMove, long heapFraction, boolean lastShrank) {
    return growStep(current, ceiling, lastMove, heapFraction, lastShrank, 0, 0L, lastMove);
  }

  static long growStep(
      long current,
      long ceiling,
      long lastMove,
      long heapFraction,
      boolean lastShrank,
      int approachStreak,
      long memAvailable,
      long opening) {
    if (current <= 0L || current >= ceiling) {
      return current;
    }
    long add =
        growDelta(
            current,
            ceiling,
            lastMove,
            heapFraction,
            lastShrank,
            approachStreak,
            memAvailable,
            opening);
    return Math.min(current + add, ceiling);
  }

  /** True when a calm hour may double the step, and the step is still under the opening move. */
  static boolean restoring(long heapFraction, long memAvailable, long lastMove, long opening) {
    if (heapFraction < 0L
        || heapFraction >= RESTORE_PERCENT
        || memAvailable < RESTORE_MIN_AVAILABLE) {
      return false;
    }
    long move = lastMove > 0L ? lastMove : STEP_FLOOR_BYTES;
    long cap = opening > 0L ? opening : move;
    return move < cap;
  }

  /** Twice {@code lastMove}, and not past the opening step or under one mebibyte. */
  static long doubledMove(long lastMove, long opening) {
    long move = lastMove > 0L ? lastMove : STEP_FLOOR_BYTES;
    long cap = opening > 0L ? opening : move;
    return Math.min(Math.max(move * 2L, STEP_FLOOR_BYTES), cap);
  }

  static boolean filled(long gatherBytes, long budget) {
    return budget > 0L && gatherBytes * 10L >= budget * 9L;
  }

  static BudgetStep adjust(
      long current,
      long ceiling,
      int calmStreak,
      int hotStreak,
      long heapFraction,
      long memAvailable,
      long gatherBytes,
      long lastMove,
      boolean lastShrank) {
    return adjust(
        current,
        ceiling,
        calmStreak,
        hotStreak,
        heapFraction,
        memAvailable,
        gatherBytes,
        lastMove,
        lastShrank,
        0,
        0L);
  }

  /**
   * Step down by half the last move after {@link #SHRINK_STREAK} gathers in a row at {@link
   * #SHRINK_PERCENT}. Otherwise count a cool gather, and step up by the remembered move only after
   * {@link #GROW_STREAK} of them when the latest gather filled the cap.
   */
  static BudgetStep adjust(
      long current,
      long ceiling,
      int calmStreak,
      int hotStreak,
      long heapFraction,
      long memAvailable,
      long gatherBytes,
      long lastMove,
      boolean lastShrank,
      int approachStreak,
      long openingMove) {
    long move = lastMove > 0L ? lastMove : STEP_FLOOR_BYTES;
    // An empty gather is the pause, not a batch that was too big. It does not count as hot, and it
    // does not forget a hot gather that came before it.
    if (gatherBytes <= 0L) {
      int idleStreak =
          heapFraction >= 0L && heapFraction < GROW_PERCENT && memAvailable >= GROW_MIN_AVAILABLE
              ? calmStreak + 1
              : 0;
      return new BudgetStep(current, idleStreak, hotStreak, move, lastShrank, approachStreak);
    }
    if (heapFraction >= SHRINK_PERCENT) {
      int hot = hotStreak + 1;
      if (hot >= SHRINK_STREAK) {
        long next = nextBudget(current, heapFraction, move);
        long applied = current - next;
        if (applied <= 0L) {
          return new BudgetStep(current, 0, 0, move, lastShrank, 0);
        }
        return new BudgetStep(next, 0, 0, Math.max(applied, STEP_FLOOR_BYTES), true, 0);
      }
      return new BudgetStep(current, 0, hot, move, lastShrank, 0);
    }
    boolean calm =
        heapFraction >= 0L && heapFraction < GROW_PERCENT && memAvailable >= GROW_MIN_AVAILABLE;
    int streak = calm ? calmStreak + 1 : 0;
    int approached = calm && heapFraction >= APPROACH_PERCENT ? approachStreak + 1 : 0;
    if (calm && heapFraction < APPROACH_PERCENT) {
      approached = approachStreak;
    }
    if (streak >= GROW_STREAK && filled(gatherBytes, current) && current < ceiling) {
      long grown =
          growStep(
              current,
              ceiling,
              move,
              heapFraction,
              lastShrank,
              approached,
              memAvailable,
              openingMove);
      if (grown > current) {
        long applied = grown - current;
        return new BudgetStep(grown, 0, 0, Math.max(applied, STEP_FLOOR_BYTES), false, 0);
      }
    }
    return new BudgetStep(current, streak, 0, move, lastShrank, approached);
  }

  static final class BudgetStep {
    final long budget;
    final int calmStreak;
    final int hotStreak;
    final long lastMove;
    final boolean lastShrank;
    final int approachStreak;

    BudgetStep(
        long budget,
        int calmStreak,
        int hotStreak,
        long lastMove,
        boolean lastShrank,
        int approachStreak) {
      this.budget = budget;
      this.calmStreak = calmStreak;
      this.hotStreak = hotStreak;
      this.lastMove = lastMove;
      this.lastShrank = lastShrank;
      this.approachStreak = approachStreak;
    }
  }

  static String pauseReason(boolean heap, boolean host) {
    if (heap && host) {
      return "heap and host";
    }
    if (host) {
      return "host";
    }
    if (heap) {
      return "heap";
    }
    return null;
  }

  /** True when a real reading is under {@link #HOST_RESERVE_BYTES}. {@code -1} does not pause. */
  static boolean belowHostReserve(long memAvailable) {
    return memAvailable >= 0L && memAvailable < HOST_RESERVE_BYTES;
  }

  /**
   * One collect per pause, after the in-flight cycle can have finished. A null cycle budget is
   * {@code cycleBudgetMillis <= 0} and does not collect.
   */
  static boolean collectDue(
      long pauseSinceMillis, long nowMillis, long cycleBudgetMillis, boolean alreadyCollected) {
    if (alreadyCollected || cycleBudgetMillis <= 0L || pauseSinceMillis == Long.MIN_VALUE) {
      return false;
    }
    return nowMillis - pauseSinceMillis >= cycleBudgetMillis;
  }

  static long residentBytes() {
    try {
      return residentBytes(Files.readString(Path.of("/proc/self/status")));
    } catch (IOException e) {
      return -1L;
    }
  }

  static long residentBytes(String status) {
    return fieldBytes(status, "VmRSS:");
  }

  static long availableBytes() {
    return meminfoBytes("MemAvailable:");
  }

  static long totalBytes() {
    return meminfoBytes("MemTotal:");
  }

  static long meminfoBytes(String meminfo, String field) {
    return fieldBytes(meminfo, field);
  }

  private static long meminfoBytes(String field) {
    try {
      return meminfoBytes(Files.readString(Path.of("/proc/meminfo")), field);
    } catch (IOException e) {
      return -1L;
    }
  }

  private static long fieldBytes(String text, String field) {
    if (text == null) {
      return -1L;
    }
    for (String textLine : text.split("\n")) {
      if (!textLine.startsWith(field)) {
        continue;
      }
      long kib = 0L;
      boolean digit = false;
      for (int i = 0; i < textLine.length(); i++) {
        char c = textLine.charAt(i);
        if (c >= '0' && c <= '9') {
          digit = true;
          kib = kib * 10L + (c - '0');
        } else if (digit) {
          break;
        }
      }
      return digit ? kib * 1024L : -1L;
    }
    return -1L;
  }

  static String startup(long heapMax, long memTotal, long memoryBaseline, long memoryLimit) {
    return String.format(
        "HEAP: startup heapMax=%d memTotal=%d memoryBaseline=%d memoryLimit=%d",
        heapMax, memTotal, memoryBaseline, memoryLimit);
  }

  static String budgetChange(long previous, long next) {
    return String.format("HEAP: memoryBudget %d -> %d", previous, next);
  }

  static String readings(long heapUsed, long heapFraction, long rssBytes, long memAvailable) {
    return String.format(
        "heapUsed=%d heapFraction=%d rss=%d memAvailable=%d",
        heapUsed, heapFraction, rssBytes, memAvailable);
  }

  static String heapStop(long heapUsed, long heapFraction, String readings) {
    return String.format(
        "HEAP: stopped taking more messages because driver heap used is %d bytes"
            + " (heapFraction=%d), at or above the %d%% limit; %s",
        heapUsed, heapFraction, CEILING_PERCENT, readings);
  }

  static String line(
      String at,
      long heapMax,
      long heapUsed,
      long heapFraction,
      long heapChange,
      long rssBytes,
      long rssChange,
      long memAvailable,
      long memChange,
      long memTotal,
      long memFraction,
      long gatherBytes,
      int gatherMsgs,
      long queuedBytes,
      int queuedMsgs,
      long memoryBudget) {
    return String.format(
        "HEAP: at=%s heapMax=%d heapUsed=%d heapFraction=%d heapChange=%d rss=%d rssChange=%d"
            + " memAvailable=%d memChange=%d memTotal=%d memFraction=%d memoryBudget=%d"
            + " gatherBytes=%d gatherMsgs=%d queuedBytes=%d queuedMsgs=%d",
        at,
        heapMax,
        heapUsed,
        heapFraction,
        heapChange,
        rssBytes,
        rssChange,
        memAvailable,
        memChange,
        memTotal,
        memFraction,
        memoryBudget,
        gatherBytes,
        gatherMsgs,
        queuedBytes,
        queuedMsgs);
  }
}
