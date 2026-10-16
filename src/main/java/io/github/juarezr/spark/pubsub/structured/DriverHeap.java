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
  static final long BUDGET_FLOOR_BYTES = 4L * 1024L * 1024L;
  static final long GROW_MIN_AVAILABLE = 2L * 1024L * 1024L * 1024L;
  static final int BUDGET_DIVISOR = 64;
  static final int CEILING_DIVISOR = 32;

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
   * fraction of the driver heap so the first gather cannot commit the whole heap.
   */
  static long seed(long batchSize, long heapMax) {
    return cap(batchSize, heapMax, BUDGET_DIVISOR);
  }

  /** Highest gather cap for this process. Outstanding bytes are twice this, set once. */
  static long ceiling(long batchSize, long heapMax) {
    return cap(batchSize, heapMax, CEILING_DIVISOR);
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

  /** Next gather cap. Shrinks one step at {@link #SHRINK_PERCENT} and never grows. */
  static long nextBudget(long current, long heapFraction) {
    if (current <= BUDGET_FLOOR_BYTES || heapFraction < SHRINK_PERCENT) {
      return current;
    }
    return Math.max(current / 2L, BUDGET_FLOOR_BYTES);
  }

  /** One eighth more, and not past {@code ceiling}. */
  static long growStep(long current, long ceiling) {
    if (current <= 0L || current >= ceiling) {
      return current;
    }
    long stepped = current + Math.max(current / 8L, 1L);
    return Math.min(stepped, ceiling);
  }

  static boolean filled(long gatherBytes, long budget) {
    return budget > 0L && gatherBytes * 10L >= budget * 9L;
  }

  /**
   * Halve immediately at {@link #SHRINK_PERCENT}. Otherwise count a cool gather, and step up only
   * after {@link #GROW_STREAK} of them when the latest gather filled the cap.
   */
  static BudgetStep adjust(
      long current,
      long ceiling,
      int calmStreak,
      long heapFraction,
      long memAvailable,
      long gatherBytes) {
    long halved = nextBudget(current, heapFraction);
    if (halved < current) {
      return new BudgetStep(halved, 0);
    }
    int streak =
        heapFraction >= 0L && heapFraction < GROW_PERCENT && memAvailable >= GROW_MIN_AVAILABLE
            ? calmStreak + 1
            : 0;
    if (streak >= GROW_STREAK && filled(gatherBytes, current) && current < ceiling) {
      long grown = growStep(current, ceiling);
      if (grown > current) {
        return new BudgetStep(grown, 0);
      }
    }
    return new BudgetStep(current, streak);
  }

  static final class BudgetStep {
    final long budget;
    final int calmStreak;

    BudgetStep(long budget, int calmStreak) {
      this.budget = budget;
      this.calmStreak = calmStreak;
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

  static String startup(long heapMax, long memTotal, long memoryCeiling) {
    return String.format(
        "HEAP: startup heapMax=%d memTotal=%d memoryCeiling=%d", heapMax, memTotal, memoryCeiling);
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
