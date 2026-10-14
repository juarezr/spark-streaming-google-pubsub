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

  static String line(
      String at,
      long heapMax,
      long heapUsed,
      long rssBytes,
      long memAvailable,
      long memTotal,
      long gatherBytes,
      long queuedBytes) {
    return String.format(
        "HEAP: at=%s heapMax=%d heapUsed=%d rss=%d memAvailable=%d memTotal=%d"
            + " gatherBytes=%d queuedBytes=%d",
        at, heapMax, heapUsed, rssBytes, memAvailable, memTotal, gatherBytes, queuedBytes);
  }
}
