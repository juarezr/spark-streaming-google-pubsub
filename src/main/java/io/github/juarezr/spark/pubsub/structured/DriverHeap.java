package io.github.juarezr.spark.pubsub.structured;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Formats the soak reading of driver heap against bytes the connector is holding. */
final class DriverHeap {
  static final int CEILING_PERCENT = 60;
  static final long HOUR_MILLIS = 3_600_000L;

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

  static long residentBytes() {
    try {
      return residentBytes(Files.readString(Path.of("/proc/self/status")));
    } catch (IOException e) {
      return -1L;
    }
  }

  static long residentBytes(String status) {
    if (status == null) {
      return -1L;
    }
    for (String statusLine : status.split("\n")) {
      if (!statusLine.startsWith("VmRSS:")) {
        continue;
      }
      long kib = 0L;
      boolean digit = false;
      for (int i = 0; i < statusLine.length(); i++) {
        char c = statusLine.charAt(i);
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
      String at, long heapMax, long heapUsed, long rssBytes, long gatherBytes, long queuedBytes) {
    return String.format(
        "HEAP: at=%s heapMax=%d heapUsed=%d rss=%d gatherBytes=%d queuedBytes=%d",
        at, heapMax, heapUsed, rssBytes, gatherBytes, queuedBytes);
  }
}
