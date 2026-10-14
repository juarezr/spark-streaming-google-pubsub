package io.github.juarezr.spark.pubsub.structured;

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

  static String line(String at, long heapMax, long heapUsed, long gatherBytes, long queuedBytes) {
    return String.format(
        "HEAP: at=%s heapMax=%d heapUsed=%d gatherBytes=%d queuedBytes=%d",
        at, heapMax, heapUsed, gatherBytes, queuedBytes);
  }
}
