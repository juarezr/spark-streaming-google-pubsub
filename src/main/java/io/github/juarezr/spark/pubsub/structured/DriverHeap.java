package io.github.juarezr.spark.pubsub.structured;

/** Formats the soak reading of driver heap against bytes the connector is holding. */
final class DriverHeap {
  private DriverHeap() {}

  static long used(long totalMemory, long freeMemory) {
    return Math.max(0L, totalMemory - freeMemory);
  }

  static String line(String at, long heapMax, long heapUsed, long gatherBytes, long queuedBytes) {
    return String.format(
        "HEAP: at=%s heapMax=%d heapUsed=%d gatherBytes=%d queuedBytes=%d",
        at, heapMax, heapUsed, gatherBytes, queuedBytes);
  }
}
