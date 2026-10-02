package io.github.juarezr.spark.pubsub.config;

import java.util.Locale;

/** Controls how messages dequeued from the streaming-pull buffer are grouped into a micro-batch. */
public enum GatherMode {
  /** Collect until {@code receiveTime}, {@code batchSize}, or {@code batchCount}. */
  BATCH,
  /**
   * Return a micro-batch as soon as the queue has messages (one poll per gather when Spark sets no
   * {@code minRows}).
   */
  IMMEDIATE;

  static GatherMode fromString(String value) {
    if (value == null || value.isBlank()) {
      return BATCH;
    }
    String normalized = value.trim().toLowerCase(Locale.ROOT).replace('-', '_');
    switch (normalized) {
      case "batch":
        return BATCH;
      case "immediate":
        return IMMEDIATE;
      default:
        break;
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid gatherMode: '" + value + "'. Expected batch or immediate", e);
    }
  }
}
