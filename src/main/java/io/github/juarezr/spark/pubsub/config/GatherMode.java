package io.github.juarezr.spark.pubsub.config;

import java.util.Locale;

/** Controls how messages dequeued from the streaming-pull buffer are grouped into a micro-batch. */
public enum GatherMode {
  BATCH,
  PULL;

  static GatherMode fromString(String value) {
    if (value == null || value.isBlank()) {
      return BATCH;
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid gatherMode '" + value + "'. Expected batch or pull.", e);
    }
  }
}
