package io.github.juarezr.spark.pubsub.config;

import java.util.Locale;

/** Controls how Pub/Sub Pull responses are grouped into a Spark micro-batch. */
public enum GatherMode {
  BATCH,
  PULL;

  public static GatherMode fromString(String value) {
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
