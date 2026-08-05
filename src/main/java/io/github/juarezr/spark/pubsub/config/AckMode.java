package io.github.juarezr.spark.pubsub.config;

/** How messages are acknowledged relative to Spark processing. */
public enum AckMode {
  /**
   * Acknowledge only after Spark successfully commits the micro-batch (or after the receiver stores
   * and the batch completes for DStreams). At-least-once; duplicates possible on failure.
   */
  AFTER_COMMIT,
  /**
   * Acknowledge soon after a successful pull/store (Legacy-like). Higher risk of loss if the process
   * crashes before Spark finishes processing.
   */
  EARLY;

  public static AckMode fromString(String value) {
    if (value == null || value.isBlank()) {
      return AFTER_COMMIT;
    }
    String normalized = value.trim().toLowerCase().replace('-', '_');
    switch (normalized) {
      case "aftercommit":
      case "after_commit":
        return AFTER_COMMIT;
      case "early":
        return EARLY;
      default:
        throw new IllegalArgumentException(
            "Unknown ackMode '" + value + "'. Expected: afterCommit, early");
    }
  }
}
