package io.github.juarezr.spark.pubsub.config;

/** How messages are acknowledged relative to Spark processing. */
public enum AckMode {
  /**
   * Acknowledge when Spark calls {@code commit()}, after the next gather. At-least-once; duplicates
   * possible on failure.
   */
  AFTER_COMMIT,
  /**
   * Acknowledge a batch that is already in the commit log, at the start of the next gather, and
   * drop that payload copy. A crash during the write is unchanged.
   */
  AFTER_CHECKPOINT,
  /**
   * Acknowledge soon after messages are received from the queue. Higher risk of loss if the process
   * crashes before Spark finishes processing.
   */
  EARLY;

  static AckMode fromString(String value) {
    if (value == null || value.isBlank()) {
      return AFTER_COMMIT;
    }
    String normalized = value.trim().toLowerCase().replace('-', '_');
    switch (normalized) {
      case "aftercommit":
      case "after_commit":
        return AFTER_COMMIT;
      case "aftercheckpoint":
      case "after_checkpoint":
        return AFTER_CHECKPOINT;
      case "early":
        return EARLY;
      default:
        throw new IllegalArgumentException(
            "Unknown ackMode '" + value + "'. Expected: afterCommit, afterCheckpoint, early");
    }
  }
}
