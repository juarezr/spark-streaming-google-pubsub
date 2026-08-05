package io.github.juarezr.spark.pubsub.config;

/**
 * Optional seek applied once when the connector starts. Default is {@link #NONE} — the subscription
 * cursor is left unchanged (no rewind on restart).
 */
public enum SeekMode {
  /** Do not seek; continue from the existing subscription cursor. */
  NONE,
  /** Seek to the beginning of the topic backlog retained for the subscription. */
  BEGINNING,
  /** Seek to a specific timestamp (RFC-3339 / epoch millis via option). */
  TIMESTAMP,
  /** Seek to a Pub/Sub snapshot resource name. */
  SNAPSHOT;

  public static SeekMode fromString(String value) {
    if (value == null || value.isBlank() || "none".equalsIgnoreCase(value.trim())) {
      return NONE;
    }
    String normalized = value.trim().toLowerCase();
    switch (normalized) {
      case "beginning":
      case "start":
        return BEGINNING;
      case "timestamp":
      case "time":
        return TIMESTAMP;
      case "snapshot":
        return SNAPSHOT;
      default:
        throw new IllegalArgumentException(
            "Unknown seek '" + value + "'. Expected: none, beginning, timestamp, snapshot");
    }
  }
}
