package io.github.juarezr.spark.pubsub.config;

import java.util.Locale;

/** What {@code SELECT *} / {@code load()} returns. */
public enum SchemaMode {
  RAW,
  BASIC,
  SLIM,
  DYNAMIC,
  MIXED;

  static SchemaMode fromString(String value) {
    if (value == null || value.isBlank()) {
      return BASIC;
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid schemaMode '" + value + "'. Expected raw, basic, slim, dynamic, or mixed.", e);
    }
  }

  public boolean decodesPayload() {
    return this == DYNAMIC || this == MIXED;
  }
}
