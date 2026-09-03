package io.github.juarezr.spark.pubsub.config;

import java.util.Locale;

/** Opt-in envelope columns that are not already on the table schema. */
public enum MetadataMode {
  NONE,
  BASIC,
  SLIM,
  FULL;

  static MetadataMode fromString(String value) {
    if (value == null || value.isBlank()) {
      return NONE;
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid metadataMode '" + value + "'. Expected none, basic, slim, or full.", e);
    }
  }
}
