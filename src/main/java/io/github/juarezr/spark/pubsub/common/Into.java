package io.github.juarezr.spark.pubsub.common;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Locale;

public final class Into {
  static final String NONE = "none";

  public static String timeIso(Long millis) {
    if (millis == null) {
      return NONE;
    }
    return timeIso(millis.longValue());
  }

  public static String timeIso(long millis) {
    if (millis == Long.MIN_VALUE) {
      return NONE;
    }
    return Instant.ofEpochMilli(millis).toString();
  }

  public static String elapsed(Duration duration) {
    return elapsed(duration, "-");
  }

  public static String elapsed(Duration duration, String alternative) {
    if (duration == null) {
      return alternative;
    }
    final long ms = duration.toMillis();
    return elapsed(ms);
  }

  public static String elapsed(long ms) {
    if (ms % 60000L == 0L) {
      return (ms / 60000L) + "min";
    } else if (ms % 1000L == 0L) {
      return (ms / 1000L) + "s";
    }
    return ms + "ms";
  }

  public static String abrevBytes(long size) {
    if (size <= 0) {
      return "0";
    }
    if (size < 1024) {
      return size + "B";
    }
    if (size < 1024 * 1024) {
      return (size / 1024) + "KB";
    }
    if (size < 1024 * 1024 * 1024) {
      return (size / 1024 / 1024) + "MB";
    }
    if (size < 1024 * 1024 * 1024 * 1024) {
      return (size / 1024 / 1024 / 1024) + "GB";
    }
    if (size < 1024 * 1024 * 1024 * 1024) {
      return (size / 1024 / 1024 / 1024 / 1024) + "TB";
    }
    return size + "B";
  }

  public static String abrevCount(long size) {
    if ((size >= 1024) && (size < 1024 * 1024)) {
      return (size / 1024) + "k";
    }
    if (size < 1024 * 1024 * 1024) {
      return (size / 1024 / 1024) + "m";
    }
    if (size < 1024 * 1024 * 1024 * 1024) {
      return (size / 1024 / 1024 / 1024) + "g";
    }
    if (size < 1024 * 1024 * 1024 * 1024) {
      return (size / 1024 / 1024 / 1024 / 1024) + "t";
    }
    return size + "";
  }

  public static long parseSize(String option, String raw) {
    if (raw == null || raw.isBlank()) {
      return 0L;
    }
    String value = raw.trim().toLowerCase(Locale.ROOT);
    long multiplier = 1L;
    char suffix = value.charAt(value.length() - 1);
    if (suffix == 'k' || suffix == 'm' || suffix == 'g') {
      value = value.substring(0, value.length() - 1);
      multiplier = suffix == 'k' ? 1024L : suffix == 'm' ? 1024L * 1024L : 1024L * 1024L * 1024L;
    }
    try {
      return Math.multiplyExact(Long.parseLong(value), multiplier);
    } catch (ArithmeticException | NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid " + option + " '" + raw + "'. Use bytes or a k, m, or g suffix.", e);
    }
  }

  public static Duration parseDuration(String option, String raw) {
    if (raw == null || raw.isBlank()) {
      throw new IllegalArgumentException(option + " must not be blank");
    }
    String value = raw.trim().toLowerCase(Locale.ROOT);
    try {
      if (value.endsWith("ms")) {
        return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
      }
      if (value.endsWith("s")) {
        return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
      }
      if (value.endsWith("m")) {
        return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
      }
      return Duration.ofSeconds(Long.parseLong(value));
    } catch (ArithmeticException | NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid " + option + ": '" + raw + "'. Use a number with ms, s, or m.", e);
    }
  }

  public static Instant parseInstant(String option, String raw) {
    String value = raw == null ? "" : raw.trim();
    try {
      if (!value.isEmpty() && value.chars().allMatch(Character::isDigit)) {
        return Instant.ofEpochMilli(Long.parseLong(value));
      }
      return OffsetDateTime.parse(value).toInstant();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid " + option + " '" + raw + "'. Use epoch milliseconds or RFC-3339 with Z/offset.",
          e);
    }
  }
}
