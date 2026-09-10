package io.github.juarezr.spark.pubsub.structured;

import java.time.Instant;
import java.util.List;

/** Oldest and newest Pub/Sub publish times from a gather, for logs and age. */
final class PublishTimeWindow {
  static final String NONE = "none";

  private final long oldestMillis;
  private final long newestMillis;
  private final int messageCount;

  private PublishTimeWindow(long oldestMillis, long newestMillis, int messageCount) {
    this.oldestMillis = oldestMillis;
    this.newestMillis = newestMillis;
    this.messageCount = messageCount;
  }

  static PublishTimeWindow of(List<PulledMessage> messages) {
    if (messages == null || messages.isEmpty()) {
      return null;
    }
    long oldest = Long.MAX_VALUE;
    long newest = Long.MIN_VALUE;
    for (PulledMessage message : messages) {
      long publish = message.publishTimeMillis();
      oldest = Math.min(oldest, publish);
      newest = Math.max(newest, publish);
    }
    return new PublishTimeWindow(oldest, newest, messages.size());
  }

  long oldestMillis() {
    return oldestMillis;
  }

  long newestMillis() {
    return newestMillis;
  }

  int messageCount() {
    return messageCount;
  }

  long newestAgeMs(long nowMillis) {
    return Math.max(0L, nowMillis - newestMillis);
  }

  String oldestIso() {
    return formatIso(oldestMillis);
  }

  String newestIso() {
    return formatIso(newestMillis);
  }

  static String formatIso(Long millis) {
    if (millis == null) {
      return NONE;
    }
    return formatIso(millis.longValue());
  }

  static String formatIso(long millis) {
    if (millis == Long.MIN_VALUE) {
      return NONE;
    }
    return Instant.ofEpochMilli(millis).toString();
  }
}
