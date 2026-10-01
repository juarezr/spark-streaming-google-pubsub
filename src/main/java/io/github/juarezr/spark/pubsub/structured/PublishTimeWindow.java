package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.common.Into;
import java.util.List;

/** Oldest and newest Pub/Sub publish times from a gather, for logs and age. */
final class PublishTimeWindow {

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
    return Into.timeIso(oldestMillis);
  }

  String newestIso() {
    return Into.timeIso(newestMillis);
  }

  String formatStats(long batchId, String prefix) {
    return String.format(
        "%s batchId=%d messages=%d oldestPublishTime=%s newestPublishTime=%s newestAgeMs=%d",
        prefix == null ? "" : prefix,
        batchId,
        messageCount,
        oldestIso(),
        newestIso(),
        newestAgeMs(System.currentTimeMillis()));
  }
}
