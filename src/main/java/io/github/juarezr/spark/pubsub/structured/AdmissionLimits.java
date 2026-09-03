package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.GatherMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import org.apache.spark.sql.connector.read.streaming.CompositeReadLimit;
import org.apache.spark.sql.connector.read.streaming.ReadAllAvailable;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.ReadMaxRows;
import org.apache.spark.sql.connector.read.streaming.ReadMinRows;

/**
 * Effective gather caps: Spark {@link ReadLimit} composed with connector {@code batchCount} /
 * {@code batchSize} / {@code batchTime}. {@code ReadMaxBytes} is read by name so this class
 * compiles on Spark 3.5 (the type exists from Spark 4.0).
 */
final class AdmissionLimits {
  static final long UNLIMITED = Long.MAX_VALUE;

  private final long maxRows;
  private final long maxBytes;
  private final long minRows;
  private final Duration waitTime;
  private final boolean singlePull;

  private AdmissionLimits(
      long maxRows, long maxBytes, long minRows, Duration waitTime, boolean singlePull) {
    this.maxRows = maxRows;
    this.maxBytes = maxBytes;
    this.minRows = minRows;
    this.waitTime = waitTime;
    this.singlePull = singlePull;
  }

  static AdmissionLimits from(PubSubConfig config, ReadLimit limit) {
    Parsed spark = Parsed.parse(limit);
    long maxRows = minPositive(config.batchCount(), spark.maxRows);
    long maxBytes = minPositive(config.batchSize(), spark.maxBytes);
    Duration waitTime = config.batchTime();
    if (spark.maxTriggerDelayMs > 0 && spark.maxTriggerDelayMs < waitTime.toMillis()) {
      waitTime = Duration.ofMillis(spark.maxTriggerDelayMs);
    }
    boolean singlePull = config.gatherMode() == GatherMode.PULL && spark.minRows <= 0;
    return new AdmissionLimits(maxRows, maxBytes, spark.minRows, waitTime, singlePull);
  }

  long maxRows() {
    return maxRows;
  }

  long maxBytes() {
    return maxBytes;
  }

  long minRows() {
    return minRows;
  }

  Duration waitTime() {
    return waitTime;
  }

  boolean singlePull() {
    return singlePull;
  }

  boolean reachedMax(int rows, long payloadBytes) {
    return (maxRows != UNLIMITED && rows >= maxRows)
        || (maxBytes != UNLIMITED && payloadBytes >= maxBytes);
  }

  boolean minRowsMet(int rows) {
    return minRows <= 0 || rows >= minRows;
  }

  int messagesForNextPull(int already, int pullMaxMessages) {
    int pullMax = Math.max(1, pullMaxMessages);
    if (maxRows == UNLIMITED) {
      return pullMax;
    }
    long remaining = maxRows - already;
    if (remaining <= 0) {
      return 0;
    }
    return (int) Math.min(pullMax, remaining);
  }

  static long minPositive(long left, long right) {
    if (left <= 0) {
      return right <= 0 ? UNLIMITED : right;
    }
    if (right <= 0) {
      return left;
    }
    return Math.min(left, right);
  }

  private static final class Parsed {
    long maxRows;
    long maxBytes;
    long minRows;
    long maxTriggerDelayMs;

    static Parsed parse(ReadLimit limit) {
      Parsed parsed = new Parsed();
      collect(limit, parsed);
      return parsed;
    }

    private static void collect(ReadLimit limit, Parsed parsed) {
      if (limit == null || limit instanceof ReadAllAvailable) {
        return;
      }
      if (limit instanceof CompositeReadLimit) {
        for (ReadLimit child : ((CompositeReadLimit) limit).getReadLimits()) {
          collect(child, parsed);
        }
        return;
      }
      if (limit instanceof ReadMaxRows) {
        parsed.maxRows = minPositive(parsed.maxRows, ((ReadMaxRows) limit).maxRows());
        return;
      }
      if (limit instanceof ReadMinRows) {
        ReadMinRows min = (ReadMinRows) limit;
        parsed.minRows = Math.max(parsed.minRows, min.minRows());
        if (min.maxTriggerDelayMs() > 0) {
          parsed.maxTriggerDelayMs =
              parsed.maxTriggerDelayMs <= 0
                  ? min.maxTriggerDelayMs()
                  : Math.min(parsed.maxTriggerDelayMs, min.maxTriggerDelayMs());
        }
        return;
      }
      Long maxBytes = sparkMaxBytes(limit);
      if (maxBytes != null) {
        parsed.maxBytes = minPositive(parsed.maxBytes, maxBytes);
      }
    }

    private static Long sparkMaxBytes(ReadLimit limit) {
      if (!"ReadMaxBytes".equals(limit.getClass().getSimpleName())) {
        return null;
      }
      try {
        Object value = limit.getClass().getMethod("maxBytes").invoke(limit);
        return value instanceof Number ? ((Number) value).longValue() : null;
      } catch (ReflectiveOperationException e) {
        return null;
      }
    }
  }
}
