package io.github.juarezr.spark.pubsub.structured;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Exponential backoff with jitter for transient Pub/Sub failures. */
final class RetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(RetryPolicy.class);

  private static final String FAILURE_ON_ATTEMPT_MSG =
      "Transient failure on {} (attempt {}/{}): {}. Retrying in {} ms";

  private final long initialBackoffMs;
  private final long maxBackoffMs;
  private final int maxAttempts;
  private final long maxRetryTimeMs;
  private final AtomicLong retryAttempts;

  RetryPolicy(long initialBackoffMs, long maxBackoffMs, int maxAttempts) {
    this(initialBackoffMs, maxBackoffMs, maxAttempts, Long.MAX_VALUE);
  }

  RetryPolicy(long initialBackoffMs, long maxBackoffMs, int maxAttempts, long maxRetryTimeMs) {
    this.initialBackoffMs = initialBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
    this.maxAttempts = maxAttempts;
    this.maxRetryTimeMs = maxRetryTimeMs;
    this.retryAttempts = new AtomicLong(0);
  }

  static RetryPolicy defaults(Duration maxRetryTime) {
    return new RetryPolicy(100L, 10_000L, 1000, maxRetryTime.toMillis());
  }

  /** Lifetime count of retryable failures that slept and retried. Resets with this instance. */
  long retryAttempts() {
    return retryAttempts.get();
  }

  <T> T execute(String operation, RetryableCallable<T> callable) {
    RuntimeException last = null;
    long backoff = this.initialBackoffMs;
    long startedAt = System.nanoTime();
    long lastWarnAt = Long.MIN_VALUE;
    int suppressedWarnings = 0;

    for (int attempt = 1; attempt <= this.maxAttempts; attempt++) {
      try {
        return callable.call();
      } catch (RuntimeException e) {
        last = e;
        final boolean retryable = isRetryable(e);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        if (attempt == this.maxAttempts || !retryable || elapsedMs >= maxRetryTimeMs) {
          throw e;
        }
        retryAttempts.incrementAndGet();
        final long jitterBound = Math.max(1, backoff / 4);
        final long nextBackoffMs = backoff + ThreadLocalRandom.current().nextLong(0, jitterBound);
        final long remainingRetryMs = Math.max(0L, maxRetryTimeMs - elapsedMs);
        final long sleep = Math.min(Math.min(nextBackoffMs, this.maxBackoffMs), remainingRetryMs);
        backoff = Math.min(backoff * 2, this.maxBackoffMs);
        if (attempt == 1 || elapsedMs - lastWarnAt >= 15_000L) {
          String failure =
              suppressedWarnings == 0
                  ? e.toString()
                  : e + " (" + suppressedWarnings + " retry warnings suppressed)";
          LOG.warn(FAILURE_ON_ATTEMPT_MSG, operation, attempt, this.maxAttempts, failure, sleep);
          lastWarnAt = elapsedMs;
          suppressedWarnings = 0;
        } else {
          suppressedWarnings++;
        }
        try {
          TimeUnit.MILLISECONDS.sleep(sleep);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
      } catch (Exception e) {
        throw new RuntimeException("Unexpected failure during " + operation, e);
      }
    }
    throw last == null ? new RuntimeException("Retry exhausted for " + operation) : last;
  }

  void executeVoid(String operation, RetryableRunnable runnable) {
    execute(
        operation,
        () -> {
          runnable.run();
          return null;
        });
  }

  static boolean isRetryable(Throwable t) {
    String msg = t.getMessage() == null ? "" : t.getMessage().toLowerCase();
    if (msg.contains("unavailable")
        || msg.contains("deadline")
        || msg.contains("resource_exhausted")
        || msg.contains("aborted")
        || msg.contains("internal")
        || msg.contains("timeout")
        || msg.contains("temporarily")) {
      return true;
    }
    Throwable cause = t.getCause();
    return cause != null && cause != t && isRetryable(cause);
  }

  @FunctionalInterface
  interface RetryableCallable<T> {
    T call() throws Exception;
  }

  @FunctionalInterface
  interface RetryableRunnable {
    void run() throws Exception;
  }
}
