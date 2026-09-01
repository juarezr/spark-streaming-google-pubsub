package io.github.juarezr.spark.pubsub.client;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Exponential backoff with jitter for transient Pub/Sub failures. */
public final class RetryPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RetryPolicy.class);

  private final long initialBackoffMs;
  private final long maxBackoffMs;
  private final int maxAttempts;
  private final AtomicLong retryAttempts = new AtomicLong(0);

  public RetryPolicy(long initialBackoffMs, long maxBackoffMs, int maxAttempts) {
    this.initialBackoffMs = initialBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
    this.maxAttempts = maxAttempts;
  }

  public static RetryPolicy defaults() {
    return new RetryPolicy(100L, 10_000L, 8);
  }

  /** Lifetime count of retryable failures that slept and retried. Resets with this instance. */
  public long retryAttempts() {
    return retryAttempts.get();
  }

  public <T> T execute(String operation, RetryableCallable<T> callable) {
    long backoff = initialBackoffMs;
    RuntimeException last = null;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return callable.call();
      } catch (RuntimeException e) {
        last = e;
        if (attempt == maxAttempts || !isRetryable(e)) {
          throw e;
        }
        retryAttempts.incrementAndGet();
        long sleep = backoff + ThreadLocalRandom.current().nextLong(0, Math.max(1, backoff / 4));
        sleep = Math.min(sleep, maxBackoffMs);
        LOG.warn(
            "Transient failure on {} (attempt {}/{}): {}. Retrying in {} ms",
            operation,
            attempt,
            maxAttempts,
            e.toString(),
            sleep);
        try {
          TimeUnit.MILLISECONDS.sleep(sleep);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
        backoff = Math.min(backoff * 2, maxBackoffMs);
      } catch (Exception e) {
        throw new RuntimeException("Unexpected failure during " + operation, e);
      }
    }
    throw last == null ? new RuntimeException("Retry exhausted for " + operation) : last;
  }

  public void executeVoid(String operation, RetryableRunnable runnable) {
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
  public interface RetryableCallable<T> {
    T call() throws Exception;
  }

  @FunctionalInterface
  public interface RetryableRunnable {
    void run() throws Exception;
  }
}
