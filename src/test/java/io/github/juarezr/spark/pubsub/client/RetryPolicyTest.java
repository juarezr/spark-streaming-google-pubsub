package io.github.juarezr.spark.pubsub.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

  @Test
  void retriesThenSucceeds() {
    RetryPolicy policy = new RetryPolicy(1L, 5L, 5);
    AtomicInteger attempts = new AtomicInteger();
    String result =
        policy.execute(
            "op",
            () -> {
              if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("UNAVAILABLE");
              }
              return "ok";
            });
    assertEquals("ok", result);
    assertEquals(3, attempts.get());
  }

  @Test
  void doesNotRetryNonRetryable() {
    RetryPolicy policy = new RetryPolicy(1L, 5L, 5);
    AtomicInteger attempts = new AtomicInteger();
    assertThrows(
        RuntimeException.class,
        () ->
            policy.execute(
                "op",
                () -> {
                  attempts.incrementAndGet();
                  throw new RuntimeException("PERMISSION_DENIED");
                }));
    assertEquals(1, attempts.get());
  }

  @Test
  void isRetryableHeuristics() {
    assertTrue(RetryPolicy.isRetryable(new RuntimeException("resource_exhausted")));
    assertTrue(
        RetryPolicy.isRetryable(
            new RuntimeException("wrap", new RuntimeException("timeout waiting"))));
  }
}
