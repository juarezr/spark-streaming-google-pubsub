package io.github.juarezr.spark.pubsub.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.juarezr.spark.pubsub.config.AckMode;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class AckCoordinatorTest {

  private static PulledMessage msg(String ackId, int bytes) {
    return new PulledMessage("id-" + ackId, new byte[bytes], Collections.emptyMap(), 0L, "", ackId);
  }

  @Test
  void afterCommitAcksOnlyOnCommit() {
    PubSubClient client = mock(PubSubClient.class);
    AckCoordinator coordinator = new AckCoordinator(AckMode.AFTER_COMMIT);
    List<PulledMessage> messages = List.of(msg("a1", 10), msg("a2", 20));

    coordinator.registerBatch("1", messages);
    coordinator.onPulled(client, "1");
    verify(client, never()).acknowledge(anyList());

    coordinator.commit(client, "1");
    verify(client, times(1)).acknowledge(List.of("a1", "a2"));
    verify(client, times(1)).releaseBytes(30L);
    assertEquals(0, coordinator.pendingBatchCount());
  }

  @Test
  void earlyAcksOnPulled() {
    PubSubClient client = mock(PubSubClient.class);
    AckCoordinator coordinator = new AckCoordinator(AckMode.EARLY);
    List<PulledMessage> messages = List.of(msg("e1", 5));

    coordinator.registerBatch("9", messages);
    coordinator.onPulled(client, "9");
    verify(client, times(1)).acknowledge(List.of("e1"));
    assertEquals(0, coordinator.pendingBatchCount());
  }

  @Test
  void abortNacksAfterCommitMode() {
    PubSubClient client = mock(PubSubClient.class);
    AckCoordinator coordinator = new AckCoordinator(AckMode.AFTER_COMMIT);
    coordinator.registerBatch("2", List.of(msg("n1", 1)));
    coordinator.abort(client, "2");
    verify(client, times(1)).nack(List.of("n1"));
    assertEquals(0, coordinator.pendingBatchCount());
  }

  @Test
  void retryPolicyDetectsRetryable() {
    assertTrue(RetryPolicy.isRetryable(new RuntimeException("UNAVAILABLE: backend")));
    assertTrue(RetryPolicy.isRetryable(new RuntimeException("deadline exceeded")));
  }
}
