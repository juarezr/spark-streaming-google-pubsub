package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.client.PulledMessage;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubOffsetTest {

  @Test
  void roundTripsThroughJson() {
    PulledMessage message =
        new PulledMessage(
            "mid-1", "hello".getBytes(), Map.of("k", "v"), 1_700_000_000_000L, "order-1", "ack-1");
    PubSubOffset original = new PubSubOffset(42L, List.of(message));

    PubSubOffset restored = PubSubOffset.fromJson(original.json());

    assertEquals(42L, restored.batchId());
    assertEquals(1, restored.messages().size());
    PulledMessage m = restored.messages().get(0);
    assertEquals("mid-1", m.messageId());
    assertEquals("hello", new String(m.data()));
    assertEquals("v", m.attributes().get("k"));
    assertEquals("ack-1", m.ackId());
    assertEquals(List.of("ack-1"), restored.ackIds());
  }

  @Test
  void emptyOffset() {
    PubSubOffset empty = PubSubOffset.empty(0L);
    assertTrue(empty.messages().isEmpty());
    PubSubOffset restored = PubSubOffset.fromJson(empty.json());
    assertEquals(0L, restored.batchId());
    assertTrue(restored.messages().isEmpty());
  }

  @Test
  void equalsConsidersBatchAndMessages() {
    PubSubOffset a =
        new PubSubOffset(
            1L,
            List.of(new PulledMessage("m", new byte[] {1}, Collections.emptyMap(), 0L, "", "a")));
    PubSubOffset b =
        new PubSubOffset(
            1L,
            List.of(new PulledMessage("m", new byte[] {1}, Collections.emptyMap(), 0L, "", "a")));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }
}
