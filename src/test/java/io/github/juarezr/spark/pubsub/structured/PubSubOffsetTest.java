package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PubSubOffsetTest {

  @Test
  void roundTripsThroughJson() {
    PubSubOffset original = new PubSubOffset(42L);

    PubSubOffset restored = PubSubOffset.fromJson(original.json());

    assertEquals(42L, restored.batchId());
    assertEquals("{\"batchId\":42}", original.json());
    assertTrue(!original.json().contains("data"));
    assertTrue(!original.json().contains("ack"));
  }

  @Test
  void emptyOffset() {
    PubSubOffset empty = PubSubOffset.empty(0L);
    PubSubOffset restored = PubSubOffset.fromJson(empty.json());
    assertEquals(0L, restored.batchId());
  }

  @Test
  void equalsConsidersBatchAndMessages() {
    PubSubOffset a = new PubSubOffset(1L);
    PubSubOffset b = new PubSubOffset(1L);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }
}
