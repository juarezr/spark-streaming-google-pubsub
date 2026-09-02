package io.github.juarezr.spark.pubsub.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Tests flow-control byte accounting helpers used when a batch is nacked or abandoned. */
class PubSubClientReleaseMessagesTest {

  @Test
  void releaseMessagesDecrementsOutstandingBytes() throws Exception {
    PubSubClient client = newClientWithOutstanding(35L);
    PulledMessage a =
        new PulledMessage("m1", new byte[10], Collections.emptyMap(), 0L, "", "ack-1");
    PulledMessage b =
        new PulledMessage("m2", new byte[25], Collections.emptyMap(), 0L, "", "ack-2");

    client.releaseMessages(List.of(a, b));

    assertEquals(0L, client.outstandingBytes());
  }

  @Test
  void releaseMessagesNoOpForEmpty() throws Exception {
    PubSubClient client = newClientWithOutstanding(7L);
    client.releaseMessages(Collections.emptyList());
    assertEquals(7L, client.outstandingBytes());
  }

  @Test
  void resetOutstandingBytesClearsCounter() throws Exception {
    PubSubClient client = newClientWithOutstanding(99L);
    client.resetOutstandingBytes();
    assertEquals(0L, client.outstandingBytes());
  }

  private static PubSubClient newClientWithOutstanding(long bytes) throws Exception {
    PubSubClient client =
        new PubSubClient(PubSubConfig.builder().projectId("p").subscription("s").build());
    Field field = PubSubClient.class.getDeclaredField("outstandingBytes");
    field.setAccessible(true);
    field.set(client, new AtomicLong(bytes));
    return client;
  }
}
