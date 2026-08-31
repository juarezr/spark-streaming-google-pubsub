package io.github.juarezr.spark.pubsub.structured;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.juarezr.spark.pubsub.client.PubSubClient;
import io.github.juarezr.spark.pubsub.client.PulledMessage;

class UncommittedBatchTest {

  @Test
  void abandonNacksAndReleasesBytes() {
    PubSubClient client = mock(PubSubClient.class);
    PulledMessage message = new PulledMessage("m", new byte[10], Collections.emptyMap(), 0L, "", "ack-1");
    PubSubOffset offset = new PubSubOffset(3L, List.of(message));

    PubSubMicroBatchStream.abandon(client, offset);

    verify(client, times(1)).nack(List.of("ack-1"));
    verify(client, times(1)).releaseMessages(anyList());
  }

  @Test
  void abandonStillReleasesWhenNackFails() {
    PubSubClient client = mock(PubSubClient.class);
    doThrow(new RuntimeException("nack failed")).when(client).nack(anyList());
    PulledMessage message = new PulledMessage("m", new byte[4], Collections.emptyMap(), 0L, "", "ack-2");
    PubSubOffset offset = new PubSubOffset(1L, List.of(message));

    PubSubMicroBatchStream.abandon(client, offset);

    verify(client, times(1)).releaseMessages(anyList());
  }

  @Test
  void abandonNoopsForEmptyOffset() {
    PubSubClient client = mock(PubSubClient.class);
    PubSubMicroBatchStream.abandon(client, PubSubOffset.empty(0L));
    PubSubMicroBatchStream.abandon(client, null);
    verify(client, never()).nack(anyList());
    verify(client, never()).releaseMessages(anyList());
  }
}
