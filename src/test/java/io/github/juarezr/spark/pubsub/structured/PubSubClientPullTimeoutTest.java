package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class PubSubClientPullTimeoutTest {

  @Test
  void omittedAckDeadlineUsesThreeIntervals() {
    PubSubConfig config = PubSubConfig.builder().projectId("p").subscription("s").build();
    assertEquals(Duration.ofSeconds(180), config.effectiveAckDeadline(null));
    assertEquals(Duration.ofSeconds(180), config.effectiveAckDeadline(Duration.ofSeconds(60)));
    assertEquals(Duration.ofSeconds(90), config.effectiveMaxRetryTime());
  }

  @Test
  void omittedMaxRetryTimeClampsToAckDeadline() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackDeadline(Duration.ofSeconds(30))
            .build();
    assertEquals(Duration.ofSeconds(30), config.effectiveMaxRetryTime());
    assertFalse(config.maxRetryTimeSet());
  }

  @Test
  void explicitMaxRetryTimeWins() {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .ackDeadline(Duration.ofSeconds(30))
            .maxRetryTime(Duration.ofSeconds(20))
            .build();
    assertEquals(Duration.ofSeconds(20), config.effectiveMaxRetryTime());
    assertTrue(config.maxRetryTimeSet());
  }
}
