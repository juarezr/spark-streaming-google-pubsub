package io.github.juarezr.spark.pubsub.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class PubSubSeekTimeTest {

  @Test
  void parsesEpochMillisAndRfc3339() {
    assertEquals(
        Instant.ofEpochMilli(1_723_050_029_028L), PubSubClient.parseSeekTime("1723050029028"));
    assertEquals(
        Instant.parse("2024-08-07T15:00:29.028Z"),
        PubSubClient.parseSeekTime("2024-08-07T12:00:29.028-03:00"));
  }

  @Test
  void rejectsNaiveDatetime() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubClient.parseSeekTime("2024-08-07 12:00:29.028"));
  }
}
