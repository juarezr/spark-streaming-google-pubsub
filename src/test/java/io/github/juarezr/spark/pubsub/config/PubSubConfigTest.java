package io.github.juarezr.spark.pubsub.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubConfigTest {

  @Test
  void fromOptionsParsesRequiredAndDefaults() {
    Map<String, String> options = new HashMap<>();
    options.put("projectId", "my-project");
    options.put("subscription", "my-sub");

    PubSubConfig config = PubSubConfig.fromOptions(options);

    assertEquals("my-project", config.projectId());
    assertEquals("my-sub", config.subscription());
    assertEquals(AckMode.AFTER_COMMIT, config.ackMode());
    assertEquals(SeekMode.NONE, config.seekMode());
    assertEquals(GatherMode.BATCH, config.gatherMode());
    assertEquals(Duration.ofSeconds(10), config.batchTime());
    assertEquals(128L * 1024 * 1024, config.batchSize());
    assertEquals(1, config.numWriters());
    assertEquals("projects/my-project/subscriptions/my-sub", config.subscriptionPath());
  }

  @Test
  void fromOptionsParsesAckModeAndSeek() {
    Map<String, String> options = new HashMap<>();
    options.put("project", "p");
    options.put("subscription", "s");
    options.put("ackMode", "early");
    options.put("seek", "timestamp");
    options.put("seekTime", "1700000000000");
    options.put("pullMaxMessages", "50");
    options.put("pullDeadline", "5s");
    options.put("maxRetryTime", "90s");

    PubSubConfig config = PubSubConfig.fromOptions(options);

    assertEquals(AckMode.EARLY, config.ackMode());
    assertEquals(SeekMode.TIMESTAMP, config.seekMode());
    assertEquals("1700000000000", config.seekTime().orElseThrow());
    assertEquals(50, config.pullMaxMessages());
    assertEquals(Duration.ofSeconds(5), config.pullDeadline());
    assertEquals(Duration.ofSeconds(90), config.maxRetryTime());
  }

  @Test
  void rejectsBlankProject() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubConfig.builder().projectId(" ").subscription("s").build());
  }

  @Test
  void seekTimestampRequiresSeekTime() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubSubConfig.builder()
                .projectId("p")
                .subscription("s")
                .seekMode(SeekMode.TIMESTAMP)
                .build());
  }

  @Test
  void ackModeFromString() {
    assertEquals(AckMode.AFTER_COMMIT, AckMode.fromString("afterCommit"));
    assertEquals(AckMode.AFTER_COMMIT, AckMode.fromString("after_commit"));
    assertEquals(AckMode.EARLY, AckMode.fromString("early"));
    assertThrows(IllegalArgumentException.class, () -> AckMode.fromString("never"));
  }

  @Test
  void topicPathOptional() {
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").topic("t").build();
    assertTrue(config.topicPath().isPresent());
    assertEquals("projects/p/topics/t", config.topicPath().orElseThrow());
  }

  @Test
  void parsesDurationsSizesAndGatherOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("projectId", "p");
    options.put("subscription", "s");
    options.put("gatherMode", "pull");
    options.put("batchTime", "5000ms");
    options.put("batchSize", "2m");
    options.put("batchCount", "3000");
    options.put("numWriters", "auto");

    PubSubConfig config = PubSubConfig.fromOptions(options);

    assertEquals(GatherMode.PULL, config.gatherMode());
    assertEquals(Duration.ofSeconds(5), config.batchTime());
    assertEquals(2L * 1024 * 1024, config.batchSize());
    assertEquals(3000L, config.batchCount());
    assertTrue(config.numWriters() >= 1);
  }

  @Test
  void validatesRanges() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubSubConfig.builder().projectId("p").subscription("s").pullMaxMessages(1001).build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubSubConfig.builder()
                .projectId("p")
                .subscription("s")
                .ackDeadline(Duration.ofSeconds(5))
                .build());
    assertThrows(IllegalArgumentException.class, () -> PubSubConfig.parseDuration("test", "10x"));
    assertThrows(
        IllegalArgumentException.class, () -> PubSubConfig.parseSeekTime("2024-08-07 12:00:00"));
  }
}
