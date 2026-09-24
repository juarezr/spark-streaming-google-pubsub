package io.github.juarezr.spark.pubsub.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
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
    assertEquals(null, config.receiveTime());
    assertEquals(128L * 1024 * 1024, config.batchSize());
    assertEquals(1, config.numWriters());
    assertEquals(SchemaMode.BASIC, config.schemaMode());
    assertEquals(MetadataMode.NONE, config.metadataMode());
    assertEquals("projects/my-project/subscriptions/my-sub", config.subscriptionPath());
  }

  @Test
  void fromOptionsParsesSchemaAndMetadataModes() {
    Map<String, String> options = new HashMap<>();
    options.put("projectId", "p");
    options.put("subscription", "s");
    options.put("schemaMode", "mixed");
    options.put("metadataMode", "slim");

    PubSubConfig config = PubSubConfig.fromOptions(options);

    assertEquals(SchemaMode.MIXED, config.schemaMode());
    assertEquals(MetadataMode.SLIM, config.metadataMode());
  }

  @Test
  void rejectsUnknownSchemaAndMetadataModes() {
    Map<String, String> schema = new HashMap<>();
    schema.put("projectId", "p");
    schema.put("subscription", "s");
    schema.put("schemaMode", "full");
    assertThrows(IllegalArgumentException.class, () -> PubSubConfig.fromOptions(schema));

    Map<String, String> metadata = new HashMap<>();
    metadata.put("projectId", "p");
    metadata.put("subscription", "s");
    metadata.put("metadataMode", "dynamic");
    assertThrows(IllegalArgumentException.class, () -> PubSubConfig.fromOptions(metadata));
  }

  @Test
  void fromOptionsParsesAckModeAndSeek() {
    Map<String, String> options = new HashMap<>();
    options.put("project", "p");
    options.put("subscription", "s");
    options.put("ackMode", "early");
    options.put("seek", "timestamp");
    options.put("seekTime", "1700000000000");
    options.put("maxRetryTime", "90s");

    PubSubConfig config = PubSubConfig.fromOptions(options);

    assertEquals(AckMode.EARLY, config.ackMode());
    assertEquals(SeekMode.TIMESTAMP, config.seekMode());
    assertEquals("1700000000000", config.seekTime().orElseThrow());
    assertEquals(Duration.ofSeconds(90), config.maxRetryTime());
    assertTrue(config.maxRetryTimeSet());
    assertEquals(Duration.ofSeconds(180), config.effectiveAckDeadline(null));
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
    options.put("receiveTime", "5000ms");
    options.put("batchSize", "2m");
    options.put("batchCount", "3000");
    options.put("numWriters", "auto");

    PubSubConfig config = PubSubConfig.fromOptions(options);

    assertEquals(GatherMode.PULL, config.gatherMode());
    assertEquals(Duration.ofSeconds(5), config.receiveTime());
    assertEquals(2L * 1024 * 1024, config.batchSize());
    assertEquals(3000L, config.batchCount());
    assertTrue(config.numWriters() >= 1);
  }

  @Test
  void validatesRanges() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubSubConfig.builder()
                .projectId("p")
                .subscription("s")
                .ackDeadline(Duration.ofSeconds(5))
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubSubConfig.builder()
                .projectId("p")
                .subscription("s")
                .receiveTime(Duration.ZERO)
                .build());
    assertThrows(IllegalArgumentException.class, () -> PubSubConfig.parseDuration("test", "10x"));
    assertThrows(
        IllegalArgumentException.class, () -> PubSubConfig.parseSeekTime("2024-08-07 12:00:00"));
  }

  @Test
  void parseSeekTimeAcceptsEpochMillisAndRfc3339() {
    assertEquals(
        Instant.ofEpochMilli(1_723_050_029_028L), PubSubConfig.parseSeekTime("1723050029028"));
    Instant rfc3339 = Instant.parse("2024-08-07T15:00:29.028Z");
    assertEquals(rfc3339, PubSubConfig.parseSeekTime("2024-08-07T15:00:29.028Z"));
    assertEquals(rfc3339, PubSubConfig.parseSeekTime("2024-08-07T12:00:29.028-03:00"));
  }

  @Test
  void parseSeekTimeRejectsNaiveDatetime() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubConfig.parseSeekTime("2024-08-07 12:00:29.028"));
  }

  @Test
  void fromOptionsParsesLimitTime() {
    Map<String, String> options = new HashMap<>();
    options.put("projectId", "p");
    options.put("subscription", "s");
    options.put("limitTime", "2024-08-07T15:00:29.028Z");

    PubSubConfig config = PubSubConfig.fromOptions(options);

    assertEquals("2024-08-07T15:00:29.028Z", config.limitTime().orElseThrow());
    assertEquals(
        Instant.parse("2024-08-07T15:00:29.028Z"), config.limitTimeAsInstant().orElseThrow());
    assertTrue(config.startupSummary().contains("limitTime=2024-08-07T15:00:29.028Z"));
  }

  @Test
  void blankLimitTimeStaysUnset() {
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").limitTime("  ").build();
    assertTrue(config.limitTime().isEmpty());
    assertTrue(config.startupSummary().contains("limitTime=-"));
  }

  @Test
  void limitTimeMustBeAfterSeekTime() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubSubConfig.builder()
                .projectId("p")
                .subscription("s")
                .seekMode(SeekMode.TIMESTAMP)
                .seekTime("2000")
                .limitTime("2000")
                .build());
    assertDoesNotThrow(
        () ->
            PubSubConfig.builder()
                .projectId("p")
                .subscription("s")
                .seekMode(SeekMode.TIMESTAMP)
                .seekTime("2000")
                .limitTime("2001")
                .build());
  }

  @Test
  void parseInstantNamesTheOption() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> PubSubConfig.parseInstant(PubSubConfig.LIMIT_TIME, "not-a-time"));
    assertTrue(ex.getMessage().contains("limitTime"));
  }

  @Test
  void rejectsRemovedUnaryAndBatchTimeOptions() {
    Map<String, String> batchTime = new HashMap<>();
    batchTime.put("projectId", "p");
    batchTime.put("subscription", "s");
    batchTime.put("batchTime", "10s");
    IllegalArgumentException batchEx =
        assertThrows(IllegalArgumentException.class, () -> PubSubConfig.fromOptions(batchTime));
    assertTrue(batchEx.getMessage().contains("receiveTime"));

    Map<String, String> pullMax = new HashMap<>();
    pullMax.put("projectId", "p");
    pullMax.put("subscription", "s");
    pullMax.put("pullMaxMessages", "1000");
    assertThrows(IllegalArgumentException.class, () -> PubSubConfig.fromOptions(pullMax));

    Map<String, String> pullDeadline = new HashMap<>();
    pullDeadline.put("projectId", "p");
    pullDeadline.put("subscription", "s");
    pullDeadline.put("pullDeadline", "20s");
    assertThrows(IllegalArgumentException.class, () -> PubSubConfig.fromOptions(pullDeadline));
  }

  @Test
  void implementationVersionDoesNotThrow() {
    String version = assertDoesNotThrow(PubSubConfig::implementationVersion);
    assertFalse(version == null || version.isBlank());
  }
}
