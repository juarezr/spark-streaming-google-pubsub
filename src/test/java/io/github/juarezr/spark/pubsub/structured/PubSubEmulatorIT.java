package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectTopicName;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Integration tests for {@link PubSubEmulator} when {@code PUBSUB_EMULATOR_HOST} is set. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PubSubEmulatorIT {

  private static final String PROJECT = "test-project";

  private String emulatorHost;

  @BeforeAll
  void setUp() {
    emulatorHost = PubSubEmulatorFixtures.requireEmulatorHost();
  }

  @Test
  void configureTopicAdminCreatesAndDeletesTopic() throws Exception {
    String topicId = "pubsub-emulator-it-" + UUID.randomUUID();
    ProjectTopicName topicName = ProjectTopicName.of(PROJECT, topicId);

    try (PubSubEmulator emulator = new PubSubEmulator(emulatorHost)) {
      TopicAdminSettings.Builder settings = TopicAdminSettings.newBuilder();
      emulator.configureTopicAdmin(settings);
      try (TopicAdminClient admin = TopicAdminClient.create(settings.build())) {
        admin.createTopic(topicName.toString());
        admin.deleteTopic(topicName.toString());
      }
    }
  }

  @Test
  void configureSubscriptionAdminBuildsClient() throws Exception {
    try (PubSubEmulator emulator = new PubSubEmulator(emulatorHost)) {
      com.google.cloud.pubsub.v1.SubscriptionAdminSettings.Builder settings =
          com.google.cloud.pubsub.v1.SubscriptionAdminSettings.newBuilder();
      emulator.configureSubscriptionAdmin(settings);
      try (com.google.cloud.pubsub.v1.SubscriptionAdminClient admin =
          com.google.cloud.pubsub.v1.SubscriptionAdminClient.create(settings.build())) {
        assertNotNull(admin);
      }
    }
  }

  @Test
  void closeIsIdempotent() {
    PubSubEmulator emulator = new PubSubEmulator(emulatorHost);
    emulator.close();
    assertDoesNotThrow(emulator::close);
  }
}
