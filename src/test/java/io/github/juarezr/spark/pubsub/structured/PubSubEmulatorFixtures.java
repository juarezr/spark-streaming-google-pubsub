package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.SchemaServiceSettings;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaName;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.Topic;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/** Shared Pub/Sub emulator wiring for integration tests. */
final class PubSubEmulatorFixtures {

  private PubSubEmulatorFixtures() {}

  static String requireEmulatorHost() {
    String host = System.getenv("PUBSUB_EMULATOR_HOST");
    assumeTrue(
        host != null && !host.isBlank(),
        "Skipping IT: set PUBSUB_EMULATOR_HOST to run against the emulator");
    return host;
  }

  static EmulatorChannel openChannel(String emulatorHost) {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build();
    FixedTransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    NoCredentialsProvider credentialsProvider = NoCredentialsProvider.create();
    return new EmulatorChannel(channel, channelProvider, credentialsProvider);
  }

  static void recreateTopicAndSubscription(
      EmulatorChannel emulator, String project, String topic, String subscription)
      throws Exception {
    ProjectTopicName topicName = ProjectTopicName.of(project, topic);
    ProjectSubscriptionName subName = ProjectSubscriptionName.of(project, subscription);

    TopicAdminSettings topicSettings =
        TopicAdminSettings.newBuilder()
            .setTransportChannelProvider(emulator.channelProvider())
            .setCredentialsProvider(emulator.credentialsProvider())
            .build();
    SubscriptionAdminSettings subSettings =
        SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(emulator.channelProvider())
            .setCredentialsProvider(emulator.credentialsProvider())
            .build();

    try (TopicAdminClient topicAdmin = TopicAdminClient.create(topicSettings);
        SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(subSettings)) {
      try {
        topicAdmin.deleteTopic(topicName.toString());
      } catch (Exception ignored) {
        // first run
      }
      try {
        subAdmin.deleteSubscription(subName.toString());
      } catch (Exception ignored) {
        // first run
      }
      topicAdmin.createTopic(topicName.toString());
      subAdmin.createSubscription(
          subName.toString(), topicName.toString(), PushConfig.getDefaultInstance(), 60);
    }
  }

  /**
   * Creates an Avro schema (JSON encoding on the topic), topic, and pull subscription. Requires a
   * Pub/Sub emulator that supports the schema service.
   */
  static void recreateSchemaTopicAndSubscription(
      EmulatorChannel emulator,
      String project,
      String schemaId,
      String topic,
      String subscription,
      String avroDefinition)
      throws Exception {
    ProjectTopicName topicName = ProjectTopicName.of(project, topic);
    ProjectSubscriptionName subName = ProjectSubscriptionName.of(project, subscription);
    SchemaName schemaName = SchemaName.of(project, schemaId);
    String parent = ProjectName.of(project).toString();

    SchemaServiceSettings schemaSettings =
        SchemaServiceSettings.newBuilder()
            .setTransportChannelProvider(emulator.channelProvider())
            .setCredentialsProvider(emulator.credentialsProvider())
            .build();
    TopicAdminSettings topicSettings =
        TopicAdminSettings.newBuilder()
            .setTransportChannelProvider(emulator.channelProvider())
            .setCredentialsProvider(emulator.credentialsProvider())
            .build();
    SubscriptionAdminSettings subSettings =
        SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(emulator.channelProvider())
            .setCredentialsProvider(emulator.credentialsProvider())
            .build();

    try (SchemaServiceClient schemaAdmin = SchemaServiceClient.create(schemaSettings);
        TopicAdminClient topicAdmin = TopicAdminClient.create(topicSettings);
        SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(subSettings)) {
      try {
        schemaAdmin.deleteSchema(schemaName.toString());
      } catch (Exception ignored) {
        // first run
      }
      try {
        topicAdmin.deleteTopic(topicName.toString());
      } catch (Exception ignored) {
        // first run
      }
      try {
        subAdmin.deleteSubscription(subName.toString());
      } catch (Exception ignored) {
        // first run
      }

      Schema schema =
          Schema.newBuilder().setType(Schema.Type.AVRO).setDefinition(avroDefinition).build();
      schemaAdmin.createSchema(parent, schema, schemaId);

      Topic topicResource =
          Topic.newBuilder()
              .setName(topicName.toString())
              .setSchemaSettings(
                  SchemaSettings.newBuilder()
                      .setSchema(schemaName.toString())
                      .setEncoding(Encoding.JSON)
                      .build())
              .build();
      topicAdmin.createTopic(topicResource);
      subAdmin.createSubscription(
          subName.toString(), topicName.toString(), PushConfig.getDefaultInstance(), 60);
    }
  }

  static void recreateSubscriptionOnTopic(
      EmulatorChannel emulator, String project, String topic, String subscription)
      throws Exception {
    ProjectTopicName topicName = ProjectTopicName.of(project, topic);
    ProjectSubscriptionName subName = ProjectSubscriptionName.of(project, subscription);
    SubscriptionAdminSettings subSettings =
        SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(emulator.channelProvider())
            .setCredentialsProvider(emulator.credentialsProvider())
            .build();
    try (SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(subSettings)) {
      try {
        subAdmin.deleteSubscription(subName.toString());
      } catch (Exception ignored) {
        // first run
      }
      subAdmin.createSubscription(
          subName.toString(), topicName.toString(), PushConfig.getDefaultInstance(), 60);
    }
  }

  static Publisher newPublisher(EmulatorChannel emulator, String project, String topic)
      throws Exception {
    return newPublisher(emulator, project, topic, false);
  }

  /**
   * Publisher with {@code setEnableMessageOrdering(true)} for messages that set an ordering key.
   */
  static Publisher newOrderingPublisher(EmulatorChannel emulator, String project, String topic)
      throws Exception {
    return newPublisher(emulator, project, topic, true);
  }

  static Publisher newPublisher(
      EmulatorChannel emulator, String project, String topic, boolean enableMessageOrdering)
      throws Exception {
    ProjectTopicName topicName = ProjectTopicName.of(project, topic);
    Publisher.Builder builder =
        Publisher.newBuilder(topicName.toString())
            .setChannelProvider(emulator.channelProvider())
            .setCredentialsProvider(emulator.credentialsProvider());
    if (enableMessageOrdering) {
      builder.setEnableMessageOrdering(true);
    }
    return builder.build();
  }

  static final class EmulatorChannel implements AutoCloseable {
    private final ManagedChannel channel;
    private final FixedTransportChannelProvider channelProvider;
    private final NoCredentialsProvider credentialsProvider;

    EmulatorChannel(
        ManagedChannel channel,
        FixedTransportChannelProvider channelProvider,
        NoCredentialsProvider credentialsProvider) {
      this.channel = channel;
      this.channelProvider = channelProvider;
      this.credentialsProvider = credentialsProvider;
    }

    FixedTransportChannelProvider channelProvider() {
      return channelProvider;
    }

    NoCredentialsProvider credentialsProvider() {
      return credentialsProvider;
    }

    @Override
    public void close() {
      channel.shutdownNow();
    }
  }
}
