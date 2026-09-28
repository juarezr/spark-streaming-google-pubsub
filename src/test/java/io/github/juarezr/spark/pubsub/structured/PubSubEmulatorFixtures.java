package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
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
    ProjectTopicName topicName = ProjectTopicName.of(project, topic);
    return Publisher.newBuilder(topicName.toString())
        .setChannelProvider(emulator.channelProvider())
        .setCredentialsProvider(emulator.credentialsProvider())
        .build();
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
