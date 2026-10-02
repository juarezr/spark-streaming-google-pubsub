package io.github.juarezr.spark.pubsub.structured;

import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.SchemaServiceSettings;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.Topic;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.io.IOException;
import org.apache.spark.sql.types.StructType;

/** Loads a topic's Avro/JSON schema from Pub/Sub Schema Service. */
final class PubSubTopicSchema {
  private PubSubTopicSchema() {}

  static StructType fetchPayloadSchema(PubSubConfig config) {
    try {
      return fetch(config);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load Pub/Sub topic schema", e);
    }
  }

  static StructType fromTopicSchema(Encoding encoding, Schema schema) {
    if (encoding != Encoding.JSON) {
      throw new IllegalArgumentException(
          "Unsupported schema encoding "
              + encoding
              + "; only JSON is supported (BINARY and protobuf are not)");
    }
    if (schema == null || schema.getType() != Schema.Type.AVRO) {
      Schema.Type type = schema == null ? Schema.Type.TYPE_UNSPECIFIED : schema.getType();
      throw new IllegalArgumentException(
          "Unsupported schema type " + type + "; only AVRO is supported");
    }
    return PubSubAvroSchemaMapper.toStructType(schema.getDefinition());
  }

  private static StructType fetch(PubSubConfig config) throws IOException {
    PubSubCredentialsProvider credentialsProvider =
        new PubSubCredentialsProvider(config.credentialsFile().orElse(null));
    String topicPath = resolveTopicPath(config, credentialsProvider);
    Topic topic = getTopic(config, credentialsProvider, topicPath);
    if (!topic.hasSchemaSettings()) {
      throw new IllegalArgumentException(
          "Topic "
              + topicPath
              + " has no schema; schemaMode=dynamic/mixed requires a topic schema");
    }
    SchemaSettings settings = topic.getSchemaSettings();
    Schema schema = getSchema(config, credentialsProvider, settings.getSchema());
    return fromTopicSchema(settings.getEncoding(), schema);
  }

  private static String resolveTopicPath(
      PubSubConfig config, PubSubCredentialsProvider credentialsProvider) throws IOException {
    if (config.topicPath().isPresent()) {
      return config.topicPath().get();
    }
    SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
    try (PubSubEmulator emulator = config.emulatorHost().map(PubSubEmulator::new).orElse(null)) {
      if (emulator != null) {
        emulator.configureSubscriptionAdmin(builder);
      } else {
        builder.setCredentialsProvider(credentialsProvider.getProvider());
      }
      try (SubscriptionAdminClient admin = SubscriptionAdminClient.create(builder.build())) {
        return admin.getSubscription(config.subscriptionPath()).getTopic();
      }
    }
  }

  private static Topic getTopic(
      PubSubConfig config, PubSubCredentialsProvider credentialsProvider, String topicPath)
      throws IOException {
    TopicAdminSettings.Builder builder = TopicAdminSettings.newBuilder();
    try (PubSubEmulator emulator = config.emulatorHost().map(PubSubEmulator::new).orElse(null)) {
      if (emulator != null) {
        emulator.configureTopicAdmin(builder);
      } else {
        builder.setCredentialsProvider(credentialsProvider.getProvider());
      }
      try (TopicAdminClient admin = TopicAdminClient.create(builder.build())) {
        return admin.getTopic(topicPath);
      }
    }
  }

  private static Schema getSchema(
      PubSubConfig config, PubSubCredentialsProvider credentialsProvider, String schemaName)
      throws IOException {
    SchemaServiceSettings.Builder builder = SchemaServiceSettings.newBuilder();
    try (PubSubEmulator emulator = config.emulatorHost().map(PubSubEmulator::new).orElse(null)) {
      if (emulator != null) {
        emulator.configureSchema(builder);
      } else {
        builder.setCredentialsProvider(credentialsProvider.getProvider());
      }
      try (SchemaServiceClient client = SchemaServiceClient.create(builder.build())) {
        return client.getSchema(schemaName);
      }
    }
  }
}
