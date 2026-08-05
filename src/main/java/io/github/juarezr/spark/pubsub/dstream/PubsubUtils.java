package io.github.juarezr.spark.pubsub.dstream;

import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Legacy-compatible factory for Pub/Sub DStreams (Java API).
 *
 * <p>Package differs from Legacy ({@code io.github.juarezr.spark.pubsub.dstream} instead of {@code
 * org.apache.spark.streaming.pubsub}); method signatures match for an easy migration from Java
 * applications such as spark-event.
 */
public final class PubsubUtils {
  private PubsubUtils() {}

  public static JavaReceiverInputDStream<SparkPubsubMessage> createStream(
      JavaStreamingContext jssc,
      String project,
      String subscription,
      SparkGCPCredentials credentials,
      StorageLevel storageLevel) {
    return createStream(jssc, project, null, subscription, credentials, storageLevel, true);
  }

  public static JavaReceiverInputDStream<SparkPubsubMessage> createStream(
      JavaStreamingContext jssc,
      String project,
      String subscription,
      SparkGCPCredentials credentials,
      StorageLevel storageLevel,
      boolean autoAcknowledge) {
    return createStream(
        jssc, project, null, subscription, credentials, storageLevel, autoAcknowledge);
  }

  public static JavaReceiverInputDStream<SparkPubsubMessage> createStream(
      JavaStreamingContext jssc,
      String project,
      String topic,
      String subscription,
      SparkGCPCredentials credentials,
      StorageLevel storageLevel) {
    return createStream(jssc, project, topic, subscription, credentials, storageLevel, true);
  }

  public static JavaReceiverInputDStream<SparkPubsubMessage> createStream(
      JavaStreamingContext jssc,
      String project,
      String topic,
      String subscription,
      SparkGCPCredentials credentials,
      StorageLevel storageLevel,
      boolean autoAcknowledge) {
    PubSubConfig.Builder builder =
        PubSubConfig.builder()
            .projectId(project)
            .subscription(subscription)
            .topic(topic)
            .ackMode(autoAcknowledge ? AckMode.EARLY : AckMode.AFTER_COMMIT)
            .credentialsFile(credentials == null ? null : credentials.provider().credentialsFile());
    PubsubReceiver receiver = new PubsubReceiver(builder.build(), autoAcknowledge, storageLevel);
    return jssc.receiverStream(receiver);
  }

  /**
   * Extended Java factory exposing Structured Streaming-style options (ackMode, emulator, seek).
   */
  public static JavaReceiverInputDStream<SparkPubsubMessage> createStream(
      JavaStreamingContext jssc, PubSubConfig config, StorageLevel storageLevel) {
    boolean autoAck = true;
    PubsubReceiver receiver = new PubsubReceiver(config, autoAck, storageLevel);
    return jssc.receiverStream(receiver);
  }
}
