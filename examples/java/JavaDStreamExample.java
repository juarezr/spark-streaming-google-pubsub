package io.github.juarezr.spark.pubsub.examples;

import io.github.juarezr.spark.pubsub.dstream.PubsubUtils;
import io.github.juarezr.spark.pubsub.dstream.SparkGCPCredentials;
import io.github.juarezr.spark.pubsub.dstream.SparkPubsubMessage;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/** Minimal DStreams (Legacy-compatible) example for Java. */
public final class JavaDStreamExample {
  private JavaDStreamExample() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: JavaDStreamExample <projectId> <topic> <subscription>");
      System.exit(1);
    }
    String projectId = args[0];
    String topic = args[1];
    String subscription = args[2];

    SparkConf conf = new SparkConf().setAppName("pubsub-dstream-example");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));

    SparkGCPCredentials credentials = SparkGCPCredentials.builder().build();
    JavaReceiverInputDStream<SparkPubsubMessage> stream =
        PubsubUtils.createStream(
            jssc,
            projectId,
            topic,
            subscription,
            credentials,
            StorageLevel.MEMORY_AND_DISK_SER());

    stream
        .map(SparkPubsubMessage::getDataAsString)
        .print();

    jssc.start();
    jssc.awaitTermination();
  }
}
