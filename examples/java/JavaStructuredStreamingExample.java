package io.github.juarezr.spark.pubsub.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Minimal Structured Streaming example.
 *
 * <pre>
 * spark-submit --class io.github.juarezr.spark.pubsub.examples.JavaStructuredStreamingExample \
 *   --packages io.github.juarezr:spark-streaming-google-pubsub_2.12:0.4.1 \
 *   your-app.jar project-id subscription-id /tmp/checkpoint /tmp/out
 * Use spark-streaming-google-pubsub_2.13:0.4.1 on Spark 4.0–4.2.
 * </pre>
 */
public final class JavaStructuredStreamingExample {
  private JavaStructuredStreamingExample() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println(
          "Usage: JavaStructuredStreamingExample <projectId> <subscription> <checkpoint> <output>");
      System.exit(1);
    }
    String projectId = args[0];
    String subscription = args[1];
    String checkpoint = args[2];
    String output = args[3];

    SparkSession spark =
        SparkSession.builder().appName("pubsub-structured-example").getOrCreate();

    Dataset<Row> messages =
        spark
            .readStream()
            .format("google-pubsub")
            .option("projectId", projectId)
            .option("subscription", subscription)
            .option("ackMode", "afterCommit")
            .option("gatherMode", "batch")
            .load();

    StreamingQuery query =
        messages
            .drop("ackId")
            .writeStream()
            .format("json")
            .option("path", output)
            .option("checkpointLocation", checkpoint)
            .trigger(Trigger.ProcessingTime("1 second"))
            .start();

    query.awaitTermination();
  }
}
