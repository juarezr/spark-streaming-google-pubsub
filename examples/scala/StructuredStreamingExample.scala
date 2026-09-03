// Minimal Scala Structured Streaming example (illustrative).
// spark-shell --packages io.github.juarezr:spark-streaming-google-pubsub_2.12:0.6.0
// Spark 4.x: spark-streaming-google-pubsub_2.13:0.6.0

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

val spark = SparkSession.builder.appName("pubsub-scala-example").getOrCreate()

val messages = spark.readStream
  .format("google-pubsub")
  .option("projectId", sys.env.getOrElse("GCP_PROJECT", "my-project"))
  .option("subscription", sys.env.getOrElse("PUBSUB_SUBSCRIPTION", "my-subscription"))
  .option("ackMode", "afterCommit")
  .option("gatherMode", "batch")
  .load()

// Default schemaMode=basic: body, messageid, publishtime. Watermark on SELECT *:
// messages.withWatermark("publishtime", "10 minutes")

val query = messages
  .writeStream
  .format("console")
  .option("truncate", "false")
  .option("checkpointLocation", "/tmp/pubsub-scala-checkpoint")
  .trigger(Trigger.ProcessingTime("1 second"))
  .start()

query.awaitTermination()
