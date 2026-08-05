// Minimal Scala Structured Streaming example (illustrative).
// spark-shell --packages io.github.juarezr:spark-streaming-google-pubsub_2.12:0.1.0

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("pubsub-scala-example").getOrCreate()

val messages = spark.readStream
  .format("google-pubsub")
  .option("projectId", sys.env.getOrElse("GCP_PROJECT", "my-project"))
  .option("subscription", sys.env.getOrElse("PUBSUB_SUBSCRIPTION", "my-subscription"))
  .option("ackMode", "afterCommit")
  .load()

val query = messages
  .selectExpr("messageId", "CAST(data AS STRING) AS payload", "publishTime", "attributes")
  .writeStream
  .format("console")
  .option("truncate", "false")
  .option("checkpointLocation", "/tmp/pubsub-scala-checkpoint")
  .start()

query.awaitTermination()
