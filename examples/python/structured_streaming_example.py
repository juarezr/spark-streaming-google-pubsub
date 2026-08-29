# Minimal PySpark Structured Streaming example.
# Requires the connector JAR on the driver/executor classpath, e.g.:
#   pyspark --packages io.github.juarezr:spark-streaming-google-pubsub_2.12:0.3.0
# Spark 4.x (Scala 2.13):
#   pyspark --packages io.github.juarezr:spark-streaming-google-pubsub_2.13:0.3.0
# or:
#   spark-submit --packages ... examples/python/structured_streaming_example.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pubsub-pyspark-example").getOrCreate()

messages = (
    spark.readStream.format("google-pubsub")
    .option("projectId", "my-project")
    .option("subscription", "my-subscription")
    .option("ackMode", "afterCommit")
    .load()
)

query = (
    messages.selectExpr(
        "messageId",
        "CAST(data AS STRING) AS payload",
        "publishTime",
        "attributes",
    )
    .writeStream.format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/pubsub-pyspark-checkpoint")
    .start()
)

query.awaitTermination()
