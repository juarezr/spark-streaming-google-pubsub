# Minimal PySpark Structured Streaming example.
# Requires the connector JAR on the driver/executor classpath, e.g.:
#   pyspark --packages io.github.juarezr:spark-streaming-google-pubsub_2.12:0.6.0
# Spark 4.x (Scala 2.13):
#   pyspark --packages io.github.juarezr:spark-streaming-google-pubsub_2.13:0.6.0
# or:
#   spark-submit --packages ... examples/python/structured_streaming_example.py

from pyspark.sql import SparkSession
from pyspark.sql.streaming import Trigger

spark = SparkSession.builder.appName("pubsub-pyspark-example").getOrCreate()

messages = (
    spark.readStream.format("google-pubsub")
    .option("projectId", "my-project")
    .option("subscription", "my-subscription")
    .option("ackMode", "afterCommit")
    .option("gatherMode", "batch")
    .load()
)

query = (
    messages.writeStream.format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/pubsub-pyspark-checkpoint")
    .trigger(Trigger.ProcessingTime("1 second"))
    .start()
)

query.awaitTermination()
