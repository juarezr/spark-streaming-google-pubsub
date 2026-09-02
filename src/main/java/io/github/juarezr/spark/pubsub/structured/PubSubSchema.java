package io.github.juarezr.spark.pubsub.structured;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Fixed schema for Pub/Sub messages exposed via Structured Streaming. */
final class PubSubSchema {
  private PubSubSchema() {}

  public static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("messageId", DataTypes.StringType, false, Metadata.empty()),
            new StructField("data", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField(
                "attributes",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false),
                false,
                Metadata.empty()),
            new StructField("publishTime", DataTypes.TimestampType, false, Metadata.empty()),
            new StructField("orderingKey", DataTypes.StringType, false, Metadata.empty()),
            new StructField("ackId", DataTypes.StringType, true, Metadata.empty())
          });
}
