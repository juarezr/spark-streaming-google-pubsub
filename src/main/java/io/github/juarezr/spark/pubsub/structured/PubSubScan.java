package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

/** Scan that exposes a Pub/Sub {@link MicroBatchStream}. */
final class PubSubScan implements Scan {
  private final PubSubConfig config;
  private final StructType readSchema;

  PubSubScan(PubSubConfig config, StructType readSchema) {
    this.config = config;
    this.readSchema = readSchema;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public String description() {
    return "Google Pub/Sub scan: " + config.subscriptionPath();
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new PubSubMicroBatchStream(config, config.numWriters(), readSchema);
  }
}
