package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

/** Builds a {@link PubSubScan}. */
final class PubSubScanBuilder implements SupportsPushDownRequiredColumns {
  private final PubSubConfig config;
  private StructType requiredSchema;

  PubSubScanBuilder(PubSubConfig config, StructType tableSchema) {
    this.config = config;
    this.requiredSchema = tableSchema;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Scan build() {
    return new PubSubScan(config, requiredSchema);
  }
}
