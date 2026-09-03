package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark catalog table for Google Pub/Sub streaming reads. */
final class PubSubTable implements SupportsRead, SupportsMetadataColumns {
  private final PubSubConfig config;
  private final StructType tableSchema;

  PubSubTable(PubSubConfig config, StructType tableSchema) {
    this.config = config;
    this.tableSchema =
        tableSchema == null || tableSchema.isEmpty()
            ? PubSubSchema.inferTableSchema(config)
            : tableSchema;
  }

  @Override
  public String name() {
    return "pubsub:" + config.subscriptionPath();
  }

  @Override
  public StructType schema() {
    return tableSchema;
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return PubSubSchema.metadataColumns(config, tableSchema);
  }

  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> caps = new HashSet<>();
    caps.add(TableCapability.MICRO_BATCH_READ);
    return caps;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    PubSubConfig merged =
        options.isEmpty() ? config : PubSubConfig.fromOptions(options.asCaseSensitiveMap());
    return new PubSubScanBuilder(merged, tableSchema);
  }
}
