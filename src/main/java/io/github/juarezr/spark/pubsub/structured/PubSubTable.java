package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Spark catalog table for Google Pub/Sub streaming reads. */
public final class PubSubTable implements SupportsRead {
  private final PubSubConfig config;

  public PubSubTable(PubSubConfig config) {
    this.config = config;
  }

  @Override
  public String name() {
    return "pubsub:" + config.subscriptionPath();
  }

  @Override
  public StructType schema() {
    return PubSubSchema.SCHEMA;
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
    return new PubSubScanBuilder(merged);
  }
}
