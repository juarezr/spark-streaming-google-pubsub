package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Entry point for {@code spark.readStream().format("pubsub")} / {@code format("google-pubsub")}.
 */
public final class PubSubTableProvider implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return PubSubConfig.SHORT_NAME;
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return PubSubSchema.SCHEMA;
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new PubSubTable(PubSubConfig.fromOptions(properties));
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }
}
