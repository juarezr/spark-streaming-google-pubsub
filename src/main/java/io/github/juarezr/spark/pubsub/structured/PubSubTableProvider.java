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
 *
 * <pre>
 * @startuml
 * !theme vibrant
 * title PubSubTableProvider
 * Application -> SparkSession : readStream()
 * SparkSession -> Application : DataStreamReader reader
 * Application -> DataStreamReader : load()
 * DataStreamReader -> Application : Dataset<Row> messages
 * Application -> Dataset : writeStream()
 * Dataset -> Application : DataStreamWriter<Row> writer
 * Application -> DataStreamWriter : start()
 * DataStreamWriter -> Application : StreamingQuery query
 * Application -> DataStreamWriter : awaitTermination()
 * DataStreamWriter -> Spark : load(PubSubTableProvider)
 * Spark -> PubSubTableProvider : shortName()
 * PubSubTableProvider -> Spark : return PubSubConfig.SHORT_NAME
 * Spark -> PubSubTableProvider : inferSchema(CaseInsensitiveStringMap)
 * PubSubTableProvider -> PubSubSchema : inferTableSchema(PubSubConfig)
 * PubSubSchema -> PubSubTableProvider : return StructType
 * Spark -> PubSubTableProvider : getTable(StructType, Transform[], Map<String, String>)
 * PubSubTableProvider -> PubSubTable : new(PubSubConfig, StructType)
 * PubSubTable -> Spark : return Table
 * @enduml
 * </pre>
 */
public final class PubSubTableProvider implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return PubSubConfig.SHORT_NAME;
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    PubSubConfig.logVersionOnce();
    return PubSubSchema.inferTableSchema(PubSubConfig.fromOptions(options.asCaseSensitiveMap()));
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    PubSubConfig.logVersionOnce();
    PubSubConfig config = PubSubConfig.fromOptions(properties);
    config.logStartupSummaryOnce();
    StructType table =
        schema != null && schema.length() > 0 ? schema : PubSubSchema.inferTableSchema(config);
    return new PubSubTable(config, table);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public String toString() {
    return shortName();
  }
}
