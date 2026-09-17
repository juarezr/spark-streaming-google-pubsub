package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for {@code spark.readStream().format("pubsub")} / {@code format("google-pubsub")}.
 */
public final class PubSubTableProvider implements TableProvider, DataSourceRegister {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubTableProvider.class);
  private static final AtomicBoolean VERSION_LOGGED = new AtomicBoolean();
  private static final AtomicBoolean CONFIG_LOGGED = new AtomicBoolean();

  @Override
  public String shortName() {
    return PubSubConfig.SHORT_NAME;
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    logVersionOnce();
    return PubSubSchema.inferTableSchema(PubSubConfig.fromOptions(options.asCaseSensitiveMap()));
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    logVersionOnce();
    PubSubConfig config = PubSubConfig.fromOptions(properties);
    if (CONFIG_LOGGED.compareAndSet(false, true)) {
      LOG.info("config {}", config.startupSummary());
    }
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

  private static void logVersionOnce() {
    if (!VERSION_LOGGED.compareAndSet(false, true)) {
      return;
    }
    LOG.info("format={}", PubSubConfig.SHORT_NAME);
    LOG.info(
        "version={} built={} git={}",
        implementationVersion(),
        PubSubBuildInfo.built(),
        PubSubBuildInfo.git());
  }

  static String implementationVersion() {
    return PubSubBuildInfo.version();
  }
}
