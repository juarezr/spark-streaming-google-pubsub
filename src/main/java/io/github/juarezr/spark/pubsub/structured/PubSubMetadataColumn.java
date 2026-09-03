package io.github.juarezr.spark.pubsub.structured;

import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.types.DataType;

final class PubSubMetadataColumn implements MetadataColumn {
  private final String name;
  private final DataType dataType;
  private final boolean nullable;

  PubSubMetadataColumn(String name, DataType dataType, boolean nullable) {
    this.name = name;
    this.dataType = dataType;
    this.nullable = nullable;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public DataType dataType() {
    return dataType;
  }

  @Override
  public boolean isNullable() {
    return nullable;
  }
}
