package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

class PubSubTableProviderTest {

  @Test
  void inferSchemaUsesBasicTableByDefault() {
    PubSubTableProvider provider = new PubSubTableProvider();
    StructType inferred =
        provider.inferSchema(
            new CaseInsensitiveStringMap(Map.of("projectId", "p", "subscription", "s")));
    assertArrayEquals(new String[] {"body", "messageid", "publishtime"}, inferred.fieldNames());
  }

  @Test
  void getTableKeepsUserSchema() {
    PubSubTableProvider provider = new PubSubTableProvider();
    StructType user =
        new StructType(
            new StructField[] {
              new StructField("deviceid", DataTypes.StringType, false, Metadata.empty()),
              new StructField("eventtime", DataTypes.LongType, false, Metadata.empty())
            });
    Table table =
        provider.getTable(user, new Transform[0], Map.of("projectId", "p", "subscription", "s"));
    assertEquals(user, CatalogV2Util.v2ColumnsToStructType(table.columns()));
    assertEquals("pubsub:projects/p/subscriptions/s", table.name());
    assertEquals(table.name(), table.toString());
    assertEquals("google-pubsub", provider.toString());
  }

  @Test
  void implementationVersionDoesNotThrow() {
    String version = assertDoesNotThrow(PubSubTableProvider::implementationVersion);
    assertFalse(version == null || version.isBlank());
  }
}
