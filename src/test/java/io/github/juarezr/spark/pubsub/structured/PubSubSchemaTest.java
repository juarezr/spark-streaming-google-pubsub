package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.juarezr.spark.pubsub.config.MetadataMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SchemaMode;
import java.util.Arrays;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class PubSubSchemaTest {

  @Test
  void basicTableHasBodyIdAndTime() {
    StructType table = PubSubSchema.tableSchema(config(SchemaMode.BASIC, MetadataMode.NONE), null);
    assertArrayEquals(new String[] {"body", "messageid", "publishtime"}, table.fieldNames());
  }

  @Test
  void rawTableIsBodyOnly() {
    StructType table = PubSubSchema.tableSchema(config(SchemaMode.RAW, MetadataMode.NONE), null);
    assertArrayEquals(new String[] {"body"}, table.fieldNames());
  }

  @Test
  void slimTableAddsOrderingKey() {
    StructType table = PubSubSchema.tableSchema(config(SchemaMode.SLIM, MetadataMode.NONE), null);
    assertArrayEquals(
        new String[] {"body", "messageid", "publishtime", "orderingkey"}, table.fieldNames());
  }

  @Test
  void metadataSubtractsTableFields() {
    PubSubConfig basic = config(SchemaMode.BASIC, MetadataMode.BASIC);
    MetadataColumn[] empty =
        PubSubSchema.metadataColumns(basic, PubSubSchema.tableSchema(basic, null));
    assertEquals(0, empty.length);

    PubSubConfig rawSlim = config(SchemaMode.RAW, MetadataMode.SLIM);
    MetadataColumn[] rawMeta =
        PubSubSchema.metadataColumns(rawSlim, PubSubSchema.tableSchema(rawSlim, null));
    assertArrayEquals(
        new String[] {"messageid", "publishtime", "orderingkey", "ackid"}, names(rawMeta));

    PubSubConfig slimSlim = config(SchemaMode.SLIM, MetadataMode.SLIM);
    MetadataColumn[] leftover =
        PubSubSchema.metadataColumns(slimSlim, PubSubSchema.tableSchema(slimSlim, null));
    assertArrayEquals(new String[] {"ackid"}, names(leftover));

    StructType payload =
        new StructType(
            new StructField[] {
              new StructField("deviceid", DataTypes.StringType, false, Metadata.empty())
            });
    PubSubConfig mixedBasic = config(SchemaMode.MIXED, MetadataMode.BASIC);
    assertEquals(
        0,
        PubSubSchema.metadataColumns(mixedBasic, PubSubSchema.tableSchema(mixedBasic, payload))
            .length);

    PubSubConfig dynamicBasic = config(SchemaMode.DYNAMIC, MetadataMode.BASIC);
    MetadataColumn[] dynamicMeta =
        PubSubSchema.metadataColumns(dynamicBasic, PubSubSchema.tableSchema(dynamicBasic, payload));
    assertArrayEquals(new String[] {"messageid", "publishtime"}, names(dynamicMeta));
  }

  @Test
  void mixedAppendsEnvelopeToPayload() {
    StructType payload =
        new StructType(
            new StructField[] {
              new StructField("deviceid", DataTypes.StringType, false, Metadata.empty())
            });
    StructType table =
        PubSubSchema.tableSchema(config(SchemaMode.MIXED, MetadataMode.NONE), payload);
    assertArrayEquals(new String[] {"deviceid", "messageid", "publishtime"}, table.fieldNames());
  }

  @Test
  void dynamicRequiresPayload() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubSchema.tableSchema(config(SchemaMode.DYNAMIC, MetadataMode.NONE), null));
  }

  @Test
  void fullMetadataAddsAttributesAfterSubtract() {
    PubSubConfig rawFull = config(SchemaMode.RAW, MetadataMode.FULL);
    assertArrayEquals(
        new String[] {"messageid", "publishtime", "orderingkey", "ackid", "attributes"},
        names(PubSubSchema.metadataColumns(rawFull, PubSubSchema.tableSchema(rawFull, null))));
  }

  private static String[] names(MetadataColumn[] columns) {
    return Arrays.stream(columns).map(MetadataColumn::name).toArray(String[]::new);
  }

  private static PubSubConfig config(SchemaMode schemaMode, MetadataMode metadataMode) {
    return PubSubConfig.builder()
        .projectId("p")
        .subscription("s")
        .schemaMode(schemaMode)
        .metadataMode(metadataMode)
        .build();
  }
}
