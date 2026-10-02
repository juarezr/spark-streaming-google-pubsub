package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.config.MetadataMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SchemaMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class PubSubPartitionReaderTest {

  private static final PulledMessage MESSAGE =
      new PulledMessage(
          "id-1",
          "{\"deviceid\":\"202315780530\",\"eventtime\":1700000000000}"
              .getBytes(StandardCharsets.UTF_8),
          Map.of("k", "v"),
          1_700_000_000_000L,
          "order-1",
          "ack-1");

  @Test
  void basicSchemaWritesBodyIdAndTime() throws Exception {
    StructType schema =
        PubSubSchema.tableSchema(
            PubSubConfig.builder()
                .projectId("p")
                .subscription("s")
                .schemaMode(SchemaMode.BASIC)
                .build(),
            null);
    PubSubPartitionReader reader = readerFor(schema);

    assertTrue(reader.next());
    InternalRow row = reader.get();
    assertEquals(3, row.numFields());
    assertArrayEquals(MESSAGE.data(), row.getBinary(0));
    assertEquals("id-1", row.getUTF8String(1).toString());
    assertEquals(1_700_000_000_000L * 1000L, row.getLong(2));
    assertFalse(reader.next());
    reader.close();
  }

  @Test
  void prunedSchemaEmitsOnlyRequestedColumns() throws Exception {
    StructType pruned =
        new StructType(
            new StructField[] {
              new StructField("body", DataTypes.BinaryType, false, Metadata.empty()),
              new StructField("publishtime", DataTypes.TimestampType, false, Metadata.empty())
            });
    PubSubPartitionReader reader = readerFor(pruned);

    assertTrue(reader.next());
    InternalRow row = reader.get();
    assertEquals(2, row.numFields());
    assertArrayEquals(MESSAGE.data(), row.getBinary(0));
    assertEquals(1_700_000_000_000L * 1000L, row.getLong(1));
    reader.close();
  }

  @Test
  void factoryUsesReadSchema() throws Exception {
    StructType pruned =
        new StructType(
            new StructField[] {
              new StructField("messageid", DataTypes.StringType, false, Metadata.empty())
            });
    PubSubPartitionReaderFactory factory = new PubSubPartitionReaderFactory(pruned);
    PubSubPartitionReader reader =
        (PubSubPartitionReader) factory.createReader(new PubSubInputPartition(List.of(MESSAGE)));

    assertTrue(reader.next());
    InternalRow row = reader.get();
    assertEquals(1, row.numFields());
    assertEquals("id-1", row.getUTF8String(0).toString());
    reader.close();
  }

  @Test
  void decodesJsonPayloadFields() throws Exception {
    StructType payload =
        new StructType(
            new StructField[] {
              new StructField("deviceid", DataTypes.StringType, false, Metadata.empty()),
              new StructField("eventtime", DataTypes.LongType, false, Metadata.empty())
            });
    PubSubPartitionReader reader = readerFor(payload);

    assertTrue(reader.next());
    InternalRow row = reader.get();
    assertEquals("202315780530", row.getUTF8String(0).toString());
    assertEquals(1_700_000_000_000L, row.getLong(1));
    reader.close();
  }

  @Test
  void metadataAckIdIsReadableWhenRequested() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("body", DataTypes.BinaryType, false, Metadata.empty()),
              new StructField("ackid", DataTypes.StringType, true, Metadata.empty())
            });
    PubSubPartitionReader reader = readerFor(schema);
    assertTrue(reader.next());
    assertEquals("ack-1", reader.get().getUTF8String(1).toString());
    reader.close();
  }

  @Test
  void rawWithFullMetadataReadsAttributesMap() throws Exception {
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .schemaMode(SchemaMode.RAW)
            .metadataMode(MetadataMode.FULL)
            .build();
    StructType table = PubSubSchema.tableSchema(config, null);
    StructType readSchema =
        appendMetadataFields(table, PubSubSchema.metadataColumns(config, table));
    PubSubPartitionReader reader = readerFor(readSchema);
    assertTrue(reader.next());
    InternalRow row = reader.get();
    int attrIndex = readSchema.fieldIndex("attributes");
    ArrayBasedMapData map = (ArrayBasedMapData) row.getMap(attrIndex);
    assertTrue(map.numElements() >= 1);
    reader.close();
  }

  @Test
  void slimTableIncludesOrderingKey() throws Exception {
    PubSubConfig config =
        PubSubConfig.builder().projectId("p").subscription("s").schemaMode(SchemaMode.SLIM).build();
    StructType schema = PubSubSchema.tableSchema(config, null);
    PubSubPartitionReader reader = readerFor(schema);
    assertTrue(reader.next());
    InternalRow row = reader.get();
    assertEquals("order-1", row.getUTF8String(schema.fieldIndex("orderingkey")).toString());
    reader.close();
  }

  @Test
  void mixedSchemaDecodesPayloadAndEnvelope() throws Exception {
    StructType payload =
        new StructType(
            new StructField[] {
              new StructField("deviceid", DataTypes.StringType, false, Metadata.empty()),
              new StructField("eventtime", DataTypes.LongType, false, Metadata.empty())
            });
    PubSubConfig config =
        PubSubConfig.builder()
            .projectId("p")
            .subscription("s")
            .schemaMode(SchemaMode.MIXED)
            .build();
    StructType schema = PubSubSchema.tableSchema(config, payload);
    PubSubPartitionReader reader = readerFor(schema);
    assertTrue(reader.next());
    InternalRow row = reader.get();
    assertEquals("202315780530", row.getUTF8String(0).toString());
    assertEquals("id-1", row.getUTF8String(schema.fieldIndex("messageid")).toString());
    reader.close();
  }

  @Test
  void publishTimeColumnNameIsCaseInsensitive() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("PublishTime", DataTypes.TimestampType, false, Metadata.empty())
            });
    PubSubPartitionReader reader = readerFor(schema);
    assertTrue(reader.next());
    assertEquals(1_700_000_000_000L * 1000L, reader.get().getLong(0));
    reader.close();
  }

  private static StructType appendMetadataFields(
      StructType table, MetadataColumn[] metadataColumns) {
    List<StructField> fields = new ArrayList<>();
    for (StructField field : table.fields()) {
      fields.add(field);
    }
    for (MetadataColumn column : metadataColumns) {
      fields.add(
          new StructField(column.name(), column.dataType(), column.isNullable(), Metadata.empty()));
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  private static PubSubPartitionReader readerFor(StructType schema) {
    return new PubSubPartitionReader(new PubSubInputPartition(List.of(MESSAGE)), schema);
  }
}
