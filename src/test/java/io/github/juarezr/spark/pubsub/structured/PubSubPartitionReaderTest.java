package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class PubSubPartitionReaderTest {

  private static final PulledMessage MESSAGE =
      new PulledMessage(
          "id-1", new byte[] {9, 8}, Map.of("k", "v"), 1_700_000_000_000L, "order-1", "ack-1");

  @Test
  void fullSchemaWritesSixColumnsInOrder() throws Exception {
    PubSubPartitionReader reader = readerFor(PubSubSchema.SCHEMA);

    assertTrue(reader.next());
    InternalRow row = reader.get();
    assertEquals(6, row.numFields());
    assertEquals("id-1", row.getUTF8String(0).toString());
    assertArrayEquals(new byte[] {9, 8}, row.getBinary(1));
    assertEquals("k", row.getMap(2).keyArray().getUTF8String(0).toString());
    assertEquals("v", row.getMap(2).valueArray().getUTF8String(0).toString());
    assertEquals(1_700_000_000_000L * 1000L, row.getLong(3));
    assertEquals("order-1", row.getUTF8String(4).toString());
    assertEquals("ack-1", row.getUTF8String(5).toString());
    assertFalse(reader.next());
    reader.close();
  }

  @Test
  void prunedSchemaEmitsOnlyRequestedColumns() throws Exception {
    StructType pruned =
        new StructType(
            new StructField[] {
              new StructField("data", DataTypes.BinaryType, false, Metadata.empty()),
              new StructField("publishTime", DataTypes.TimestampType, false, Metadata.empty())
            });
    PubSubPartitionReader reader = readerFor(pruned);

    assertTrue(reader.next());
    InternalRow row = reader.get();
    assertEquals(2, row.numFields());
    assertArrayEquals(new byte[] {9, 8}, row.getBinary(0));
    assertEquals(1_700_000_000_000L * 1000L, row.getLong(1));
    reader.close();
  }

  @Test
  void factoryUsesReadSchema() throws Exception {
    StructType pruned =
        new StructType(
            new StructField[] {
              new StructField("messageId", DataTypes.StringType, false, Metadata.empty())
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
  void unknownColumnIsRejected() {
    StructType unknown =
        new StructType(
            new StructField[] {
              new StructField("notAPubSubColumn", DataTypes.StringType, true, Metadata.empty())
            });
    PubSubPartitionReader reader = readerFor(unknown);
    assertTrue(reader.next());
    assertThrows(IllegalArgumentException.class, reader::get);
  }

  private static PubSubPartitionReader readerFor(StructType schema) {
    return new PubSubPartitionReader(new PubSubInputPartition(List.of(MESSAGE)), schema);
  }
}
