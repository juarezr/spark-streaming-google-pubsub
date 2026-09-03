package io.github.juarezr.spark.pubsub.structured;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/** Reads {@link PulledMessage} values into Spark {@link InternalRow}s. */
final class PubSubPartitionReader implements PartitionReader<InternalRow> {
  private final Iterator<PulledMessage> iterator;
  private final StructType readSchema;
  private PulledMessage current;

  PubSubPartitionReader(PubSubInputPartition partition, StructType readSchema) {
    this.iterator = partition.messages().iterator();
    this.readSchema = readSchema == null ? PubSubSchema.SCHEMA : readSchema;
  }

  @Override
  public boolean next() {
    if (iterator.hasNext()) {
      current = iterator.next();
      return true;
    }
    return false;
  }

  @Override
  public InternalRow get() {
    GenericInternalRow row = new GenericInternalRow(readSchema.length());
    StructField[] fields = readSchema.fields();
    for (int i = 0; i < fields.length; i++) {
      row.update(i, valueFor(fields[i].name()));
    }
    return row;
  }

  private Object valueFor(String name) {
    switch (name) {
      case "messageId":
        return UTF8String.fromString(nullToEmpty(current.messageId()));
      case "data":
        return current.data();
      case "attributes":
        return toMapData(current.attributes());
      case "publishTime":
        // TimestampType is stored as microseconds since epoch
        return current.publishTimeMillis() * 1000L;
      case "orderingKey":
        return UTF8String.fromString(nullToEmpty(current.orderingKey()));
      case "ackId":
        return UTF8String.fromString(nullToEmpty(current.ackId()));
      default:
        throw new IllegalArgumentException("Unsupported Pub/Sub column: " + name);
    }
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private static MapData toMapData(Map<String, String> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return new ArrayBasedMapData(
          ArrayData.toArrayData(new Object[0]), ArrayData.toArrayData(new Object[0]));
    }
    Object[] keys = new Object[attributes.size()];
    Object[] values = new Object[attributes.size()];
    int i = 0;
    for (Map.Entry<String, String> e : attributes.entrySet()) {
      keys[i] = UTF8String.fromString(e.getKey());
      values[i] = UTF8String.fromString(e.getValue() == null ? "" : e.getValue());
      i++;
    }
    return new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values));
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
