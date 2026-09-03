package io.github.juarezr.spark.pubsub.structured;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/** Reads {@link PulledMessage} values into Spark {@link InternalRow}s. */
final class PubSubPartitionReader implements PartitionReader<InternalRow> {
  private final Iterator<PulledMessage> iterator;
  private final StructType readSchema;
  private PulledMessage current;
  private JsonNode payloadJson;

  PubSubPartitionReader(PubSubInputPartition partition, StructType readSchema) {
    this.iterator = partition.messages().iterator();
    this.readSchema = readSchema;
  }

  @Override
  public boolean next() {
    if (iterator.hasNext()) {
      current = iterator.next();
      payloadJson = null;
      return true;
    }
    return false;
  }

  @Override
  public InternalRow get() {
    GenericInternalRow row = new GenericInternalRow(readSchema.length());
    StructField[] fields = readSchema.fields();
    for (int i = 0; i < fields.length; i++) {
      row.update(i, valueFor(fields[i]));
    }
    return row;
  }

  private Object valueFor(StructField field) {
    String key = PubSubSchema.normalize(field.name());
    switch (key) {
      case PubSubSchema.MESSAGEID:
        return UTF8String.fromString(nullToEmpty(current.messageId()));
      case PubSubSchema.BODY:
      case PubSubSchema.DATA:
        return current.data();
      case PubSubSchema.ATTRIBUTES:
        return toMapData(current.attributes());
      case PubSubSchema.PUBLISHTIME:
        return current.publishTimeMillis() * 1000L;
      case PubSubSchema.ORDERINGKEY:
        return UTF8String.fromString(nullToEmpty(current.orderingKey()));
      case PubSubSchema.ACKID:
        return UTF8String.fromString(nullToEmpty(current.ackId()));
      default:
        return payloadValue(field.name(), field.dataType());
    }
  }

  private Object payloadValue(String name, DataType dataType) {
    if (payloadJson == null) {
      payloadJson = PubSubJsonRowDecoder.parse(current.data());
    }
    return PubSubJsonRowDecoder.value(payloadJson, name, dataType);
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
