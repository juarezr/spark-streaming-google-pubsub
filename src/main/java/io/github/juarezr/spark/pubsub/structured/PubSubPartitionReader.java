package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.client.PulledMessage;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;

/** Reads {@link PulledMessage} values into Spark {@link InternalRow}s. */
public final class PubSubPartitionReader implements PartitionReader<InternalRow> {
  private final Iterator<PulledMessage> iterator;
  private PulledMessage current;

  public PubSubPartitionReader(PubSubInputPartition partition) {
    this.iterator = partition.messages().iterator();
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
    GenericInternalRow row = new GenericInternalRow(6);
    row.update(0, UTF8String.fromString(nullToEmpty(current.messageId())));
    row.update(1, current.data());
    row.update(2, toMapData(current.attributes()));
    // TimestampType is stored as microseconds since epoch
    row.setLong(3, current.publishTimeMillis() * 1000L);
    row.update(4, UTF8String.fromString(nullToEmpty(current.orderingKey())));
    row.update(5, UTF8String.fromString(nullToEmpty(current.ackId())));
    return row;
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
