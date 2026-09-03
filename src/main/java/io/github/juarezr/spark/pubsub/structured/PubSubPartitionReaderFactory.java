package io.github.juarezr.spark.pubsub.structured;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/** Factory that creates {@link PubSubPartitionReader} instances. */
final class PubSubPartitionReaderFactory implements PartitionReaderFactory {
  private static final long serialVersionUID = 1L;

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return new PubSubPartitionReader((PubSubInputPartition) partition);
  }
}
