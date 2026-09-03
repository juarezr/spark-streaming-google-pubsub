package io.github.juarezr.spark.pubsub.structured;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/** Factory that creates {@link PubSubPartitionReader} instances. */
final class PubSubPartitionReaderFactory implements PartitionReaderFactory {

  private static final long serialVersionUID = -629855020L;

  private final StructType readSchema;

  PubSubPartitionReaderFactory(StructType readSchema) {
    this.readSchema = readSchema == null ? PubSubSchema.SCHEMA : readSchema;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return new PubSubPartitionReader((PubSubInputPartition) partition, readSchema);
  }
}
