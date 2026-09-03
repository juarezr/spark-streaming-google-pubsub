package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class PubSubAvroSchemaMapperTest {

  @Test
  void mapsRecordWithNullableUnion() {
    String avro =
        "{"
            + "\"type\":\"record\",\"name\":\"Event\","
            + "\"fields\":["
            + "{\"name\":\"deviceid\",\"type\":\"string\"},"
            + "{\"name\":\"eventtime\",\"type\":\"long\"},"
            + "{\"name\":\"label\",\"type\":[\"null\",\"string\"],\"default\":null}"
            + "]}";
    StructType schema = PubSubAvroSchemaMapper.toStructType(avro);
    assertEquals(3, schema.length());
    assertEquals(DataTypes.StringType, schema.apply("deviceid").dataType());
    assertEquals(DataTypes.LongType, schema.apply("eventtime").dataType());
    assertTrue(schema.apply("label").nullable());
  }

  @Test
  void rejectsNonRecordRoot() {
    assertThrows(
        IllegalArgumentException.class, () -> PubSubAvroSchemaMapper.toStructType("\"string\""));
  }
}
