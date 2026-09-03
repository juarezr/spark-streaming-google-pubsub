package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.Schema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class PubSubTopicSchemaTest {

  private static final String AVRO =
      "{\"type\":\"record\",\"name\":\"E\",\"fields\":[{\"name\":\"deviceid\",\"type\":\"string\"}]}";

  @Test
  void acceptsJsonAvro() {
    Schema schema = Schema.newBuilder().setType(Schema.Type.AVRO).setDefinition(AVRO).build();
    StructType spark = PubSubTopicSchema.fromTopicSchema(Encoding.JSON, schema);
    assertEquals("deviceid", spark.fieldNames()[0]);
  }

  @Test
  void rejectsBinaryEncoding() {
    Schema schema = Schema.newBuilder().setType(Schema.Type.AVRO).setDefinition(AVRO).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubTopicSchema.fromTopicSchema(Encoding.BINARY, schema));
  }

  @Test
  void rejectsProtobufType() {
    Schema schema =
        Schema.newBuilder()
            .setType(Schema.Type.PROTOCOL_BUFFER)
            .setDefinition("syntax = \"proto3\";")
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubTopicSchema.fromTopicSchema(Encoding.JSON, schema));
  }
}
