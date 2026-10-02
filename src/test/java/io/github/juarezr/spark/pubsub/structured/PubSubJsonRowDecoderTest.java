package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

class PubSubJsonRowDecoderTest {

  @Test
  void parseRejectsInvalidJson() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubJsonRowDecoder.parse("{not-json".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void parseReturnsNullForEmptyBody() {
    assertNull(PubSubJsonRowDecoder.parse(null));
    assertNull(PubSubJsonRowDecoder.parse(new byte[0]));
  }

  @Test
  void convertsPrimitives() {
    JsonNode root =
        PubSubJsonRowDecoder.parse(
            "{\"s\":\"x\",\"b\":true,\"i\":7,\"l\":9,\"f\":1.5,\"d\":2.5}"
                .getBytes(StandardCharsets.UTF_8));
    assertEquals(
        UTF8String.fromString("x"), PubSubJsonRowDecoder.value(root, "s", DataTypes.StringType));
    assertEquals(true, PubSubJsonRowDecoder.value(root, "b", DataTypes.BooleanType));
    assertEquals(7, PubSubJsonRowDecoder.value(root, "i", DataTypes.IntegerType));
    assertEquals(9L, PubSubJsonRowDecoder.value(root, "l", DataTypes.LongType));
    assertEquals(1.5f, PubSubJsonRowDecoder.value(root, "f", DataTypes.FloatType));
    assertEquals(2.5d, PubSubJsonRowDecoder.value(root, "d", DataTypes.DoubleType));
  }

  @Test
  void convertsDecimalStructArrayAndMap() {
    JsonNode root =
        PubSubJsonRowDecoder.parse(
            ("{\"amount\":3.14,\"nested\":{\"id\":\"n1\"},\"tags\":[\"a\",\"b\"],"
                    + "\"stats\":{\"rebounds\":2}}")
                .getBytes(StandardCharsets.UTF_8));
    Decimal decimal = (Decimal) PubSubJsonRowDecoder.value(root, "amount", new DecimalType(10, 2));
    assertEquals(Decimal.apply("3.14"), decimal);

    GenericInternalRow nested =
        (GenericInternalRow)
            PubSubJsonRowDecoder.value(
                root,
                "nested",
                new StructType(
                    new StructField[] {
                      new StructField("id", DataTypes.StringType, true, Metadata.empty())
                    }));
    assertEquals("n1", nested.getUTF8String(0).toString());

    ArrayData tags =
        (ArrayData)
            PubSubJsonRowDecoder.value(
                root, "tags", DataTypes.createArrayType(DataTypes.StringType));
    assertEquals(2, tags.numElements());

    ArrayBasedMapData stats =
        (ArrayBasedMapData)
            PubSubJsonRowDecoder.value(
                root,
                "stats",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType));
    assertNotNull(stats);
    assertTrue(stats.numElements() >= 1);
  }

  @Test
  void convertsTimestampFromIsoAndEpochMillis() {
    JsonNode iso =
        PubSubJsonRowDecoder.parse(
            "{\"t\":\"2024-01-15T10:30:00Z\"}".getBytes(StandardCharsets.UTF_8));
    long isoMicros = (long) PubSubJsonRowDecoder.value(iso, "t", DataTypes.TimestampType);
    assertTrue(isoMicros > 0);

    JsonNode millis =
        PubSubJsonRowDecoder.parse("{\"t\":1705312200000}".getBytes(StandardCharsets.UTF_8));
    long millisMicros = (long) PubSubJsonRowDecoder.value(millis, "t", DataTypes.TimestampType);
    assertTrue(millisMicros > isoMicros / 2);
  }

  @Test
  void fieldLookupIsCaseInsensitive() {
    JsonNode root =
        PubSubJsonRowDecoder.parse("{\"DeviceId\":\"abc\"}".getBytes(StandardCharsets.UTF_8));
    assertEquals(
        UTF8String.fromString("abc"),
        PubSubJsonRowDecoder.value(root, "deviceid", DataTypes.StringType));
  }

  @Test
  void missingFieldReturnsNull() {
    JsonNode root = PubSubJsonRowDecoder.parse("{}".getBytes(StandardCharsets.UTF_8));
    assertNull(PubSubJsonRowDecoder.value(root, "missing", DataTypes.StringType));
  }

  @Test
  void rejectsUnsupportedType() {
    JsonNode root = PubSubJsonRowDecoder.parse("{\"x\":1}".getBytes(StandardCharsets.UTF_8));
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubJsonRowDecoder.value(root, "x", DataTypes.DateType));
  }
}
