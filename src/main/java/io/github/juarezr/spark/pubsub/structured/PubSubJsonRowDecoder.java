package io.github.juarezr.spark.pubsub.structured;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/** Maps a JSON payload onto Spark field values. */
final class PubSubJsonRowDecoder {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PubSubJsonRowDecoder() {}

  static JsonNode parse(byte[] body) {
    if (body == null || body.length == 0) {
      return null;
    }
    try {
      return MAPPER.readTree(body);
    } catch (IOException e) {
      throw new IllegalArgumentException("Payload is not valid JSON", e);
    }
  }

  static Object value(JsonNode root, String fieldName, DataType dataType) {
    if (root == null || root.isNull()) {
      return null;
    }
    JsonNode node = find(root, fieldName);
    return convert(node, dataType);
  }

  private static JsonNode find(JsonNode root, String fieldName) {
    if (root.has(fieldName)) {
      return root.get(fieldName);
    }
    String wanted = fieldName.toLowerCase(Locale.ROOT);
    Iterator<String> names = root.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      if (wanted.equals(name.toLowerCase(Locale.ROOT))) {
        return root.get(name);
      }
    }
    return null;
  }

  private static Object convert(JsonNode node, DataType dataType) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return null;
    }
    if (dataType == DataTypes.StringType) {
      return UTF8String.fromString(node.isValueNode() ? node.asText() : node.toString());
    }
    if (dataType == DataTypes.BinaryType) {
      if (node.isBinary()) {
        try {
          return node.binaryValue();
        } catch (IOException e) {
          throw new IllegalArgumentException("Invalid JSON bytes", e);
        }
      }
      return node.asText("").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    if (dataType == DataTypes.BooleanType) {
      return node.asBoolean();
    }
    if (dataType == DataTypes.IntegerType) {
      return node.asInt();
    }
    if (dataType == DataTypes.LongType) {
      return node.asLong();
    }
    if (dataType == DataTypes.FloatType) {
      return (float) node.asDouble();
    }
    if (dataType == DataTypes.DoubleType) {
      return node.asDouble();
    }
    if (dataType == DataTypes.TimestampType) {
      return toMicros(node);
    }
    if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      return Decimal.apply(node.decimalValue(), decimalType.precision(), decimalType.scale());
    }
    if (dataType instanceof StructType) {
      return toRow(node, (StructType) dataType);
    }
    if (dataType instanceof ArrayType) {
      return toArray(node, (ArrayType) dataType);
    }
    if (dataType instanceof MapType) {
      return toMap(node, (MapType) dataType);
    }
    throw new IllegalArgumentException("Unsupported JSON field type: " + dataType);
  }

  private static GenericInternalRow toRow(JsonNode node, StructType schema) {
    GenericInternalRow row = new GenericInternalRow(schema.length());
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      row.update(i, value(node, fields[i].name(), fields[i].dataType()));
    }
    return row;
  }

  private static ArrayData toArray(JsonNode node, ArrayType arrayType) {
    if (!node.isArray()) {
      return ArrayData.toArrayData(new Object[] {convert(node, arrayType.elementType())});
    }
    Object[] values = new Object[node.size()];
    for (int i = 0; i < node.size(); i++) {
      values[i] = convert(node.get(i), arrayType.elementType());
    }
    return ArrayData.toArrayData(values);
  }

  private static ArrayBasedMapData toMap(JsonNode node, MapType mapType) {
    if (!node.isObject()) {
      return new ArrayBasedMapData(
          ArrayData.toArrayData(new Object[0]), ArrayData.toArrayData(new Object[0]));
    }
    List<Object> keys = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      keys.add(convert(MAPPER.getNodeFactory().textNode(entry.getKey()), mapType.keyType()));
      values.add(convert(entry.getValue(), mapType.valueType()));
    }
    return new ArrayBasedMapData(
        ArrayData.toArrayData(keys.toArray()), ArrayData.toArrayData(values.toArray()));
  }

  private static long toMicros(JsonNode node) {
    if (node.isNumber()) {
      long value = node.asLong();
      // Values that look like epoch millis (13+ digits) are converted to microseconds.
      return value >= 1_000_000_000_000L ? value * 1000L : value * 1_000_000L;
    }
    Instant instant = Instant.parse(node.asText());
    return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
  }
}
