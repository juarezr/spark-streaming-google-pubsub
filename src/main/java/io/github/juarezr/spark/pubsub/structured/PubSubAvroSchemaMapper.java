package io.github.juarezr.spark.pubsub.structured;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Converts a Pub/Sub Avro schema definition (JSON) into a Spark {@link StructType}. */
final class PubSubAvroSchemaMapper {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PubSubAvroSchemaMapper() {}

  static StructType toStructType(String avroDefinition) {
    if (avroDefinition == null || avroDefinition.isBlank()) {
      throw new IllegalArgumentException("Avro schema definition must not be blank");
    }
    try {
      JsonNode root = MAPPER.readTree(avroDefinition);
      DataType type = toDataType(root);
      if (!(type instanceof StructType)) {
        throw new IllegalArgumentException("Avro schema root must be a record");
      }
      return (StructType) type;
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid Avro schema JSON", e);
    }
  }

  private static DataType toDataType(JsonNode node) {
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException("Avro type must not be null");
    }
    if (node.isTextual()) {
      return primitive(node.asText());
    }
    if (node.isArray()) {
      return unionType(node);
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("Unsupported Avro type node: " + node);
    }
    if (node.has("type") && node.get("type").isTextual()) {
      String type = node.get("type").asText();
      if ("record".equals(type)) {
        return recordType(node);
      }
      if ("array".equals(type)) {
        return DataTypes.createArrayType(toDataType(node.get("items")), true);
      }
      if ("map".equals(type)) {
        return DataTypes.createMapType(DataTypes.StringType, toDataType(node.get("values")), true);
      }
      if ("enum".equals(type)) {
        return DataTypes.StringType;
      }
      return primitive(type);
    }
    if (node.has("type")) {
      return toDataType(node.get("type"));
    }
    throw new IllegalArgumentException("Unsupported Avro type: " + node);
  }

  private static StructType recordType(JsonNode node) {
    JsonNode fields = node.get("fields");
    if (fields == null || !fields.isArray()) {
      throw new IllegalArgumentException("Avro record must have fields");
    }
    List<StructField> sparkFields = new ArrayList<>();
    for (JsonNode field : fields) {
      String name = field.path("name").asText(null);
      if (name == null || name.isBlank()) {
        throw new IllegalArgumentException("Avro field is missing a name");
      }
      boolean nullable = isNullable(field.get("type"));
      DataType dataType = unwrapUnion(field.get("type"));
      sparkFields.add(new StructField(name, dataType, nullable, Metadata.empty()));
    }
    return new StructType(sparkFields.toArray(new StructField[0]));
  }

  private static DataType unionType(JsonNode union) {
    return unwrapUnion(union);
  }

  private static boolean isNullable(JsonNode type) {
    if (type == null || !type.isArray()) {
      return false;
    }
    for (JsonNode member : type) {
      if (member.isTextual() && "null".equals(member.asText())) {
        return true;
      }
    }
    return false;
  }

  private static DataType unwrapUnion(JsonNode type) {
    if (type != null && type.isArray()) {
      JsonNode nonNull = null;
      int nonNullCount = 0;
      for (JsonNode member : type) {
        if (member.isTextual() && "null".equals(member.asText())) {
          continue;
        }
        nonNull = member;
        nonNullCount++;
      }
      if (nonNullCount != 1) {
        throw new IllegalArgumentException("Only Avro unions of [null, T] are supported: " + type);
      }
      return toDataType(nonNull);
    }
    return toDataType(type);
  }

  private static DataType primitive(String type) {
    switch (type) {
      case "string":
        return DataTypes.StringType;
      case "boolean":
        return DataTypes.BooleanType;
      case "int":
        return DataTypes.IntegerType;
      case "long":
        return DataTypes.LongType;
      case "float":
        return DataTypes.FloatType;
      case "double":
        return DataTypes.DoubleType;
      case "bytes":
        return DataTypes.BinaryType;
      case "null":
        throw new IllegalArgumentException("Avro type null is not a standalone column type");
      default:
        throw new IllegalArgumentException("Unsupported Avro type '" + type + "'");
    }
  }
}
