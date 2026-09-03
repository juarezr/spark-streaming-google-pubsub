package io.github.juarezr.spark.pubsub.structured;

import io.github.juarezr.spark.pubsub.config.MetadataMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SchemaMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Table and metadata schemas for {@code schemaMode} / {@code metadataMode}. */
final class PubSubSchema {
  static final String MESSAGEID = "messageid";
  static final String BODY = "body";
  static final String DATA = "data";
  static final String PUBLISHTIME = "publishtime";
  static final String ORDERINGKEY = "orderingkey";
  static final String ACKID = "ackid";
  static final String ATTRIBUTES = "attributes";

  private PubSubSchema() {}

  static StructType inferTableSchema(PubSubConfig config) {
    StructType payload = null;
    if (config.schemaMode().decodesPayload()) {
      payload = PubSubTopicSchema.fetchPayloadSchema(config);
    }
    return tableSchema(config, payload);
  }

  static StructType tableSchema(PubSubConfig config, StructType payload) {
    switch (config.schemaMode()) {
      case RAW:
        return struct(bodyField());
      case BASIC:
        return struct(bodyField(), messageIdField(), publishTimeField());
      case SLIM:
        return struct(bodyField(), messageIdField(), publishTimeField(), orderingKeyField());
      case DYNAMIC:
        return requirePayload(config.schemaMode(), payload);
      case MIXED:
        return appendEnvelope(
            requirePayload(config.schemaMode(), payload), messageIdField(), publishTimeField());
      default:
        throw new IllegalArgumentException("Unknown schemaMode: " + config.schemaMode());
    }
  }

  static MetadataColumn[] metadataColumns(PubSubConfig config, StructType table) {
    Set<String> tableNames = names(table);
    List<MetadataColumn> columns = new ArrayList<>();
    for (StructField field : metadataCandidates(config.metadataMode())) {
      if (!tableNames.contains(field.name())) {
        columns.add(new PubSubMetadataColumn(field.name(), field.dataType(), field.nullable()));
      }
    }
    return columns.toArray(new MetadataColumn[0]);
  }

  static boolean isEnvelopeField(String name) {
    String key = normalize(name);
    return MESSAGEID.equals(key)
        || BODY.equals(key)
        || DATA.equals(key)
        || PUBLISHTIME.equals(key)
        || ORDERINGKEY.equals(key)
        || ACKID.equals(key)
        || ATTRIBUTES.equals(key);
  }

  static String normalize(String name) {
    return name == null ? "" : name.toLowerCase(Locale.ROOT);
  }

  private static List<StructField> metadataCandidates(MetadataMode mode) {
    List<StructField> fields = new ArrayList<>();
    if (mode == MetadataMode.NONE) {
      return fields;
    }
    fields.add(messageIdField());
    fields.add(publishTimeField());
    if (mode == MetadataMode.BASIC) {
      return fields;
    }
    fields.add(orderingKeyField());
    fields.add(ackIdField());
    if (mode == MetadataMode.FULL) {
      fields.add(attributesField());
    }
    return fields;
  }

  private static StructType requirePayload(SchemaMode mode, StructType payload) {
    if (payload == null || payload.isEmpty()) {
      throw new IllegalArgumentException(
          "schemaMode="
              + mode.name().toLowerCase(Locale.ROOT)
              + " requires a topic or user schema");
    }
    return payload;
  }

  private static StructType appendEnvelope(StructType payload, StructField... extras) {
    Set<String> existing = names(payload);
    List<StructField> fields = new ArrayList<>();
    for (StructField field : payload.fields()) {
      fields.add(field);
    }
    for (StructField extra : extras) {
      if (!existing.contains(extra.name())) {
        fields.add(extra);
      }
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  private static Set<String> names(StructType schema) {
    Set<String> names = new HashSet<>();
    if (schema == null) {
      return names;
    }
    for (StructField field : schema.fields()) {
      names.add(normalize(field.name()));
    }
    return names;
  }

  private static StructType struct(StructField... fields) {
    return new StructType(fields);
  }

  private static StructField bodyField() {
    return new StructField(BODY, DataTypes.BinaryType, false, Metadata.empty());
  }

  private static StructField messageIdField() {
    return new StructField(MESSAGEID, DataTypes.StringType, false, Metadata.empty());
  }

  private static StructField publishTimeField() {
    return new StructField(PUBLISHTIME, DataTypes.TimestampType, false, Metadata.empty());
  }

  private static StructField orderingKeyField() {
    return new StructField(ORDERINGKEY, DataTypes.StringType, false, Metadata.empty());
  }

  private static StructField ackIdField() {
    return new StructField(ACKID, DataTypes.StringType, true, Metadata.empty());
  }

  private static StructField attributesField() {
    return new StructField(
        ATTRIBUTES,
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false),
        false,
        Metadata.empty());
  }
}
