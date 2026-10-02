package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.juarezr.spark.pubsub.config.MetadataMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SchemaMode;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PubSubSchemaDynamicAndMixedTest {

  private static final StructType SAMPLE_PAYLOAD =
      new StructType(
          new StructField[] {
            new StructField("deviceid", DataTypes.StringType, false, Metadata.empty())
          });

  @ParameterizedTest
  @MethodSource("tableSchemaCases")
  void tableSchemaMatchesSchemaMode(
      SchemaMode mode, String[] expectedFields, boolean needsPayload) {
    StructType payload = needsPayload ? SAMPLE_PAYLOAD : null;
    StructType table = PubSubSchema.tableSchema(config(mode, MetadataMode.NONE), payload);
    assertArrayEquals(expectedFields, table.fieldNames());
  }

  static Stream<Arguments> tableSchemaCases() {
    return Stream.of(
        Arguments.of(SchemaMode.RAW, new String[] {"body"}, false),
        Arguments.of(SchemaMode.BASIC, new String[] {"body", "messageid", "publishtime"}, false),
        Arguments.of(
            SchemaMode.SLIM,
            new String[] {"body", "messageid", "publishtime", "orderingkey"},
            false),
        Arguments.of(SchemaMode.DYNAMIC, new String[] {"deviceid"}, true),
        Arguments.of(
            SchemaMode.MIXED, new String[] {"deviceid", "messageid", "publishtime"}, true));
  }

  @Test
  void dynamicAndMixedDecodePayload() {
    assertTrue(SchemaMode.DYNAMIC.decodesPayload());
    assertTrue(SchemaMode.MIXED.decodesPayload());
    assertTrue(!SchemaMode.BASIC.decodesPayload());
  }

  @ParameterizedTest
  @MethodSource("metadataMatrixCases")
  void metadataColumnsSubtractTableFields(
      SchemaMode schemaMode,
      MetadataMode metadataMode,
      String[] expectedMeta,
      boolean withPayload) {
    PubSubConfig cfg = config(schemaMode, metadataMode);
    StructType payload = withPayload ? SAMPLE_PAYLOAD : null;
    StructType table = PubSubSchema.tableSchema(cfg, payload);
    assertArrayEquals(expectedMeta, names(PubSubSchema.metadataColumns(cfg, table)));
  }

  static Stream<Arguments> metadataMatrixCases() {
    return Stream.of(
        Arguments.of(SchemaMode.BASIC, MetadataMode.NONE, new String[] {}, false),
        Arguments.of(SchemaMode.BASIC, MetadataMode.BASIC, new String[] {}, false),
        Arguments.of(
            SchemaMode.BASIC, MetadataMode.SLIM, new String[] {"orderingkey", "ackid"}, false),
        Arguments.of(
            SchemaMode.BASIC,
            MetadataMode.FULL,
            new String[] {"orderingkey", "ackid", "attributes"},
            false),
        Arguments.of(
            SchemaMode.RAW, MetadataMode.BASIC, new String[] {"messageid", "publishtime"}, false),
        Arguments.of(
            SchemaMode.RAW,
            MetadataMode.SLIM,
            new String[] {"messageid", "publishtime", "orderingkey", "ackid"},
            false),
        Arguments.of(
            SchemaMode.RAW,
            MetadataMode.FULL,
            new String[] {"messageid", "publishtime", "orderingkey", "ackid", "attributes"},
            false),
        Arguments.of(SchemaMode.SLIM, MetadataMode.SLIM, new String[] {"ackid"}, false),
        Arguments.of(
            SchemaMode.DYNAMIC,
            MetadataMode.BASIC,
            new String[] {"messageid", "publishtime"},
            true),
        Arguments.of(SchemaMode.MIXED, MetadataMode.BASIC, new String[] {}, true));
  }

  @Test
  void dynamicRequiresPayload() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PubSubSchema.tableSchema(config(SchemaMode.DYNAMIC, MetadataMode.NONE), null));
  }

  private static String[] names(MetadataColumn[] columns) {
    return Arrays.stream(columns).map(column -> column.name()).toArray(String[]::new);
  }

  private static PubSubConfig config(SchemaMode schemaMode, MetadataMode metadataMode) {
    return PubSubConfig.builder()
        .projectId("p")
        .subscription("s")
        .schemaMode(schemaMode)
        .metadataMode(metadataMode)
        .build();
  }
}
