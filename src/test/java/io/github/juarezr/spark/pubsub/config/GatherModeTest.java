package io.github.juarezr.spark.pubsub.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GatherModeTest {

  @Test
  void fromStringAcceptsBatchAndPullCaseInsensitive() {
    assertEquals(GatherMode.BATCH, GatherMode.fromString("batch"));
    assertEquals(GatherMode.BATCH, GatherMode.fromString("BATCH"));
    assertEquals(GatherMode.PULL, GatherMode.fromString("pull"));
    assertEquals(GatherMode.PULL, GatherMode.fromString("  Pull  "));
  }

  @Test
  void fromStringDefaultsBlankToBatch() {
    assertEquals(GatherMode.BATCH, GatherMode.fromString(null));
    assertEquals(GatherMode.BATCH, GatherMode.fromString(""));
    assertEquals(GatherMode.BATCH, GatherMode.fromString("   "));
  }

  @Test
  void fromStringRejectsUnknownValues() {
    assertThrows(IllegalArgumentException.class, () -> GatherMode.fromString("streaming"));
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Map<String, String> options = new HashMap<>();
          options.put("projectId", "p");
          options.put("subscription", "s");
          options.put("gatherMode", "batch-and-pull");
          PubSubConfig.fromOptions(options);
        });
  }
}
