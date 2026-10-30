package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class IntakePaceTest {

  @Test
  void risingAgeWarnsOnceAtTheHour() {
    IntakePace pace = new IntakePace();
    long trigger = 60_000L;

    assertNull(pace.observe(0L, 1_000L, trigger));
    assertNull(pace.observe(IntakePace.HOUR_MILLIS - 1, 2_000L, trigger));

    String warning = pace.observe(IntakePace.HOUR_MILLIS, 5_000L, trigger);

    assertTrue(warning.contains("rose"));
    assertTrue(warning.contains("1000ms"));
    assertTrue(warning.contains("5000ms"));
    assertNull(pace.observe(IntakePace.HOUR_MILLIS + 1, 9_000L, trigger));
  }

  @Test
  void flatAgeUnderTheTriggerDoesNotWarn() {
    IntakePace pace = new IntakePace();

    assertNull(pace.observe(0L, 1_000L, 60_000L));
    assertNull(pace.observe(IntakePace.HOUR_MILLIS, 1_000L, 60_000L));
  }

  @Test
  void ageThatStaysAboveTheTriggerWarnsOnce() {
    IntakePace pace = new IntakePace();
    long trigger = 60_000L;

    assertNull(pace.observe(0L, 120_000L, trigger));
    String warning = pace.observe(IntakePace.HOUR_MILLIS, 90_000L, trigger);

    assertTrue(warning.contains("stayed above"));
    assertNull(pace.observe(IntakePace.HOUR_MILLIS + 60_000L, 90_000L, trigger));
  }
}
