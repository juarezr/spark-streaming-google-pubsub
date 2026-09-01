package io.github.juarezr.spark.pubsub.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AckLeaseWatchdogTest {

  @Test
  void extendIntervalIsOneThirdOfDeadlineAtLeastOneSecond() {
    assertEquals(1, AckLeaseWatchdog.extendIntervalSeconds(1));
    assertEquals(1, AckLeaseWatchdog.extendIntervalSeconds(2));
    assertEquals(20, AckLeaseWatchdog.extendIntervalSeconds(60));
    assertEquals(200, AckLeaseWatchdog.extendIntervalSeconds(600));
  }

  @Test
  void stopIsIdempotent() {
    try (AckLeaseWatchdog watchdog = new AckLeaseWatchdog()) {
      assertDoesNotThrow(watchdog::stop);
      assertDoesNotThrow(watchdog::stop);
    }
  }
}
