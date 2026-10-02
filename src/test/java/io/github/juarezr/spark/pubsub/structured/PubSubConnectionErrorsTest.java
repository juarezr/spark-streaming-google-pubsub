package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class PubSubConnectionErrorsTest {

  @Test
  void statusRuntimeNotFoundIsFatal() {
    assertTrue(
        PubSubConnectionErrors.isFatal(
            new StatusRuntimeException(Status.NOT_FOUND.withDescription("subscription"))));
  }

  @Test
  void statusRuntimePermissionDeniedIsFatal() {
    assertTrue(
        PubSubConnectionErrors.isFatal(
            new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("denied"))));
  }

  @Test
  void genericRuntimeIsNotFatal() {
    assertFalse(PubSubConnectionErrors.isFatal(new RuntimeException("timeout")));
  }
}
