package io.github.juarezr.spark.pubsub.structured;

import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Filter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * The Pub/Sub client logs a stack when a deadline timer fires on a subscriber that is already
 * shutting down. That race does not fail the gather. Other dispatcher warnings stay.
 */
final class MessageDispatcherLog {

  static final String LOGGER_NAME = "com.google.cloud.pubsub.v1.MessageDispatcher";

  private static final String PERIODIC_JOB = "failed to run periodic job";

  private static final Filter FILTER = MessageDispatcherLog::keep;

  private MessageDispatcherLog() {}

  static void install() {
    Logger.getLogger(LOGGER_NAME).setFilter(FILTER);
  }

  /** {@code false} drops the record. A shutdown rejection is the only drop. */
  static boolean keep(LogRecord record) {
    if (record == null || !PERIODIC_JOB.equals(record.getMessage())) {
      return true;
    }
    Throwable thrown = record.getThrown();
    if (!(thrown instanceof RejectedExecutionException)) {
      return true;
    }
    String detail = thrown.getMessage();
    return detail == null || !detail.contains("Shutting down");
  }
}
