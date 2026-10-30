package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class MessageDispatcherLogTest {

  @Test
  void dropsOnlyAShuttingDownRejection() {
    assertFalse(MessageDispatcherLog.keep(shutdownRejection()));
    assertTrue(MessageDispatcherLog.keep(otherRejection()));
    assertTrue(MessageDispatcherLog.keep(otherFailure()));
    assertTrue(MessageDispatcherLog.keep(otherMessage()));
    assertTrue(MessageDispatcherLog.keep(null));
  }

  @Test
  void installAttachesTheFilter() {
    MessageDispatcherLog.install();
    Logger logger = Logger.getLogger(MessageDispatcherLog.LOGGER_NAME);
    assertFalse(logger.getFilter().isLoggable(shutdownRejection()));
    assertTrue(logger.getFilter().isLoggable(otherFailure()));
    assertSame(logger.getFilter(), Logger.getLogger(MessageDispatcherLog.LOGGER_NAME).getFilter());
  }

  private static LogRecord shutdownRejection() {
    LogRecord record = periodicJob();
    record.setThrown(
        new RejectedExecutionException(
            "Task rejected from ScheduledThreadPoolExecutor@[Shutting down, pool size = 1]"));
    return record;
  }

  private static LogRecord otherRejection() {
    LogRecord record = periodicJob();
    record.setThrown(new RejectedExecutionException("queue full"));
    return record;
  }

  private static LogRecord otherFailure() {
    LogRecord record = periodicJob();
    record.setThrown(new IllegalStateException("deadline extension failed"));
    return record;
  }

  private static LogRecord otherMessage() {
    LogRecord record = new LogRecord(Level.WARNING, "nack failed");
    record.setThrown(
        new RejectedExecutionException(
            "Task rejected from ScheduledThreadPoolExecutor@[Shutting down, pool size = 1]"));
    return record;
  }

  private static LogRecord periodicJob() {
    return new LogRecord(Level.WARNING, "failed to run periodic job");
  }
}
