package io.github.juarezr.spark.pubsub.structured;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter.Result;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

class MessageDispatcherLogTest {

  @Test
  void dropsOnlyAShuttingDownRejection() {
    assertFalse(MessageDispatcherLog.keep(shutdownRejection()));
    assertFalse(MessageDispatcherLog.keep(terminatedPromise()));
    assertFalse(MessageDispatcherLog.keep(terminatedCancel()));
    assertTrue(MessageDispatcherLog.keep(otherRejection()));
    assertTrue(MessageDispatcherLog.keep(otherFailure()));
    assertTrue(MessageDispatcherLog.keep(otherMessage()));
    assertTrue(MessageDispatcherLog.keep(null));
  }

  @Test
  void installAttachesTheJulFilter() {
    MessageDispatcherLog.install();
    Logger logger = Logger.getLogger(MessageDispatcherLog.DISPATCHER);
    assertFalse(logger.getFilter().isLoggable(shutdownRejection()));
    assertTrue(logger.getFilter().isLoggable(otherFailure()));
    Logger promise = Logger.getLogger(MessageDispatcherLog.DEFAULT_PROMISE);
    assertFalse(promise.getFilter().isLoggable(terminatedPromise()));
  }

  @Test
  void log4jDropsTheShutdownStackAndKeepsOtherWarnings() {
    MessageDispatcherLog.install();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
    org.apache.logging.log4j.core.config.LoggerConfig loggerConfig =
        config.getLoggerConfig(MessageDispatcherLog.DISPATCHER);
    if (!MessageDispatcherLog.DISPATCHER.equals(loggerConfig.getName())) {
      loggerConfig =
          new org.apache.logging.log4j.core.config.LoggerConfig(
              MessageDispatcherLog.DISPATCHER, org.apache.logging.log4j.Level.ALL, true);
      config.addLogger(MessageDispatcherLog.DISPATCHER, loggerConfig);
    }
    CountingAppender appender = new CountingAppender();
    appender.start();
    loggerConfig.addAppender(appender, org.apache.logging.log4j.Level.ALL, null);
    loggerConfig.setLevel(org.apache.logging.log4j.Level.ALL);
    ctx.updateLoggers();
    org.apache.logging.log4j.Logger logger = LogManager.getLogger(MessageDispatcherLog.DISPATCHER);
    try {
      logger.warn(
          "failed to run periodic job",
          new RejectedExecutionException(
              "rejected from ScheduledThreadPoolExecutor@[Shutting down, pool size = 1]"));
      logger.warn("failed to run periodic job", new RejectedExecutionException("queue full"));
      logger.warn("nack failed", new IllegalStateException("deadline extension failed"));
      assertEquals(2, appender.count);
    } finally {
      loggerConfig.removeAppender(appender.getName());
    }
    MessageDispatcherLog.Log4jFilter filter = new MessageDispatcherLog.Log4jFilter();
    filter.start();
    assertEquals(Result.DENY, filter.filter(logEvent(terminatedCancel())));
    assertEquals(Result.NEUTRAL, filter.filter(logEvent(otherFailure())));
  }

  private static LogEvent logEvent(LogRecord record) {
    return org.apache.logging.log4j.core.impl.Log4jLogEvent.newBuilder()
        .setMessage(new org.apache.logging.log4j.message.SimpleMessage(record.getMessage()))
        .setThrown(record.getThrown())
        .build();
  }

  private static LogRecord shutdownRejection() {
    LogRecord record = new LogRecord(Level.WARNING, "failed to run periodic job");
    record.setThrown(
        new RejectedExecutionException(
            "Task rejected from ScheduledThreadPoolExecutor@[Shutting down, pool size = 1]"));
    return record;
  }

  private static LogRecord terminatedPromise() {
    LogRecord record =
        new LogRecord(
            Level.WARNING, "An exception was thrown by NettyClientStream$Sink.operationComplete()");
    record.setThrown(
        new RejectedExecutionException(
            "rejected from ScheduledThreadPoolExecutor@[Terminated, pool size = 0]"));
    return record;
  }

  private static LogRecord terminatedCancel() {
    LogRecord record =
        new LogRecord(
            Level.SEVERE, "Exception while executing runnable ClientCallImpl$CancellationHandler");
    record.setThrown(
        new StatusRuntimeException(
            Status.UNKNOWN.withCause(
                new RejectedExecutionException(
                    "rejected from ScheduledThreadPoolExecutor@[Terminated, pool size = 0]"))));
    return record;
  }

  private static LogRecord otherRejection() {
    LogRecord record = new LogRecord(Level.WARNING, "failed to run periodic job");
    record.setThrown(new RejectedExecutionException("queue full"));
    return record;
  }

  private static LogRecord otherFailure() {
    LogRecord record = new LogRecord(Level.WARNING, "failed to run periodic job");
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

  private static final class CountingAppender extends AbstractAppender {

    private int count;

    CountingAppender() {
      super("shutdown-count", null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      count++;
    }
  }
}
