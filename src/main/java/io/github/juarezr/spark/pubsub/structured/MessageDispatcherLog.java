package io.github.juarezr.spark.pubsub.structured;

import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Filter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.AbstractFilter;

/**
 * Drops the stack a subscriber logs while it is already shutting down. Spark prints that stack
 * through log4j, so the filter is installed there. Other warnings from these loggers stay.
 */
final class MessageDispatcherLog {

  static final String DISPATCHER = "com.google.cloud.pubsub.v1.MessageDispatcher";

  static final String DEFAULT_PROMISE =
      "io.grpc.netty.shaded.io.netty.util.concurrent.DefaultPromise";

  static final String CANCEL_RUNNABLE = "io.grpc.internal.LogExceptionRunnable";

  private static final String PERIODIC_JOB = "failed to run periodic job";

  private static final String OPERATION_COMPLETE = ".operationComplete()";

  private static final String EXECUTING_RUNNABLE = "Exception while executing runnable";

  private static final String[] LOGGER_NAMES = {DISPATCHER, DEFAULT_PROMISE, CANCEL_RUNNABLE};

  private static final Filter JUL_FILTER = MessageDispatcherLog::keep;

  private MessageDispatcherLog() {}

  static void install() {
    for (String name : LOGGER_NAMES) {
      Logger.getLogger(name).setFilter(JUL_FILTER);
    }
    installLog4j();
  }

  /** {@code false} drops the record. A shutdown rejection is the only drop. */
  static boolean keep(LogRecord record) {
    if (record == null) {
      return true;
    }
    return !drop(record.getMessage(), record.getThrown());
  }

  static boolean drop(String message, Throwable thrown) {
    if (!shutdownMessage(message) || !shutdownRejection(thrown)) {
      return false;
    }
    return true;
  }

  private static boolean shutdownMessage(String message) {
    if (message == null) {
      return false;
    }
    return PERIODIC_JOB.equals(message)
        || message.contains(OPERATION_COMPLETE)
        || message.startsWith(EXECUTING_RUNNABLE);
  }

  private static boolean shutdownRejection(Throwable thrown) {
    Throwable current = thrown;
    while (current != null) {
      if (current instanceof RejectedExecutionException) {
        String detail = current.getMessage();
        if (detail != null && (detail.contains("Shutting down") || detail.contains("Terminated"))) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  private static void installLog4j() {
    try {
      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
      Configuration config = ctx.getConfiguration();
      if (config == null) {
        return;
      }
      for (String name : LOGGER_NAMES) {
        Log4jFilter filter = new Log4jFilter();
        filter.start();
        LoggerConfig existing = config.getLoggerConfig(name);
        if (!name.equals(existing.getName())) {
          LoggerConfig created = new LoggerConfig(name, existing.getLevel(), true);
          created.addFilter(filter);
          config.addLogger(name, created);
        } else {
          existing.addFilter(filter);
        }
      }
      ctx.updateLoggers();
    } catch (Exception | LinkageError ignored) {
      // A unit test without Spark's log4j still has the JUL filter.
    }
  }

  static final class Log4jFilter extends AbstractFilter {

    @Override
    public Result filter(LogEvent event) {
      if (event == null || event.getMessage() == null) {
        return Result.NEUTRAL;
      }
      if (drop(event.getMessage().getFormattedMessage(), event.getThrown())) {
        return Result.DENY;
      }
      return Result.NEUTRAL;
    }
  }
}
