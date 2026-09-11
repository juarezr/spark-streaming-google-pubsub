package io.github.juarezr.spark.pubsub.structured;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically extends Pub/Sub ack deadlines on the driver while a micro-batch is in flight. {@link
 * #stop()} is idempotent.
 */
final class AckLeaseWatchdog implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(AckLeaseWatchdog.class);

  private final Object lock = new Object();
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> future;
  private volatile List<String> ackIds = List.of();

  static int extendIntervalSeconds(int ackDeadlineSeconds) {
    return Math.max(1, ackDeadlineSeconds / 3);
  }

  void start(PubSubClient client, List<String> ackIds, int ackDeadlineSeconds) {
    stop();
    if (client == null || ackIds == null || ackIds.isEmpty() || ackDeadlineSeconds <= 0) {
      return;
    }
    this.ackIds = new ArrayList<>(ackIds);
    int intervalSeconds = extendIntervalSeconds(ackDeadlineSeconds);
    synchronized (lock) {
      this.executor = Executors.newSingleThreadScheduledExecutor(newAckLeaseWatchdogFactory());
      final Runnable extender = newExtendAckDeadlineFactory(client, ackDeadlineSeconds);
      this.future =
          this.executor.scheduleAtFixedRate(
              extender, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }
  }

  private ThreadFactory newAckLeaseWatchdogFactory() {
    return r -> {
      final Thread t = new Thread(r, "pubsub-ack-lease");
      t.setDaemon(true);
      return t;
    };
  }

  private Runnable newExtendAckDeadlineFactory(PubSubClient client, int ackDeadlineSeconds) {
    return () -> extendAckDeadlineBy(client, ackDeadlineSeconds);
  }

  private void extendAckDeadlineBy(PubSubClient client, int ackDeadlineSeconds) {
    final List<String> snapshot = this.ackIds;
    final int total = snapshot.size();
    try {
      client.extendAckDeadline(snapshot, ackDeadlineSeconds);
      LOG.debug("WATCHDOG: Successfully extended ack deadline for {} messages", total);
    } catch (RuntimeException e) {
      LOG.warn("WATCHDOG: Failed to extend ack deadline for {} messages", total, e.getMessage());
    }
  }

  /** Replaces the ids extended by an already-running watchdog. */
  void update(List<String> ackIds) {
    this.ackIds = ackIds == null ? List.of() : new ArrayList<>(ackIds);
  }

  void stop() {
    synchronized (lock) {
      if (future != null) {
        future.cancel(false);
        future = null;
      }
      if (executor != null) {
        executor.shutdownNow();
        executor = null;
      }
      ackIds = List.of();
    }
  }

  @Override
  public void close() {
    stop();
  }
}
