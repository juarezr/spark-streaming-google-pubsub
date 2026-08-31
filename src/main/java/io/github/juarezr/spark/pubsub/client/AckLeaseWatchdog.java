package io.github.juarezr.spark.pubsub.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically extends Pub/Sub ack deadlines on the driver while a micro-batch
 * is in flight. {@link
 * #stop()} is idempotent.
 */
public final class AckLeaseWatchdog implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(AckLeaseWatchdog.class);

  private final Object lock = new Object();
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> future;

  static int extendIntervalSeconds(int ackDeadlineSeconds) {
    return Math.max(1, ackDeadlineSeconds / 3);
  }

  public void start(PubSubClient client, List<String> ackIds, int ackDeadlineSeconds) {
    stop();
    if (client == null || ackIds == null || ackIds.isEmpty() || ackDeadlineSeconds <= 0) {
      return;
    }
    List<String> snapshot = new ArrayList<>(ackIds);
    int intervalSeconds = extendIntervalSeconds(ackDeadlineSeconds);
    synchronized (lock) {
      executor = Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "pubsub-ack-lease");
            t.setDaemon(true);
            return t;
          });
      future = executor.scheduleAtFixedRate(
          () -> {
            try {
              client.extendAckDeadline(snapshot, ackDeadlineSeconds);
            } catch (RuntimeException e) {
              LOG.warn("Failed to extend ack deadline for {} messages", snapshot.size(), e);
            }
          },
          intervalSeconds,
          intervalSeconds,
          TimeUnit.SECONDS);
    }
  }

  public void stop() {
    synchronized (lock) {
      if (future != null) {
        future.cancel(false);
        future = null;
      }
      if (executor != null) {
        executor.shutdownNow();
        executor = null;
      }
    }
  }

  @Override
  public void close() {
    stop();
  }
}
