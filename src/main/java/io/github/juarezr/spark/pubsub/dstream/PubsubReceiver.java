package io.github.juarezr.spark.pubsub.dstream;

import io.github.juarezr.spark.pubsub.client.AckCoordinator;
import io.github.juarezr.spark.pubsub.client.PubSubClient;
import io.github.juarezr.spark.pubsub.client.PulledMessage;
import io.github.juarezr.spark.pubsub.config.AckMode;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Receiver that pulls Google Pub/Sub messages into a Spark Streaming DStream. */
public final class PubsubReceiver extends Receiver<SparkPubsubMessage> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PubsubReceiver.class);

  private final PubSubConfig config;
  private final boolean autoAcknowledge;
  private transient Thread worker;
  private transient AtomicBoolean running;
  private transient PubSubClient client;
  private transient AckCoordinator ackCoordinator;
  private transient AtomicLong batchCounter;

  public PubsubReceiver(PubSubConfig config, boolean autoAcknowledge, StorageLevel storageLevel) {
    super(storageLevel);
    this.config = config;
    this.autoAcknowledge = autoAcknowledge;
  }

  @Override
  public void onStart() {
    running = new AtomicBoolean(true);
    batchCounter = new AtomicLong(0);
    AckMode mode = autoAcknowledge ? config.ackMode() : AckMode.AFTER_COMMIT;
    // When autoAcknowledge is false, never ack inside the receiver.
    PubSubConfig effective =
        PubSubConfig.builder()
            .projectId(config.projectId())
            .subscription(config.subscription())
            .topic(config.topic().orElse(null))
            .ackMode(autoAcknowledge ? mode : AckMode.AFTER_COMMIT)
            .maxMessagesPerPull(config.maxMessagesPerPull())
            .maxBytesOutstanding(config.maxBytesOutstanding())
            .ackDeadlineSeconds(config.ackDeadlineSeconds())
            .pullTimeoutSeconds(config.pullTimeoutSeconds())
            .seekMode(config.seekMode())
            .seekTime(config.seekTime().orElse(null))
            .seekSnapshot(config.seekSnapshot().orElse(null))
            .credentialsFile(config.credentialsFile().orElse(null))
            .emulatorHost(config.emulatorHost().orElse(null))
            .returnImmediately(config.returnImmediately())
            .build();

    client = new PubSubClient(effective);
    ackCoordinator =
        new AckCoordinator(autoAcknowledge ? effective.ackMode() : AckMode.AFTER_COMMIT);
    try {
      client.start();
    } catch (Exception e) {
      restart("Failed to start Pub/Sub client", e);
      return;
    }
    worker = new Thread(this::receiveLoop, "pubsub-receiver-" + config.subscription());
    worker.setDaemon(true);
    worker.start();
  }

  private void receiveLoop() {
    try {
      while (running.get() && !isStopped()) {
        List<PulledMessage> pulled = client.pull();
        if (pulled.isEmpty()) {
          continue;
        }
        String batchId = Long.toString(batchCounter.getAndIncrement());
        ackCoordinator.registerBatch(batchId, pulled);

        List<SparkPubsubMessage> sparkMessages =
            pulled.stream().map(this::toSparkMessage).collect(Collectors.toList());
        store(sparkMessages.iterator());

        if (autoAcknowledge) {
          if (config.ackMode() == AckMode.EARLY) {
            ackCoordinator.onPulled(client, batchId);
          } else {
            // afterCommit for DStreams: ack after successful store (receiver WAL boundary)
            ackCoordinator.commit(client, batchId);
          }
        } else {
          // Leave ack ids on messages for manual handling; release tracking only
          ackCoordinator.clear();
        }
      }
    } catch (Throwable t) {
      if (running.get() && !isStopped()) {
        restart("Error receiving Pub/Sub messages", t);
      }
    }
  }

  private SparkPubsubMessage toSparkMessage(PulledMessage m) {
    return new SparkPubsubMessage(
        m.data(),
        m.attributes(),
        m.messageId(),
        Instant.ofEpochMilli(m.publishTimeMillis()).toString(),
        m.ackId(),
        m.orderingKey());
  }

  @Override
  public void onStop() {
    if (running != null) {
      running.set(false);
    }
    if (worker != null) {
      try {
        worker.join(5000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (client != null) {
      client.close();
    }
    if (ackCoordinator != null) {
      ackCoordinator.clear();
    }
    LOG.info("Pub/Sub receiver stopped for {}", config.subscriptionPath());
  }
}
