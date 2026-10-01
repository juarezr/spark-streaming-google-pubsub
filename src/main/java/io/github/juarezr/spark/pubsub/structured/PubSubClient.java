package io.github.juarezr.spark.pubsub.structured;

import com.google.api.core.ApiService;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SeekRequest;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SeekMode;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Warm {@link Subscriber} (StreamingPull) plus a bounded queue. The receiver never acks; Spark
 * commit calls {@link #acknowledge}.
 */
final class PubSubClient implements Closeable, Serializable {

  private static final long serialVersionUID = -3861530486L;
  private static final Logger LOG = LoggerFactory.getLogger(PubSubClient.class);

  /** Pub/Sub Acknowledge / ModifyAckDeadline cap. */
  static final int ACK_CHUNK = 1000;

  private final RetryPolicy retryPolicy;
  private PubSubConnectionErrors connectionErrors;
  private transient PubSubEmulator emulator;

  private final PubSubConfig config;
  private final PubSubCredentialsProvider credentialsProvider;
  private transient Subscriber subscriber;
  private transient LinkedBlockingQueue<HeldMessage> queue;
  private transient ConcurrentHashMap<String, AckReplyConsumer> consumers;
  private transient AtomicLong outstandingBytes;
  private transient AtomicLong queuedBytes;
  private transient AtomicBoolean stopped;
  private transient boolean seekApplied;

  /** Caps the next {@link #poll(Duration)} so overflow stays queued for the following batch. */
  private transient int nextPollMax = Integer.MAX_VALUE;

  private final AtomicLong lastSeenNewestPublishMillis = new AtomicLong(Long.MIN_VALUE);

  final class PubSubMessageReceiver implements MessageReceiver {

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
      if (stopped != null && stopped.get()) {
        consumer.nack();
        return;
      }
      PulledMessage pulled = toPulled(message);
      long cap = config.batchSize() > 0 ? config.batchSize() : PubSubConfig.DEFAULT_BATCH_SIZE;
      while (queuedBytes != null && queuedBytes.get() >= cap && stopped != null && !stopped.get()) {
        try {
          TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          consumer.nack();
          return;
        }
      }
      if (stopped != null && stopped.get()) {
        consumer.nack();
        return;
      }
      consumers.put(pulled.ackId(), consumer);
      queuedBytes.addAndGet(pulled.data().length);
      outstandingBytes.addAndGet(pulled.data().length);
      try {
        queue.put(new HeldMessage(pulled, consumer));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        consumers.remove(pulled.ackId());
        queuedBytes.addAndGet(-pulled.data().length);
        outstandingBytes.addAndGet(-pulled.data().length);
        consumer.nack();
      }
    }

    private PulledMessage toPulled(PubsubMessage message) {
      byte[] data = message.getData().toByteArray();
      long publishMillis =
          message.getPublishTime().getSeconds() * 1000L
              + message.getPublishTime().getNanos() / 1_000_000L;
      String messageId = message.getMessageId();
      return new PulledMessage(
          messageId,
          data,
          message.getAttributesMap(),
          publishMillis,
          message.getOrderingKey(),
          messageId);
    }
  }

  static final class HeldMessage {
    final PulledMessage message;
    final AckReplyConsumer consumer;

    HeldMessage(PulledMessage message, AckReplyConsumer consumer) {
      this.message = message;
      this.consumer = consumer;
    }
  }

  PubSubClient(PubSubConfig config) {
    this(
        config,
        new PubSubCredentialsProvider(config.credentialsFile().orElse(null)),
        RetryPolicy.defaults(config.effectiveMaxRetryTime()));
  }

  PubSubClient(
      PubSubConfig config, PubSubCredentialsProvider credentialsProvider, RetryPolicy retryPolicy) {
    this.config = config;
    this.credentialsProvider = credentialsProvider;
    this.retryPolicy = retryPolicy;
    this.retryPolicy.lastPublishTime(this.lastSeenNewestPublishMillis::get);
  }

  synchronized void start() throws IOException {
    if (this.subscriber != null) {
      return;
    }
    this.outstandingBytes = new AtomicLong(0);
    this.queuedBytes = new AtomicLong(0);
    this.queue = new LinkedBlockingQueue<>();
    this.consumers = new ConcurrentHashMap<>();
    this.stopped = new AtomicBoolean(false);
    this.connectionErrors = new PubSubConnectionErrors(this.config.subscriptionPath());

    this.emulator = this.config.emulatorHost().map(PubSubEmulator::new).orElse(null);
    try {
      Credentials credentials = this.credentialsProvider.getCredentials();
      if (!this.config.emulatorHost().isPresent()) {
        connectionErrors.verifySubscriptionAccessible(credentials);
      }
      applySeekIfNeeded();

      Subscriber.Builder builder =
          Subscriber.newBuilder(this.config.subscriptionPath(), new PubSubMessageReceiver());
      builder.setFlowControlSettings(flowControl());

      long ackExtendSecs = config.effectiveAckDeadline(null).getSeconds();
      long ackExtendMax = Math.max(60L, ackExtendSecs);
      org.threeten.bp.Duration ackExtend = org.threeten.bp.Duration.ofSeconds(ackExtendMax);
      org.threeten.bp.Duration ackMaxPeriod = org.threeten.bp.Duration.ofMinutes(60);
      builder.setMaxAckExtensionPeriod(ackMaxPeriod);
      builder.setMaxDurationPerAckExtension(ackExtend);

      if (this.emulator != null) {
        this.emulator.configureSubscriber(builder);
      } else {
        builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
      }
      this.subscriber = builder.build();
      this.subscriber.addListener(connectionErrors, MoreExecutors.directExecutor());
      this.subscriber.startAsync().awaitRunning();

      this.connectionErrors.throwIfFatalConnection();
    } catch (Exception e) {
      this.close();
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Unable to start Pub/Sub subscriber", e);
    }
  }

  private FlowControlSettings flowControl() {
    long bytes = config.batchSize() > 0 ? config.batchSize() : PubSubConfig.DEFAULT_BATCH_SIZE;
    long outstandingBytes = bytes > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : bytes * 2;
    FlowControlSettings.Builder flow =
        FlowControlSettings.newBuilder()
            .setMaxOutstandingRequestBytes(outstandingBytes)
            .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block);
    if (config.batchCount() > 0) {
      long count = config.batchCount();
      long elements = count > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : count * 2;
      flow.setMaxOutstandingElementCount(elements);
    }
    return flow.build();
  }

  private void applySeekIfNeeded() throws IOException {
    if (seekApplied || this.config.seekMode() == SeekMode.NONE) {
      return;
    }
    final SeekRequest.Builder seek = getSeekRequestBuilder();
    final SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
    try (PubSubEmulator adminEmulator =
        this.config.emulatorHost().map(PubSubEmulator::new).orElse(null)) {
      if (adminEmulator != null) {
        adminEmulator.configureSubscriptionAdmin(builder);
      } else {
        builder.setCredentialsProvider(
            FixedCredentialsProvider.create(this.credentialsProvider.getCredentials()));
      }
      try (SubscriptionAdminClient admin = SubscriptionAdminClient.create(builder.build())) {
        LOG.warn(
            "CONNECTION: Applying seek={} on subscription {} (explicit rewind requested)",
            this.config.seekMode(),
            this.config.subscriptionPath());
        admin.seek(seek.build());
      }
    }
    seekApplied = true;
  }

  private SeekRequest.Builder getSeekRequestBuilder() {
    final SeekRequest.Builder seek =
        SeekRequest.newBuilder().setSubscription(this.config.subscriptionPath());

    switch (this.config.seekMode()) {
      case BEGINNING:
        seek.setTime(Timestamp.newBuilder().setSeconds(0).setNanos(0).build());
        break;
      case TIMESTAMP:
        final Instant seekInstant = this.config.seekTimeAsInstant();
        final Timestamp seekTime =
            Timestamp.newBuilder()
                .setSeconds(seekInstant.getEpochSecond())
                .setNanos(seekInstant.getNano())
                .build();
        seek.setTime(seekTime);
        break;
      case SNAPSHOT:
        seek.setSnapshot(this.config.seekSnapshot().orElseThrow());
        break;
      case NONE:
      default:
    }
    return seek;
  }

  private void ensureStarted() {
    if (this.subscriber == null) {
      try {
        start();
      } catch (IOException e) {
        throw new IllegalStateException("CONNECTION: Failed to start Pub/Sub client", e);
      }
    }
    if (this.connectionErrors != null) {
      try {
        this.connectionErrors.throwIfFatalConnection();
      } catch (IOException e) {
        throw new IllegalStateException(e.getMessage(), e);
      }
    }
  }

  @Override
  public synchronized void close() {
    if (stopped != null) {
      stopped.set(true);
    }
    if (this.subscriber != null) {
      try {
        ApiService service = this.subscriber.stopAsync();
        service.awaitTerminated(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.warn("CONNECTION: Error shutting down subscriber", e);
      } finally {
        this.subscriber = null;
      }
    }
    nackQueued();
    if (this.emulator != null) {
      this.emulator.close();
      this.emulator = null;
    }
  }

  private void nackQueued() {
    if (queue == null) {
      return;
    }
    HeldMessage held;
    while ((held = queue.poll()) != null) {
      try {
        held.consumer.nack();
      } catch (RuntimeException e) {
        LOG.warn("CONNECTION: Failed to nack queued message on close: {}", e.getMessage());
      }
    }
    if (consumers != null) {
      consumers.clear();
    }
    if (queuedBytes != null) {
      queuedBytes.set(0);
    }
  }

  /**
   * Upper bound for the next {@link #poll(Duration)}. Messages beyond the cap stay in the queue.
   * {@code maxMessages <= 0} means no cap.
   */
  void limitNextPoll(int maxMessages) {
    this.nextPollMax = maxMessages <= 0 ? Integer.MAX_VALUE : maxMessages;
  }

  /**
   * Wait up to {@code timeout} for messages, up to {@link #limitNextPoll(int)}. When capped, keeps
   * waiting within the same timeout until the cap is reached or the deadline passes. When uncapped,
   * waits only for the first message, then drains the queue without blocking. Does not ack.
   */
  List<PulledMessage> poll(Duration timeout) {
    ensureStarted();
    int max = this.nextPollMax;
    this.nextPollMax = Integer.MAX_VALUE;
    List<PulledMessage> messages = new ArrayList<>();
    if (max <= 0) {
      return messages;
    }
    long deadlineNanos = System.nanoTime() + Math.max(1L, timeout.toNanos());
    try {
      long waitNanos = deadlineNanos - System.nanoTime();
      if (waitNanos <= 0L) {
        return messages;
      }
      HeldMessage first = queue.poll(waitNanos, TimeUnit.NANOSECONDS);
      if (first == null) {
        return messages;
      }
      take(first, messages);
      if (max == Integer.MAX_VALUE) {
        HeldMessage next;
        while ((next = queue.poll()) != null) {
          take(next, messages);
        }
      } else {
        while (messages.size() < max) {
          waitNanos = deadlineNanos - System.nanoTime();
          if (waitNanos <= 0L) {
            break;
          }
          HeldMessage next = queue.poll(waitNanos, TimeUnit.NANOSECONDS);
          if (next == null) {
            break;
          }
          take(next, messages);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    final PublishTimeWindow window = PublishTimeWindow.of(messages);
    if (window != null) {
      this.lastSeenNewestPublishMillis.set(window.newestMillis());
    }
    return messages;
  }

  private void take(HeldMessage held, List<PulledMessage> messages) {
    messages.add(held.message);
    if (queuedBytes != null) {
      queuedBytes.addAndGet(-held.message.data().length);
    }
  }

  void acknowledge(List<String> ackIds) {
    reply(ackIds, true);
  }

  void nack(List<String> ackIds) {
    reply(ackIds, false);
  }

  /** Subscriber renews leases; kept for {@link AckLeaseWatchdog}. */
  void extendAckDeadline(List<String> ackIds, int deadlineSeconds) {}

  private void reply(List<String> ackIds, boolean ack) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    if (consumers == null) {
      return;
    }
    for (String id : ackIds) {
      AckReplyConsumer consumer = consumers.remove(id);
      if (consumer == null) {
        continue;
      }
      try {
        if (ack) {
          consumer.ack();
        } else {
          consumer.nack();
        }
      } catch (RuntimeException e) {
        LOG.warn("ACK: Failed to {} {}: {}", ack ? "ack" : "nack", id, e.getMessage());
      }
    }
  }

  void releaseMessages(List<PulledMessage> messages) {
    if (this.outstandingBytes != null) {
      long bytes = PulledMessage.payloadBytes(messages);
      if (bytes > 0) {
        this.outstandingBytes.addAndGet(-bytes);
      }
    }
  }

  void resetOutstandingBytes() {
    if (this.outstandingBytes != null) {
      this.outstandingBytes.set(0);
    }
  }

  long outstandingBytes() {
    return this.outstandingBytes == null ? 0L : this.outstandingBytes.get();
  }

  long retryAttempts() {
    return retryPolicy.retryAttempts();
  }

  long lastSeenNewestPublishMillis() {
    return lastSeenNewestPublishMillis.get();
  }

  public static String asString(final Duration duration) {
    return duration.toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1").toLowerCase();
  }
}
