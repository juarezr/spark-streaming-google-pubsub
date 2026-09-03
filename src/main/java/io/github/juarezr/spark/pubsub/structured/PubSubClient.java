package io.github.juarezr.spark.pubsub.structured;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SeekRequest;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SeekMode;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Thin wrapper around the Pub/Sub subscriber stub with pull/ack/nack, retries, and optional seek.
 */
final class PubSubClient implements Closeable, Serializable {

  private static final long serialVersionUID = -3861530485L;
  private static final Logger LOG = LoggerFactory.getLogger(PubSubClient.class);

  private final RetryPolicy retryPolicy;
  private transient PubSubEmulator emulator;

  private final PubSubConfig config;
  private final PubSubCredentialsProvider credentialsProvider;
  private transient SubscriberStub subscriberStub;
  private transient AtomicLong outstandingBytes;
  private transient boolean seekApplied;

  PubSubClient(PubSubConfig config) {
    this(
        config,
        new PubSubCredentialsProvider(config.credentialsFile().orElse(null)),
        RetryPolicy.defaults(config.maxRetryTime()));
  }

  PubSubClient(
      PubSubConfig config, PubSubCredentialsProvider credentialsProvider, RetryPolicy retryPolicy) {
    this.config = config;
    this.credentialsProvider = credentialsProvider;
    this.retryPolicy = retryPolicy;
  }

  synchronized void start() throws IOException {
    if (this.subscriberStub != null) {
      return;
    }
    this.outstandingBytes = new AtomicLong(0);

    final SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();
    this.emulator = this.config.emulatorHost().map(PubSubEmulator::new).orElse(null);
    if (this.emulator != null) {
      this.emulator.configureSubscriber(builder);
    } else {
      builder.setCredentialsProvider(
          FixedCredentialsProvider.create(this.credentialsProvider.getCredentials()));
    }
    try {
      this.subscriberStub = GrpcSubscriberStub.create(builder.build());
      applySeekIfNeeded();
    } catch (Exception e) {
      this.close();
      throw e;
    }
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
        adminEmulator.configureAdmin(builder);
      } else {
        builder.setCredentialsProvider(
            FixedCredentialsProvider.create(this.credentialsProvider.getCredentials()));
      }
      try (SubscriptionAdminClient admin = SubscriptionAdminClient.create(builder.build())) {
        LOG.warn(
            "Applying seek={} on subscription {} (explicit rewind requested)",
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
        final Instant instant = this.config.seekTimeAsInstant();
        final Timestamp seekTime =
            Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
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
    if (this.subscriberStub == null) {
      try {
        start();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to start Pub/Sub client", e);
      }
    }
  }

  @Override
  public synchronized void close() {
    if (this.subscriberStub != null) {
      try {
        this.subscriberStub.shutdown();
        this.subscriberStub.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while shutting down subscriber stub", e);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.warn("Error shutting down subscriber stub", e);
      } finally {
        this.subscriberStub = null;
      }
    }
    if (this.emulator != null) {
      this.emulator.close();
      this.emulator = null;
    }
  }

  List<PulledMessage> pull(java.time.Duration deadline) {
    return pull(deadline, config.pullMaxMessages());
  }

  List<PulledMessage> pull(java.time.Duration deadline, int maxMessages) {
    ensureStarted();
    final int capped = Math.max(1, Math.min(this.config.pullMaxMessages(), maxMessages));
    return retryPolicy.execute("pull", () -> pullMessagesFromSubscription(deadline, capped));
  }

  private List<PulledMessage> pullMessagesFromSubscription(
      java.time.Duration deadline, int maxMessages) {
    final PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(this.config.subscriptionPath())
            .setMaxMessages(maxMessages)
            .build();
    final GrpcCallContext callContext =
        GrpcCallContext.createDefault()
            .withTimeout(Duration.ofMillis(Math.max(1L, deadline.toMillis())));
    final PullResponse response = subscriberStub.pullCallable().call(request, callContext);
    final int receivedCount = response.getReceivedMessagesCount();
    final List<PulledMessage> messages = new ArrayList<>(receivedCount);

    for (ReceivedMessage received : response.getReceivedMessagesList()) {
      final PubsubMessage message = received.getMessage();
      final byte[] data = message.getData().toByteArray();
      this.outstandingBytes.addAndGet(data.length);
      final long publishMillis =
          message.getPublishTime().getSeconds() * 1000L
              + message.getPublishTime().getNanos() / 1_000_000L;
      final String messageId = message.getMessageId();
      final Map<String, String> attributes = message.getAttributesMap();
      final String orderingKey = message.getOrderingKey();
      final String ackId = received.getAckId();
      final PulledMessage converted =
          new PulledMessage(messageId, data, attributes, publishMillis, orderingKey, ackId);
      messages.add(converted);
    }
    return messages;
  }

  void acknowledge(List<String> ackIds) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    ensureStarted();
    for (List<String> chunk : chunks(ackIds)) {
      retryPolicy.executeVoid("acknowledge", () -> acknowledgeRequest(chunk));
    }
  }

  private void acknowledgeRequest(List<String> ackIds) {
    final AcknowledgeRequest request =
        AcknowledgeRequest.newBuilder()
            .setSubscription(this.config.subscriptionPath())
            .addAllAckIds(ackIds)
            .build();
    this.subscriberStub.acknowledgeCallable().call(request);
  }

  void nack(List<String> ackIds) {
    modifyAckDeadline(ackIds, 0, "nack");
  }

  void extendAckDeadline(List<String> ackIds, int deadlineSeconds) {
    modifyAckDeadline(ackIds, deadlineSeconds, "modifyAckDeadline");
  }

  private void modifyAckDeadline(List<String> ackIds, int deadlineSeconds, String operation) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    ensureStarted();
    for (List<String> chunk : chunks(ackIds)) {
      retryPolicy.executeVoid(operation, () -> modifyAckDeadlineRequest(chunk, deadlineSeconds));
    }
  }

  private List<List<String>> chunks(List<String> ackIds) {
    List<List<String>> chunks = new ArrayList<>();
    int size = this.config.pullMaxMessages();
    for (int start = 0; start < ackIds.size(); start += size) {
      chunks.add(ackIds.subList(start, Math.min(start + size, ackIds.size())));
    }
    return chunks;
  }

  private void modifyAckDeadlineRequest(List<String> ackIds, int deadlineSeconds) {
    ModifyAckDeadlineRequest request =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription(this.config.subscriptionPath())
            .addAllAckIds(ackIds)
            .setAckDeadlineSeconds(deadlineSeconds)
            .build();
    this.subscriberStub.modifyAckDeadlineCallable().call(request);
  }

  void releaseMessages(List<PulledMessage> messages) {
    if (this.outstandingBytes != null) {
      long bytes = PulledMessage.payloadBytes(messages);
      if (bytes > 0) {
        this.outstandingBytes.addAndGet(-bytes);
      }
    }
  }

  /** Clears outstanding byte accounting (e.g. on receiver shutdown). */
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
}
