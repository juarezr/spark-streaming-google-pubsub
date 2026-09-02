package io.github.juarezr.spark.pubsub.client;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
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
import com.google.pubsub.v1.SubscriptionName;
import io.github.juarezr.spark.pubsub.auth.PubSubCredentialsProvider;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SeekMode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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
public final class PubSubClient implements Closeable, Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PubSubClient.class);

  private final PubSubConfig config;
  private final PubSubCredentialsProvider credentialsProvider;
  private final RetryPolicy retryPolicy;

  private transient SubscriberStub subscriberStub;
  private transient ManagedChannel channel;
  private transient AtomicLong outstandingBytes;
  private transient boolean seekApplied;

  public PubSubClient(PubSubConfig config) {
    this(
        config,
        new PubSubCredentialsProvider(config.credentialsFile().orElse(null)),
        RetryPolicy.defaults());
  }

  public PubSubClient(
      PubSubConfig config, PubSubCredentialsProvider credentialsProvider, RetryPolicy retryPolicy) {
    this.config = config;
    this.credentialsProvider = credentialsProvider;
    this.retryPolicy = retryPolicy;
  }

  public synchronized void start() throws IOException {
    if (this.subscriberStub != null) {
      return;
    }
    this.outstandingBytes = new AtomicLong(0);
    this.seekApplied = false;

    final SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();
    final boolean emulated = this.config.emulatorHost().isPresent();
    if (emulated) {
      startEmulatorHost(builder);
      builder.setCredentialsProvider(NoCredentialsProvider.create());
    } else {
      builder.setCredentialsProvider(
          FixedCredentialsProvider.create(this.credentialsProvider.getCredentials()));
    }
    this.subscriberStub = GrpcSubscriberStub.create(builder.build());
    applySeekIfNeeded();
  }

  private void applySeekIfNeeded() throws IOException {
    if (seekApplied || this.config.seekMode() == SeekMode.NONE) {
      return;
    }
    final SeekRequest.Builder seek = getSeekRequestBuilder();

    ManagedChannel adminChannel = null;
    try {
      final SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
      if (this.config.emulatorHost().isPresent()) {
        adminChannel = createAdminChannel(builder);
      } else {
        builder.setCredentialsProvider(
            FixedCredentialsProvider.create(credentialsProvider.getCredentials()));
      }
      try (SubscriptionAdminClient admin = SubscriptionAdminClient.create(builder.build())) {
        LOG.warn(
            "Applying seek={} on subscription {} (explicit rewind requested)",
            this.config.seekMode(),
            this.config.subscriptionPath());
        admin.seek(seek.build());
      }
    } finally {
      shutdownAdminChannel(adminChannel);
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
        final Instant instant = parseSeekTime(this.config.seekTime().orElseThrow());
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

  private void startEmulatorHost(final SubscriberStubSettings.Builder builder) {
    final String host = this.config.emulatorHost().get();
    this.channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
    final FixedTransportChannelProvider transportChannelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(this.channel));
    builder.setTransportChannelProvider(transportChannelProvider);
    LOG.info("Connecting Pub/Sub client to emulator at {}", host);
  }

  private ManagedChannel createAdminChannel(final SubscriptionAdminSettings.Builder builder) {
    final String host = this.config.emulatorHost().get();
    final ManagedChannel adminChannel =
        ManagedChannelBuilder.forTarget(host).usePlaintext().build();
    builder.setTransportChannelProvider(
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(adminChannel)));
    builder.setCredentialsProvider(NoCredentialsProvider.create());
    return adminChannel;
  }

  private void shutdownAdminChannel(ManagedChannel adminChannel) {
    if (adminChannel != null) {
      adminChannel.shutdownNow();
      try {
        adminChannel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static Instant parseSeekTime(String seekTime) {
    String value = seekTime.trim();
    try {
      if (value.chars().allMatch(Character::isDigit)) {
        return Instant.ofEpochMilli(Long.parseLong(value));
      }
      return Instant.parse(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid seekTime '" + seekTime + "'. Use epoch millis or RFC-3339 instant.", e);
    }
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
      } catch (Exception e) {
        LOG.warn("Error shutting down subscriber stub", e);
      } finally {
        subscriberStub = null;
      }
    }
    if (channel != null) {
      channel.shutdownNow();
      channel = null;
    }
  }

  public List<PulledMessage> pull() {
    ensureStarted();
    final long currentBytes = this.outstandingBytes.get();
    long maxBytes = this.config.maxBytesOutstanding();
    if (currentBytes >= maxBytes) {
      LOG.debug("Skipping pull; outstanding bytes {} >= limit {}", currentBytes, maxBytes);
      return Collections.emptyList();
    }
    return retryPolicy.execute("pull", this::pullMessagesFromSubscription);
  }

  private List<PulledMessage> pullMessagesFromSubscription() {
    final PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(this.config.subscriptionPath())
            .setMaxMessages(this.config.maxMessagesPerPull())
            .build();
    final GrpcCallContext callContext =
        GrpcCallContext.createDefault()
            .withTimeout(Duration.ofSeconds(this.config.pullTimeoutSeconds()));
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

  public void acknowledge(List<String> ackIds) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    ensureStarted();
    retryPolicy.executeVoid("acknowledge", () -> acknowledgeRequest(ackIds));
  }

  private void acknowledgeRequest(List<String> ackIds) {
    final AcknowledgeRequest request =
        AcknowledgeRequest.newBuilder()
            .setSubscription(this.config.subscriptionPath())
            .addAllAckIds(ackIds)
            .build();
    this.subscriberStub.acknowledgeCallable().call(request);
  }

  public void nack(List<String> ackIds) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    ensureStarted();
    retryPolicy.executeVoid("nack", () -> nackRequest(ackIds));
  }

  private void nackRequest(List<String> ackIds) {
    ModifyAckDeadlineRequest request =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription(this.config.subscriptionPath())
            .addAllAckIds(ackIds)
            .setAckDeadlineSeconds(0)
            .build();
    this.subscriberStub.modifyAckDeadlineCallable().call(request);
  }

  public void extendAckDeadline(List<String> ackIds, int deadlineSeconds) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    ensureStarted();
    retryPolicy.executeVoid(
        "modifyAckDeadline", () -> extendDeadlineRequest(ackIds, deadlineSeconds));
  }

  private void extendDeadlineRequest(List<String> ackIds, int deadlineSeconds) {
    ModifyAckDeadlineRequest request =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription(this.config.subscriptionPath())
            .addAllAckIds(ackIds)
            .setAckDeadlineSeconds(deadlineSeconds)
            .build();
    this.subscriberStub.modifyAckDeadlineCallable().call(request);
  }

  public void releaseBytes(long bytes) {
    if (this.outstandingBytes != null && bytes > 0) {
      this.outstandingBytes.addAndGet(-bytes);
    }
  }

  /** Releases flow-control byte accounting for the given messages without acknowledging them. */
  public void releaseMessages(List<PulledMessage> messages) {
    if (messages == null || messages.isEmpty()) {
      return;
    }
    long bytes = 0L;
    for (PulledMessage message : messages) {
      bytes += message.data().length;
    }
    releaseBytes(bytes);
  }

  /** Clears outstanding byte accounting (e.g. on receiver shutdown). */
  public void resetOutstandingBytes() {
    if (this.outstandingBytes != null) {
      this.outstandingBytes.set(0);
    }
  }

  /** Visible for tests. */
  public long outstandingBytes() {
    return this.outstandingBytes == null ? 0L : this.outstandingBytes.get();
  }

  public long retryAttempts() {
    return retryPolicy.retryAttempts();
  }

  public PubSubConfig config() {
    return config;
  }

  public String subscriptionPath() {
    return this.config.subscriptionPath();
  }

  public SubscriptionName subscriptionName() {
    return SubscriptionName.parse(this.config.subscriptionPath());
  }
}
