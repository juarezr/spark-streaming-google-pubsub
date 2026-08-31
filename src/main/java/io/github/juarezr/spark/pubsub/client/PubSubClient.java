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
    if (subscriberStub != null) {
      return;
    }
    outstandingBytes = new AtomicLong(0);
    seekApplied = false;

    SubscriberStubSettings.Builder settingsBuilder = SubscriberStubSettings.newBuilder();
    if (config.emulatorHost().isPresent()) {
      String host = config.emulatorHost().get();
      channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
      settingsBuilder.setTransportChannelProvider(
          FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
      settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
      LOG.info("Connecting Pub/Sub client to emulator at {}", host);
    } else {
      settingsBuilder.setCredentialsProvider(
          FixedCredentialsProvider.create(credentialsProvider.getCredentials()));
    }
    subscriberStub = GrpcSubscriberStub.create(settingsBuilder.build());
    applySeekIfNeeded();
  }

  private void applySeekIfNeeded() throws IOException {
    if (seekApplied || config.seekMode() == SeekMode.NONE) {
      return;
    }
    SeekRequest.Builder seek = SeekRequest.newBuilder().setSubscription(config.subscriptionPath());
    switch (config.seekMode()) {
      case BEGINNING:
        seek.setTime(Timestamp.newBuilder().setSeconds(0).setNanos(0).build());
        break;
      case TIMESTAMP:
        Instant instant = parseSeekTime(config.seekTime().orElseThrow());
        seek.setTime(
            Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build());
        break;
      case SNAPSHOT:
        seek.setSnapshot(config.seekSnapshot().orElseThrow());
        break;
      case NONE:
      default:
        return;
    }

    ManagedChannel adminChannel = null;
    try {
      SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
      if (config.emulatorHost().isPresent()) {
        String host = config.emulatorHost().get();
        adminChannel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
        builder.setTransportChannelProvider(
            FixedTransportChannelProvider.create(GrpcTransportChannel.create(adminChannel)));
        builder.setCredentialsProvider(NoCredentialsProvider.create());
      } else {
        builder.setCredentialsProvider(
            FixedCredentialsProvider.create(credentialsProvider.getCredentials()));
      }
      try (SubscriptionAdminClient admin = SubscriptionAdminClient.create(builder.build())) {
        admin.seek(seek.build());
        LOG.warn(
            "Applied seek={} on subscription {} (explicit rewind requested)",
            config.seekMode(),
            config.subscriptionPath());
      }
    } finally {
      if (adminChannel != null) {
        adminChannel.shutdownNow();
        try {
          adminChannel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    seekApplied = true;
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

  public List<PulledMessage> pull() {
    ensureStarted();
    if (outstandingBytes.get() >= config.maxBytesOutstanding()) {
      LOG.debug(
          "Skipping pull; outstanding bytes {} >= limit {}",
          outstandingBytes.get(),
          config.maxBytesOutstanding());
      return Collections.emptyList();
    }
    return retryPolicy.execute(
        "pull",
        () -> {
          PullRequest request =
              PullRequest.newBuilder()
                  .setSubscription(config.subscriptionPath())
                  .setMaxMessages(config.maxMessagesPerPull())
                  .setReturnImmediately(config.returnImmediately())
                  .build();
          GrpcCallContext callContext =
              GrpcCallContext.createDefault()
                  .withTimeout(Duration.ofSeconds(config.pullTimeoutSeconds()));
          PullResponse response = subscriberStub.pullCallable().call(request, callContext);
          List<PulledMessage> messages = new ArrayList<>(response.getReceivedMessagesCount());
          for (ReceivedMessage received : response.getReceivedMessagesList()) {
            byte[] data = received.getMessage().getData().toByteArray();
            outstandingBytes.addAndGet(data.length);
            long publishMillis =
                received.getMessage().getPublishTime().getSeconds() * 1000L
                    + received.getMessage().getPublishTime().getNanos() / 1_000_000L;
            messages.add(
                new PulledMessage(
                    received.getMessage().getMessageId(),
                    data,
                    received.getMessage().getAttributesMap(),
                    publishMillis,
                    received.getMessage().getOrderingKey(),
                    received.getAckId()));
          }
          return messages;
        });
  }

  public void acknowledge(List<String> ackIds) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    ensureStarted();
    retryPolicy.executeVoid(
        "acknowledge",
        () -> {
          AcknowledgeRequest request =
              AcknowledgeRequest.newBuilder()
                  .setSubscription(config.subscriptionPath())
                  .addAllAckIds(ackIds)
                  .build();
          subscriberStub.acknowledgeCallable().call(request);
        });
  }

  public void nack(List<String> ackIds) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    ensureStarted();
    retryPolicy.executeVoid(
        "nack",
        () -> {
          ModifyAckDeadlineRequest request =
              ModifyAckDeadlineRequest.newBuilder()
                  .setSubscription(config.subscriptionPath())
                  .addAllAckIds(ackIds)
                  .setAckDeadlineSeconds(0)
                  .build();
          subscriberStub.modifyAckDeadlineCallable().call(request);
        });
  }

  public void extendAckDeadline(List<String> ackIds, int deadlineSeconds) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    ensureStarted();
    retryPolicy.executeVoid(
        "modifyAckDeadline",
        () -> {
          ModifyAckDeadlineRequest request =
              ModifyAckDeadlineRequest.newBuilder()
                  .setSubscription(config.subscriptionPath())
                  .addAllAckIds(ackIds)
                  .setAckDeadlineSeconds(deadlineSeconds)
                  .build();
          subscriberStub.modifyAckDeadlineCallable().call(request);
        });
  }

  public void releaseBytes(long bytes) {
    if (outstandingBytes != null && bytes > 0) {
      outstandingBytes.addAndGet(-bytes);
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
    if (outstandingBytes != null) {
      outstandingBytes.set(0);
    }
  }

  /** Visible for tests. */
  public long outstandingBytes() {
    return outstandingBytes == null ? 0L : outstandingBytes.get();
  }

  public long retryAttempts() {
    return retryPolicy.retryAttempts();
  }

  public PubSubConfig config() {
    return config;
  }

  public String subscriptionPath() {
    return config.subscriptionPath();
  }

  public SubscriptionName subscriptionName() {
    return SubscriptionName.parse(config.subscriptionPath());
  }

  private void ensureStarted() {
    if (subscriberStub == null) {
      try {
        start();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to start Pub/Sub client", e);
      }
    }
  }

  @Override
  public synchronized void close() {
    if (subscriberStub != null) {
      try {
        subscriberStub.shutdown();
        subscriberStub.awaitTermination(5, TimeUnit.SECONDS);
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
}
