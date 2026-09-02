package io.github.juarezr.spark.pubsub.client;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PubSubEmulator implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubEmulator.class);

  private ManagedChannel channel;

  PubSubEmulator(final String host) {
    LOG.info("Connecting Pub/Sub client to emulator at {}", host);
    this.channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
  }

  void configureSubscriber(final SubscriberStubSettings.Builder builder) {
    builder.setTransportChannelProvider(transportProvider());
    builder.setCredentialsProvider(NoCredentialsProvider.create());
  }

  void configureAdmin(final SubscriptionAdminSettings.Builder builder) {
    builder.setTransportChannelProvider(transportProvider());
    builder.setCredentialsProvider(NoCredentialsProvider.create());
  }

  private FixedTransportChannelProvider transportProvider() {
    return FixedTransportChannelProvider.create(GrpcTransportChannel.create(this.channel));
  }

  @Override
  public synchronized void close() {
    if (this.channel != null) {
      LOG.info("Shutting down Pub/Sub emulator channel");
      this.channel.shutdownNow();
      try {
        this.channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while shutting down emulator channel", e);
        Thread.currentThread().interrupt();
      } finally {
        this.channel = null;
      }
    }
  }
}
