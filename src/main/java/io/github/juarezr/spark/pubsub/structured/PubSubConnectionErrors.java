package io.github.juarezr.spark.pubsub.structured;

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.UnauthenticatedException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Classifies Pub/Sub client failures that should fail the Spark source immediately. */
final class PubSubConnectionErrors extends Listener implements Serializable {

  private static final String CONNECTION_FAILED =
      "CONNECTION: Pub/Sub subscriber failed from state {} on subscription {}: {}";

  private static final long serialVersionUID = -8927898261L;

  private static final Logger LOG = LoggerFactory.getLogger(PubSubConnectionErrors.class);

  private transient AtomicReference<Throwable> fatalConnectionError;
  private final String subscriptionPath;

  PubSubConnectionErrors(final String subscriptionPath) {

    this.fatalConnectionError = new AtomicReference<>();
    this.subscriptionPath = subscriptionPath;
  }

  @Override
  public void failed(State from, Throwable failure) {

    String failureMessage = failure.toString();
    if (isFatal(failure)) {
      fatalConnectionError.compareAndSet(null, failure);
      LOG.error(CONNECTION_FAILED, from, this.subscriptionPath, failureMessage);
    } else {
      LOG.warn(CONNECTION_FAILED, from, this.subscriptionPath, "may retry from: " + failureMessage);
    }
  }

  static boolean isFatal(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof NotFoundException
          || t instanceof PermissionDeniedException
          || t instanceof UnauthenticatedException) {
        return true;
      }
      if (t instanceof StatusRuntimeException) {
        Status.Code code = ((StatusRuntimeException) t).getStatus().getCode();
        if (code == Status.Code.NOT_FOUND
            || code == Status.Code.PERMISSION_DENIED
            || code == Status.Code.UNAUTHENTICATED
            || code == Status.Code.INVALID_ARGUMENT) {
          return true;
        }
      }
    }
    return false;
  }

  void verifySubscriptionAccessible(CredentialsProvider credentialsProvider) throws IOException {

    SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
    builder.setCredentialsProvider(credentialsProvider);

    try (SubscriptionAdminClient admin = SubscriptionAdminClient.create(builder.build())) {
      admin.getSubscription(subscriptionPath);
    } catch (Exception e) {
      throw new IOException(
          "CONNECTION: Pub/Sub subscription not found or not accessible: " + subscriptionPath, e);
    }
  }

  void throwIfFatalConnection() throws IOException {
    Throwable failure = fatalConnectionError == null ? null : fatalConnectionError.get();
    if (failure != null) {
      throw new IOException(
          "CONNECTION: Pub/Sub subscription unavailable: " + this.subscriptionPath, failure);
    }
  }
}
