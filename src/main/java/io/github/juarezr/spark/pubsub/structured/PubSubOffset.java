package io.github.juarezr.spark.pubsub.structured;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.connector.read.streaming.Offset;

/**
 * Synthetic micro-batch offset. Payloads and ack ids deliberately stay out of checkpoint JSON;
 * after a driver restart Pub/Sub redelivers unacknowledged messages.
 */
final class PubSubOffset extends Offset implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

  private final long batchId;

  PubSubOffset(long batchId) {
    this.batchId = batchId;
  }

  long batchId() {
    return batchId;
  }

  @Override
  public String json() {
    Envelope envelope = new Envelope();
    envelope.batchId = batchId;
    return GSON.toJson(envelope);
  }

  static PubSubOffset fromJson(String json) {
    Envelope envelope = GSON.fromJson(json, Envelope.class);
    if (envelope == null) {
      return new PubSubOffset(0L);
    }
    return new PubSubOffset(envelope.batchId);
  }

  static PubSubOffset empty(long batchId) {
    return new PubSubOffset(batchId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PubSubOffset)) {
      return false;
    }
    PubSubOffset that = (PubSubOffset) o;
    return batchId == that.batchId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(batchId);
  }

  @Override
  public String toString() {
    return "PubSubOffset{batchId=" + batchId + "}";
  }

  private static final class Envelope {
    long batchId;
  }
}
