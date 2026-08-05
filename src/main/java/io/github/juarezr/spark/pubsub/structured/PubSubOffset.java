package io.github.juarezr.spark.pubsub.structured;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.github.juarezr.spark.pubsub.client.PulledMessage;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.read.streaming.Offset;

/**
 * Synthetic micro-batch offset. Message payloads are embedded so batches can be reconstructed after
 * a driver restart from checkpoint (Pub/Sub cannot re-read by ack id).
 */
public final class PubSubOffset extends Offset implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

  private final long batchId;
  private final List<SerializedMessage> messages;

  public PubSubOffset(long batchId, List<PulledMessage> pulled) {
    this.batchId = batchId;
    if (pulled == null || pulled.isEmpty()) {
      this.messages = Collections.emptyList();
    } else {
      this.messages = pulled.stream().map(SerializedMessage::from).collect(Collectors.toList());
    }
  }

  private PubSubOffset(long batchId, List<SerializedMessage> messages, boolean unused) {
    this.batchId = batchId;
    this.messages = messages == null ? Collections.emptyList() : messages;
  }

  public long batchId() {
    return batchId;
  }

  public List<PulledMessage> messages() {
    List<PulledMessage> result = new ArrayList<>(messages.size());
    for (SerializedMessage m : messages) {
      result.add(m.toPulled());
    }
    return result;
  }

  public List<String> ackIds() {
    return messages.stream().map(m -> m.ackId).collect(Collectors.toList());
  }

  @Override
  public String json() {
    Envelope envelope = new Envelope();
    envelope.batchId = batchId;
    envelope.messages = messages;
    return GSON.toJson(envelope);
  }

  public static PubSubOffset fromJson(String json) {
    Envelope envelope = GSON.fromJson(json, Envelope.class);
    if (envelope == null) {
      return new PubSubOffset(0L, Collections.emptyList());
    }
    return new PubSubOffset(envelope.batchId, envelope.messages, true);
  }

  public static PubSubOffset empty(long batchId) {
    return new PubSubOffset(batchId, Collections.emptyList());
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
    return batchId == that.batchId && Objects.equals(messages, that.messages);
  }

  @Override
  public int hashCode() {
    return Objects.hash(batchId, messages);
  }

  @Override
  public String toString() {
    return "PubSubOffset{batchId=" + batchId + ", messages=" + messages.size() + "}";
  }

  private static final class Envelope {
    long batchId;
    List<SerializedMessage> messages;
  }

  static final class SerializedMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    String messageId;
    String dataBase64;
    Map<String, String> attributes;
    long publishTimeMillis;
    String orderingKey;
    String ackId;

    static SerializedMessage from(PulledMessage m) {
      SerializedMessage s = new SerializedMessage();
      s.messageId = m.messageId();
      s.dataBase64 = Base64.getEncoder().encodeToString(m.data());
      s.attributes = m.attributes();
      s.publishTimeMillis = m.publishTimeMillis();
      s.orderingKey = m.orderingKey();
      s.ackId = m.ackId();
      return s;
    }

    PulledMessage toPulled() {
      byte[] data =
          dataBase64 == null
              ? new byte[0]
              : Base64.getDecoder().decode(dataBase64.getBytes(StandardCharsets.UTF_8));
      return new PulledMessage(messageId, data, attributes, publishTimeMillis, orderingKey, ackId);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SerializedMessage)) {
        return false;
      }
      SerializedMessage that = (SerializedMessage) o;
      return publishTimeMillis == that.publishTimeMillis
          && Objects.equals(messageId, that.messageId)
          && Objects.equals(dataBase64, that.dataBase64)
          && Objects.equals(attributes, that.attributes)
          && Objects.equals(orderingKey, that.orderingKey)
          && Objects.equals(ackId, that.ackId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(messageId, dataBase64, attributes, publishTimeMillis, orderingKey, ackId);
    }
  }
}
