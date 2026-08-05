package io.github.juarezr.spark.pubsub.client;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** A Pub/Sub message pulled into Spark, including the ack id for later acknowledgement. */
public final class PulledMessage implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String messageId;
  private final byte[] data;
  private final Map<String, String> attributes;
  private final long publishTimeMillis;
  private final String orderingKey;
  private final String ackId;

  public PulledMessage(
      String messageId,
      byte[] data,
      Map<String, String> attributes,
      long publishTimeMillis,
      String orderingKey,
      String ackId) {
    this.messageId = messageId;
    this.data = data == null ? new byte[0] : data;
    this.attributes =
        attributes == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(attributes));
    this.publishTimeMillis = publishTimeMillis;
    this.orderingKey = orderingKey == null ? "" : orderingKey;
    this.ackId = Objects.requireNonNull(ackId, "ackId");
  }

  public String messageId() {
    return messageId;
  }

  public byte[] data() {
    return data;
  }

  public Map<String, String> attributes() {
    return attributes;
  }

  public long publishTimeMillis() {
    return publishTimeMillis;
  }

  public String orderingKey() {
    return orderingKey;
  }

  public String ackId() {
    return ackId;
  }
}
