package io.github.juarezr.spark.pubsub.structured;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A Pub/Sub message pulled into Spark, including the ack id for later acknowledgement. */
final class PulledMessage implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String messageId;
  private final byte[] data;
  private final Map<String, String> attributes;
  private final long publishTimeMillis;
  private final String orderingKey;
  private final String ackId;

  PulledMessage(
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

  String messageId() {
    return messageId;
  }

  byte[] data() {
    return data;
  }

  Map<String, String> attributes() {
    return attributes;
  }

  long publishTimeMillis() {
    return publishTimeMillis;
  }

  String orderingKey() {
    return orderingKey;
  }

  String ackId() {
    return ackId;
  }

  static List<String> ackIds(List<PulledMessage> messages) {
    if (messages == null || messages.isEmpty()) {
      return List.of();
    }
    List<String> ids = new ArrayList<>(messages.size());
    for (PulledMessage message : messages) {
      ids.add(message.ackId());
    }
    return ids;
  }

  static long payloadBytes(List<PulledMessage> messages) {
    if (messages == null || messages.isEmpty()) {
      return 0L;
    }
    long bytes = 0L;
    for (PulledMessage message : messages) {
      bytes += message.data().length;
    }
    return bytes;
  }
}
