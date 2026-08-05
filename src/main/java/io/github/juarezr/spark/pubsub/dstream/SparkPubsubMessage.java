package io.github.juarezr.spark.pubsub.dstream;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Legacy-compatible message wrapper for the DStreams API.
 *
 * <p>Method names mirror {@code org.apache.spark.streaming.pubsub.SparkPubsubMessage}.
 */
public final class SparkPubsubMessage implements Serializable {
  private static final long serialVersionUID = 1L;

  private byte[] data = new byte[0];
  private Map<String, String> attributes = Collections.emptyMap();
  private String messageId = "";
  private String publishTime = "";
  private String ackId = "";
  private String orderingKey = "";

  public SparkPubsubMessage() {}

  public SparkPubsubMessage(
      byte[] data,
      Map<String, String> attributes,
      String messageId,
      String publishTime,
      String ackId,
      String orderingKey) {
    this.data = data == null ? new byte[0] : data;
    this.attributes =
        attributes == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(attributes));
    this.messageId = messageId == null ? "" : messageId;
    this.publishTime = publishTime == null ? "" : publishTime;
    this.ackId = ackId == null ? "" : ackId;
    this.orderingKey = orderingKey == null ? "" : orderingKey;
  }

  public byte[] getData() {
    return data;
  }

  public String getDataAsString() {
    return new String(data, StandardCharsets.UTF_8);
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public String getMessageId() {
    return messageId;
  }

  public String getPublishTime() {
    return publishTime;
  }

  public String getAckId() {
    return ackId;
  }

  public String getOrderingKey() {
    return orderingKey;
  }

  public void setData(byte[] data) {
    this.data = data == null ? new byte[0] : data;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes =
        attributes == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(attributes));
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId == null ? "" : messageId;
  }

  public void setPublishTime(String publishTime) {
    this.publishTime = publishTime == null ? "" : publishTime;
  }

  public void setAckId(String ackId) {
    this.ackId = ackId == null ? "" : ackId;
  }

  public void setOrderingKey(String orderingKey) {
    this.orderingKey = orderingKey == null ? "" : orderingKey;
  }
}
