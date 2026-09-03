package io.github.juarezr.spark.pubsub.structured;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.connector.read.InputPartition;

/** One Spark task partition holding a slice of pulled Pub/Sub messages. */
final class PubSubInputPartition implements InputPartition {
  private static final long serialVersionUID = 1L;

  private final List<PulledMessage> messages;

  PubSubInputPartition(List<PulledMessage> messages) {
    this.messages =
        messages == null
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(messages));
  }

  List<PulledMessage> messages() {
    return messages;
  }
}
