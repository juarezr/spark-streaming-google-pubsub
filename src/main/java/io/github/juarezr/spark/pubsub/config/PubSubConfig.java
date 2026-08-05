package io.github.juarezr.spark.pubsub.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Validated connector configuration shared by Structured Streaming and DStreams. */
public final class PubSubConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String FORMAT = "google-pubsub";
  public static final String SHORT_NAME = "google-pubsub";

  public static final String PROJECT_ID = "projectId";
  public static final String SUBSCRIPTION = "subscription";
  public static final String TOPIC = "topic";
  public static final String ACK_MODE = "ackMode";
  public static final String MAX_MESSAGES_PER_PULL = "maxMessagesPerPull";
  public static final String MAX_BYTES_OUTSTANDING = "maxBytesOutstanding";
  public static final String ACK_DEADLINE_SECONDS = "ackDeadlineSeconds";
  public static final String PULL_TIMEOUT_SECONDS = "pullTimeoutSeconds";
  public static final String SEEK = "seek";
  public static final String SEEK_TIME = "seekTime";
  public static final String SEEK_SNAPSHOT = "seekSnapshot";
  public static final String CREDENTIALS_FILE = "credentialsFile";
  public static final String EMULATOR_HOST = "emulatorHost";
  public static final String RETURN_IMMEDIATELY = "returnImmediately";

  public static final int DEFAULT_MAX_MESSAGES_PER_PULL = 1000;
  public static final long DEFAULT_MAX_BYTES_OUTSTANDING = 100L * 1024 * 1024;
  public static final int DEFAULT_ACK_DEADLINE_SECONDS = 60;
  public static final int DEFAULT_PULL_TIMEOUT_SECONDS = 20;

  private final String projectId;
  private final String subscription;
  private final String topic;
  private final AckMode ackMode;
  private final int maxMessagesPerPull;
  private final long maxBytesOutstanding;
  private final int ackDeadlineSeconds;
  private final int pullTimeoutSeconds;
  private final SeekMode seekMode;
  private final String seekTime;
  private final String seekSnapshot;
  private final String credentialsFile;
  private final String emulatorHost;
  private final boolean returnImmediately;

  private PubSubConfig(Builder builder) {
    this.projectId = Objects.requireNonNull(builder.projectId, "projectId is required");
    this.subscription = Objects.requireNonNull(builder.subscription, "subscription is required");
    this.topic = builder.topic;
    this.ackMode = builder.ackMode == null ? AckMode.AFTER_COMMIT : builder.ackMode;
    this.maxMessagesPerPull = builder.maxMessagesPerPull;
    this.maxBytesOutstanding = builder.maxBytesOutstanding;
    this.ackDeadlineSeconds = builder.ackDeadlineSeconds;
    this.pullTimeoutSeconds = builder.pullTimeoutSeconds;
    this.seekMode = builder.seekMode == null ? SeekMode.NONE : builder.seekMode;
    this.seekTime = builder.seekTime;
    this.seekSnapshot = builder.seekSnapshot;
    this.credentialsFile = builder.credentialsFile;
    this.emulatorHost = builder.emulatorHost;
    this.returnImmediately = builder.returnImmediately;
    validate();
  }

  private void validate() {
    if (projectId.isBlank()) {
      throw new IllegalArgumentException("projectId must not be blank");
    }
    if (subscription.isBlank()) {
      throw new IllegalArgumentException("subscription must not be blank");
    }
    if (maxMessagesPerPull <= 0) {
      throw new IllegalArgumentException("maxMessagesPerPull must be > 0");
    }
    if (maxBytesOutstanding <= 0) {
      throw new IllegalArgumentException("maxBytesOutstanding must be > 0");
    }
    if (ackDeadlineSeconds <= 0) {
      throw new IllegalArgumentException("ackDeadlineSeconds must be > 0");
    }
    if (pullTimeoutSeconds <= 0) {
      throw new IllegalArgumentException("pullTimeoutSeconds must be > 0");
    }
    if (seekMode == SeekMode.TIMESTAMP && (seekTime == null || seekTime.isBlank())) {
      throw new IllegalArgumentException("seek=timestamp requires seekTime");
    }
    if (seekMode == SeekMode.SNAPSHOT && (seekSnapshot == null || seekSnapshot.isBlank())) {
      throw new IllegalArgumentException("seek=snapshot requires seekSnapshot");
    }
  }

  public static PubSubConfig fromOptions(Map<String, String> options) {
    Map<String, String> normalized = new HashMap<>();
    for (Map.Entry<String, String> e : options.entrySet()) {
      normalized.put(e.getKey().toLowerCase(Locale.ROOT), e.getValue());
    }
    Builder b = new Builder();
    b.projectId(first(normalized, "projectid", "project"));
    b.subscription(first(normalized, "subscription", "subscriptionname"));
    b.topic(first(normalized, "topic", "topicname"));
    String ack = first(normalized, "ackmode");
    if (ack != null) {
      b.ackMode(AckMode.fromString(ack));
    }
    String maxMsg = first(normalized, "maxmessagesperpull", "maxmessages");
    if (maxMsg != null) {
      b.maxMessagesPerPull(Integer.parseInt(maxMsg));
    }
    String maxBytes = first(normalized, "maxbytesoutstanding");
    if (maxBytes != null) {
      b.maxBytesOutstanding(Long.parseLong(maxBytes));
    }
    String ackDeadline = first(normalized, "ackdeadlineseconds");
    if (ackDeadline != null) {
      b.ackDeadlineSeconds(Integer.parseInt(ackDeadline));
    }
    String pullTimeout = first(normalized, "pulltimeoutseconds");
    if (pullTimeout != null) {
      b.pullTimeoutSeconds(Integer.parseInt(pullTimeout));
    }
    String seek = first(normalized, "seek");
    if (seek != null) {
      b.seekMode(SeekMode.fromString(seek));
    }
    b.seekTime(first(normalized, "seektime"));
    b.seekSnapshot(first(normalized, "seeksnapshot"));
    b.credentialsFile(first(normalized, "credentialsfile", "credentials"));
    b.emulatorHost(first(normalized, "emulatorhost"));
    String returnImm = first(normalized, "returnimmediately");
    if (returnImm != null) {
      b.returnImmediately(Boolean.parseBoolean(returnImm));
    }
    return b.build();
  }

  private static String first(Map<String, String> map, String... keys) {
    for (String key : keys) {
      if (map.containsKey(key) && map.get(key) != null) {
        return map.get(key);
      }
    }
    return null;
  }

  public String projectId() {
    return projectId;
  }

  public String subscription() {
    return subscription;
  }

  public Optional<String> topic() {
    return Optional.ofNullable(topic).filter(t -> !t.isBlank());
  }

  public AckMode ackMode() {
    return ackMode;
  }

  public int maxMessagesPerPull() {
    return maxMessagesPerPull;
  }

  public long maxBytesOutstanding() {
    return maxBytesOutstanding;
  }

  public int ackDeadlineSeconds() {
    return ackDeadlineSeconds;
  }

  public int pullTimeoutSeconds() {
    return pullTimeoutSeconds;
  }

  public SeekMode seekMode() {
    return seekMode;
  }

  public Optional<String> seekTime() {
    return Optional.ofNullable(seekTime).filter(t -> !t.isBlank());
  }

  public Optional<String> seekSnapshot() {
    return Optional.ofNullable(seekSnapshot).filter(t -> !t.isBlank());
  }

  public Optional<String> credentialsFile() {
    return Optional.ofNullable(credentialsFile).filter(t -> !t.isBlank());
  }

  public Optional<String> emulatorHost() {
    return Optional.ofNullable(emulatorHost).filter(t -> !t.isBlank());
  }

  public boolean returnImmediately() {
    return returnImmediately;
  }

  public String subscriptionPath() {
    if (subscription.startsWith("projects/")) {
      return subscription;
    }
    return String.format("projects/%s/subscriptions/%s", projectId, subscription);
  }

  public Optional<String> topicPath() {
    return topic()
        .map(
            t ->
                t.startsWith("projects/")
                    ? t
                    : String.format("projects/%s/topics/%s", projectId, t));
  }

  public Map<String, String> toOptionsMap() {
    Map<String, String> map = new HashMap<>();
    map.put(PROJECT_ID, projectId);
    map.put(SUBSCRIPTION, subscription);
    topic().ifPresent(t -> map.put(TOPIC, t));
    map.put(ACK_MODE, ackMode.name().toLowerCase(Locale.ROOT).replace('_', '-'));
    map.put(MAX_MESSAGES_PER_PULL, Integer.toString(maxMessagesPerPull));
    map.put(MAX_BYTES_OUTSTANDING, Long.toString(maxBytesOutstanding));
    map.put(ACK_DEADLINE_SECONDS, Integer.toString(ackDeadlineSeconds));
    map.put(PULL_TIMEOUT_SECONDS, Integer.toString(pullTimeoutSeconds));
    map.put(SEEK, seekMode.name().toLowerCase(Locale.ROOT));
    seekTime().ifPresent(t -> map.put(SEEK_TIME, t));
    seekSnapshot().ifPresent(t -> map.put(SEEK_SNAPSHOT, t));
    credentialsFile().ifPresent(t -> map.put(CREDENTIALS_FILE, t));
    emulatorHost().ifPresent(t -> map.put(EMULATOR_HOST, t));
    map.put(RETURN_IMMEDIATELY, Boolean.toString(returnImmediately));
    return map;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String projectId;
    private String subscription;
    private String topic;
    private AckMode ackMode = AckMode.AFTER_COMMIT;
    private int maxMessagesPerPull = DEFAULT_MAX_MESSAGES_PER_PULL;
    private long maxBytesOutstanding = DEFAULT_MAX_BYTES_OUTSTANDING;
    private int ackDeadlineSeconds = DEFAULT_ACK_DEADLINE_SECONDS;
    private int pullTimeoutSeconds = DEFAULT_PULL_TIMEOUT_SECONDS;
    private SeekMode seekMode = SeekMode.NONE;
    private String seekTime;
    private String seekSnapshot;
    private String credentialsFile;
    private String emulatorHost;
    private boolean returnImmediately = false;

    public Builder projectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder subscription(String subscription) {
      this.subscription = subscription;
      return this;
    }

    public Builder topic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder ackMode(AckMode ackMode) {
      this.ackMode = ackMode;
      return this;
    }

    public Builder maxMessagesPerPull(int maxMessagesPerPull) {
      this.maxMessagesPerPull = maxMessagesPerPull;
      return this;
    }

    public Builder maxBytesOutstanding(long maxBytesOutstanding) {
      this.maxBytesOutstanding = maxBytesOutstanding;
      return this;
    }

    public Builder ackDeadlineSeconds(int ackDeadlineSeconds) {
      this.ackDeadlineSeconds = ackDeadlineSeconds;
      return this;
    }

    public Builder pullTimeoutSeconds(int pullTimeoutSeconds) {
      this.pullTimeoutSeconds = pullTimeoutSeconds;
      return this;
    }

    public Builder seekMode(SeekMode seekMode) {
      this.seekMode = seekMode;
      return this;
    }

    public Builder seekTime(String seekTime) {
      this.seekTime = seekTime;
      return this;
    }

    public Builder seekSnapshot(String seekSnapshot) {
      this.seekSnapshot = seekSnapshot;
      return this;
    }

    public Builder credentialsFile(String credentialsFile) {
      this.credentialsFile = credentialsFile;
      return this;
    }

    public Builder emulatorHost(String emulatorHost) {
      this.emulatorHost = emulatorHost;
      return this;
    }

    public Builder returnImmediately(boolean returnImmediately) {
      this.returnImmediately = returnImmediately;
      return this;
    }

    public PubSubConfig build() {
      return new PubSubConfig(this);
    }
  }
}
