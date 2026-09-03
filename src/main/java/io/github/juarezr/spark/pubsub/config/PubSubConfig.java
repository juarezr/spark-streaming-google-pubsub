package io.github.juarezr.spark.pubsub.config;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Validated connector configuration for Structured Streaming. */
public final class PubSubConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SHORT_NAME = "google-pubsub";

  public static final String PROJECT_ID = "projectId";
  public static final String SUBSCRIPTION = "subscription";
  public static final String TOPIC = "topic";
  public static final String ACK_MODE = "ackMode";
  public static final String PULL_MAX_MESSAGES = "pullMaxMessages";
  public static final String MAX_RETRY_TIME = "maxRetryTime";
  public static final String PULL_DEADLINE = "pullDeadline";
  public static final String ACK_DEADLINE = "ackDeadline";
  public static final String GATHER_MODE = "gatherMode";
  public static final String BATCH_TIME = "batchTime";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_COUNT = "batchCount";
  public static final String NUM_WRITERS = "numWriters";
  public static final String SEEK = "seek";
  public static final String SEEK_TIME = "seekTime";
  public static final String SEEK_SNAPSHOT = "seekSnapshot";
  public static final String CREDENTIALS_FILE = "credentialsFile";
  public static final String EMULATOR_HOST = "emulatorHost";

  public static final int DEFAULT_PULL_MAX_MESSAGES = 1000;
  public static final Duration DEFAULT_MAX_RETRY_TIME = Duration.ofSeconds(90);
  public static final Duration DEFAULT_PULL_DEADLINE = Duration.ofSeconds(20);
  public static final Duration DEFAULT_ACK_DEADLINE = Duration.ofSeconds(60);
  public static final Duration DEFAULT_BATCH_TIME = Duration.ofSeconds(10);
  public static final long DEFAULT_BATCH_SIZE = 128L * 1024 * 1024;

  private final String projectId;
  private final String subscription;
  private final String topic;
  private final AckMode ackMode;
  private final int pullMaxMessages;
  private final Duration maxRetryTime;
  private final Duration pullDeadline;
  private final Duration ackDeadline;
  private final GatherMode gatherMode;
  private final Duration batchTime;
  private final long batchSize;
  private final long batchCount;
  private final String numWriters;
  private final SeekMode seekMode;
  private final String seekTime;
  private final String seekSnapshot;
  private final String credentialsFile;
  private final String emulatorHost;

  private PubSubConfig(Builder builder) {
    this.projectId = Objects.requireNonNull(builder.projectId, "projectId is required");
    this.subscription = Objects.requireNonNull(builder.subscription, "subscription is required");
    this.topic = builder.topic;
    this.ackMode = builder.ackMode == null ? AckMode.AFTER_COMMIT : builder.ackMode;
    this.pullMaxMessages = builder.pullMaxMessages;
    this.maxRetryTime = builder.maxRetryTime;
    this.pullDeadline = builder.pullDeadline;
    this.ackDeadline = builder.ackDeadline;
    this.gatherMode = builder.gatherMode;
    this.batchTime = builder.batchTime;
    this.batchSize = builder.batchSize;
    this.batchCount = builder.batchCount;
    this.numWriters = builder.numWriters;
    this.seekMode = builder.seekMode == null ? SeekMode.NONE : builder.seekMode;
    this.seekTime = builder.seekTime;
    this.seekSnapshot = builder.seekSnapshot;
    this.credentialsFile = builder.credentialsFile;
    this.emulatorHost = builder.emulatorHost;
    validate();
  }

  private void validate() {
    if (projectId.isBlank()) {
      throw new IllegalArgumentException("projectId must not be blank");
    }
    if (subscription.isBlank()) {
      throw new IllegalArgumentException("subscription must not be blank");
    }
    if (pullMaxMessages <= 0 || pullMaxMessages > 1000) {
      throw new IllegalArgumentException("pullMaxMessages must be between 1 and 1000");
    }
    if (batchTime == null || batchTime.isZero() || batchTime.isNegative()) {
      throw new IllegalArgumentException("batchTime must be > 0");
    }
    if (batchSize != 0 && batchSize < 1024L * 1024L) {
      throw new IllegalArgumentException("batchSize must be 0/blank or at least 1m");
    }
    if (batchCount < 0) {
      throw new IllegalArgumentException("batchCount must be >= 0");
    }
    if (pullDeadline == null || pullDeadline.isZero() || pullDeadline.isNegative()) {
      throw new IllegalArgumentException("pullDeadline must be > 0");
    }
    if (pullDeadline.compareTo(Duration.ofSeconds(600)) > 0) {
      throw new IllegalArgumentException("pullDeadline must be <= 600s");
    }
    if (ackDeadline == null
        || ackDeadline.compareTo(Duration.ofSeconds(10)) < 0
        || ackDeadline.compareTo(Duration.ofSeconds(600)) > 0
        || ackDeadline.toMillis() % 1000L != 0L) {
      throw new IllegalArgumentException(
          "ackDeadline must be a whole number of seconds between 10s and 600s");
    }
    if (maxRetryTime == null
        || maxRetryTime.isNegative()
        || maxRetryTime.compareTo(Duration.ofMinutes(30)) > 0) {
      throw new IllegalArgumentException("maxRetryTime must be between 0 and 30m");
    }
    if (!"auto".equalsIgnoreCase(numWriters)) {
      try {
        if (Integer.parseInt(numWriters) < 1) {
          throw new IllegalArgumentException("numWriters must be auto or an integer >= 1");
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("numWriters must be auto or an integer >= 1", e);
      }
    }
    if (seekMode == SeekMode.TIMESTAMP && (seekTime == null || seekTime.isBlank())) {
      throw new IllegalArgumentException("seek=timestamp requires seekTime");
    }
    if (seekMode == SeekMode.TIMESTAMP) {
      parseSeekTime(seekTime);
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
    String maxMsg = first(normalized, "pullmaxmessages");
    if (maxMsg != null) {
      b.pullMaxMessages(Integer.parseInt(maxMsg));
    }
    String maxRetry = first(normalized, "maxretrytime");
    if (maxRetry != null) {
      b.maxRetryTime(parseDuration(MAX_RETRY_TIME, maxRetry));
    }
    String pullDeadline = first(normalized, "pulldeadline");
    if (pullDeadline != null) {
      b.pullDeadline(parseDuration(PULL_DEADLINE, pullDeadline));
    }
    String ackDeadline = first(normalized, "ackdeadline");
    if (ackDeadline != null) {
      b.ackDeadline(parseDuration(ACK_DEADLINE, ackDeadline));
    }
    String gatherMode = first(normalized, "gathermode");
    if (gatherMode != null) {
      b.gatherMode(GatherMode.fromString(gatherMode));
    }
    String batchTime = first(normalized, "batchtime");
    if (batchTime != null) {
      b.batchTime(parseDuration(BATCH_TIME, batchTime));
    }
    String batchSize = first(normalized, "batchsize");
    if (batchSize != null) {
      b.batchSize(parseSize(BATCH_SIZE, batchSize));
    }
    String batchCount = first(normalized, "batchcount");
    if (batchCount != null && !batchCount.isBlank()) {
      b.batchCount(Long.parseLong(batchCount));
    }
    String numWriters = first(normalized, "numwriters");
    if (numWriters != null) {
      b.numWriters(numWriters);
    }
    String seek = first(normalized, "seek");
    if (seek != null) {
      b.seekMode(SeekMode.fromString(seek));
    }
    b.seekTime(first(normalized, "seektime"));
    b.seekSnapshot(first(normalized, "seeksnapshot"));
    b.credentialsFile(first(normalized, "credentialsfile", "credentials"));
    b.emulatorHost(first(normalized, "emulatorhost"));
    return b.build();
  }

  static Duration parseDuration(String option, String raw) {
    if (raw == null || raw.isBlank()) {
      throw new IllegalArgumentException(option + " must not be blank");
    }
    String value = raw.trim().toLowerCase(Locale.ROOT);
    try {
      if (value.endsWith("ms")) {
        return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
      }
      if (value.endsWith("s")) {
        return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
      }
      if (value.endsWith("m")) {
        return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
      }
      return Duration.ofSeconds(Long.parseLong(value));
    } catch (ArithmeticException | NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid " + option + " '" + raw + "'. Use a number with ms, s, or m.", e);
    }
  }

  static long parseSize(String option, String raw) {
    if (raw == null || raw.isBlank()) {
      return 0L;
    }
    String value = raw.trim().toLowerCase(Locale.ROOT);
    long multiplier = 1L;
    char suffix = value.charAt(value.length() - 1);
    if (suffix == 'k' || suffix == 'm' || suffix == 'g') {
      value = value.substring(0, value.length() - 1);
      multiplier = suffix == 'k' ? 1024L : suffix == 'm' ? 1024L * 1024L : 1024L * 1024L * 1024L;
    }
    try {
      return Math.multiplyExact(Long.parseLong(value), multiplier);
    } catch (ArithmeticException | NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid " + option + " '" + raw + "'. Use bytes or a k, m, or g suffix.", e);
    }
  }

  static Instant parseSeekTime(String raw) {
    String value = raw == null ? "" : raw.trim();
    try {
      if (!value.isEmpty() && value.chars().allMatch(Character::isDigit)) {
        return Instant.ofEpochMilli(Long.parseLong(value));
      }
      return OffsetDateTime.parse(value).toInstant();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid seekTime '" + raw + "'. Use epoch milliseconds or RFC-3339 with Z/offset.", e);
    }
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

  public int pullMaxMessages() {
    return pullMaxMessages;
  }

  public Duration maxRetryTime() {
    return maxRetryTime;
  }

  public Duration pullDeadline() {
    return pullDeadline;
  }

  public Duration ackDeadline() {
    return ackDeadline;
  }

  public GatherMode gatherMode() {
    return gatherMode;
  }

  public Duration batchTime() {
    return batchTime;
  }

  public long batchSize() {
    return batchSize;
  }

  public long batchCount() {
    return batchCount;
  }

  public int numWriters() {
    return "auto".equalsIgnoreCase(numWriters)
        ? Math.max(1, Runtime.getRuntime().availableProcessors())
        : Integer.parseInt(numWriters);
  }

  public SeekMode seekMode() {
    return seekMode;
  }

  public Optional<String> seekTime() {
    return Optional.ofNullable(seekTime).filter(t -> !t.isBlank());
  }

  /** Parsed {@link #seekTime()} value; valid after construction when seek is timestamp. */
  public Instant seekTimeAsInstant() {
    return parseSeekTime(seekTime);
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

  public String subscriptionPath() {
    if (subscription.startsWith("projects/")) {
      return subscription;
    }
    return String.format("projects/%s/subscriptions/%s", projectId, subscription);
  }

  Optional<String> topicPath() {
    return topic()
        .map(
            t ->
                t.startsWith("projects/")
                    ? t
                    : String.format("projects/%s/topics/%s", projectId, t));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String projectId;
    private String subscription;
    private String topic;
    private AckMode ackMode = AckMode.AFTER_COMMIT;
    private int pullMaxMessages = DEFAULT_PULL_MAX_MESSAGES;
    private Duration maxRetryTime = DEFAULT_MAX_RETRY_TIME;
    private Duration pullDeadline = DEFAULT_PULL_DEADLINE;
    private Duration ackDeadline = DEFAULT_ACK_DEADLINE;
    private GatherMode gatherMode = GatherMode.BATCH;
    private Duration batchTime = DEFAULT_BATCH_TIME;
    private long batchSize = DEFAULT_BATCH_SIZE;
    private long batchCount;
    private String numWriters = "1";
    private SeekMode seekMode = SeekMode.NONE;
    private String seekTime;
    private String seekSnapshot;
    private String credentialsFile;
    private String emulatorHost;

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

    public Builder pullMaxMessages(int pullMaxMessages) {
      this.pullMaxMessages = pullMaxMessages;
      return this;
    }

    public Builder maxRetryTime(Duration maxRetryTime) {
      this.maxRetryTime = maxRetryTime;
      return this;
    }

    public Builder pullDeadline(Duration pullDeadline) {
      this.pullDeadline = pullDeadline;
      return this;
    }

    public Builder ackDeadline(Duration ackDeadline) {
      this.ackDeadline = ackDeadline;
      return this;
    }

    public Builder gatherMode(GatherMode gatherMode) {
      this.gatherMode = gatherMode;
      return this;
    }

    public Builder batchTime(Duration batchTime) {
      this.batchTime = batchTime;
      return this;
    }

    public Builder batchSize(long batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder batchCount(long batchCount) {
      this.batchCount = batchCount;
      return this;
    }

    public Builder numWriters(String numWriters) {
      this.numWriters = numWriters;
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

    public PubSubConfig build() {
      return new PubSubConfig(this);
    }
  }
}
