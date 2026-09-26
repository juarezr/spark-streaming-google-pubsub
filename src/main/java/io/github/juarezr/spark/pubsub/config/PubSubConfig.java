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
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Validated connector configuration for Structured Streaming. */
public final class PubSubConfig implements Serializable {

  private static final long serialVersionUID = 75047261L;

  public static final String SHORT_NAME = "google-pubsub";

  public static final String PROJECT_ID = "projectId";
  public static final String SUBSCRIPTION = "subscription";
  public static final String TOPIC = "topic";
  public static final String ACK_MODE = "ackMode";
  public static final String MAX_RETRY_TIME = "maxRetryTime";
  public static final String ACK_DEADLINE = "ackDeadline";
  public static final String GATHER_MODE = "gatherMode";
  public static final String RECEIVE_TIME = "receiveTime";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_COUNT = "batchCount";
  public static final String NUM_WRITERS = "numWriters";
  public static final String SEEK = "seek";
  public static final String SEEK_TIME = "seekTime";
  public static final String SEEK_SNAPSHOT = "seekSnapshot";
  public static final String LIMIT_TIME = "limitTime";
  public static final String CREDENTIALS_FILE = "credentialsFile";
  public static final String EMULATOR_HOST = "emulatorHost";
  public static final String SCHEMA_MODE = "schemaMode";
  public static final String METADATA_MODE = "metadataMode";

  public static final Duration DEFAULT_MAX_RETRY_TIME = Duration.ofSeconds(90);
  public static final Duration ACK_DEADLINE_SEED = Duration.ofSeconds(180);
  public static final long DEFAULT_BATCH_SIZE = 64L * 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(PubSubConfig.class);
  private static final AtomicBoolean VERSION_LOGGED = new AtomicBoolean();
  private static final AtomicBoolean CONFIG_LOGGED = new AtomicBoolean();

  private final String projectId;
  private final String subscription;
  private final String topic;
  private final AckMode ackMode;
  private final Duration maxRetryTime;
  private final boolean maxRetryTimeSet;
  private final Duration ackDeadline;
  private final GatherMode gatherMode;
  private final Duration receiveTime;
  private final long batchSize;
  private final long batchCount;
  private final String numWriters;
  private final SeekMode seekMode;
  private final String seekTime;
  private final String seekSnapshot;
  private final String limitTime;
  private final String credentialsFile;
  private final String emulatorHost;
  private final SchemaMode schemaMode;
  private final MetadataMode metadataMode;

  private PubSubConfig(Builder builder) {
    this.projectId = Objects.requireNonNull(builder.projectId, "projectId is required");
    this.subscription = Objects.requireNonNull(builder.subscription, "subscription is required");
    this.topic = builder.topic;
    this.ackMode = builder.ackMode == null ? AckMode.AFTER_COMMIT : builder.ackMode;
    this.maxRetryTimeSet = builder.maxRetryTimeSet;
    this.ackDeadline = builder.ackDeadline;
    this.maxRetryTime =
        resolveMaxRetryTime(builder.maxRetryTime, builder.maxRetryTimeSet, ackDeadline);
    this.gatherMode = builder.gatherMode;
    this.receiveTime = builder.receiveTime;
    this.batchSize = builder.batchSize;
    this.batchCount = builder.batchCount;
    this.numWriters = builder.numWriters;
    this.seekMode = builder.seekMode == null ? SeekMode.NONE : builder.seekMode;
    this.seekTime = builder.seekTime;
    this.seekSnapshot = builder.seekSnapshot;
    this.limitTime = builder.limitTime;
    this.credentialsFile = builder.credentialsFile;
    this.emulatorHost = builder.emulatorHost;
    this.schemaMode = builder.schemaMode == null ? SchemaMode.BASIC : builder.schemaMode;
    this.metadataMode = builder.metadataMode == null ? MetadataMode.NONE : builder.metadataMode;
    validate();
  }

  private void validate() {
    if (projectId.isBlank()) {
      throw new IllegalArgumentException("projectId must not be blank");
    }
    if (subscription.isBlank()) {
      throw new IllegalArgumentException("subscription must not be blank");
    }
    if (receiveTime != null && (receiveTime.isZero() || receiveTime.isNegative())) {
      throw new IllegalArgumentException("receiveTime must be > 0");
    }
    if (batchSize != 0 && batchSize < 1024L * 1024L) {
      throw new IllegalArgumentException("batchSize must be 0/blank or at least 1m");
    }
    if (batchCount < 0) {
      throw new IllegalArgumentException("batchCount must be >= 0");
    }
    if (ackDeadline != null
        && (ackDeadline.compareTo(Duration.ofSeconds(10)) < 0
            || ackDeadline.compareTo(Duration.ofSeconds(600)) > 0
            || ackDeadline.toMillis() % 1000L != 0L)) {
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
    if (limitTime != null && !limitTime.isBlank()) {
      Instant limit = parseInstant(LIMIT_TIME, limitTime);
      if (seekMode == SeekMode.TIMESTAMP) {
        Instant seek = parseSeekTime(seekTime);
        if (!limit.isAfter(seek)) {
          throw new IllegalArgumentException("limitTime must be > seekTime");
        }
      }
    }
  }

  public static PubSubConfig fromOptions(Map<String, String> options) {
    Map<String, String> normalized = new HashMap<>();
    for (Map.Entry<String, String> e : options.entrySet()) {
      normalized.put(e.getKey().toLowerCase(Locale.ROOT), e.getValue());
    }
    rejectRemovedOptions(normalized);
    Builder b = new Builder();
    b.projectId(first(normalized, "projectid", "project"));
    b.subscription(first(normalized, "subscription", "subscriptionname"));
    b.topic(first(normalized, "topic", "topicname"));
    String ack = first(normalized, "ackmode");
    if (ack != null) {
      b.ackMode(AckMode.fromString(ack));
    }
    String maxRetry = first(normalized, "maxretrytime");
    if (maxRetry != null) {
      b.maxRetryTime(parseDuration(MAX_RETRY_TIME, maxRetry));
    }
    String ackDeadline = first(normalized, "ackdeadline");
    if (ackDeadline != null) {
      b.ackDeadline(parseDuration(ACK_DEADLINE, ackDeadline));
    }
    String gatherMode = first(normalized, "gathermode");
    if (gatherMode != null) {
      b.gatherMode(GatherMode.fromString(gatherMode));
    }
    String receiveTime = first(normalized, "receivetime");
    if (receiveTime != null) {
      b.receiveTime(parseDuration(RECEIVE_TIME, receiveTime));
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
    b.limitTime(first(normalized, "limittime"));
    b.credentialsFile(first(normalized, "credentialsfile", "credentials"));
    b.emulatorHost(first(normalized, "emulatorhost"));
    String schemaMode = first(normalized, "schemamode");
    if (schemaMode != null) {
      b.schemaMode(SchemaMode.fromString(schemaMode));
    }
    String metadataMode = first(normalized, "metadatamode");
    if (metadataMode != null) {
      b.metadataMode(MetadataMode.fromString(metadataMode));
    }
    return b.build();
  }

  private static void rejectRemovedOptions(Map<String, String> normalized) {
    if (normalized.containsKey("batchtime")) {
      throw new IllegalArgumentException(
          "batchTime was removed in 0.9.0; use receiveTime (no alias)");
    }
    if (normalized.containsKey("pullmaxmessages")) {
      throw new IllegalArgumentException(
          "pullMaxMessages was removed in 0.9.0; unary Pull is gone");
    }
    if (normalized.containsKey("pulldeadline")) {
      throw new IllegalArgumentException("pullDeadline was removed in 0.9.0; unary Pull is gone");
    }
  }

  static Duration resolveMaxRetryTime(Duration requested, boolean explicit, Duration ackDeadline) {
    if (explicit) {
      return requested == null ? Duration.ZERO : requested;
    }
    Duration lease = ackDeadline == null ? ACK_DEADLINE_SEED : ackDeadline;
    Duration cap = DEFAULT_MAX_RETRY_TIME;
    return cap.compareTo(lease) <= 0 ? cap : lease;
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
    return parseInstant(SEEK_TIME, raw);
  }

  static Instant parseInstant(String option, String raw) {
    String value = raw == null ? "" : raw.trim();
    try {
      if (!value.isEmpty() && value.chars().allMatch(Character::isDigit)) {
        return Instant.ofEpochMilli(Long.parseLong(value));
      }
      return OffsetDateTime.parse(value).toInstant();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid " + option + " '" + raw + "'. Use epoch milliseconds or RFC-3339 with Z/offset.",
          e);
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

  public Duration maxRetryTime() {
    return maxRetryTime;
  }

  public boolean maxRetryTimeSet() {
    return maxRetryTimeSet;
  }

  public Duration ackDeadline() {
    return ackDeadline;
  }

  /**
   * Lease when {@code ackDeadline} is omitted. Uses {@code interval} when known, otherwise 180s.
   */
  public Duration effectiveAckDeadline(Duration batchInterval) {
    if (ackDeadline != null) {
      return ackDeadline;
    }
    if (batchInterval == null || batchInterval.isZero() || batchInterval.isNegative()) {
      return ACK_DEADLINE_SEED;
    }
    long seconds = Math.max(1L, batchInterval.getSeconds() * 3L);
    Duration inferred = Duration.ofSeconds(seconds);
    if (inferred.compareTo(Duration.ofSeconds(60)) < 0) {
      return Duration.ofSeconds(60);
    }
    if (inferred.compareTo(Duration.ofSeconds(600)) > 0) {
      return Duration.ofSeconds(600);
    }
    return inferred;
  }

  public Duration effectiveMaxRetryTime() {
    return maxRetryTime;
  }

  public GatherMode gatherMode() {
    return gatherMode;
  }

  public Duration receiveTime() {
    return receiveTime;
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

  public Optional<String> limitTime() {
    return Optional.ofNullable(limitTime).filter(t -> !t.isBlank());
  }

  /** Parsed {@link #limitTime()} value; empty when unset. */
  public Optional<Instant> limitTimeAsInstant() {
    return limitTime().map(t -> parseInstant(LIMIT_TIME, t));
  }

  public Optional<String> credentialsFile() {
    return Optional.ofNullable(credentialsFile).filter(t -> !t.isBlank());
  }

  public Optional<String> emulatorHost() {
    return Optional.ofNullable(emulatorHost).filter(t -> !t.isBlank());
  }

  public SchemaMode schemaMode() {
    return schemaMode;
  }

  public MetadataMode metadataMode() {
    return metadataMode;
  }

  public static void logVersionOnce() {
    if (!VERSION_LOGGED.compareAndSet(false, true)) {
      return;
    }
    LOG.info("format={}", PubSubConfig.SHORT_NAME);
    LOG.info(
        "version={} built={} git={}",
        implementationVersion(),
        PubSubBuildInfo.built(),
        PubSubBuildInfo.git());
  }

  static String implementationVersion() {
    return PubSubBuildInfo.version();
  }

  /** One-line option snapshot for startup logs. Omits credentials. */
  public void logStartupSummaryOnce() {
    if (CONFIG_LOGGED.compareAndSet(false, true)) {
      LOG.info("config {}", this.startupSummary());
    }
  }

  String startupSummary() {
    String receive = receiveTime == null ? "auto" : formatDuration(receiveTime);
    String size = batchSize <= 0 ? "off" : batchSize + "b";
    String count = batchCount <= 0 ? "off" : Long.toString(batchCount);
    StringBuilder line = new StringBuilder();
    line.append("gatherMode=")
        .append(gatherMode)
        .append(" receiveTime=")
        .append(receive)
        .append(" ackDeadline=")
        .append(ackDeadline == null ? "auto" : formatDuration(ackDeadline))
        .append(" maxRetryTime=")
        .append(formatDuration(maxRetryTime))
        .append(" ackMode=")
        .append(ackMode)
        .append(" seek=")
        .append(seekMode)
        .append(" limitTime=")
        .append(limitTime().orElse("-"))
        .append(" batchSize=")
        .append(size)
        .append(" batchCount=")
        .append(count);
    emulatorHost().ifPresent(host -> line.append(" emulatorHost=").append(host));
    return line.toString();
  }

  private static String formatDuration(Duration duration) {
    if (duration == null) {
      return "-";
    }
    long ms = duration.toMillis();
    if (ms % 60000L == 0L) {
      return (ms / 60000L) + "min";
    } else if (ms % 1000L == 0L) {
      return (ms / 1000L) + "s";
    }
    return ms + "ms";
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

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String projectId;
    private String subscription;
    private String topic;
    private AckMode ackMode = AckMode.AFTER_COMMIT;
    private Duration maxRetryTime = DEFAULT_MAX_RETRY_TIME;
    private boolean maxRetryTimeSet;
    private Duration ackDeadline;
    private GatherMode gatherMode = GatherMode.BATCH;
    private Duration receiveTime;
    private long batchSize = DEFAULT_BATCH_SIZE;
    private long batchCount;
    private String numWriters = "1";
    private SeekMode seekMode = SeekMode.NONE;
    private String seekTime;
    private String seekSnapshot;
    private String limitTime;
    private String credentialsFile;
    private String emulatorHost;
    private SchemaMode schemaMode = SchemaMode.BASIC;
    private MetadataMode metadataMode = MetadataMode.NONE;

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

    public Builder maxRetryTime(Duration maxRetryTime) {
      this.maxRetryTime = maxRetryTime;
      this.maxRetryTimeSet = true;
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

    public Builder receiveTime(Duration receiveTime) {
      this.receiveTime = receiveTime;
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

    public Builder limitTime(String limitTime) {
      this.limitTime = limitTime;
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

    public Builder schemaMode(SchemaMode schemaMode) {
      this.schemaMode = schemaMode;
      return this;
    }

    public Builder metadataMode(MetadataMode metadataMode) {
      this.metadataMode = metadataMode;
      return this;
    }

    public PubSubConfig build() {
      return new PubSubConfig(this);
    }
  }
}
