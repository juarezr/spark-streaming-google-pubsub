package io.github.juarezr.spark.pubsub.structured;

import com.google.api.core.ApiService;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SeekRequest;
import io.github.juarezr.spark.pubsub.config.PubSubConfig;
import io.github.juarezr.spark.pubsub.config.SeekMode;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Warm {@link Subscriber} (StreamingPull) plus a bounded queue. The receiver never acks; Spark
 * commit calls {@link #acknowledge}.
 */
final class PubSubClient implements Closeable, Serializable {

  private static final long serialVersionUID = -3861530486L;
  private static final Logger LOG = LoggerFactory.getLogger(PubSubClient.class);

  private final RetryPolicy retryPolicy;
  private PubSubConnectionErrors connectionErrors;
  private transient PubSubEmulator emulator;

  private final PubSubConfig config;
  private final PubSubCredentialsProvider credentialsProvider;
  private transient Subscriber subscriber;
  private transient LinkedBlockingQueue<HeldMessage> queue;
  private transient ConcurrentHashMap<String, AckReplyConsumer> consumers;
  private transient AtomicLong outstandingBytes;
  private transient AtomicLong queuedBytes;
  private transient AtomicBoolean stopped;
  private transient boolean seekApplied;

  /** Caps the next {@link #poll(Duration)} so overflow stays queued for the following batch. */
  private transient int nextPollMax = Integer.MAX_VALUE;

  private final AtomicLong lastSeenNewestPublishMillis = new AtomicLong(Long.MIN_VALUE);
  private final AtomicLong lastHeapCeilingWarnMillis = new AtomicLong(Long.MIN_VALUE);
  private final AtomicLong lastHostReserveWarnMillis = new AtomicLong(Long.MIN_VALUE);
  private final AtomicBoolean intakePaused = new AtomicBoolean(false);
  private volatile long memoryBudget;
  private long baselineBytes;
  private long flowSeedBytes;
  private long memoryLimitBytes;
  private int calmStreak;
  private int hotStreak;
  private long lastMove;
  private boolean lastShrank;
  private int approachStreak;
  private volatile String pauseReason;
  private final AtomicLong cachedAvailableBytes = new AtomicLong(Long.MIN_VALUE);
  private final AtomicLong cachedAvailableAtMillis = new AtomicLong(Long.MIN_VALUE);
  private volatile long cycleBudgetMillis;
  private long hostPauseSinceMillis = Long.MIN_VALUE;
  private boolean hostCollected;

  final class PubSubMessageReceiver implements MessageReceiver {

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
      if (stopped != null && stopped.get()) {
        consumer.nack();
        return;
      }
      PulledMessage pulled = toPulled(message);
      while (aboveHeapCeiling() && stopped != null && !stopped.get()) {
        noteHeapCeiling();
        collectHostReserveOnce();
        try {
          TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          consumer.nack();
          return;
        }
      }
      if (!DriverHeap.belowHostReserve(cachedAvailable(System.currentTimeMillis()))) {
        resetHostPause();
      }
      if (stopped != null && stopped.get()) {
        consumer.nack();
        return;
      }
      if (!awaitReserve(pulled.data().length)) {
        consumer.nack();
        return;
      }
      consumers.put(pulled.ackId(), consumer);
      outstandingBytes.addAndGet(pulled.data().length);
      try {
        queue.put(new HeldMessage(pulled, consumer));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        consumers.remove(pulled.ackId());
        subtractQueued(pulled.data().length);
        outstandingBytes.addAndGet(-pulled.data().length);
        consumer.nack();
      }
    }

    private PulledMessage toPulled(PubsubMessage message) {
      byte[] data = message.getData().toByteArray();
      long publishMillis =
          message.getPublishTime().getSeconds() * 1000L
              + message.getPublishTime().getNanos() / 1_000_000L;
      String messageId = message.getMessageId();
      return new PulledMessage(
          messageId,
          data,
          message.getAttributesMap(),
          publishMillis,
          message.getOrderingKey(),
          messageId);
    }
  }

  static final class HeldMessage {
    final PulledMessage message;
    final AckReplyConsumer consumer;

    HeldMessage(PulledMessage message, AckReplyConsumer consumer) {
      this.message = message;
      this.consumer = consumer;
    }
  }

  PubSubClient(PubSubConfig config) {
    this(
        config,
        new PubSubCredentialsProvider(config.credentialsFile().orElse(null)),
        RetryPolicy.defaults(config.effectiveMaxRetryTime()));
  }

  PubSubClient(
      PubSubConfig config, PubSubCredentialsProvider credentialsProvider, RetryPolicy retryPolicy) {
    this.config = config;
    this.credentialsProvider = credentialsProvider;
    this.retryPolicy = retryPolicy;
    this.retryPolicy.lastPublishTime(this.lastSeenNewestPublishMillis::get);
  }

  synchronized void start() throws IOException {
    if (this.subscriber != null) {
      return;
    }
    this.outstandingBytes = new AtomicLong(0);
    this.queuedBytes = new AtomicLong(0);
    this.queue = new LinkedBlockingQueue<>();
    this.consumers = new ConcurrentHashMap<>();
    this.stopped = new AtomicBoolean(false);
    this.connectionErrors = new PubSubConnectionErrors(this.config.subscriptionPath());
    long heapMax = Runtime.getRuntime().maxMemory();
    long seeded = DriverHeap.seed(config.batchSize(), heapMax);
    if (seeded <= 0L) {
      seeded = config.batchSize() > 0 ? config.batchSize() : PubSubConfig.DEFAULT_BATCH_SIZE;
    }
    long limit = DriverHeap.memoryLimit(config.batchSize(), heapMax);
    long outstanding = seeded > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : seeded * 2L;
    if (limit < seeded) {
      limit = seeded;
    }
    if (limit > outstanding) {
      limit = outstanding;
    }
    this.memoryBudget = seeded;
    // Baseline stays put. Outstanding starts at twice that seed. A later empty gather rebuilds
    // the subscriber so outstanding follows memoryBudget. The climb stops at memoryLimitBytes.
    this.baselineBytes = seeded;
    this.flowSeedBytes = seeded;
    this.memoryLimitBytes = limit;
    this.lastMove = DriverHeap.openingMove(seeded);
    this.lastShrank = false;
    this.approachStreak = 0;

    this.emulator = this.config.emulatorHost().map(PubSubEmulator::new).orElse(null);
    try {
      CredentialsProvider authProvider = this.credentialsProvider.providerFor(this.config);
      if (this.emulator == null) {
        connectionErrors.verifySubscriptionAccessible(authProvider);
      }
      applySeekIfNeeded(authProvider);

      startSubscriberAsyncWith(authProvider);

      this.connectionErrors.throwIfFatalConnection();
    } catch (Exception e) {
      this.close();
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Unable to start Pub/Sub subscriber", e);
    }
  }

  /**
   * When the cap has moved, stop the subscriber and start another whose outstanding bytes are twice
   * {@code memoryBudget}. A prefetch still in the queue is nacked first. Bytes above that prefetch
   * stay outstanding. A failed start leaves the running subscriber in place.
   */
  synchronized void retargetFlow() {
    if (queuedBytes == null || outstandingBytes == null) {
      return;
    }
    if (!capMoved()) {
      return;
    }
    long nacked = nackQueued();
    if (nacked > 0L) {
      long remainder = outstandingBytes.get() - nacked;
      outstandingBytes.set(remainder > 0L ? remainder : 0L);
    }
    long previousSeed = flowSeedBytes;
    long nextSeed = memoryBudget;
    if (this.subscriber == null) {
      this.flowSeedBytes = nextSeed;
      logSubscriberOutstanding(previousSeed, nextSeed);
      return;
    }
    this.flowSeedBytes = nextSeed;
    Subscriber replacement;
    try {
      CredentialsProvider authProvider = this.credentialsProvider.providerFor(this.config);
      replacement = startSubscriber(authProvider);
    } catch (Exception e) {
      this.flowSeedBytes = previousSeed;
      LOG.warn(
          "HEAP: subscriber rebuild failed; outstanding stays {}",
          outstandingLimit(previousSeed),
          e);
      return;
    }
    Subscriber previous = this.subscriber;
    this.subscriber = replacement;
    stopSubscriber(previous);
    logSubscriberOutstanding(previousSeed, nextSeed);
  }

  private void startSubscriberAsyncWith(CredentialsProvider authProvider) {
    this.subscriber = startSubscriber(authProvider);
  }

  private Subscriber startSubscriber(CredentialsProvider authProvider) {

    Subscriber.Builder builder =
        Subscriber.newBuilder(this.config.subscriptionPath(), new PubSubMessageReceiver());
    builder.setFlowControlSettings(flowControl());

    long ackExtendSecs = config.effectiveAckDeadline(null).getSeconds();
    long ackExtendMax = Math.max(60L, ackExtendSecs);
    org.threeten.bp.Duration ackExtend = org.threeten.bp.Duration.ofSeconds(ackExtendMax);
    org.threeten.bp.Duration ackMaxPeriod = org.threeten.bp.Duration.ofMinutes(60);
    builder.setMaxAckExtensionPeriod(ackMaxPeriod);
    builder.setMaxDurationPerAckExtension(ackExtend);

    if (this.emulator != null) {
      this.emulator.configureSubscriber(builder);
    } else {
      builder.setCredentialsProvider(authProvider);
    }
    Subscriber created = builder.build();
    created.addListener(connectionErrors, MoreExecutors.directExecutor());
    created.startAsync().awaitRunning();
    return created;
  }

  private void stopSubscriber(Subscriber previous) {
    if (previous == null) {
      return;
    }
    try {
      previous.stopAsync().awaitTerminated(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // The replacement is already serving. The old one can finish stopping after this gather.
      LOG.info(
          "CONNECTION: new subscriber is already serving; the previous one was still stopping");
    } catch (Exception e) {
      LOG.warn("CONNECTION: Error shutting down subscriber", e);
    }
  }

  private void logSubscriberOutstanding(long previousSeed, long nextSeed) {
    // TODO: drop this soak line to debug before a provider release.
    LOG.info(
        "HEAP: subscriber outstanding {} -> {}",
        outstandingLimit(previousSeed),
        outstandingLimit(nextSeed));
  }

  private static long outstandingLimit(long seed) {
    if (seed <= 0L) {
      return 0L;
    }
    return seed > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : seed * 2L;
  }

  private FlowControlSettings flowControl() {
    long bytes = flowSeedBytes > 0L ? flowSeedBytes : queueCap();
    FlowControlSettings.Builder flow =
        FlowControlSettings.newBuilder()
            .setMaxOutstandingRequestBytes(outstandingLimit(bytes))
            .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block);
    if (config.batchCount() > 0) {
      long count = config.batchCount();
      long elements = count > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : count * 2;
      flow.setMaxOutstandingElementCount(elements);
    }
    return flow.build();
  }

  private void applySeekIfNeeded(CredentialsProvider authProvider) throws IOException {
    if (seekApplied || this.config.seekMode() == SeekMode.NONE) {
      return;
    }
    final SeekRequest.Builder seek = getSeekRequestBuilder();
    final SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
    try (PubSubEmulator adminEmulator =
        this.config.emulatorHost().map(PubSubEmulator::new).orElse(null)) {
      if (adminEmulator != null) {
        adminEmulator.configureSubscriptionAdmin(builder);
      } else {
        builder.setCredentialsProvider(authProvider);
      }
      try (SubscriptionAdminClient admin = SubscriptionAdminClient.create(builder.build())) {
        LOG.warn(
            "CONNECTION: Applying seek={} on subscription {} (explicit rewind requested)",
            this.config.seekMode(),
            this.config.subscriptionPath());
        admin.seek(seek.build());
      }
    }
    seekApplied = true;
  }

  private SeekRequest.Builder getSeekRequestBuilder() {
    final SeekRequest.Builder seek =
        SeekRequest.newBuilder().setSubscription(this.config.subscriptionPath());

    switch (this.config.seekMode()) {
      case BEGINNING:
        seek.setTime(Timestamp.newBuilder().setSeconds(0).setNanos(0).build());
        break;
      case TIMESTAMP:
        final Instant seekInstant = this.config.seekTimeAsInstant();
        final Timestamp seekTime =
            Timestamp.newBuilder()
                .setSeconds(seekInstant.getEpochSecond())
                .setNanos(seekInstant.getNano())
                .build();
        seek.setTime(seekTime);
        break;
      case SNAPSHOT:
        seek.setSnapshot(this.config.seekSnapshot().orElseThrow());
        break;
      case NONE:
      default:
    }
    return seek;
  }

  private void ensureStarted() {
    if (this.subscriber == null) {
      try {
        start();
      } catch (IOException e) {
        throw new IllegalStateException("CONNECTION: Failed to start Pub/Sub client", e);
      }
    }
    if (this.connectionErrors != null) {
      try {
        this.connectionErrors.throwIfFatalConnection();
      } catch (IOException e) {
        throw new IllegalStateException(e.getMessage(), e);
      }
    }
  }

  @Override
  public synchronized void close() {
    if (stopped != null) {
      stopped.set(true);
    }
    if (this.subscriber != null) {
      try {
        ApiService service = this.subscriber.stopAsync();
        service.awaitTerminated(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.warn("CONNECTION: Error shutting down subscriber", e);
      } finally {
        this.subscriber = null;
      }
    }
    nackQueued();
    nackRemainingConsumers();
    if (this.emulator != null) {
      this.emulator.close();
      this.emulator = null;
    }
  }

  /**
   * Nacks messages sitting in the queue. A reservation that has not been queued yet stays in {@code
   * queuedBytes}; zeroing the counter there made the next {@code take} go negative and {@code fits}
   * then refused every later message.
   *
   * @return bytes removed from the queue
   */
  private long nackQueued() {
    if (queue == null) {
      return 0L;
    }
    long nacked = 0L;
    HeldMessage held;
    while ((held = queue.poll()) != null) {
      nacked += held.message.data().length;
      if (consumers != null) {
        consumers.remove(held.message.ackId());
      }
      try {
        held.consumer.nack();
      } catch (RuntimeException e) {
        LOG.warn("CONNECTION: Failed to nack queued message on close: {}", e.getMessage());
      }
    }
    subtractQueued(nacked);
    return nacked;
  }

  private void nackRemainingConsumers() {
    if (consumers == null || consumers.isEmpty()) {
      return;
    }
    for (AckReplyConsumer consumer : consumers.values()) {
      try {
        consumer.nack();
      } catch (RuntimeException e) {
        LOG.warn("CONNECTION: Failed to nack consumer on close: {}", e.getMessage());
      }
    }
    consumers.clear();
  }

  private void subtractQueued(long bytes) {
    if (queuedBytes == null || bytes <= 0L) {
      return;
    }
    while (true) {
      long held = queuedBytes.get();
      long next = held > bytes ? held - bytes : 0L;
      if (queuedBytes.compareAndSet(held, next)) {
        return;
      }
    }
  }

  /**
   * Upper bound for the next {@link #poll(Duration)}. Messages beyond the cap stay in the queue.
   * {@code maxMessages <= 0} means no cap.
   */
  void limitNextPoll(int maxMessages) {
    this.nextPollMax = maxMessages <= 0 ? Integer.MAX_VALUE : maxMessages;
  }

  /**
   * Wait up to {@code timeout} for messages, up to {@link #limitNextPoll(int)}. When capped, keeps
   * waiting within the same timeout until the cap is reached or the deadline passes. When uncapped,
   * waits only for the first message, then drains the queue without blocking. Does not ack.
   */
  List<PulledMessage> poll(Duration timeout) {
    ensureStarted();
    int max = this.nextPollMax;
    this.nextPollMax = Integer.MAX_VALUE;
    List<PulledMessage> messages = new ArrayList<>();
    if (max <= 0) {
      return messages;
    }
    long deadlineNanos = System.nanoTime() + Math.max(1L, timeout.toNanos());
    try {
      long waitNanos = deadlineNanos - System.nanoTime();
      if (waitNanos <= 0L) {
        return messages;
      }
      HeldMessage first = queue.poll(waitNanos, TimeUnit.NANOSECONDS);
      if (first == null) {
        return messages;
      }
      take(first, messages);
      if (max == Integer.MAX_VALUE) {
        HeldMessage next;
        while ((next = queue.poll()) != null) {
          take(next, messages);
        }
      } else {
        while (messages.size() < max) {
          waitNanos = deadlineNanos - System.nanoTime();
          if (waitNanos <= 0L) {
            break;
          }
          HeldMessage next = queue.poll(waitNanos, TimeUnit.NANOSECONDS);
          if (next == null) {
            break;
          }
          take(next, messages);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    final PublishTimeWindow window = PublishTimeWindow.of(messages);
    if (window != null) {
      this.lastSeenNewestPublishMillis.set(window.newestMillis());
    }
    return messages;
  }

  private void take(HeldMessage held, List<PulledMessage> messages) {
    messages.add(held.message);
    subtractQueued(held.message.data().length);
  }

  void acknowledge(List<String> ackIds) {
    reply(ackIds, true);
  }

  void nack(List<String> ackIds) {
    reply(ackIds, false);
  }

  private void reply(List<String> ackIds, boolean ack) {
    if (ackIds == null || ackIds.isEmpty()) {
      return;
    }
    if (consumers == null) {
      return;
    }
    for (String id : ackIds) {
      AckReplyConsumer consumer = consumers.remove(id);
      if (consumer == null) {
        continue;
      }
      try {
        if (ack) {
          consumer.ack();
        } else {
          consumer.nack();
        }
      } catch (RuntimeException e) {
        LOG.warn("ACK: Failed to {} {}: {}", ack ? "ack" : "nack", id, e.getMessage());
      }
    }
  }

  void releaseMessages(List<PulledMessage> messages) {
    if (this.outstandingBytes != null) {
      long bytes = PulledMessage.payloadBytes(messages);
      if (bytes > 0) {
        this.outstandingBytes.addAndGet(-bytes);
      }
    }
  }

  void resetOutstandingBytes() {
    if (this.outstandingBytes != null) {
      this.outstandingBytes.set(0);
    }
  }

  long outstandingBytes() {
    return this.outstandingBytes == null ? 0L : this.outstandingBytes.get();
  }

  long queuedBytes() {
    return this.queuedBytes == null ? 0L : this.queuedBytes.get();
  }

  int queuedCount() {
    return queue == null ? 0 : queue.size();
  }

  long memoryBudget() {
    return memoryBudget;
  }

  boolean capMoved() {
    return memoryBudget > 0L && memoryBudget != flowSeedBytes;
  }

  long memoryBaseline() {
    return baselineBytes;
  }

  long flowOutstandingBytes() {
    long bytes = flowSeedBytes > 0L ? flowSeedBytes : queueCap();
    return outstandingLimit(bytes);
  }

  long memoryLimit() {
    return memoryLimitBytes;
  }

  void noteGather(long heapFraction, long memAvailable, long gatherBytes) {
    long ceiling = memoryLimitBytes > 0L ? memoryLimitBytes : memoryBudget;
    long move = lastMove;
    if (move <= 0L) {
      long seed = baselineBytes > 0L ? baselineBytes : memoryBudget;
      move = DriverHeap.openingMove(seed);
    }
    long opening = DriverHeap.openingMove(baselineBytes > 0L ? baselineBytes : memoryBudget);
    DriverHeap.BudgetStep step =
        DriverHeap.adjust(
            memoryBudget,
            ceiling,
            calmStreak,
            hotStreak,
            heapFraction,
            memAvailable,
            gatherBytes,
            move,
            lastShrank,
            approachStreak,
            opening);
    if (step.budget != memoryBudget) {
      // TODO: drop this soak line to debug before a provider release.
      LOG.info(DriverHeap.budgetChange(memoryBudget, step.budget));
      memoryBudget = step.budget;
      lastMove = step.lastMove;
      lastShrank = step.lastShrank;
    }
    calmStreak = step.calmStreak;
    hotStreak = step.hotStreak;
    approachStreak = step.approachStreak;
  }

  /**
   * Takes {@code size} bytes only when they fit under the cap. An empty queue takes one message
   * that is larger than the cap, because waiting cannot make that message fit. A failed
   * compare-and-set retries. Bytes already above the cap are left for a gather.
   */
  boolean tryReserve(int size) {
    if (queuedBytes == null || size < 0) {
      return false;
    }
    long cap = queueCap();
    while (true) {
      long held = queuedBytes.get();
      if (!fits(held, size, cap)) {
        return false;
      }
      if (queuedBytes.compareAndSet(held, held + size)) {
        return true;
      }
    }
  }

  static boolean fits(long held, int size, long cap) {
    if (held < 0L || size < 0) {
      return false;
    }
    if (held == 0L) {
      return true;
    }
    if (held >= cap) {
      return false;
    }
    long room = cap - held;
    return size <= room;
  }

  private boolean awaitReserve(int size) {
    while (stopped != null && !stopped.get()) {
      if (tryReserve(size)) {
        return true;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  private long queueCap() {
    if (memoryBudget > 0L) {
      return memoryBudget;
    }
    return config.batchSize() > 0 ? config.batchSize() : PubSubConfig.DEFAULT_BATCH_SIZE;
  }

  private String memoryReadings() {
    Runtime runtime = Runtime.getRuntime();
    long heapMax = runtime.maxMemory();
    long heapUsed = DriverHeap.used(runtime.totalMemory(), runtime.freeMemory());
    return DriverHeap.readings(
        heapUsed,
        DriverHeap.percent(heapUsed, heapMax),
        DriverHeap.residentBytes(),
        DriverHeap.availableBytes());
  }

  void noteCycleBudget(Duration cycle) {
    cycleBudgetMillis = cycle == null ? 0L : cycle.toMillis();
  }

  boolean aboveHeapCeiling() {
    boolean heap = heapAboveCeiling();
    boolean host = DriverHeap.belowHostReserve(cachedAvailable(System.currentTimeMillis()));
    if (!heap && !host && intakePaused.compareAndSet(true, false)) {
      LOG.info(
          "HEAP: resumed taking messages reason={} {}",
          pauseReason == null ? "unknown" : pauseReason,
          memoryReadings());
    }
    return heap || host;
  }

  private boolean heapAboveCeiling() {
    Runtime runtime = Runtime.getRuntime();
    return DriverHeap.aboveCeiling(
        DriverHeap.used(runtime.totalMemory(), runtime.freeMemory()), runtime.maxMemory());
  }

  private long cachedAvailable(long nowMillis) {
    long at = cachedAvailableAtMillis.get();
    long cached = cachedAvailableBytes.get();
    if (at != Long.MIN_VALUE
        && cached != Long.MIN_VALUE
        && nowMillis >= at
        && nowMillis - at < DriverHeap.HOST_CACHE_MILLIS) {
      return cached;
    }
    long read = DriverHeap.availableBytes();
    cachedAvailableBytes.set(read);
    cachedAvailableAtMillis.set(nowMillis);
    return read;
  }

  private void collectHostReserveOnce() {
    long now = System.currentTimeMillis();
    if (!DriverHeap.belowHostReserve(cachedAvailable(now))) {
      resetHostPause();
      return;
    }
    if (hostPauseSinceMillis == Long.MIN_VALUE) {
      hostPauseSinceMillis = now;
    }
    if (DriverHeap.collectDue(hostPauseSinceMillis, now, cycleBudgetMillis, hostCollected)) {
      hostCollected = true;
      LOG.info(
          "HEAP: collected once after cycleBudget while host MemAvailable is below {} bytes; {}",
          DriverHeap.HOST_RESERVE_BYTES,
          memoryReadings());
      System.gc();
    }
  }

  private void resetHostPause() {
    hostPauseSinceMillis = Long.MIN_VALUE;
    hostCollected = false;
  }

  void noteHeapCeiling() {
    long now = System.currentTimeMillis();
    boolean heap = heapAboveCeiling();
    boolean host = DriverHeap.belowHostReserve(cachedAvailable(now));
    if ((heap || host) && intakePaused.compareAndSet(false, true)) {
      pauseReason = DriverHeap.pauseReason(heap, host);
      LOG.info("HEAP: paused intake reason={} {}", pauseReason, memoryReadings());
    }
    if (heap && claimHourly(lastHeapCeilingWarnMillis, now)) {
      Runtime runtime = Runtime.getRuntime();
      long heapMax = runtime.maxMemory();
      long heapUsed = DriverHeap.used(runtime.totalMemory(), runtime.freeMemory());
      LOG.warn(
          DriverHeap.heapStop(heapUsed, DriverHeap.percent(heapUsed, heapMax), memoryReadings()));
    }
    if (host && claimHourly(lastHostReserveWarnMillis, now)) {
      LOG.warn(
          "HEAP: stopped taking more messages because host MemAvailable is {} bytes, below {}"
              + " bytes; {}",
          DriverHeap.availableBytes(),
          DriverHeap.HOST_RESERVE_BYTES,
          memoryReadings());
    }
  }

  private static boolean claimHourly(AtomicLong clock, long now) {
    long previous = clock.get();
    if (!DriverHeap.warnDue(previous, now)) {
      return false;
    }
    return clock.compareAndSet(previous, now);
  }

  /**
   * Puts messages that did not fit this batch back on the local queue. Does not ack or nack them.
   * The subscriber still blocks at one {@code batchSize}; this does not raise that cap.
   */
  void requeue(List<PulledMessage> messages) {
    if (messages == null || messages.isEmpty()) {
      return;
    }
    List<String> failed = new ArrayList<>();
    for (PulledMessage message : messages) {
      if (!putBack(message)) {
        failed.add(message.ackId());
      }
    }
    if (!failed.isEmpty()) {
      nack(failed);
    }
  }

  private boolean putBack(PulledMessage message) {
    if (message == null || queue == null || consumers == null) {
      return false;
    }
    AckReplyConsumer consumer = consumers.get(message.ackId());
    if (consumer == null) {
      return false;
    }
    int bytes = message.data().length;
    if (queuedBytes != null) {
      queuedBytes.addAndGet(bytes);
    }
    try {
      queue.put(new HeldMessage(message, consumer));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      subtractQueued(bytes);
      return false;
    }
  }

  long retryAttempts() {
    return retryPolicy.retryAttempts();
  }

  long lastSeenNewestPublishMillis() {
    return lastSeenNewestPublishMillis.get();
  }

  public static String asString(final Duration duration) {
    return duration.toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1").toLowerCase();
  }
}
