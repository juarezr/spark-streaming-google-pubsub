package io.github.juarezr.spark.pubsub.structured;

/**
 * Once an hour, notices when the newest gathered message is getting older, or has stayed older than
 * one trigger. Does not change receive, write, or ack.
 */
final class IntakePace {
  static final long HOUR_MILLIS = 3_600_000L;

  private long windowStartMillis = Long.MIN_VALUE;
  private long ageAtWindowStart;
  private boolean stayedAboveTrigger;

  /**
   * @return a warning line when the hour shows the pull falling behind, otherwise {@code null}
   */
  String observe(long nowMillis, long newestAgeMs, long triggerMs) {
    if (windowStartMillis == Long.MIN_VALUE) {
      open(nowMillis, newestAgeMs, triggerMs);
      return null;
    }
    if (triggerMs > 0L && newestAgeMs <= triggerMs) {
      stayedAboveTrigger = false;
    }
    if (nowMillis - windowStartMillis < HOUR_MILLIS) {
      return null;
    }
    String warning = null;
    if (newestAgeMs > ageAtWindowStart) {
      warning =
          String.format(
              "INTAKE: newest message age rose from %dms to %dms over the last hour",
              ageAtWindowStart, newestAgeMs);
    } else if (stayedAboveTrigger && triggerMs > 0L && newestAgeMs > triggerMs) {
      warning =
          String.format(
              "INTAKE: newest message age stayed above the trigger (%dms) for an hour; age=%dms",
              triggerMs, newestAgeMs);
    }
    open(nowMillis, newestAgeMs, triggerMs);
    return warning;
  }

  private void open(long nowMillis, long newestAgeMs, long triggerMs) {
    windowStartMillis = nowMillis;
    ageAtWindowStart = newestAgeMs;
    stayedAboveTrigger = triggerMs > 0L && newestAgeMs > triggerMs;
  }
}
