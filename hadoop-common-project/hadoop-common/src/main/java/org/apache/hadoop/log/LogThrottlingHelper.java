/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.log;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.util.Timer;

/**
 * This is a class to help easily throttle log statements, so that they will
 * not be emitted more frequently than a certain rate. It is useful to help
 * prevent flooding the application logs with redundant messages.
 *
 * The instantiator specifies a minimum period at which statements should be
 * logged. When {@link #record(double...)} is called, if enough time has elapsed
 * since the last time it was called, the return value will indicate to the
 * caller that it should write to its actual log. Note that this class does not
 * write to any actual log; it only records information about how many times
 * {@code record} has been called and with what arguments, and indicates to the
 * caller whether or not it should write to its log. If not enough time has yet
 * elapsed, this class records the arguments and updates its summary
 * information, and indicates to the caller that it should not log.
 *
 * For example, say that you want to know whenever too large of a request is
 * received, but want to avoid flooding the logs if many such requests are
 * received.
 * <pre>{@code
 *   // Helper with a minimum period of 5 seconds
 *   private LogThrottlingHelper helper = new LogThrottlingHelper(5000);
 *
 *   public void receiveRequest(int requestedObjects) {
 *     if (requestedObjects > MAXIMUM_REQUEST_SIZE) {
 *       LogAction logAction = helper.record(requestedObjects);
 *       if (logAction.shouldLog()) {
 *         LOG.warn("Received {} large request(s) with a total of {} objects " +
 *             "requested; maximum objects requested was {}",
 *             logAction.getCount(), logAction.getStats(0).getSum(),
 *             logAction.getStats(0).getMax());
 *       }
 *     }
 *   }
 * }</pre>
 * The above snippet allows you to record extraneous events, but if they become
 * frequent, to limit their presence in the log to only every 5 seconds while
 * still maintaining overall information about how many large requests were
 * received.
 *
 * <p>This class can also be used to coordinate multiple logging points; see
 * {@link #record(String, long, double...)} for more details.
 *
 * <p>This class is not thread-safe.
 */
public class LogThrottlingHelper {

  /**
   * An indication of what action the caller should take. If
   * {@link #shouldLog()} is false, no other action should be taken, and it is
   * an error to try to access any of the summary information. If
   * {@link #shouldLog()} is true, then the caller should write to its log, and
   * can use the {@link #getCount()} and {@link #getStats(int)} methods to
   * determine summary information about what has been recorded into this
   * helper.
   *
   * All summary information in this action only represents
   * {@link #record(double...)} statements which were called <i>after</i> the
   * last time the caller logged something; that is, since the last time a log
   * action was returned with a true value for {@link #shouldLog()}. Information
   * about the {@link #record(double...)} statement which created this log
   * action is included.
   */
  public interface LogAction {

    /**
     * Return the number of records encapsulated in this action; that is, the
     * number of times {@code record} was called to produce this action,
     * including the current one.
     */
    int getCount();

    /**
     * Return summary information for the value that was recorded at index
     * {@code idx}. Corresponds to the ordering of values passed to
     * {@link #record(double...)}.
     */
    SummaryStatistics getStats(int idx);

    /**
     * If this is true, the caller should write to its log. Otherwise, the
     * caller should take no action, and it is an error to call other methods
     * on this object.
     */
    boolean shouldLog();

  }

  /**
   * A {@link LogAction} representing a state that should not yet be logged.
   * If any attempt is made to extract information from this, it will throw
   * an {@link IllegalStateException}.
   */
  public static final LogAction DO_NOT_LOG = new NoLogAction();
  private static final String DEFAULT_RECORDER_NAME =
      "__DEFAULT_RECORDER_NAME__";

  /**
   * This throttler will not trigger log statements more frequently than this
   * period.
   */
  private final long minLogPeriodMs;
  /**
   * The name of the recorder treated as the primary; this is the only one which
   * will trigger logging. Other recorders are dependent on the state of this
   * recorder. This may be null, in which case a primary has not yet been set.
   */
  private String primaryRecorderName;
  private final Timer timer;
  private final Map<String, LoggingAction> currentLogs;

  private long lastLogTimestampMs = Long.MIN_VALUE;

  /**
   * Create a log helper without any primary recorder.
   *
   * @see #LogThrottlingHelper(long, String)
   */
  public LogThrottlingHelper(long minLogPeriodMs) {
    this(minLogPeriodMs, null);
  }

  /**
   * Create a log helper with a specified primary recorder name; this can be
   * used in conjunction with {@link #record(String, long, double...)} to set up
   * primary and dependent recorders. See
   * {@link #record(String, long, double...)} for more details.
   *
   * @param minLogPeriodMs The minimum period with which to log; do not log
   *                       more frequently than this.
   * @param primaryRecorderName The name of the primary recorder.
   */
  public LogThrottlingHelper(long minLogPeriodMs, String primaryRecorderName) {
    this(minLogPeriodMs, primaryRecorderName, new Timer());
  }

  @VisibleForTesting
  LogThrottlingHelper(long minLogPeriodMs, String primaryRecorderName,
      Timer timer) {
    this.minLogPeriodMs = minLogPeriodMs;
    this.primaryRecorderName = primaryRecorderName;
    this.timer = timer;
    this.currentLogs = new HashMap<>();
  }

  /**
   * Record some set of values at the current time into this helper. Note that
   * this does <i>not</i> actually write information to any log. Instead, this
   * will return a LogAction indicating whether or not the caller should write
   * to its own log. The LogAction will additionally contain summary information
   * about the values specified since the last time the caller was expected to
   * write to its log.
   *
   * <p>Specifying multiple values will maintain separate summary statistics
   * about each value. For example:
   * <pre>{@code
   *   helper.record(1, 0);
   *   LogAction action = helper.record(3, 100);
   *   action.getStats(0); // == 2
   *   action.getStats(1); // == 50
   * }</pre>
   *
   * @param values The values about which to maintain summary information. Every
   *               time this method is called, the same number of values must
   *               be specified.
   * @return A LogAction indicating whether or not the caller should write to
   *         its log.
   */
  public LogAction record(double... values) {
    return record(DEFAULT_RECORDER_NAME, timer.monotonicNow(), values);
  }

  /**
   * Record some set of values at the specified time into this helper. This can
   * be useful to avoid fetching the current time twice if the caller has
   * already done so for other purposes. This additionally allows the caller to
   * specify a name for this recorder. When multiple names are used, one is
   * denoted as the primary recorder. Only recorders named as the primary
   * will trigger logging; other names not matching the primary can <i>only</i>
   * be triggered by following the primary. This is used to coordinate multiple
   * logging points. A primary can be set via the
   * {@link #LogThrottlingHelper(long, String)} constructor. If no primary
   * is set in the constructor, then the first recorder name used becomes the
   * primary.
   *
   * If multiple names are used, they maintain entirely different sets of values
   * and summary information. For example:
   * <pre>{@code
   *   // Initialize "pre" as the primary recorder name
   *   LogThrottlingHelper helper = new LogThrottlingHelper(1000, "pre");
   *   LogAction preLog = helper.record("pre", Time.monotonicNow());
   *   if (preLog.shouldLog()) {
   *     // ...
   *   }
   *   double eventsProcessed = ... // perform some action
   *   LogAction postLog =
   *       helper.record("post", Time.monotonicNow(), eventsProcessed);
   *   if (postLog.shouldLog()) {
   *     // ...
   *     // Can use postLog.getStats(0) to access eventsProcessed information
   *   }
   * }</pre>
   * Since "pre" is the primary recorder name, logging to "pre" will trigger a
   * log action if enough time has elapsed. This will indicate that "post"
   * should log as well. This ensures that "post" is always logged in the same
   * iteration as "pre", yet each one is able to maintain its own summary
   * information.
   *
   * <p>Other behavior is the same as {@link #record(double...)}.
   *
   * @param recorderName The name of the recorder. This is used to check if the
   *                     current recorder is the primary. Other names are
   *                     arbitrary and are only used to differentiate between
   *                     distinct recorders.
   * @param currentTimeMs The current time.
   * @param values The values to log.
   * @return The LogAction for the specified recorder.
   *
   * @see #record(double...)
   */
  public LogAction record(String recorderName, long currentTimeMs,
      double... values) {
    if (primaryRecorderName == null) {
      primaryRecorderName = recorderName;
    }
    LoggingAction currentLog = currentLogs.get(recorderName);
    if (currentLog == null || currentLog.hasLogged()) {
      currentLog = new LoggingAction(values.length);
      if (!currentLogs.containsKey(recorderName)) {
        // Always log newly created loggers
        currentLog.setShouldLog();
      }
      currentLogs.put(recorderName, currentLog);
    }
    currentLog.recordValues(values);
    if (primaryRecorderName.equals(recorderName) &&
        currentTimeMs - minLogPeriodMs >= lastLogTimestampMs) {
      lastLogTimestampMs = currentTimeMs;
      for (LoggingAction log : currentLogs.values()) {
        log.setShouldLog();
      }
    }
    if (currentLog.shouldLog()) {
      currentLog.setHasLogged();
      return currentLog;
    } else {
      return DO_NOT_LOG;
    }
  }

  /**
   * Return the summary information for given index.
   *
   * @param recorderName The name of the recorder.
   * @param idx The index value.
   * @return The summary information.
   */
  public SummaryStatistics getCurrentStats(String recorderName, int idx) {
    LoggingAction currentLog = currentLogs.get(recorderName);
    if (currentLog != null) {
      return currentLog.getStats(idx);
    }

    return null;
  }

  /**
   * Helper function to create a message about how many log statements were
   * suppressed in the provided log action. If no statements were suppressed,
   * this returns an empty string. The message has the format (without quotes):
   *
   * <p>' (suppressed logging <i>{suppression_count}</i> times)'</p>
   *
   * @param action The log action to produce a message about.
   * @return A message about suppression within this action.
   */
  public static String getLogSupressionMessage(LogAction action) {
    if (action.getCount() > 1) {
      return " (suppressed logging " + (action.getCount() - 1) + " times)";
    } else {
      return "";
    }
  }

  /**
   * A standard log action which keeps track of all of the values which have
   * been logged. This is also used for internal bookkeeping via its private
   * fields and methods; it will maintain whether or not it is ready to be
   * logged ({@link #shouldLog()}) as well as whether or not it has been
   * returned for logging yet ({@link #hasLogged()}).
   */
  private static class LoggingAction implements LogAction {

    private int count = 0;
    private final SummaryStatistics[] stats;
    private boolean shouldLog = false;
    private boolean hasLogged = false;

    LoggingAction(int valueCount) {
      stats = new SummaryStatistics[valueCount];
      for (int i = 0; i < stats.length; i++) {
        stats[i] = new SummaryStatistics();
      }
    }

    public int getCount() {
      return count;
    }

    public SummaryStatistics getStats(int idx) {
      if (idx < 0 || idx >= stats.length) {
        throw new IllegalArgumentException("Requested stats at idx " + idx +
            " but this log only maintains " + stats.length + " stats");
      }
      return stats[idx];
    }

    public boolean shouldLog() {
      return shouldLog;
    }

    private void setShouldLog() {
      shouldLog = true;
    }

    private boolean hasLogged() {
      return hasLogged;
    }

    private void setHasLogged() {
      hasLogged = true;
    }

    private void recordValues(double... values) {
      if (values.length != stats.length) {
        throw new IllegalArgumentException("received " + values.length +
            " values but expected " + stats.length);
      }
      count++;
      for (int i = 0; i < values.length; i++) {
        stats[i].addValue(values[i]);
      }
    }

  }

  /**
   * A non-logging action.
   *
   * @see #DO_NOT_LOG
   */
  private static class NoLogAction implements LogAction {

    public int getCount() {
      throw new IllegalStateException("Cannot be logged yet!");
    }

    public SummaryStatistics getStats(int idx) {
      throw new IllegalStateException("Cannot be logged yet!");
    }

    public boolean shouldLog() {
      return false;
    }

  }

}
