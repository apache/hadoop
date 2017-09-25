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

package org.apache.hadoop.ipc;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDoubleArray;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.metrics2.util.Metrics2Util.NameValuePair;
import org.apache.hadoop.metrics2.util.Metrics2Util.TopN;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The decay RPC scheduler counts incoming requests in a map, then
 * decays the counts at a fixed time interval. The scheduler is optimized
 * for large periods (on the order of seconds), as it offloads work to the
 * decay sweep.
 */
public class DecayRpcScheduler implements RpcScheduler,
    DecayRpcSchedulerMXBean, MetricsSource {
  /**
   * Period controls how many milliseconds between each decay sweep.
   */
  public static final String IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY =
      "decay-scheduler.period-ms";
  public static final long IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_DEFAULT =
      5000;
  @Deprecated
  public static final String IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY =
    "faircallqueue.decay-scheduler.period-ms";

  /**
   * Decay factor controls how much each count is suppressed by on each sweep.
   * Valid numbers are > 0 and < 1. Decay factor works in tandem with period
   * to control how long the scheduler remembers an identity.
   */
  public static final String IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_KEY =
      "decay-scheduler.decay-factor";
  public static final double IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_DEFAULT =
      0.5;
  @Deprecated
  public static final String IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY =
    "faircallqueue.decay-scheduler.decay-factor";

 /**
   * Thresholds are specified as integer percentages, and specify which usage
   * range each queue will be allocated to. For instance, specifying the list
   *  10, 40, 80
   * implies 4 queues, with
   * - q3 from 80% up
   * - q2 from 40 up to 80
   * - q1 from 10 up to 40
   * - q0 otherwise.
   */
  public static final String IPC_DECAYSCHEDULER_THRESHOLDS_KEY =
      "decay-scheduler.thresholds";
  @Deprecated
  public static final String IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY =
      "faircallqueue.decay-scheduler.thresholds";

  // Specifies the identity to use when the IdentityProvider cannot handle
  // a schedulable.
  public static final String DECAYSCHEDULER_UNKNOWN_IDENTITY =
      "IdentityProvider.Unknown";

  public static final String
      IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_KEY =
      "decay-scheduler.backoff.responsetime.enable";
  public static final Boolean
      IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_DEFAULT = false;

  // Specifies the average response time (ms) thresholds of each
  // level to trigger backoff
  public static final String
      IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_THRESHOLDS_KEY =
      "decay-scheduler.backoff.responsetime.thresholds";

  // Specifies the top N user's call count and scheduler decision
  // Metrics2 Source
  public static final String DECAYSCHEDULER_METRICS_TOP_USER_COUNT =
      "decay-scheduler.metrics.top.user.count";
  public static final int DECAYSCHEDULER_METRICS_TOP_USER_COUNT_DEFAULT = 10;

  public static final Logger LOG =
      LoggerFactory.getLogger(DecayRpcScheduler.class);

  private static final ObjectWriter WRITER = new ObjectMapper().writer();

  // Track the decayed and raw (no decay) number of calls for each schedulable
  // identity from all previous decay windows: idx 0 for decayed call count and
  // idx 1 for the raw call count
  private final ConcurrentHashMap<Object, List<AtomicLong>> callCounts =
      new ConcurrentHashMap<Object, List<AtomicLong>>();

  // Should be the sum of all AtomicLongs in decayed callCounts
  private final AtomicLong totalDecayedCallCount = new AtomicLong();
  // The sum of all AtomicLongs in raw callCounts
  private final AtomicLong totalRawCallCount = new AtomicLong();


  // Track total call count and response time in current decay window
  private final AtomicLongArray responseTimeCountInCurrWindow;
  private final AtomicLongArray responseTimeTotalInCurrWindow;

  // Track average response time in previous decay window
  private final AtomicDoubleArray responseTimeAvgInLastWindow;
  private final AtomicLongArray responseTimeCountInLastWindow;

  // Pre-computed scheduling decisions during the decay sweep are
  // atomically swapped in as a read-only map
  private final AtomicReference<Map<Object, Integer>> scheduleCacheRef =
    new AtomicReference<Map<Object, Integer>>();

  // Tune the behavior of the scheduler
  private final long decayPeriodMillis; // How long between each tick
  private final double decayFactor; // nextCount = currentCount * decayFactor
  private final int numLevels;
  private final double[] thresholds;
  private final IdentityProvider identityProvider;
  private final boolean backOffByResponseTimeEnabled;
  private final long[] backOffResponseTimeThresholds;
  private final String namespace;
  private final int topUsersCount; // e.g., report top 10 users' metrics
  private static final double PRECISION = 0.0001;
  private MetricsProxy metricsProxy;

  /**
   * This TimerTask will call decayCurrentCounts until
   * the scheduler has been garbage collected.
   */
  public static class DecayTask extends TimerTask {
    private WeakReference<DecayRpcScheduler> schedulerRef;
    private Timer timer;

    public DecayTask(DecayRpcScheduler scheduler, Timer timer) {
      this.schedulerRef = new WeakReference<DecayRpcScheduler>(scheduler);
      this.timer = timer;
    }

    @Override
    public void run() {
      DecayRpcScheduler sched = schedulerRef.get();
      if (sched != null) {
        sched.decayCurrentCounts();
      } else {
        // Our scheduler was garbage collected since it is no longer in use,
        // so we should terminate the timer as well
        timer.cancel();
        timer.purge();
      }
    }
  }

  /**
   * Create a decay scheduler.
   * @param numLevels number of priority levels
   * @param ns config prefix, so that we can configure multiple schedulers
   *           in a single instance.
   * @param conf configuration to use.
   */
  public DecayRpcScheduler(int numLevels, String ns, Configuration conf) {
    if(numLevels < 1) {
      throw new IllegalArgumentException("Number of Priority Levels must be " +
          "at least 1");
    }
    this.numLevels = numLevels;
    this.namespace = ns;
    this.decayFactor = parseDecayFactor(ns, conf);
    this.decayPeriodMillis = parseDecayPeriodMillis(ns, conf);
    this.identityProvider = this.parseIdentityProvider(ns, conf);
    this.thresholds = parseThresholds(ns, conf, numLevels);
    this.backOffByResponseTimeEnabled = parseBackOffByResponseTimeEnabled(ns,
        conf);
    this.backOffResponseTimeThresholds =
        parseBackOffResponseTimeThreshold(ns, conf, numLevels);

    // Setup response time metrics
    responseTimeTotalInCurrWindow = new AtomicLongArray(numLevels);
    responseTimeCountInCurrWindow = new AtomicLongArray(numLevels);
    responseTimeAvgInLastWindow = new AtomicDoubleArray(numLevels);
    responseTimeCountInLastWindow = new AtomicLongArray(numLevels);

    topUsersCount =
        conf.getInt(DECAYSCHEDULER_METRICS_TOP_USER_COUNT,
            DECAYSCHEDULER_METRICS_TOP_USER_COUNT_DEFAULT);
    Preconditions.checkArgument(topUsersCount > 0,
        "the number of top users for scheduler metrics must be at least 1");

    // Setup delay timer
    Timer timer = new Timer();
    DecayTask task = new DecayTask(this, timer);
    timer.scheduleAtFixedRate(task, decayPeriodMillis, decayPeriodMillis);

    metricsProxy = MetricsProxy.getInstance(ns, numLevels);
    metricsProxy.setDelegate(this);
  }

  // Load configs
  private IdentityProvider parseIdentityProvider(String ns,
      Configuration conf) {
    List<IdentityProvider> providers = conf.getInstances(
      ns + "." + CommonConfigurationKeys.IPC_IDENTITY_PROVIDER_KEY,
      IdentityProvider.class);

    if (providers.size() < 1) {
      LOG.info("IdentityProvider not specified, " +
        "defaulting to UserIdentityProvider");
      return new UserIdentityProvider();
    }

    return providers.get(0); // use the first
  }

  private static double parseDecayFactor(String ns, Configuration conf) {
    double factor = conf.getDouble(ns + "." +
        IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY, 0.0);
    if (factor == 0.0) {
      factor = conf.getDouble(ns + "." +
          IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_KEY,
          IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_DEFAULT);
    } else if ((factor > 0.0) && (factor < 1)) {
      LOG.warn(IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY +
          " is deprecated. Please use " +
          IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_KEY + ".");
    }
    if (factor <= 0 || factor >= 1) {
      throw new IllegalArgumentException("Decay Factor " +
        "must be between 0 and 1");
    }

    return factor;
  }

  private static long parseDecayPeriodMillis(String ns, Configuration conf) {
    long period = conf.getLong(ns + "." +
        IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY,
        0);
    if (period == 0) {
      period = conf.getLong(ns + "." +
          IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY,
          IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_DEFAULT);
    } else if (period > 0) {
      LOG.warn((IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY +
          " is deprecated. Please use " +
          IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY));
    }
    if (period <= 0) {
      throw new IllegalArgumentException("Period millis must be >= 0");
    }

    return period;
  }

  private static double[] parseThresholds(String ns, Configuration conf,
      int numLevels) {
    int[] percentages = conf.getInts(ns + "." +
        IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY);

    if (percentages.length == 0) {
      percentages = conf.getInts(ns + "." + IPC_DECAYSCHEDULER_THRESHOLDS_KEY);
      if (percentages.length == 0) {
        return getDefaultThresholds(numLevels);
      }
    } else {
      LOG.warn(IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY +
          " is deprecated. Please use " +
          IPC_DECAYSCHEDULER_THRESHOLDS_KEY);
    }

    if (percentages.length != numLevels-1) {
      throw new IllegalArgumentException("Number of thresholds should be " +
        (numLevels-1) + ". Was: " + percentages.length);
    }

    // Convert integer percentages to decimals
    double[] decimals = new double[percentages.length];
    for (int i = 0; i < percentages.length; i++) {
      decimals[i] = percentages[i] / 100.0;
    }

    return decimals;
  }

  /**
   * Generate default thresholds if user did not specify. Strategy is
   * to halve each time, since queue usage tends to be exponential.
   * So if numLevels is 4, we would generate: double[]{0.125, 0.25, 0.5}
   * which specifies the boundaries between each queue's usage.
   * @param numLevels number of levels to compute for
   * @return array of boundaries of length numLevels - 1
   */
  private static double[] getDefaultThresholds(int numLevels) {
    double[] ret = new double[numLevels - 1];
    double div = Math.pow(2, numLevels - 1);

    for (int i = 0; i < ret.length; i++) {
      ret[i] = Math.pow(2, i)/div;
    }
    return ret;
  }

  private static long[] parseBackOffResponseTimeThreshold(String ns,
      Configuration conf, int numLevels) {
    long[] responseTimeThresholds = conf.getTimeDurations(ns + "." +
            IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_THRESHOLDS_KEY,
        TimeUnit.MILLISECONDS);
    // backoff thresholds not specified
    if (responseTimeThresholds.length == 0) {
      return getDefaultBackOffResponseTimeThresholds(numLevels);
    }
    // backoff thresholds specified but not match with the levels
    if (responseTimeThresholds.length != numLevels) {
      throw new IllegalArgumentException(
          "responseTimeThresholds must match with the number of priority " +
          "levels");
    }
    // invalid thresholds
    for (long responseTimeThreshold: responseTimeThresholds) {
      if (responseTimeThreshold <= 0) {
        throw new IllegalArgumentException(
            "responseTimeThreshold millis must be >= 0");
      }
    }
    return responseTimeThresholds;
  }

  // 10s for level 0, 20s for level 1, 30s for level 2, ...
  private static long[] getDefaultBackOffResponseTimeThresholds(int numLevels) {
    long[] ret = new long[numLevels];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = 10000*(i+1);
    }
    return ret;
  }

  private static Boolean parseBackOffByResponseTimeEnabled(String ns,
      Configuration conf) {
    return conf.getBoolean(ns + "." +
        IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_KEY,
        IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_DEFAULT);
  }

  /**
   * Decay the stored counts for each user and clean as necessary.
   * This method should be called periodically in order to keep
   * counts current.
   */
  private void decayCurrentCounts() {
    try {
      long totalDecayedCount = 0;
      long totalRawCount = 0;
      Iterator<Map.Entry<Object, List<AtomicLong>>> it =
          callCounts.entrySet().iterator();

      while (it.hasNext()) {
        Map.Entry<Object, List<AtomicLong>> entry = it.next();
        AtomicLong decayedCount = entry.getValue().get(0);
        AtomicLong rawCount = entry.getValue().get(1);


        // Compute the next value by reducing it by the decayFactor
        totalRawCount += rawCount.get();
        long currentValue = decayedCount.get();
        long nextValue = (long) (currentValue * decayFactor);
        totalDecayedCount += nextValue;
        decayedCount.set(nextValue);

        if (nextValue == 0) {
          // We will clean up unused keys here. An interesting optimization
          // might be to have an upper bound on keyspace in callCounts and only
          // clean once we pass it.
          it.remove();
        }
      }

      // Update the total so that we remain in sync
      totalDecayedCallCount.set(totalDecayedCount);
      totalRawCallCount.set(totalRawCount);

      // Now refresh the cache of scheduling decisions
      recomputeScheduleCache();

      // Update average response time with decay
      updateAverageResponseTime(true);
    } catch (Exception ex) {
      LOG.error("decayCurrentCounts exception: " +
          ExceptionUtils.getFullStackTrace(ex));
      throw ex;
    }
  }

  /**
   * Update the scheduleCache to match current conditions in callCounts.
   */
  private void recomputeScheduleCache() {
    Map<Object, Integer> nextCache = new HashMap<Object, Integer>();

    for (Map.Entry<Object, List<AtomicLong>> entry : callCounts.entrySet()) {
      Object id = entry.getKey();
      AtomicLong value = entry.getValue().get(0);

      long snapshot = value.get();
      int computedLevel = computePriorityLevel(snapshot);

      nextCache.put(id, computedLevel);
    }

    // Swap in to activate
    scheduleCacheRef.set(Collections.unmodifiableMap(nextCache));
  }

  /**
   * Get the number of occurrences and increment atomically.
   * @param identity the identity of the user to increment
   * @return the value before incrementation
   */
  private long getAndIncrementCallCounts(Object identity)
      throws InterruptedException {
    // We will increment the count, or create it if no such count exists
    List<AtomicLong> count = this.callCounts.get(identity);
    if (count == null) {
      // Create the counts since no such count exists.
      // idx 0 for decayed call count
      // idx 1 for the raw call count
      count = new ArrayList<AtomicLong>(2);
      count.add(new AtomicLong(0));
      count.add(new AtomicLong(0));

      // Put it in, or get the AtomicInteger that was put in by another thread
      List<AtomicLong> otherCount = callCounts.putIfAbsent(identity, count);
      if (otherCount != null) {
        count = otherCount;
      }
    }

    // Update the total
    totalDecayedCallCount.getAndIncrement();
    totalRawCallCount.getAndIncrement();

    // At this point value is guaranteed to be not null. It may however have
    // been clobbered from callCounts. Nonetheless, we return what
    // we have.
    count.get(1).getAndIncrement();
    return count.get(0).getAndIncrement();
  }

  /**
   * Given the number of occurrences, compute a scheduling decision.
   * @param occurrences how many occurrences
   * @return scheduling decision from 0 to numLevels - 1
   */
  private int computePriorityLevel(long occurrences) {
    long totalCallSnapshot = totalDecayedCallCount.get();

    double proportion = 0;
    if (totalCallSnapshot > 0) {
      proportion = (double) occurrences / totalCallSnapshot;
    }

    // Start with low priority levels, since they will be most common
    for(int i = (numLevels - 1); i > 0; i--) {
      if (proportion >= this.thresholds[i - 1]) {
        return i; // We've found our level number
      }
    }

    // If we get this far, we're at level 0
    return 0;
  }

  /**
   * Returns the priority level for a given identity by first trying the cache,
   * then computing it.
   * @param identity an object responding to toString and hashCode
   * @return integer scheduling decision from 0 to numLevels - 1
   */
  private int cachedOrComputedPriorityLevel(Object identity) {
    try {
      long occurrences = this.getAndIncrementCallCounts(identity);

      // Try the cache
      Map<Object, Integer> scheduleCache = scheduleCacheRef.get();
      if (scheduleCache != null) {
        Integer priority = scheduleCache.get(identity);
        if (priority != null) {
          LOG.debug("Cache priority for: {} with priority: {}", identity,
              priority);
          return priority;
        }
      }

      // Cache was no good, compute it
      int priority = computePriorityLevel(occurrences);
      LOG.debug("compute priority for " + identity + " priority " + priority);
      return priority;

    } catch (InterruptedException ie) {
      LOG.warn("Caught InterruptedException, returning low priority level");
      LOG.debug("Fallback priority for: {} with priority: {}", identity,
          numLevels - 1);
      return numLevels - 1;
    }
  }

  /**
   * Compute the appropriate priority for a schedulable based on past requests.
   * @param obj the schedulable obj to query and remember
   * @return the level index which we recommend scheduling in
   */
  @Override
  public int getPriorityLevel(Schedulable obj) {
    // First get the identity
    String identity = this.identityProvider.makeIdentity(obj);
    if (identity == null) {
      // Identity provider did not handle this
      identity = DECAYSCHEDULER_UNKNOWN_IDENTITY;
    }

    return cachedOrComputedPriorityLevel(identity);
  }

  @Override
  public boolean shouldBackOff(Schedulable obj) {
    Boolean backOff = false;
    if (backOffByResponseTimeEnabled) {
      int priorityLevel = obj.getPriorityLevel();
      if (LOG.isDebugEnabled()) {
        double[] responseTimes = getAverageResponseTime();
        LOG.debug("Current Caller: {}  Priority: {} ",
            obj.getUserGroupInformation().getUserName(),
            obj.getPriorityLevel());
        for (int i = 0; i < numLevels; i++) {
          LOG.debug("Queue: {} responseTime: {} backoffThreshold: {}", i,
              responseTimes[i], backOffResponseTimeThresholds[i]);
        }
      }
      // High priority rpc over threshold triggers back off of low priority rpc
      for (int i = 0; i < priorityLevel + 1; i++) {
        if (responseTimeAvgInLastWindow.get(i) >
            backOffResponseTimeThresholds[i]) {
          backOff = true;
          break;
        }
      }
    }
    return backOff;
  }

  @Override
  public void addResponseTime(String name, int priorityLevel, int queueTime,
      int processingTime) {
    responseTimeCountInCurrWindow.getAndIncrement(priorityLevel);
    responseTimeTotalInCurrWindow.getAndAdd(priorityLevel,
        queueTime+processingTime);
    if (LOG.isDebugEnabled()) {
      LOG.debug("addResponseTime for call: {}  priority: {} queueTime: {} " +
          "processingTime: {} ", name, priorityLevel, queueTime,
          processingTime);
    }
  }

  // Update the cached average response time at the end of the decay window
  void updateAverageResponseTime(boolean enableDecay) {
    for (int i = 0; i < numLevels; i++) {
      double averageResponseTime = 0;
      long totalResponseTime = responseTimeTotalInCurrWindow.get(i);
      long responseTimeCount = responseTimeCountInCurrWindow.get(i);
      if (responseTimeCount > 0) {
        averageResponseTime = (double) totalResponseTime / responseTimeCount;
      }
      final double lastAvg = responseTimeAvgInLastWindow.get(i);
      if (lastAvg > PRECISION || averageResponseTime > PRECISION) {
        if (enableDecay) {
          final double decayed = decayFactor * lastAvg + averageResponseTime;
          responseTimeAvgInLastWindow.set(i, decayed);
        } else {
          responseTimeAvgInLastWindow.set(i, averageResponseTime);
        }
      } else {
        responseTimeAvgInLastWindow.set(i, 0);
      }
      responseTimeCountInLastWindow.set(i, responseTimeCount);
      if (LOG.isDebugEnabled()) {
        LOG.debug("updateAverageResponseTime queue: {} Average: {} Count: {}",
            i, averageResponseTime, responseTimeCount);
      }
      // Reset for next decay window
      responseTimeTotalInCurrWindow.set(i, 0);
      responseTimeCountInCurrWindow.set(i, 0);
    }
  }

  // For testing
  @VisibleForTesting
  public double getDecayFactor() { return decayFactor; }

  @VisibleForTesting
  public long getDecayPeriodMillis() { return decayPeriodMillis; }

  @VisibleForTesting
  public double[] getThresholds() { return thresholds; }

  @VisibleForTesting
  public void forceDecay() { decayCurrentCounts(); }

  @VisibleForTesting
  public Map<Object, Long> getCallCountSnapshot() {
    HashMap<Object, Long> snapshot = new HashMap<Object, Long>();

    for (Map.Entry<Object, List<AtomicLong>> entry : callCounts.entrySet()) {
      snapshot.put(entry.getKey(), entry.getValue().get(0).get());
    }

    return Collections.unmodifiableMap(snapshot);
  }

  @VisibleForTesting
  public long getTotalCallSnapshot() {
    return totalDecayedCallCount.get();
  }

  /**
   * MetricsProxy is a singleton because we may init multiple schedulers and we
   * want to clean up resources when a new scheduler replaces the old one.
   */
  public static final class MetricsProxy implements DecayRpcSchedulerMXBean,
      MetricsSource {
    // One singleton per namespace
    private static final HashMap<String, MetricsProxy> INSTANCES =
      new HashMap<String, MetricsProxy>();

    // Weakref for delegate, so we don't retain it forever if it can be GC'd
    private WeakReference<DecayRpcScheduler> delegate;
    private double[] averageResponseTimeDefault;
    private long[] callCountInLastWindowDefault;
    private ObjectName decayRpcSchedulerInfoBeanName;

    private MetricsProxy(String namespace, int numLevels) {
      averageResponseTimeDefault = new double[numLevels];
      callCountInLastWindowDefault = new long[numLevels];
      decayRpcSchedulerInfoBeanName =
          MBeans.register(namespace, "DecayRpcScheduler", this);
      this.registerMetrics2Source(namespace);
    }

    public static synchronized MetricsProxy getInstance(String namespace,
        int numLevels) {
      MetricsProxy mp = INSTANCES.get(namespace);
      if (mp == null) {
        // We must create one
        mp = new MetricsProxy(namespace, numLevels);
        INSTANCES.put(namespace, mp);
      }
      return mp;
    }

    public static synchronized void removeInstance(String namespace) {
      MetricsProxy.INSTANCES.remove(namespace);
    }

    public void setDelegate(DecayRpcScheduler obj) {
      this.delegate = new WeakReference<DecayRpcScheduler>(obj);
    }

    void registerMetrics2Source(String namespace) {
      final String name = "DecayRpcSchedulerMetrics2." + namespace;
      DefaultMetricsSystem.instance().register(name, name, this);
    }

    void unregisterSource(String namespace) {
      final String name = "DecayRpcSchedulerMetrics2." + namespace;
      DefaultMetricsSystem.instance().unregisterSource(name);
      if (decayRpcSchedulerInfoBeanName != null) {
        MBeans.unregister(decayRpcSchedulerInfoBeanName);
      }
    }

    @Override
    public String getSchedulingDecisionSummary() {
      DecayRpcScheduler scheduler = delegate.get();
      if (scheduler == null) {
        return "No Active Scheduler";
      } else {
        return scheduler.getSchedulingDecisionSummary();
      }
    }

    @Override
    public String getCallVolumeSummary() {
      DecayRpcScheduler scheduler = delegate.get();
      if (scheduler == null) {
        return "No Active Scheduler";
      } else {
        return scheduler.getCallVolumeSummary();
      }
    }

    @Override
    public int getUniqueIdentityCount() {
      DecayRpcScheduler scheduler = delegate.get();
      if (scheduler == null) {
        return -1;
      } else {
        return scheduler.getUniqueIdentityCount();
      }
    }

    @Override
    public long getTotalCallVolume() {
      DecayRpcScheduler scheduler = delegate.get();
      if (scheduler == null) {
        return -1;
      } else {
        return scheduler.getTotalCallVolume();
      }
    }

    @Override
    public double[] getAverageResponseTime() {
      DecayRpcScheduler scheduler = delegate.get();
      if (scheduler == null) {
        return averageResponseTimeDefault;
      } else {
        return scheduler.getAverageResponseTime();
      }
    }

    public long[] getResponseTimeCountInLastWindow() {
      DecayRpcScheduler scheduler = delegate.get();
      if (scheduler == null) {
        return callCountInLastWindowDefault;
      } else {
        return scheduler.getResponseTimeCountInLastWindow();
      }
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      DecayRpcScheduler scheduler = delegate.get();
      if (scheduler != null) {
        scheduler.getMetrics(collector, all);
      }
    }
  }

  public int getUniqueIdentityCount() {
    return callCounts.size();
  }

  public long getTotalCallVolume() {
    return totalDecayedCallCount.get();
  }

  public long getTotalRawCallVolume() {
    return totalRawCallCount.get();
  }

  public long[] getResponseTimeCountInLastWindow() {
    long[] ret = new long[responseTimeCountInLastWindow.length()];
    for (int i = 0; i < responseTimeCountInLastWindow.length(); i++) {
      ret[i] = responseTimeCountInLastWindow.get(i);
    }
    return ret;
  }

  @Override
  public double[] getAverageResponseTime() {
    double[] ret = new double[responseTimeAvgInLastWindow.length()];
    for (int i = 0; i < responseTimeAvgInLastWindow.length(); i++) {
      ret[i] = responseTimeAvgInLastWindow.get(i);
    }
    return ret;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    // Metrics2 interface to act as a Metric source
    try {
      MetricsRecordBuilder rb = collector.addRecord(getClass().getName())
          .setContext(namespace);
      addDecayedCallVolume(rb);
      addUniqueIdentityCount(rb);
      addTopNCallerSummary(rb);
      addAvgResponseTimePerPriority(rb);
      addCallVolumePerPriority(rb);
      addRawCallVolume(rb);
    } catch (Exception e) {
      LOG.warn("Exception thrown while metric collection. Exception : "
          + e.getMessage());
    }
  }

  // Key: UniqueCallers
  private void addUniqueIdentityCount(MetricsRecordBuilder rb) {
    rb.addCounter(Interns.info("UniqueCallers", "Total unique callers"),
        getUniqueIdentityCount());
  }

  // Key: DecayedCallVolume
  private void addDecayedCallVolume(MetricsRecordBuilder rb) {
    rb.addCounter(Interns.info("DecayedCallVolume", "Decayed Total " +
        "incoming Call Volume"), getTotalCallVolume());
  }

  private void addRawCallVolume(MetricsRecordBuilder rb) {
    rb.addCounter(Interns.info("CallVolume", "Raw Total " +
        "incoming Call Volume"), getTotalRawCallVolume());
  }

  // Key: Priority.0.CompletedCallVolume
  private void addCallVolumePerPriority(MetricsRecordBuilder rb) {
    for (int i = 0; i < responseTimeCountInLastWindow.length(); i++) {
      rb.addGauge(Interns.info("Priority." + i + ".CompletedCallVolume",
          "Completed Call volume " +
          "of priority "+ i), responseTimeCountInLastWindow.get(i));
    }
  }

  // Key: Priority.0.AvgResponseTime
  private void addAvgResponseTimePerPriority(MetricsRecordBuilder rb) {
    for (int i = 0; i < responseTimeAvgInLastWindow.length(); i++) {
      rb.addGauge(Interns.info("Priority." + i + ".AvgResponseTime", "Average" +
          " response time of priority " + i),
          responseTimeAvgInLastWindow.get(i));
    }
  }

  // Key: Caller(xyz).Volume and Caller(xyz).Priority
  private void addTopNCallerSummary(MetricsRecordBuilder rb) {
    TopN topNCallers = getTopCallers(topUsersCount);
    Map<Object, Integer> decisions = scheduleCacheRef.get();
    final int actualCallerCount = topNCallers.size();
    for (int i = 0; i < actualCallerCount; i++) {
      NameValuePair entry =  topNCallers.poll();
      String topCaller = "Caller(" + entry.getName() + ")";
      String topCallerVolume = topCaller + ".Volume";
      String topCallerPriority = topCaller + ".Priority";
      rb.addCounter(Interns.info(topCallerVolume, topCallerVolume),
          entry.getValue());
      Integer priority = decisions.get(entry.getName());
      if (priority != null) {
        rb.addCounter(Interns.info(topCallerPriority, topCallerPriority),
            priority);
      }
    }
  }

  // Get the top N callers' raw call count and scheduler decision
  private TopN getTopCallers(int n) {
    TopN topNCallers = new TopN(n);
    Iterator<Map.Entry<Object, List<AtomicLong>>> it =
        callCounts.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Object, List<AtomicLong>> entry = it.next();
      String caller = entry.getKey().toString();
      Long count = entry.getValue().get(1).get();
      if (count > 0) {
        topNCallers.offer(new NameValuePair(caller, count));
      }
    }
    return topNCallers;
  }

  public String getSchedulingDecisionSummary() {
    Map<Object, Integer> decisions = scheduleCacheRef.get();
    if (decisions == null) {
      return "{}";
    } else {
      try {
        return WRITER.writeValueAsString(decisions);
      } catch (Exception e) {
        return "Error: " + e.getMessage();
      }
    }
  }

  public String getCallVolumeSummary() {
    try {
      return WRITER.writeValueAsString(getDecayedCallCounts());
    } catch (Exception e) {
      return "Error: " + e.getMessage();
    }
  }

  private Map<Object, Long> getDecayedCallCounts() {
    Map<Object, Long> decayedCallCounts = new HashMap<>(callCounts.size());
    Iterator<Map.Entry<Object, List<AtomicLong>>> it =
        callCounts.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Object, List<AtomicLong>> entry = it.next();
      Object user = entry.getKey();
      Long decayedCount = entry.getValue().get(0).get();
      if (decayedCount > 0) {
        decayedCallCounts.put(user, decayedCount);
      }
    }
    return decayedCallCounts;
  }

  @Override
  public void stop() {
    metricsProxy.unregisterSource(namespace);
    MetricsProxy.removeInstance(namespace);
  }
}
