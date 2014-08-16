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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.metrics2.util.MBeans;

import org.codehaus.jackson.map.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

/**
 * The decay RPC scheduler counts incoming requests in a map, then
 * decays the counts at a fixed time interval. The scheduler is optimized
 * for large periods (on the order of seconds), as it offloads work to the
 * decay sweep.
 */
public class DecayRpcScheduler implements RpcScheduler, DecayRpcSchedulerMXBean {
  /**
   * Period controls how many milliseconds between each decay sweep.
   */
  public static final String IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY =
    "faircallqueue.decay-scheduler.period-ms";
  public static final long   IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_DEFAULT =
    5000L;

  /**
   * Decay factor controls how much each count is suppressed by on each sweep.
   * Valid numbers are > 0 and < 1. Decay factor works in tandem with period
   * to control how long the scheduler remembers an identity.
   */
  public static final String IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY =
    "faircallqueue.decay-scheduler.decay-factor";
  public static final double IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_DEFAULT =
    0.5;

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
  public static final String IPC_CALLQUEUE_DECAYSCHEDULER_THRESHOLDS_KEY =
    "faircallqueue.decay-scheduler.thresholds";

  // Specifies the identity to use when the IdentityProvider cannot handle
  // a schedulable.
  public static final String DECAYSCHEDULER_UNKNOWN_IDENTITY =
    "IdentityProvider.Unknown";

  public static final Log LOG = LogFactory.getLog(DecayRpcScheduler.class);

  // Track the number of calls for each schedulable identity
  private final ConcurrentHashMap<Object, AtomicLong> callCounts =
    new ConcurrentHashMap<Object, AtomicLong>();

  // Should be the sum of all AtomicLongs in callCounts
  private final AtomicLong totalCalls = new AtomicLong();

  // Pre-computed scheduling decisions during the decay sweep are
  // atomically swapped in as a read-only map
  private final AtomicReference<Map<Object, Integer>> scheduleCacheRef =
    new AtomicReference<Map<Object, Integer>>();

  // Tune the behavior of the scheduler
  private final long decayPeriodMillis; // How long between each tick
  private final double decayFactor; // nextCount = currentCount / decayFactor
  private final int numQueues; // affects scheduling decisions, from 0 to numQueues - 1
  private final double[] thresholds;
  private final IdentityProvider identityProvider;

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
   * @param numQueues number of queues to schedule for
   * @param ns config prefix, so that we can configure multiple schedulers
   *           in a single instance.
   * @param conf configuration to use.
   */
  public DecayRpcScheduler(int numQueues, String ns, Configuration conf) {
    if (numQueues < 1) {
      throw new IllegalArgumentException("number of queues must be > 0");
    }

    this.numQueues = numQueues;
    this.decayFactor = parseDecayFactor(ns, conf);
    this.decayPeriodMillis = parseDecayPeriodMillis(ns, conf);
    this.identityProvider = this.parseIdentityProvider(ns, conf);
    this.thresholds = parseThresholds(ns, conf, numQueues);

    // Setup delay timer
    Timer timer = new Timer();
    DecayTask task = new DecayTask(this, timer);
    timer.scheduleAtFixedRate(task, 0, this.decayPeriodMillis);

    MetricsProxy prox = MetricsProxy.getInstance(ns);
    prox.setDelegate(this);
  }

  // Load configs
  private IdentityProvider parseIdentityProvider(String ns, Configuration conf) {
    List<IdentityProvider> providers = conf.getInstances(
      ns + "." + CommonConfigurationKeys.IPC_CALLQUEUE_IDENTITY_PROVIDER_KEY,
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
        IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY,
      IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_DEFAULT
    );

    if (factor <= 0 || factor >= 1) {
      throw new IllegalArgumentException("Decay Factor " +
        "must be between 0 and 1");
    }

    return factor;
  }

  private static long parseDecayPeriodMillis(String ns, Configuration conf) {
    long period = conf.getLong(ns + "." +
        IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY,
      IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_DEFAULT
    );

    if (period <= 0) {
      throw new IllegalArgumentException("Period millis must be >= 0");
    }

    return period;
  }

  private static double[] parseThresholds(String ns, Configuration conf,
      int numQueues) {
    int[] percentages = conf.getInts(ns + "." +
      IPC_CALLQUEUE_DECAYSCHEDULER_THRESHOLDS_KEY);

    if (percentages.length == 0) {
      return getDefaultThresholds(numQueues);
    } else if (percentages.length != numQueues-1) {
      throw new IllegalArgumentException("Number of thresholds should be " +
        (numQueues-1) + ". Was: " + percentages.length);
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
   * So if numQueues is 4, we would generate: double[]{0.125, 0.25, 0.5}
   * which specifies the boundaries between each queue's usage.
   * @param numQueues number of queues to compute for
   * @return array of boundaries of length numQueues - 1
   */
  private static double[] getDefaultThresholds(int numQueues) {
    double[] ret = new double[numQueues - 1];
    double div = Math.pow(2, numQueues - 1);

    for (int i = 0; i < ret.length; i++) {
      ret[i] = Math.pow(2, i)/div;
    }
    return ret;
  }

  /**
   * Decay the stored counts for each user and clean as necessary.
   * This method should be called periodically in order to keep
   * counts current.
   */
  private void decayCurrentCounts() {
    long total = 0;
    Iterator<Map.Entry<Object, AtomicLong>> it =
      callCounts.entrySet().iterator();

    while (it.hasNext()) {
      Map.Entry<Object, AtomicLong> entry = it.next();
      AtomicLong count = entry.getValue();

      // Compute the next value by reducing it by the decayFactor
      long currentValue = count.get();
      long nextValue = (long)(currentValue * decayFactor);
      total += nextValue;
      count.set(nextValue);

      if (nextValue == 0) {
        // We will clean up unused keys here. An interesting optimization might
        // be to have an upper bound on keyspace in callCounts and only
        // clean once we pass it.
        it.remove();
      }
    }

    // Update the total so that we remain in sync
    totalCalls.set(total);

    // Now refresh the cache of scheduling decisions
    recomputeScheduleCache();
  }

  /**
   * Update the scheduleCache to match current conditions in callCounts.
   */
  private void recomputeScheduleCache() {
    Map<Object, Integer> nextCache = new HashMap<Object, Integer>();

    for (Map.Entry<Object, AtomicLong> entry : callCounts.entrySet()) {
      Object id = entry.getKey();
      AtomicLong value = entry.getValue();

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
  private long getAndIncrement(Object identity) throws InterruptedException {
    // We will increment the count, or create it if no such count exists
    AtomicLong count = this.callCounts.get(identity);
    if (count == null) {
      // Create the count since no such count exists.
      count = new AtomicLong(0);

      // Put it in, or get the AtomicInteger that was put in by another thread
      AtomicLong otherCount = callCounts.putIfAbsent(identity, count);
      if (otherCount != null) {
        count = otherCount;
      }
    }

    // Update the total
    totalCalls.getAndIncrement();

    // At this point value is guaranteed to be not null. It may however have
    // been clobbered from callCounts. Nonetheless, we return what
    // we have.
    return count.getAndIncrement();
  }

  /**
   * Given the number of occurrences, compute a scheduling decision.
   * @param occurrences how many occurrences
   * @return scheduling decision from 0 to numQueues - 1
   */
  private int computePriorityLevel(long occurrences) {
    long totalCallSnapshot = totalCalls.get();

    double proportion = 0;
    if (totalCallSnapshot > 0) {
      proportion = (double) occurrences / totalCallSnapshot;
    }

    // Start with low priority queues, since they will be most common
    for(int i = (numQueues - 1); i > 0; i--) {
      if (proportion >= this.thresholds[i - 1]) {
        return i; // We've found our queue number
      }
    }

    // If we get this far, we're at queue 0
    return 0;
  }

  /**
   * Returns the priority level for a given identity by first trying the cache,
   * then computing it.
   * @param identity an object responding to toString and hashCode
   * @return integer scheduling decision from 0 to numQueues - 1
   */
  private int cachedOrComputedPriorityLevel(Object identity) {
    try {
      long occurrences = this.getAndIncrement(identity);

      // Try the cache
      Map<Object, Integer> scheduleCache = scheduleCacheRef.get();
      if (scheduleCache != null) {
        Integer priority = scheduleCache.get(identity);
        if (priority != null) {
          return priority;
        }
      }

      // Cache was no good, compute it
      return computePriorityLevel(occurrences);
    } catch (InterruptedException ie) {
      LOG.warn("Caught InterruptedException, returning low priority queue");
      return numQueues - 1;
    }
  }

  /**
   * Compute the appropriate priority for a schedulable based on past requests.
   * @param obj the schedulable obj to query and remember
   * @return the queue index which we recommend scheduling in
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

    for (Map.Entry<Object, AtomicLong> entry : callCounts.entrySet()) {
      snapshot.put(entry.getKey(), entry.getValue().get());
    }

    return Collections.unmodifiableMap(snapshot);
  }

  @VisibleForTesting
  public long getTotalCallSnapshot() {
    return totalCalls.get();
  }

  /**
   * MetricsProxy is a singleton because we may init multiple schedulers and we
   * want to clean up resources when a new scheduler replaces the old one.
   */
  private static final class MetricsProxy implements DecayRpcSchedulerMXBean {
    // One singleton per namespace
    private static final HashMap<String, MetricsProxy> INSTANCES =
      new HashMap<String, MetricsProxy>();

    // Weakref for delegate, so we don't retain it forever if it can be GC'd
    private WeakReference<DecayRpcScheduler> delegate;

    private MetricsProxy(String namespace) {
      MBeans.register(namespace, "DecayRpcScheduler", this);
    }

    public static synchronized MetricsProxy getInstance(String namespace) {
      MetricsProxy mp = INSTANCES.get(namespace);
      if (mp == null) {
        // We must create one
        mp = new MetricsProxy(namespace);
        INSTANCES.put(namespace, mp);
      }
      return mp;
    }

    public void setDelegate(DecayRpcScheduler obj) {
      this.delegate = new WeakReference<DecayRpcScheduler>(obj);
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
  }

  public int getUniqueIdentityCount() {
    return callCounts.size();
  }

  public long getTotalCallVolume() {
    return totalCalls.get();
  }

  public String getSchedulingDecisionSummary() {
    Map<Object, Integer> decisions = scheduleCacheRef.get();
    if (decisions == null) {
      return "{}";
    } else {
      try {
        ObjectMapper om = new ObjectMapper();
        return om.writeValueAsString(decisions);
      } catch (Exception e) {
        return "Error: " + e.getMessage();
      }
    }
  }

  public String getCallVolumeSummary() {
    try {
      ObjectMapper om = new ObjectMapper();
      return om.writeValueAsString(callCounts);
    } catch (Exception e) {
      return "Error: " + e.getMessage();
    }
  }
}
