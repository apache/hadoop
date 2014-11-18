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
package org.apache.hadoop.hdfs.server.namenode.top.window;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * A class to manage the set of {@link RollingWindow}s. This class is the
 * interface of metrics system to the {@link RollingWindow}s to retrieve the
 * current top metrics.
 * <p/>
 * Thread-safety is provided by each {@link RollingWindow} being thread-safe as
 * well as {@link ConcurrentHashMap} for the collection of them.
 */
@InterfaceAudience.Private
public class RollingWindowManager {
  public static final Logger LOG = LoggerFactory.getLogger(
      RollingWindowManager.class);

  private int windowLenMs;
  private int bucketsPerWindow; // e.g., 10 buckets per minute
  private int topUsersCnt; // e.g., report top 10 metrics

  /**
   * Create a metric name composed of the command and user
   *
   * @param command the command executed
   * @param user    the user
   * @return a composed metric name
   */
  @VisibleForTesting
  public static String createMetricName(String command, String user) {
    return command + "." + user;
  }

  static private class RollingWindowMap extends
      ConcurrentHashMap<String, RollingWindow> {
    private static final long serialVersionUID = -6785807073237052051L;
  }

  /**
   * A mapping from each reported metric to its {@link RollingWindowMap} that
   * maintains the set of {@link RollingWindow}s for the users that have
   * operated on that metric.
   */
  public ConcurrentHashMap<String, RollingWindowMap> metricMap =
      new ConcurrentHashMap<String, RollingWindowMap>();

  public RollingWindowManager(Configuration conf, long reportingPeriodMs) {
    windowLenMs = (int) reportingPeriodMs;
    bucketsPerWindow =
        conf.getInt(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY,
            DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_DEFAULT);
    Preconditions.checkArgument(bucketsPerWindow > 0,
        "a window should have at least one bucket");
    Preconditions.checkArgument(bucketsPerWindow <= windowLenMs,
        "the minimum size of a bucket is 1 ms");
    //same-size buckets
    Preconditions.checkArgument(windowLenMs % bucketsPerWindow == 0,
        "window size must be a multiplication of number of buckets");
    topUsersCnt =
        conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY,
            DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);
    Preconditions.checkArgument(topUsersCnt > 0,
        "the number of requested top users must be at least 1");
  }

  /**
   * Called when the metric command is changed by "delta" units at time "time"
   * via user "user"
   *
   * @param time the time of the event
   * @param command the metric that is updated, e.g., the operation name
   * @param user the user that updated the metric
   * @param delta the amount of change in the metric, e.g., +1
   */
  public void recordMetric(long time, String command, String user, long delta) {
    RollingWindow window = getRollingWindow(command, user);
    window.incAt(time, delta);
  }

  /**
   * Take a snapshot of current top users in the past period.
   *
   * @param time the current time
   * @return a map between the top metrics and their values. The user is encoded
   * in the metric name. Refer to {@link RollingWindowManager#createMetricName} for
   * the actual format.
   */
  public MetricValueMap snapshot(long time) {
    MetricValueMap map = new MetricValueMap();
    Set<String> metricNames = metricMap.keySet();
    LOG.debug("iterating in reported metrics, size={} values={}",
        metricNames.size(), metricNames);
    for (Map.Entry<String,RollingWindowMap> rwEntry: metricMap.entrySet()) {
      String metricName = rwEntry.getKey();
      RollingWindowMap rollingWindows = rwEntry.getValue();
      TopN topN = new TopN(topUsersCnt);
      Iterator<Map.Entry<String, RollingWindow>> iterator =
          rollingWindows.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, RollingWindow> entry = iterator.next();
        String userName = entry.getKey();
        RollingWindow aWindow = entry.getValue();
        long windowSum = aWindow.getSum(time);
        // do the gc here
        if (windowSum == 0) {
          LOG.debug("gc window of metric: {} userName: {}",
              metricName, userName);
          iterator.remove();
          continue;
        }
        LOG.debug("offer window of metric: {} userName: {} sum: {}",
            metricName, userName, windowSum);
        topN.offer(new NameValuePair(userName, windowSum));
      }
      int n = topN.size();
      LOG.info("topN size for command " + metricName + " is: " + n);
      if (n == 0) {
        continue;
      }
      String allMetricName =
          createMetricName(metricName, TopConf.ALL_USERS);
      map.put(allMetricName, Long.valueOf(topN.total));
      for (int i = 0; i < n; i++) {
        NameValuePair userEntry = topN.poll();
        String userMetricName =
            createMetricName(metricName, userEntry.name);
        map.put(userMetricName, Long.valueOf(userEntry.value));
      }
    }
    return map;
  }

  /**
   * Get the rolling window specified by metric and user.
   *
   * @param metric the updated metric
   * @param user the user that updated the metric
   * @return the rolling window
   */
  private RollingWindow getRollingWindow(String metric, String user) {
    RollingWindowMap rwMap = metricMap.get(metric);
    if (rwMap == null) {
      rwMap = new RollingWindowMap();
      RollingWindowMap prevRwMap = metricMap.putIfAbsent(metric, rwMap);
      if (prevRwMap != null) {
        rwMap = prevRwMap;
      }
    }
    RollingWindow window = rwMap.get(user);
    if (window != null) {
      return window;
    }
    window = new RollingWindow(windowLenMs, bucketsPerWindow);
    RollingWindow prevWindow = rwMap.putIfAbsent(user, window);
    if (prevWindow != null) {
      window = prevWindow;
    }
    return window;
  }

  /**
   * A pair of a name and its corresponding value
   */
  static private class NameValuePair implements Comparable<NameValuePair> {
    String name;
    long value;

    public NameValuePair(String metricName, long value) {
      this.name = metricName;
      this.value = value;
    }

    @Override
    public int compareTo(NameValuePair other) {
      return (int) (value - other.value);
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof NameValuePair) {
        return compareTo((NameValuePair)other) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Long.valueOf(value).hashCode();
    }
  }

  /**
   * A fixed-size priority queue, used to retrieve top-n of offered entries.
   */
  static private class TopN extends PriorityQueue<NameValuePair> {
    private static final long serialVersionUID = 5134028249611535803L;
    int n; // > 0
    private long total = 0;

    TopN(int n) {
      super(n);
      this.n = n;
    }

    @Override
    public boolean offer(NameValuePair entry) {
      updateTotal(entry.value);
      if (size() == n) {
        NameValuePair smallest = peek();
        if (smallest.value >= entry.value) {
          return false;
        }
        poll(); // remove smallest
      }
      return super.offer(entry);
    }

    private void updateTotal(long value) {
      total += value;
    }

    public long getTotal() {
      return total;
    }
  }

  /**
   * A mapping from metric names to their absolute values and their percentage
   */
  @InterfaceAudience.Private
  public static class MetricValueMap extends HashMap<String, Number> {
    private static final long serialVersionUID = 8936732010242400171L;
  }
}
