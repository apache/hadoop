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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.util.Metrics2Util.NameValuePair;
import org.apache.hadoop.metrics2.util.Metrics2Util.TopN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final int windowLenMs;
  private final int bucketsPerWindow; // e.g., 10 buckets per minute
  private final int topUsersCnt; // e.g., report top 10 metrics

  static private class RollingWindowMap extends
      ConcurrentHashMap<String, RollingWindow> {
    private static final long serialVersionUID = -6785807073237052051L;
  }

  /**
   * Represents a snapshot of the rolling window. It contains one Op per 
   * operation in the window, with ranked users for each Op.
   */
  public static class TopWindow {
    private final int windowMillis;
    private final List<Op> top;

    public TopWindow(int windowMillis) {
      this.windowMillis = windowMillis;
      this.top = Lists.newArrayList();
    }

    public void addOp(Op op) {
      top.add(op);
    }

    public int getWindowLenMs() {
      return windowMillis;
    }

    public List<Op> getOps() {
      return top;
    }
  }

  /**
   * Represents an operation within a TopWindow. It contains a ranked 
   * set of the top users for the operation.
   */
  public static class Op {
    private final String opType;
    private final List<User> topUsers;
    private final long totalCount;

    public Op(String opType, long totalCount) {
      this.opType = opType;
      this.topUsers = Lists.newArrayList();
      this.totalCount = totalCount;
    }

    public void addUser(User u) {
      topUsers.add(u);
    }

    public String getOpType() {
      return opType;
    }

    public List<User> getTopUsers() {
      return topUsers;
    }

    public long getTotalCount() {
      return totalCount;
    }
  }

  /**
   * Represents a user who called an Op within a TopWindow. Specifies the 
   * user and the number of times the user called the operation.
   */
  public static class User {
    private final String user;
    private final long count;

    public User(String user, long count) {
      this.user = user;
      this.count = count;
    }

    public String getUser() {
      return user;
    }

    public long getCount() {
      return count;
    }
  }

  /**
   * A mapping from each reported metric to its {@link RollingWindowMap} that
   * maintains the set of {@link RollingWindow}s for the users that have
   * operated on that metric.
   */
  public ConcurrentHashMap<String, RollingWindowMap> metricMap =
      new ConcurrentHashMap<String, RollingWindowMap>();

  public RollingWindowManager(Configuration conf, int reportingPeriodMs) {
    
    windowLenMs = reportingPeriodMs;
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
  public void recordMetric(long time, String command,
      String user, long delta) {
    RollingWindow window = getRollingWindow(command, user);
    window.incAt(time, delta);
  }

  /**
   * Take a snapshot of current top users in the past period.
   *
   * @param time the current time
   * @return a TopWindow describing the top users for each metric in the 
   * window.
   */
  public TopWindow snapshot(long time) {
    TopWindow window = new TopWindow(windowLenMs);
    Set<String> metricNames = metricMap.keySet();
    LOG.debug("iterating in reported metrics, size={} values={}",
        metricNames.size(), metricNames);
    for (Map.Entry<String, RollingWindowMap> entry : metricMap.entrySet()) {
      String metricName = entry.getKey();
      RollingWindowMap rollingWindows = entry.getValue();
      TopN topN = getTopUsersForMetric(time, metricName, rollingWindows);
      final int size = topN.size();
      if (size == 0) {
        continue;
      }
      Op op = new Op(metricName, topN.getTotal());
      window.addOp(op);
      // Reverse the users from the TopUsers using a stack, 
      // since we'd like them sorted in descending rather than ascending order
      Stack<NameValuePair> reverse = new Stack<NameValuePair>();
      for (int i = 0; i < size; i++) {
        reverse.push(topN.poll());
      }
      for (int i = 0; i < size; i++) {
        NameValuePair userEntry = reverse.pop();
        User user = new User(userEntry.getName(), userEntry.getValue());
        op.addUser(user);
      }
    }
    return window;
  }

  /**
   * Calculates the top N users over a time interval.
   * 
   * @param time the current time
   * @param metricName Name of metric
   * @return
   */
  private TopN getTopUsersForMetric(long time, String metricName, 
      RollingWindowMap rollingWindows) {
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
    LOG.debug("topN users size for command {} is: {}",
        metricName, topN.size());
    return topN;
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
}
