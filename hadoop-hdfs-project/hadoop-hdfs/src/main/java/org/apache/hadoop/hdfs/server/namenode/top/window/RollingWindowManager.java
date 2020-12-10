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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to manage the set of {@link RollingWindow}s. This class is the
 * interface of metrics system to the {@link RollingWindow}s to retrieve the
 * current top metrics.
 * <p>
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
      this.top = new LinkedList<>();
    }

    public void addOp(Op op) {
      if (op.getOpType().equals(TopConf.ALL_CMDS)) {
        top.add(0, op);
      } else {
        top.add(op);
      }
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
  public static class Op implements Comparable<Op> {
    private final String opType;
    private final List<User> users;
    private final long totalCount;
    private final int limit;

    public Op(String opType, UserCounts users, int limit) {
      this.opType = opType;
      this.users = new ArrayList<>(users);
      this.users.sort(Collections.reverseOrder());
      this.totalCount = users.getTotal();
      this.limit = limit;
    }

    public String getOpType() {
      return opType;
    }

    public List<User> getAllUsers() {
      return users;
    }

    public List<User> getTopUsers() {
      return (users.size() > limit) ? users.subList(0, limit) : users;
    }

    public long getTotalCount() {
      return totalCount;
    }

    @Override
    public int compareTo(Op other) {
      return Long.signum(totalCount - other.totalCount);
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof Op) && totalCount == ((Op)o).totalCount;
    }

    @Override
    public int hashCode() {
      return opType.hashCode();
    }
  }

  /**
   * Represents a user who called an Op within a TopWindow. Specifies the 
   * user and the number of times the user called the operation.
   */
  public static class User implements Comparable<User> {
    private final String user;
    private long count;

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

    public void add(long delta) {
      count += delta;
    }

    @Override
    public int compareTo(User other) {
      return Long.signum(count - other.count);
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof User) && user.equals(((User)o).user);
    }

    @Override
    public int hashCode() {
      return user.hashCode();
    }
  }

  private static class UserCounts extends ArrayList<User> {
    private long total = 0;

    UserCounts(int capacity) {
      super(capacity);
    }

    @Override
    public boolean add(User user) {
      long count = user.getCount();
      int i = indexOf(user);
      if (i == -1) {
        super.add(new User(user.getUser(), count));
      } else {
        get(i).add(count);
      }
      total += count;
      return true;
    }

    @Override
    public boolean addAll(Collection<? extends User> users) {
      users.forEach(user -> add(user));
      return true;
    }

    public long getTotal() {
      return total;
    }
  }

  /**
   * A mapping from each reported metric to its {@link RollingWindowMap} that
   * maintains the set of {@link RollingWindow}s for the users that have
   * operated on that metric.
   */
  public ConcurrentHashMap<String, RollingWindowMap> metricMap =
      new ConcurrentHashMap<>();

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
   *         window.
   */
  public TopWindow snapshot(long time) {
    TopWindow window = new TopWindow(windowLenMs);
    Set<String> metricNames = metricMap.keySet();
    LOG.debug("iterating in reported metrics, size={} values={}",
        metricNames.size(), metricNames);
    UserCounts totalCounts = new UserCounts(metricMap.size());
    for (Map.Entry<String, RollingWindowMap> entry : metricMap.entrySet()) {
      String metricName = entry.getKey();
      RollingWindowMap rollingWindows = entry.getValue();
      UserCounts topN = getTopUsersForMetric(time, metricName, rollingWindows);
      if (!topN.isEmpty()) {
        window.addOp(new Op(metricName, topN, topUsersCnt));
        totalCounts.addAll(topN);
      }
    }
    // synthesize the overall total op count with the top users for every op.
    Set<User> topUsers = new HashSet<>();
    for (Op op : window.getOps()) {
      topUsers.addAll(op.getTopUsers());
    }
    // intersect totals with the top users.
    totalCounts.retainAll(topUsers);
    // allowed to exceed the per-op topUsersCnt to capture total ops for
    // any user
    window.addOp(new Op(TopConf.ALL_CMDS, totalCounts, Integer.MAX_VALUE));
    return window;
  }

  /**
   * Calculates the top N users over a time interval.
   * 
   * @param time the current time
   * @param metricName Name of metric
   * @return
   */
  private UserCounts getTopUsersForMetric(long time, String metricName,
      RollingWindowMap rollingWindows) {
    UserCounts topN = new UserCounts(topUsersCnt);
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
      topN.add(new User(userName, windowSum));
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
