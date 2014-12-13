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
package org.apache.hadoop.hdfs.server.namenode.top.metrics;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.TopWindow;

/**
 * The interface to the top metrics.
 * <p/>
 * Metrics are collected by a custom audit logger, {@link org.apache.hadoop
 * .hdfs.server.namenode.top.TopAuditLogger}, which calls TopMetrics to
 * increment per-operation, per-user counts on every audit log call. These
 * counts are used to show the top users by NameNode operation as well as
 * across all operations.
 * <p/>
 * TopMetrics maintains these counts for a configurable number of time
 * intervals, e.g. 1min, 5min, 25min. Each interval is tracked by a
 * RollingWindowManager.
 * <p/>
 * These metrics are published as a JSON string via {@link org.apache.hadoop
 * .hdfs.server .namenode.metrics.FSNamesystemMBean#getTopWindows}. This is
 * done by calling {@link org.apache.hadoop.hdfs.server.namenode.top.window
 * .RollingWindowManager#snapshot} on each RollingWindowManager.
 * <p/>
 * Thread-safe: relies on thread-safety of RollingWindowManager
 */
@InterfaceAudience.Private
public class TopMetrics {
  public static final Logger LOG = LoggerFactory.getLogger(TopMetrics.class);

  private static void logConf(Configuration conf) {
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY));
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_NUM_USERS_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_NUM_USERS_KEY));
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY));
  }

  /**
   * A map from reporting periods to WindowManager. Thread-safety is provided by
   * the fact that the mapping is not changed after construction.
   */
  final Map<Integer, RollingWindowManager> rollingWindowManagers =
      new HashMap<Integer, RollingWindowManager>();

  public TopMetrics(Configuration conf, int[] reportingPeriods) {
    logConf(conf);
    for (int i = 0; i < reportingPeriods.length; i++) {
      rollingWindowManagers.put(reportingPeriods[i], new RollingWindowManager(
          conf, reportingPeriods[i]));
    }
  }

  /**
   * Get a list of the current TopWindow statistics, one TopWindow per tracked
   * time interval.
   */
  public List<TopWindow> getTopWindows() {
    long monoTime = Time.monotonicNow();
    List<TopWindow> windows = Lists.newArrayListWithCapacity
        (rollingWindowManagers.size());
    for (Entry<Integer, RollingWindowManager> entry : rollingWindowManagers
        .entrySet()) {
      TopWindow window = entry.getValue().snapshot(monoTime);
      windows.add(window);
    }
    return windows;
  }

  /**
   * Pick the same information that DefaultAuditLogger does before writing to a
   * log file. This is to be consistent when {@link TopMetrics} is charged with
   * data read back from log files instead of being invoked directly by the
   * FsNamesystem
   */
  public void report(boolean succeeded, String userName, InetAddress addr,
      String cmd, String src, String dst, FileStatus status) {
    // currently nntop only makes use of the username and the command
    report(userName, cmd);
  }

  public void report(String userName, String cmd) {
    long currTime = Time.monotonicNow();
    report(currTime, userName, cmd);
  }

  public void report(long currTime, String userName, String cmd) {
    LOG.debug("a metric is reported: cmd: {} user: {}", cmd, userName);
    userName = UserGroupInformation.trimLoginMethod(userName);
    for (RollingWindowManager rollingWindowManager : rollingWindowManagers
        .values()) {
      rollingWindowManager.recordMetric(currTime, cmd, userName, 1);
      rollingWindowManager.recordMetric(currTime,
          TopConf.ALL_CMDS, userName, 1);
    }
  }
}
