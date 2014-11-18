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

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;
import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.TopAuditLogger;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.MetricValueMap;

/***
 * The interface to the top metrics
 * <p/>
 * The producers use the {@link #report} method to report events and the
 * consumers use {@link #getMetrics(MetricsCollector, boolean)} to retrieve the
 * current top metrics. The default consumer is JMX but it could be any other
 * user interface.
 * <p/>
 * Thread-safe: relies on thread-safety of RollingWindowManager
 */
@InterfaceAudience.Private
public class TopMetrics implements MetricsSource {
  public static final Logger LOG = LoggerFactory.getLogger(TopMetrics.class);

  enum Singleton {
    INSTANCE;

    volatile TopMetrics impl = null;

    synchronized TopMetrics init(Configuration conf, String processName,
        String sessionId, long[] reportingPeriods) {
      if (impl == null) {
        impl =
            create(conf, processName, sessionId, reportingPeriods,
                DefaultMetricsSystem.instance());
      }
      logConf(conf);
      return impl;
    }
  }

  private static void logConf(Configuration conf) {
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY));
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_NUM_USERS_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_NUM_USERS_KEY));
    LOG.info("NNTop conf: " + DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY +
        " = " +  conf.get(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY));
  }

  /**
   * Return only the shortest periods for default
   * TODO: make it configurable
   */
  final boolean smallestOnlyDefault = true;

  /**
   * The smallest of reporting periods
   */
  long smallestPeriod = Long.MAX_VALUE;

  /**
   * processName and sessionId might later be leveraged later when we aggregate
   * report from multiple federated name nodes
   */
  final String processName, sessionId;

  /**
   * A map from reporting periods to WindowManager. Thread-safety is provided by
   * the fact that the mapping is not changed after construction.
   */
  final Map<Long, RollingWindowManager> rollingWindowManagers =
      new HashMap<Long, RollingWindowManager>();

  TopMetrics(Configuration conf, String processName, String sessionId,
      long[] reportingPeriods) {
    this.processName = processName;
    this.sessionId = sessionId;
    for (int i = 0; i < reportingPeriods.length; i++) {
      smallestPeriod = Math.min(smallestPeriod, reportingPeriods[i]);
      rollingWindowManagers.put(reportingPeriods[i], new RollingWindowManager(
          conf, reportingPeriods[i]));
    }
  }

  public static TopMetrics create(Configuration conf, String processName,
      String sessionId, long[] reportingPeriods, MetricsSystem ms) {
    return ms.register(TopConf.TOP_METRICS_REGISTRATION_NAME,
        "top metrics of the namenode in a last period of time", new TopMetrics(
            conf, processName, sessionId, reportingPeriods));
  }

  public static TopMetrics initSingleton(Configuration conf,
      String processName, String sessionId, long[] reportingPeriods) {
    return Singleton.INSTANCE.init(conf, processName, sessionId,
        reportingPeriods);
  }

  public static TopMetrics getInstance() {
    TopMetrics topMetrics = Singleton.INSTANCE.impl;
    Preconditions.checkArgument(topMetrics != null,
          "The TopMetric singleton instance is not initialized."
              + " Have you called initSingleton first?");
    return topMetrics;
  }

  /**
   * In testing, the previous initialization should be reset if the entire
   * metric system is reinitialized
   */
  @VisibleForTesting
  public static void reset() {
    Singleton.INSTANCE.impl = null;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    long realTime = Time.monotonicNow();
    getMetrics(smallestOnlyDefault, realTime, collector, all);
  }

  public void getMetrics(boolean smallestOnly, long currTime,
      MetricsCollector collector, boolean all) {
    for (Entry<Long, RollingWindowManager> entry : rollingWindowManagers
        .entrySet()) {
      if (!smallestOnly || smallestPeriod == entry.getKey()) {
        getMetrics(currTime, collector, entry.getKey(), entry.getValue(), all);
      }
    }
  }

  /**
   * Get metrics for a particular recording period and its corresponding
   * {@link RollingWindowManager}
   * <p/>
   *
   * @param collector the metric collector
   * @param period the reporting period
   * @param rollingWindowManager the window manager corresponding to the
   *          reporting period
   * @param all currently ignored
   */
  void getMetrics(long currTime, MetricsCollector collector, Long period,
      RollingWindowManager rollingWindowManager, boolean all) {
    MetricsRecordBuilder rb =
        collector.addRecord(createTopMetricsRecordName(period))
            .setContext("namenode").tag(ProcessName, processName)
            .tag(SessionId, sessionId);

    MetricValueMap snapshotMetrics = rollingWindowManager.snapshot(currTime);
    LOG.debug("calling snapshot, result size is: " + snapshotMetrics.size());
    for (Map.Entry<String, Number> entry : snapshotMetrics.entrySet()) {
      String key = entry.getKey();
      Number value = entry.getValue();
      LOG.debug("checking an entry: key: {} value: {}", key, value);
      long min = period / 1000L / 60L; //ms -> min
      String desc = "top user of name node in the past " + min + " minutes";

      if (value instanceof Integer) {
        rb.addGauge(info(key, desc), (Integer) value);
      } else if (value instanceof Long) {
        rb.addGauge(info(key, desc), (Long) value);
      } else if (value instanceof Float) {
        rb.addGauge(info(key, desc), (Float) value);
      } else if (value instanceof Double) {
        rb.addGauge(info(key, desc), (Double) value);
      } else {
        LOG.warn("Unsupported metric type: " + value.getClass());
      }
    }
    LOG.debug("END iterating over metrics, result size is: {}",
        snapshotMetrics.size());
  }

  /**
   * Pick the same information that DefaultAuditLogger does before writing to a
   * log file. This is to be consistent when {@link TopMetrics} is charged with
   * data read back from log files instead of being invoked directly by the
   * FsNamesystem
   *
   * @param succeeded
   * @param userName
   * @param addr
   * @param cmd
   * @param src
   * @param dst
   * @param status
   */
  public void report(boolean succeeded, String userName, InetAddress addr,
      String cmd, String src, String dst, FileStatus status) {
    //currently we nntop makes use of only the username and the command
    report(userName, cmd);
  }

  public void report(String userName, String cmd) {
    long currTime = Time.monotonicNow();
    report(currTime, userName, cmd);
  }

  public void report(long currTime, String userName, String cmd) {
    LOG.debug("a metric is reported: cmd: {} user: {}", cmd, userName);
    userName = UserGroupInformation.trimLoginMethod(userName);
    try {
      for (RollingWindowManager rollingWindowManager : rollingWindowManagers
          .values()) {
        rollingWindowManager.recordMetric(currTime, cmd, userName, 1);
        rollingWindowManager.recordMetric(currTime,
            TopConf.CMD_TOTAL, userName, 1);
      }
    } catch (Throwable t) {
      LOG.error("An error occurred while reflecting the event in top service, "
          + "event: (time,cmd,userName)=(" + currTime + "," + cmd + ","
          + userName);
    }
  }

  /***
   *
   * @param period the reporting period length in ms
   * @return
   */
  public static String createTopMetricsRecordName(Long period) {
    return TopConf.TOP_METRICS_RECORD_NAME + "-" + period;
  }

}
