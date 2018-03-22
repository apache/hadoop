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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * This class is for maintaining the various Router activity statistics
 * and publishing them through the metrics interfaces.
 */
@Metrics(name="RouterActivity", about="Router metrics", context="dfs")
public class RouterMetrics {

  private final MetricsRegistry registry = new MetricsRegistry("router");

  @Metric("Duration in SafeMode at startup in msec")
  private MutableGaugeInt safeModeTime;

  private JvmMetrics jvmMetrics = null;

  RouterMetrics(
      String processName, String sessionId, final JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
    registry.tag(ProcessName, processName).tag(SessionId, sessionId);
  }

  public static RouterMetrics create(Configuration conf) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    String processName = "Router";
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create(processName, sessionId, ms);

    return ms.register(new RouterMetrics(processName, sessionId, jm));
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public void setSafeModeTime(long elapsed) {
    safeModeTime.set((int) elapsed);
  }
}
