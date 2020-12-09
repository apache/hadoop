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
package org.apache.hadoop.fs.http.server.metrics;

import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.source.JvmMetrics;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * This class is for maintaining  the various HttpFSServer statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #bytesRead}.inc()
 *
 */
@InterfaceAudience.Private
@Metrics(about="HttpFSServer metrics", context="httpfs")
public class HttpFSServerMetrics {

  private @Metric MutableCounterLong bytesWritten;
  private @Metric MutableCounterLong bytesRead;

  // Write ops
  private @Metric MutableCounterLong opsCreate;
  private @Metric MutableCounterLong opsAppend;
  private @Metric MutableCounterLong opsTruncate;
  private @Metric MutableCounterLong opsDelete;
  private @Metric MutableCounterLong opsRename;
  private @Metric MutableCounterLong opsMkdir;

  // Read ops
  private @Metric MutableCounterLong opsOpen;
  private @Metric MutableCounterLong opsListing;
  private @Metric MutableCounterLong opsStat;
  private @Metric MutableCounterLong opsCheckAccess;

  private final MetricsRegistry registry = new MetricsRegistry("httpfsserver");
  private final String name;
  private JvmMetrics jvmMetrics = null;

  public HttpFSServerMetrics(String name, String sessionId,
      final JvmMetrics jvmMetrics) {
    this.name = name;
    this.jvmMetrics = jvmMetrics;
    registry.tag(SessionId, sessionId);
  }

  public static HttpFSServerMetrics create(Configuration conf,
      String serverName) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create("HttpFSServer", sessionId, ms);
    String name = "ServerActivity-"+ (serverName.isEmpty()
        ? "UndefinedServer"+ ThreadLocalRandom.current().nextInt()
        : serverName.replace(':', '-'));

    return ms.register(name, null, new HttpFSServerMetrics(name,
        sessionId, jm));
  }

  public String name() {
    return name;
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  public void incrBytesWritten(long bytes) {
    bytesWritten.incr(bytes);
  }

  public void incrBytesRead(long bytes) {
    bytesRead.incr(bytes);
  }

  public void incrOpsCreate() {
    opsCreate.incr();
  }

  public void incrOpsAppend() {
    opsAppend.incr();
  }

  public void incrOpsTruncate() {
    opsTruncate.incr();
  }

  public void incrOpsDelete() {
    opsDelete.incr();
  }

  public void incrOpsRename() {
    opsRename.incr();
  }

  public void incrOpsMkdir() {
    opsMkdir.incr();
  }

  public void incrOpsOpen() {
    opsOpen.incr();
  }

  public void incrOpsListing() {
    opsListing.incr();
  }

  public void incrOpsStat() {
    opsStat.incr();
  }

  public void incrOpsCheckAccess() {
    opsCheckAccess.incr();
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public long getOpsMkdir() {
    return opsMkdir.value();
  }

  public long getOpsListing() {
    return opsListing.value();
  }

  public long getOpsStat() {
    return opsStat.value();
  }
}
