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
package org.apache.hadoop.hdfs.server.datanode.metrics;

import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 *
 * This class is for maintaining  the various DataNode statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #blocksRead}.inc()
 *
 */
@InterfaceAudience.Private
@Metrics(about="DataNode metrics", context="dfs")
public class DataNodeMetrics {

  @Metric MutableCounterLong bytesWritten;
  @Metric MutableCounterLong bytesRead;
  @Metric MutableCounterLong blocksWritten;
  @Metric MutableCounterLong blocksRead;
  @Metric MutableCounterLong blocksReplicated;
  @Metric MutableCounterLong blocksRemoved;
  @Metric MutableCounterLong blocksVerified;
  @Metric MutableCounterLong blockVerificationFailures;
  @Metric MutableCounterLong readsFromLocalClient;
  @Metric MutableCounterLong readsFromRemoteClient;
  @Metric MutableCounterLong writesFromLocalClient;
  @Metric MutableCounterLong writesFromRemoteClient;
  
  @Metric MutableCounterLong volumeFailures;

  @Metric MutableRate readBlockOp;
  @Metric MutableRate writeBlockOp;
  @Metric MutableRate blockChecksumOp;
  @Metric MutableRate copyBlockOp;
  @Metric MutableRate replaceBlockOp;
  @Metric MutableRate heartbeats;
  @Metric MutableRate blockReports;

  final MetricsRegistry registry = new MetricsRegistry("datanode");
  final String name;

  public DataNodeMetrics(String name, String sessionId) {
    this.name = name;
    registry.tag(SessionId, sessionId);
  }

  public static DataNodeMetrics create(Configuration conf, String dnName) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics.create("DataNode", sessionId, ms);
    String name = "DataNodeActivity-"+ (dnName.isEmpty()
        ? "UndefinedDataNodeName"+ DFSUtil.getRandom().nextInt() : dnName.replace(':', '-'));
    return ms.register(name, null, new DataNodeMetrics(name, sessionId));
  }

  public String name() { return name; }

  public void addHeartbeat(long latency) {
    heartbeats.add(latency);
  }

  public void addBlockReport(long latency) {
    blockReports.add(latency);
  }

  public void incrBlocksReplicated(int delta) {
    blocksReplicated.incr(delta);
  }

  public void incrBlocksWritten() {
    blocksWritten.incr();
  }

  public void incrBlocksRemoved(int delta) {
    blocksRemoved.incr(delta);
  }

  public void incrBytesWritten(int delta) {
    bytesWritten.incr(delta);
  }

  public void incrBlockVerificationFailures() {
    blockVerificationFailures.incr();
  }

  public void incrBlocksVerified() {
    blocksVerified.incr();
  }

  public void addReadBlockOp(long latency) {
    readBlockOp.add(latency);
  }

  public void addWriteBlockOp(long latency) {
    writeBlockOp.add(latency);
  }

  public void addReplaceBlockOp(long latency) {
    replaceBlockOp.add(latency);
  }

  public void addCopyBlockOp(long latency) {
    copyBlockOp.add(latency);
  }

  public void addBlockChecksumOp(long latency) {
    blockChecksumOp.add(latency);
  }

  public void incrBytesRead(int delta) {
    bytesRead.incr(delta);
  }

  public void incrBlocksRead() {
    blocksRead.incr();
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public void incrWritesFromClient(boolean local) {
    (local ? writesFromLocalClient : writesFromRemoteClient).incr();
  }

  public void incrReadsFromClient(boolean local) {
    (local ? readsFromLocalClient : readsFromRemoteClient).incr();
  }
  
  public void incrVolumeFailures() {
    volumeFailures.incr();
  }
}
