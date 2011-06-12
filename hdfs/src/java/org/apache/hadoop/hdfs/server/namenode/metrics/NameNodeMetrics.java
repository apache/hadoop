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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import static org.apache.hadoop.metrics2.impl.MsInfo.*;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * This class is for maintaining  the various NameNode activity statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(name="NameNodeActivity", about="NameNode metrics", context="dfs")
public class NameNodeMetrics {
  final MetricsRegistry registry = new MetricsRegistry("namenode");

  @Metric MutableCounterLong createFileOps;
  @Metric MutableCounterLong filesCreated;
  @Metric MutableCounterLong filesAppended;
  @Metric MutableCounterLong getBlockLocations;
  @Metric MutableCounterLong filesRenamed;
  @Metric MutableCounterLong getListingOps;
  @Metric MutableCounterLong deleteFileOps;
  @Metric("Number of files/dirs deleted by delete or rename operations")
  MutableCounterLong filesDeleted;
  @Metric MutableCounterLong fileInfoOps;
  @Metric MutableCounterLong addBlockOps;
  @Metric MutableCounterLong getAdditionalDatanodeOps;
  @Metric MutableCounterLong createSymlinkOps;
  @Metric MutableCounterLong getLinkTargetOps;
  @Metric MutableCounterLong filesInGetListingOps;

  @Metric("Journal transactions") MutableRate transactions;
  @Metric("Journal syncs") MutableRate syncs;
  @Metric("Journal transactions batched in sync")
  MutableCounterLong transactionsBatchedInSync;
  @Metric("Block report") MutableRate blockReport;

  @Metric("Duration in SafeMode at startup") MutableGaugeInt safeModeTime;
  @Metric("Time loading FS Image at startup") MutableGaugeInt fsImageLoadTime;

  NameNodeMetrics(String processName, String sessionId) {
    registry.tag(ProcessName, processName).tag(SessionId, sessionId);
  }

  public static NameNodeMetrics create(Configuration conf, NamenodeRole r) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    String processName = r.toString();
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics.create(processName, sessionId, ms);
    return ms.register(new NameNodeMetrics(processName, sessionId));
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public void incrGetBlockLocations() {
    getBlockLocations.incr();
  }

  public void incrFilesCreated() {
    filesCreated.incr();
  }

  public void incrCreateFileOps() {
    createFileOps.incr();
  }

  public void incrFilesAppended() {
    filesAppended.incr();
  }

  public void incrAddBlockOps() {
    addBlockOps.incr();
  }

  public void incrGetAdditionalDatanodeOps() {
    getAdditionalDatanodeOps.incr();
  }

  public void incrFilesRenamed() {
    filesRenamed.incr();
  }

  public void incrFilesDeleted(int delta) {
    filesDeleted.incr(delta);
  }

  public void incrDeleteFileOps() {
    deleteFileOps.incr();
  }

  public void incrGetListingOps() {
    getListingOps.incr();
  }

  public void incrFilesInGetListingOps(int delta) {
    filesInGetListingOps.incr(delta);
  }

  public void incrFileInfoOps() {
    fileInfoOps.incr();
  }

  public void incrCreateSymlinkOps() {
    createSymlinkOps.incr();
  }

  public void incrGetLinkTargetOps() {
    getLinkTargetOps.incr();
  }

  public void addTransaction(long latency) {
    transactions.add(latency);
  }

  public void incrTransactionsBatchedInSync() {
    transactionsBatchedInSync.incr();
  }

  public void addSync(long elapsed) {
    syncs.add(elapsed);
  }

  public void setFsImageLoadTime(long elapsed) {
    fsImageLoadTime.set((int) elapsed);
  }

  public void addBlockReport(long latency) {
    blockReport.add(latency);
  }

  public void setSafeModeTime(long elapsed) {
    safeModeTime.set((int) elapsed);
  }
}
