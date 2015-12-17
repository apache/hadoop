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

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * This class is for maintaining  the various NameNode activity statistics
 * and publishing them through the metrics interfaces.
 */
@Metrics(name="NameNodeActivity", about="NameNode metrics", context="dfs")
public class NameNodeMetrics {
  final MetricsRegistry registry = new MetricsRegistry("namenode");

  @Metric MutableCounterLong createFileOps;
  @Metric MutableCounterLong filesCreated;
  @Metric MutableCounterLong filesAppended;
  @Metric MutableCounterLong getBlockLocations;
  @Metric MutableCounterLong filesRenamed;
  @Metric MutableCounterLong filesTruncated;
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
  @Metric("Number of allowSnapshot operations")
  MutableCounterLong allowSnapshotOps;
  @Metric("Number of disallowSnapshot operations")
  MutableCounterLong disallowSnapshotOps;
  @Metric("Number of createSnapshot operations")
  MutableCounterLong createSnapshotOps;
  @Metric("Number of deleteSnapshot operations")
  MutableCounterLong deleteSnapshotOps;
  @Metric("Number of renameSnapshot operations")
  MutableCounterLong renameSnapshotOps;
  @Metric("Number of listSnapshottableDirectory operations")
  MutableCounterLong listSnapshottableDirOps;
  @Metric("Number of snapshotDiffReport operations")
  MutableCounterLong snapshotDiffReportOps;
  @Metric("Number of blockReceivedAndDeleted calls")
  MutableCounterLong blockReceivedAndDeletedOps;
  @Metric("Number of blockReports from individual storages")
  MutableCounterLong storageBlockReportOps;

  @Metric("Number of file system operations")
  public long totalFileOps(){
    return
      getBlockLocations.value() +
      createFileOps.value() +
      filesAppended.value() +
      addBlockOps.value() +
      getAdditionalDatanodeOps.value() +
      filesRenamed.value() +
      filesTruncated.value() +
      deleteFileOps.value() +
      getListingOps.value() +
      fileInfoOps.value() +
      getLinkTargetOps.value() +
      createSnapshotOps.value() +
      deleteSnapshotOps.value() +
      allowSnapshotOps.value() +
      disallowSnapshotOps.value() +
      renameSnapshotOps.value() +
      listSnapshottableDirOps.value() +
      createSymlinkOps.value() +
      snapshotDiffReportOps.value();
  }


  @Metric("Journal transactions") MutableRate transactions;
  @Metric("Journal syncs") MutableRate syncs;
  final MutableQuantiles[] syncsQuantiles;
  @Metric("Journal transactions batched in sync")
  MutableCounterLong transactionsBatchedInSync;
  @Metric("Block report") MutableRate blockReport;
  final MutableQuantiles[] blockReportQuantiles;
  @Metric("Cache report") MutableRate cacheReport;
  final MutableQuantiles[] cacheReportQuantiles;

  @Metric("Duration in SafeMode at startup in msec")
  MutableGaugeInt safeModeTime;
  @Metric("Time loading FS Image at startup in msec")
  MutableGaugeInt fsImageLoadTime;

  @Metric("GetImageServlet getEdit")
  MutableRate getEdit;
  @Metric("GetImageServlet getImage")
  MutableRate getImage;
  @Metric("GetImageServlet putImage")
  MutableRate putImage;

  JvmMetrics jvmMetrics = null;
  
  NameNodeMetrics(String processName, String sessionId, int[] intervals,
      final JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
    registry.tag(ProcessName, processName).tag(SessionId, sessionId);
    
    final int len = intervals.length;
    syncsQuantiles = new MutableQuantiles[len];
    blockReportQuantiles = new MutableQuantiles[len];
    cacheReportQuantiles = new MutableQuantiles[len];
    
    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      syncsQuantiles[i] = registry.newQuantiles(
          "syncs" + interval + "s",
          "Journal syncs", "ops", "latency", interval);
      blockReportQuantiles[i] = registry.newQuantiles(
          "blockReport" + interval + "s", 
          "Block report", "ops", "latency", interval);
      cacheReportQuantiles[i] = registry.newQuantiles(
          "cacheReport" + interval + "s",
          "Cache report", "ops", "latency", interval);
    }
  }

  public static NameNodeMetrics create(Configuration conf, NamenodeRole r) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    String processName = r.toString();
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create(processName, sessionId, ms);
    
    // Percentile measurement is off by default, by watching no intervals
    int[] intervals = 
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    return ms.register(new NameNodeMetrics(processName, sessionId,
        intervals, jm));
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
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

  public void incrFilesTruncated() {
    filesTruncated.incr();
  }

  public void incrFilesDeleted(long delta) {
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

  public void incrAllowSnapshotOps() {
    allowSnapshotOps.incr();
  }
  
  public void incrDisAllowSnapshotOps() {
    disallowSnapshotOps.incr();
  }
  
  public void incrCreateSnapshotOps() {
    createSnapshotOps.incr();
  }
  
  public void incrDeleteSnapshotOps() {
    deleteSnapshotOps.incr();
  }
  
  public void incrRenameSnapshotOps() {
    renameSnapshotOps.incr();
  }
  
  public void incrListSnapshottableDirOps() {
    listSnapshottableDirOps.incr();
  }
  
  public void incrSnapshotDiffReportOps() {
    snapshotDiffReportOps.incr();
  }
  
  public void incrBlockReceivedAndDeletedOps() {
    blockReceivedAndDeletedOps.incr();
  }
  
  public void incrStorageBlockReportOps() {
    storageBlockReportOps.incr();
  }

  public void addTransaction(long latency) {
    transactions.add(latency);
  }

  public void incrTransactionsBatchedInSync() {
    transactionsBatchedInSync.incr();
  }

  public void addSync(long elapsed) {
    syncs.add(elapsed);
    for (MutableQuantiles q : syncsQuantiles) {
      q.add(elapsed);
    }
  }

  public void setFsImageLoadTime(long elapsed) {
    fsImageLoadTime.set((int) elapsed);
  }

  public void addBlockReport(long latency) {
    blockReport.add(latency);
    for (MutableQuantiles q : blockReportQuantiles) {
      q.add(latency);
    }
  }

  public void addCacheBlockReport(long latency) {
    cacheReport.add(latency);
    for (MutableQuantiles q : cacheReportQuantiles) {
      q.add(latency);
    }
  }

  public void setSafeModeTime(long elapsed) {
    safeModeTime.set((int) elapsed);
  }

  public void addGetEdit(long latency) {
    getEdit.add(latency);
  }

  public void addGetImage(long latency) {
    getImage.add(latency);
  }

  public void addPutImage(long latency) {
    putImage.add(latency);
  }
}
