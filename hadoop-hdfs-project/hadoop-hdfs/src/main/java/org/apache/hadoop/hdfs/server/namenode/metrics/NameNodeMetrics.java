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
import org.apache.hadoop.metrics2.lib.MutableStat;
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
  @Metric ("Number of successful re-replications")
  MutableCounterLong successfulReReplications;
  @Metric ("Number of times we failed to schedule a block re-replication.")
  MutableCounterLong numTimesReReplicationNotScheduled;
  @Metric("Number of timed out block re-replications")
  MutableCounterLong timeoutReReplications;
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
  @Metric("Number of blockReports and blockReceivedAndDeleted queued")
  MutableGaugeInt blockOpsQueued;
  @Metric("Number of blockReports and blockReceivedAndDeleted batch processed")
  MutableCounterLong blockOpsBatched;
  @Metric("Number of pending edits")
  MutableGaugeInt pendingEditsCount;

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
  @Metric("Journal transactions batched in sync")
  final MutableQuantiles[] numTransactionsBatchedInSync;
  @Metric("Number of blockReports from individual storages")
  MutableRate storageBlockReport;
  final MutableQuantiles[] storageBlockReportQuantiles;
  @Metric("Cache report") MutableRate cacheReport;
  final MutableQuantiles[] cacheReportQuantiles;
  @Metric("Generate EDEK time") private MutableRate generateEDEKTime;
  private final MutableQuantiles[] generateEDEKTimeQuantiles;
  @Metric("Warm-up EDEK time") private MutableRate warmUpEDEKTime;
  private final MutableQuantiles[] warmUpEDEKTimeQuantiles;
  @Metric("Resource check time") private MutableRate resourceCheckTime;
  private final MutableQuantiles[] resourceCheckTimeQuantiles;

  @Metric("Duration in SafeMode at startup in msec")
  MutableGaugeInt safeModeTime;
  @Metric("Time loading FS Image at startup in msec")
  MutableGaugeInt fsImageLoadTime;

  @Metric("Time tailing edit logs in msec")
  MutableRate editLogTailTime;
  private final MutableQuantiles[] editLogTailTimeQuantiles;
  @Metric MutableRate editLogFetchTime;
  private final MutableQuantiles[] editLogFetchTimeQuantiles;
  @Metric(value = "Number of edits loaded", valueName = "Count")
  MutableStat numEditLogLoaded;
  private final MutableQuantiles[] numEditLogLoadedQuantiles;
  @Metric("Time between edit log tailing in msec")
  MutableRate editLogTailInterval;
  private final MutableQuantiles[] editLogTailIntervalQuantiles;

  @Metric("GetImageServlet getEdit")
  MutableRate getEdit;
  @Metric("GetImageServlet getImage")
  MutableRate getImage;
  @Metric("GetImageServlet getAliasMap")
  MutableRate getAliasMap;
  @Metric("GetImageServlet putImage")
  MutableRate putImage;

  JvmMetrics jvmMetrics = null;
  
  NameNodeMetrics(String processName, String sessionId, int[] intervals,
      final JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
    registry.tag(ProcessName, processName).tag(SessionId, sessionId);
    
    final int len = intervals.length;
    syncsQuantiles = new MutableQuantiles[len];
    numTransactionsBatchedInSync = new MutableQuantiles[len];
    storageBlockReportQuantiles = new MutableQuantiles[len];
    cacheReportQuantiles = new MutableQuantiles[len];
    generateEDEKTimeQuantiles = new MutableQuantiles[len];
    warmUpEDEKTimeQuantiles = new MutableQuantiles[len];
    resourceCheckTimeQuantiles = new MutableQuantiles[len];
    editLogTailTimeQuantiles = new MutableQuantiles[len];
    editLogFetchTimeQuantiles = new MutableQuantiles[len];
    numEditLogLoadedQuantiles = new MutableQuantiles[len];
    editLogTailIntervalQuantiles = new MutableQuantiles[len];

    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      syncsQuantiles[i] = registry.newQuantiles(
          "syncs" + interval + "s",
          "Journal syncs", "ops", "latency", interval);
      numTransactionsBatchedInSync[i] = registry.newQuantiles(
          "numTransactionsBatchedInSync" + interval + "s",
          "Number of Transactions batched in sync", "ops",
          "count", interval);
      storageBlockReportQuantiles[i] = registry.newQuantiles(
          "storageBlockReport" + interval + "s",
          "Storage block report", "ops", "latency", interval);
      cacheReportQuantiles[i] = registry.newQuantiles(
          "cacheReport" + interval + "s",
          "Cache report", "ops", "latency", interval);
      generateEDEKTimeQuantiles[i] = registry.newQuantiles(
          "generateEDEKTime" + interval + "s",
          "Generate EDEK time", "ops", "latency", interval);
      warmUpEDEKTimeQuantiles[i] = registry.newQuantiles(
          "warmupEDEKTime" + interval + "s",
          "Warm up EDEK time", "ops", "latency", interval);
      resourceCheckTimeQuantiles[i] = registry.newQuantiles(
          "resourceCheckTime" + interval + "s",
          "resource check time", "ops", "latency", interval);
      editLogTailTimeQuantiles[i] = registry.newQuantiles(
          "editLogTailTime" + interval + "s",
          "Edit log tailing time", "ops", "latency", interval);
      editLogFetchTimeQuantiles[i] = registry.newQuantiles(
          "editLogFetchTime" + interval + "s",
          "Edit log fetch time", "ops", "latency", interval);
      numEditLogLoadedQuantiles[i] = registry.newQuantiles(
          "numEditLogLoaded" + interval + "s",
          "Number of edits loaded", "ops", "count", interval);
      editLogTailIntervalQuantiles[i] = registry.newQuantiles(
          "editLogTailInterval" + interval + "s",
          "Edit log tailing interval", "ops", "latency", interval);
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

  public void setBlockOpsQueued(int size) {
    blockOpsQueued.set(size);
  }

  public void addBlockOpsBatched(int count) {
    blockOpsBatched.incr(count);
  }

  public void setPendingEditsCount(int size) {
    pendingEditsCount.set(size);
  }

  public void addTransaction(long latency) {
    transactions.add(latency);
  }

  public void incrTransactionsBatchedInSync(long count) {
    transactionsBatchedInSync.incr(count);
    for (MutableQuantiles q : numTransactionsBatchedInSync) {
      q.add(count);
    }
  }

  public void incSuccessfulReReplications() {
    successfulReReplications.incr();
  }

  public void incNumTimesReReplicationNotScheduled() {
    numTimesReReplicationNotScheduled.incr();
  }

  public void incTimeoutReReplications() {
    timeoutReReplications.incr();
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

  public void addStorageBlockReport(long latency) {
    storageBlockReport.add(latency);
    for (MutableQuantiles q : storageBlockReportQuantiles) {
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

  public void addGetAliasMap(long latency) {
    getAliasMap.add(latency);
  }

  public void addPutImage(long latency) {
    putImage.add(latency);
  }

  public void addGenerateEDEKTime(long latency) {
    generateEDEKTime.add(latency);
    for (MutableQuantiles q : generateEDEKTimeQuantiles) {
      q.add(latency);
    }
  }

  public void addWarmUpEDEKTime(long latency) {
    warmUpEDEKTime.add(latency);
    for (MutableQuantiles q : warmUpEDEKTimeQuantiles) {
      q.add(latency);
    }
  }

  public void addResourceCheckTime(long latency) {
    resourceCheckTime.add(latency);
    for (MutableQuantiles q : resourceCheckTimeQuantiles) {
      q.add(latency);
    }
  }

  public void addEditLogTailTime(long elapsed) {
    editLogTailTime.add(elapsed);
    for (MutableQuantiles q : editLogTailTimeQuantiles) {
      q.add(elapsed);
    }
  }

  public void addEditLogFetchTime(long elapsed) {
    editLogFetchTime.add(elapsed);
    for (MutableQuantiles q : editLogFetchTimeQuantiles) {
      q.add(elapsed);
    }
  }

  public void addNumEditLogLoaded(long loaded) {
    numEditLogLoaded.add(loaded);
    for (MutableQuantiles q : numEditLogLoadedQuantiles) {
      q.add(loaded);
    }
  }

  public void addEditLogTailInterval(long elapsed) {
    editLogTailInterval.add(elapsed);
    for (MutableQuantiles q : editLogTailIntervalQuantiles) {
      q.add(elapsed);
    }
  }
}
