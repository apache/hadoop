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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.source.JvmMetricsSource;

public class NameNodeInstrumentation implements MetricsSource {
  static final Log LOG = LogFactory.getLog(NameNodeInstrumentation.class);

  static final String FSNAMESYSTEM_RECORD_NAME = "FSNamesystem";
  final String sessionId;
  final MetricsRegistry registry = new MetricsRegistry("namenode");
  final MetricMutableCounterInt numFilesCreated =
      registry.newCounter("FilesCreated", "", 0);
  final MetricMutableCounterInt numFilesAppended =
      registry.newCounter("FilesAppended", "", 0);
  final MetricMutableCounterInt numGetBlockLocations =
      registry.newCounter("GetBlockLocations", "", 0);
  final MetricMutableCounterInt numFilesRenamed =
      registry.newCounter("FilesRenamed", "", 0);
  final MetricMutableCounterInt numGetListingOps =
      registry.newCounter("GetListingOps", "", 0);
  final MetricMutableCounterInt numCreateFileOps =
      registry.newCounter("CreateFileOps", "", 0);
  final MetricMutableCounterInt numFilesDeleted =
      registry.newCounter("FilesDeleted", "Files deleted (inc. rename)", 0);
  final MetricMutableCounterInt numDeleteFileOps =
      registry.newCounter("DeleteFileOps", "", 0);
  final MetricMutableCounterInt numFileInfoOps =
      registry.newCounter("FileInfoOps", "", 0);
  final MetricMutableCounterInt numAddBlockOps =
      registry.newCounter("AddBlockOps", "", 0);
  final MetricMutableStat transactions = registry.newStat("Transactions");
  final MetricMutableStat syncs = registry.newStat("Syncs");
  final MetricMutableCounterInt transactionsBatchedInSync =
      registry.newCounter("JournalTransactionsBatchedInSync", "", 0);
  final MetricMutableStat blockReport = registry.newStat("blockReport");
  final MetricMutableGaugeInt safeModeTime =
      registry.newGauge("SafemodeTime", "Time spent in safe mode", 0);
  final MetricMutableGaugeInt fsImageLoadTime =
      registry.newGauge("fsImageLoadTime", "", 0);
  final MetricMutableCounterInt numFilesInGetListingOps =
      registry.newCounter("FilesInGetListingOps", "", 0);

  final MetricsSource fsNamesystemMetrics;

  NameNodeInstrumentation(Configuration conf) {
    sessionId = conf.get("session.id");
    fsNamesystemMetrics = new FSNamesystemMetrics();
    JvmMetricsSource.create("NameNode", sessionId);
    registry.setContext("dfs").tag("sessionId", "", sessionId);
  }

  public static NameNodeInstrumentation create(Configuration conf) {
    return create(conf, DefaultMetricsSystem.INSTANCE);
  }

  /**
   * Create a v2 metrics instrumentation
   * @param conf  the configuration object
   * @param ms  the metrics system instance
   * @return a metrics
   */
  public static NameNodeInstrumentation create(Configuration conf,
                                               MetricsSystem ms) {
    NameNodeInstrumentation v2 = new NameNodeInstrumentation(conf);
    ms.register("FSNamesystemState", "FS name system state",
                v2.fsNamesystemMetrics());
    return ms.register("NameNode", "NameNode metrics", v2);
  }

  public MetricsSource fsNamesystemMetrics() {
    return fsNamesystemMetrics;
  }

  public void shutdown() {
    // metrics system shutdown would suffice
  }


  public final void incrNumGetBlockLocations() {
    numGetBlockLocations.incr();
  }

  
  public final void incrNumFilesCreated() {
    numFilesCreated.incr();
  }

  
  public final void incrNumCreateFileOps() {
    numCreateFileOps.incr();
  }

  
  public final void incrNumFilesAppended() {
    numFilesAppended.incr();
  }

  
  public final void incrNumAddBlockOps() {
    numAddBlockOps.incr();
  }

  
  public final void incrNumFilesRenamed() {
    numFilesRenamed.incr();
  }

  
  public void incrFilesDeleted(int delta) {
    numFilesDeleted.incr(delta);
  }

  
  public final void incrNumDeleteFileOps() {
    numDeleteFileOps.incr();
  }

  
  public final void incrNumGetListingOps() {
    numGetListingOps.incr();
  }

  
  public final void incrNumFilesInGetListingOps(int delta) {
    numFilesInGetListingOps.incr(delta);
  }

  
  public final void incrNumFileInfoOps() {
    numFileInfoOps.incr();
  }

  
  public final void addTransaction(long latency) {
    transactions.add(latency);
  }

  
  public final void incrTransactionsBatchedInSync() {
    transactionsBatchedInSync.incr();
  }

  
  public final void addSync(long elapsed) {
    syncs.add(elapsed);
  }

  
  public final void setFsImageLoadTime(long elapsed) {
    fsImageLoadTime.set((int) elapsed);
  }

  
  public final void addBlockReport(long latency) {
    blockReport.add(latency);
  }

  
  public final void setSafeModeTime(long elapsed) {
    safeModeTime.set((int) elapsed);
  }

  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

  private static int roundBytesToGBytes(long bytes) {
    return Math.round(((float)bytes/(1024 * 1024 * 1024)));
  }

  private class FSNamesystemMetrics implements MetricsSource {

    public void getMetrics(MetricsBuilder builder, boolean all) {
      // Since fsnamesystem metrics are poll based, we just put them here
      // to avoid an extra copy per metric.
      FSNamesystem fsNamesystem = FSNamesystem.getFSNamesystem();
      if (fsNamesystem == null) {
        LOG.debug("FSNamesystem not ready yet!");
        return;
      }
      builder.addRecord(FSNAMESYSTEM_RECORD_NAME).setContext("dfs")
        .tag("sessionId", "", sessionId)
        .addGauge("FilesTotal", "", fsNamesystem.getFilesTotal())
        .addGauge("BlocksTotal", "", fsNamesystem.getBlocksTotal())
        .addGauge("CapacityTotalGB", "",
                  roundBytesToGBytes(fsNamesystem.getCapacityTotal()))
        .addGauge("CapacityUsedGB", "",
                  roundBytesToGBytes(fsNamesystem.getCapacityUsed()))
        .addGauge("CapacityRemainingGB", "",
                  roundBytesToGBytes(fsNamesystem.getCapacityRemaining()))
        .addGauge("TotalLoad", "", fsNamesystem.getTotalLoad())
        .addGauge("CorruptBlocks", "", fsNamesystem.getCorruptReplicaBlocks())
        .addGauge("ExcessBlocks", "", fsNamesystem.getExcessBlocks())
        .addGauge("PendingDeletionBlocks", "",
                  fsNamesystem.getPendingDeletionBlocks())
        .addGauge("PendingReplicationBlocks", "",
                  fsNamesystem.getPendingReplicationBlocks())
        .addGauge("UnderReplicatedBlocks", "",
                  fsNamesystem.getUnderReplicatedBlocks())
        .addGauge("ScheduledReplicationBlocks", "",
                  fsNamesystem.getScheduledReplicationBlocks())
        .addGauge("MissingBlocks", "", fsNamesystem.getMissingBlocksCount())
        .addGauge("BlockCapacity", "", fsNamesystem.getBlockCapacity());
    }

  }

}
