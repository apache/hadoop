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

  NameNodeInstrumentation(Configuration conf) {
    sessionId = conf.get("session.id");
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
    return ms.register("NameNode", "NameNode metrics",
                       new NameNodeInstrumentation(conf));
  }

  //@Override
  public void shutdown() {
    // metrics system shutdown would suffice
  }

  //@override
  public final void incrNumGetBlockLocations() {
    numGetBlockLocations.incr();
  }

  //@Override
  public final void incrNumFilesCreated() {
    numFilesCreated.incr();
  }

  //@Override
  public final void incrNumCreateFileOps() {
    numCreateFileOps.incr();
  }

  //@Override
  public final void incrNumFilesAppended() {
    numFilesAppended.incr();
  }

  //@Override
  public final void incrNumAddBlockOps() {
    numAddBlockOps.incr();
  }

  //@Override
  public final void incrNumFilesRenamed() {
    numFilesRenamed.incr();
  }

  //@Override
  public void incrFilesDeleted(int delta) {
    numFilesDeleted.incr(delta);
  }

  //@Override
  public final void incrNumDeleteFileOps() {
    numDeleteFileOps.incr();
  }

  //@Override
  public final void incrNumGetListingOps() {
    numGetListingOps.incr();
  }

  //@Override
  public final void incrNumFilesInGetListingOps(int delta) {
    numFilesInGetListingOps.incr(delta);
  }

  //@Override
  public final void incrNumFileInfoOps() {
    numFileInfoOps.incr();
  }

  //@Override
  public final void addTransaction(long latency) {
    transactions.add(latency);
  }

  //@Override
  public final void incrTransactionsBatchedInSync() {
    transactionsBatchedInSync.incr();
  }

  //@Override
  public final void addSync(long elapsed) {
    syncs.add(elapsed);
  }

  //@Override
  public final void setFsImageLoadTime(long elapsed) {
    fsImageLoadTime.set((int) elapsed);
  }

  //@Override
  public final void addBlockReport(long latency) {
    blockReport.add(latency);
  }

  //@Override
  public final void setSafeModeTime(long elapsed) {
    safeModeTime.set((int) elapsed);
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

}
