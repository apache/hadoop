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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.source.JvmMetricsSource;

public class DataNodeInstrumentation implements MetricsSource {

  final MetricsRegistry registry = new MetricsRegistry("datanode");

  final MetricMutableCounterLong bytesWritten =
      registry.newCounter("bytes_written", "", 0L);
  final MetricMutableCounterLong bytesRead =
      registry.newCounter("bytes_read", "", 0L);
  final MetricMutableCounterInt blocksWritten =
      registry.newCounter("blocks_written", "", 0);
  final MetricMutableCounterInt blocksRead =
      registry.newCounter("blocks_read", "", 0);
  final MetricMutableCounterInt blocksReplicated =
      registry.newCounter("blocks_replicated", "", 0);
  final MetricMutableCounterInt blocksRemoved =
      registry.newCounter("blocks_removed", "", 0);
  final MetricMutableCounterInt blocksVerified =
      registry.newCounter("blocks_verified", "", 0);
  final MetricMutableCounterInt blockVerificationFailures =
      registry.newCounter("block_verification_failures", "", 0);
  final MetricMutableCounterInt blocksGetLocalPathInfo = 
      registry.newCounter("blocks_get_local_pathinfo", "", 0);

  final MetricMutableCounterInt readsFromLocalClient =
      registry.newCounter("reads_from_local_client", "", 0);
  final MetricMutableCounterInt readsFromRemoteClient =
      registry.newCounter("reads_from_remote_client", "", 0);
  final MetricMutableCounterInt writesFromLocalClient =
      registry.newCounter("writes_from_local_client", "", 0);
  final MetricMutableCounterInt writesFromRemoteClient =
      registry.newCounter("writes_from_remote_client", "", 0);

  final MetricMutableStat readBlockOp = registry.newStat("readBlockOp");
  final MetricMutableStat writeBlockOp = registry.newStat("writeBlockOp");
  final MetricMutableStat blockChecksumOp = registry.newStat("blockChecksumOp");
  final MetricMutableStat copyBlockOp = registry.newStat("copyBlockOp");
  final MetricMutableStat replaceBlockOp = registry.newStat("replaceBlockOp");
  final MetricMutableStat heartbeats = registry.newStat("heartBeats");
  final MetricMutableStat blockReports = registry.newStat("blockReports");


  public DataNodeInstrumentation(Configuration conf, String storageId) {
    String sessionId = conf.get("session.id");
    JvmMetricsSource.create("DataNode", sessionId);
    registry.setContext("dfs").tag("sessionId", "", sessionId);
  }

  //@Override
  public void shutdown() {
    // metrics system shutdown would suffice
  }

  //@Override
  public void resetAllMinMax() {
    readBlockOp.resetMinMax();
    writeBlockOp.resetMinMax();
    blockChecksumOp.resetMinMax();
    copyBlockOp.resetMinMax();
    replaceBlockOp.resetMinMax();
    heartbeats.resetMinMax();
    blockReports.resetMinMax();
  }

  //@Override
  public void addHeartBeat(long latency) {
    heartbeats.add(latency);
  }

  //@Override
  public void addBlockReport(long latency) {
    blockReports.add(latency);
  }

  //@Override
  public void incrBlocksReplicated(int delta) {
    blocksReplicated.incr(delta);
  }

  //@Override
  public void incrBlocksWritten() {
    blocksWritten.incr();
  }

  //@Override
  public void incrBlocksRemoved(int delta) {
    blocksRemoved.incr(delta);
  }

  //@Override
  public void incrBytesWritten(int delta) {
    bytesWritten.incr(delta);
  }

  //@Override
  public void incrBlockVerificationFailures() {
    blockVerificationFailures.incr();
  }

  //@Override
  public void incrBlocksVerified() {
    blocksVerified.incr();
  }

  //@Override
  public void incrBlocksGetLocalPathInfo() {
    blocksGetLocalPathInfo.incr();
  }

  //@Override
  public void addReadBlockOp(long latency) {
    readBlockOp.add(latency);
  }

  //@Override
  public void incrReadsFromLocalClient() {
    readsFromLocalClient.incr();
  }

  //@Override
  public void incrReadsFromRemoteClient() {
    readsFromRemoteClient.incr();
  }

  //@Override
  public void addWriteBlockOp(long latency) {
    writeBlockOp.add(latency);
  }

  //@Override
  public void incrWritesFromLocalClient() {
    writesFromLocalClient.incr();
  }

  //@Override
  public void incrWritesFromRemoteClient() {
    writesFromRemoteClient.incr();
  }

  //@Override
  public void addReplaceBlockOp(long latency) {
    replaceBlockOp.add(latency);
  }

  //@Override
  public void addCopyBlockOp(long latency) {
    copyBlockOp.add(latency);
  }

  //@Override
  public void addBlockChecksumOp(long latency) {
    blockChecksumOp.add(latency);
  }

  //@Override
  public void incrBytesRead(int delta) {
    bytesRead.incr(delta);
  }

  //@Override
  public void incrBlocksRead() {
    blocksRead.incr();
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

  public static DataNodeInstrumentation create(Configuration conf,
                                               String storageID) {
    return create(conf, storageID, DefaultMetricsSystem.INSTANCE);
  }

  public static DataNodeInstrumentation create(Configuration conf,
                                               String storageID,
                                               MetricsSystem ms) {
    return ms.register("DataNode", "DataNode metrics",
                       new DataNodeInstrumentation(conf, storageID));
  }

}
