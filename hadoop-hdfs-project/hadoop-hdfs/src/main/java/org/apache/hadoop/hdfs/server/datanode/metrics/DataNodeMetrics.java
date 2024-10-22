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
import org.apache.hadoop.hdfs.server.protocol.DataNodeUsageReport;
import org.apache.hadoop.hdfs.server.protocol.DataNodeUsageReportUtil;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.metrics2.source.JvmMetrics;

import java.util.concurrent.ThreadLocalRandom;

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
  @Metric("Milliseconds spent writing")
  MutableCounterLong totalWriteTime;
  @Metric MutableCounterLong bytesRead;
  @Metric("Milliseconds spent reading")
  MutableCounterLong totalReadTime;
  @Metric private MutableRate readTransferRate;
  final private MutableQuantiles[] readTransferRateQuantiles;
  @Metric MutableCounterLong blocksWritten;
  @Metric MutableCounterLong blocksRead;
  @Metric MutableCounterLong blocksReplicated;
  @Metric private MutableCounterLong blocksReplicatedViaHardlink;
  @Metric MutableCounterLong blocksRemoved;
  @Metric MutableCounterLong blocksVerified;
  @Metric MutableCounterLong blockVerificationFailures;
  @Metric MutableCounterLong blocksCached;
  @Metric MutableCounterLong blocksUncached;
  @Metric MutableCounterLong readsFromLocalClient;
  @Metric MutableCounterLong readsFromRemoteClient;
  @Metric MutableCounterLong writesFromLocalClient;
  @Metric MutableCounterLong writesFromRemoteClient;
  @Metric MutableCounterLong blocksGetLocalPathInfo;
  @Metric("Bytes read by remote client")
  MutableCounterLong remoteBytesRead;
  @Metric("Bytes written by remote client")
  MutableCounterLong remoteBytesWritten;

  // RamDisk metrics on read/write
  @Metric MutableCounterLong ramDiskBlocksWrite;
  @Metric MutableCounterLong ramDiskBlocksWriteFallback;
  @Metric MutableCounterLong ramDiskBytesWrite;
  @Metric MutableCounterLong ramDiskBlocksReadHits;

  // RamDisk metrics on eviction
  @Metric MutableCounterLong ramDiskBlocksEvicted;
  @Metric MutableCounterLong ramDiskBlocksEvictedWithoutRead;
  @Metric MutableRate        ramDiskBlocksEvictionWindowMs;
  final MutableQuantiles[]   ramDiskBlocksEvictionWindowMsQuantiles;


  // RamDisk metrics on lazy persist
  @Metric MutableCounterLong ramDiskBlocksLazyPersisted;
  @Metric MutableCounterLong ramDiskBlocksDeletedBeforeLazyPersisted;
  @Metric MutableCounterLong ramDiskBytesLazyPersisted;
  @Metric MutableRate        ramDiskBlocksLazyPersistWindowMs;
  final MutableQuantiles[]   ramDiskBlocksLazyPersistWindowMsQuantiles;

  @Metric MutableCounterLong fsyncCount;

  @Metric MutableCounterLong volumeFailures;

  @Metric("Count of network errors on the datanode")
  MutableCounterLong datanodeNetworkErrors;

  @Metric("Count of active dataNode xceivers")
  private MutableGaugeInt dataNodeActiveXceiversCount;

  @Metric("Count of read active dataNode xceivers")
  private MutableGaugeInt dataNodeReadActiveXceiversCount;

  @Metric("Count of write active dataNode xceivers")
  private MutableGaugeInt dataNodeWriteActiveXceiversCount;

  @Metric("Count of active DataNode packetResponder")
  private MutableGaugeInt dataNodePacketResponderCount;

  @Metric("Count of active DataNode block recovery worker")
  private MutableGaugeInt dataNodeBlockRecoveryWorkerCount;

  @Metric MutableRate readBlockOp;
  @Metric MutableRate writeBlockOp;
  @Metric MutableRate blockChecksumOp;
  @Metric MutableRate copyBlockOp;
  @Metric private MutableRate copyBlockCrossNamespaceOp;
  @Metric MutableRate replaceBlockOp;
  @Metric MutableRate heartbeats;
  @Metric MutableRate heartbeatsTotal;
  @Metric MutableRate lifelines;
  @Metric MutableRate blockReports;
  @Metric private MutableRate blockReportsCreateCostMills;
  @Metric MutableRate incrementalBlockReports;
  @Metric MutableRate cacheReports;
  @Metric MutableRate packetAckRoundTripTimeNanos;
  final MutableQuantiles[] packetAckRoundTripTimeNanosQuantiles;
  
  @Metric MutableRate flushNanos;
  final MutableQuantiles[] flushNanosQuantiles;
  
  @Metric MutableRate fsyncNanos;
  final MutableQuantiles[] fsyncNanosQuantiles;
  
  @Metric MutableRate sendDataPacketBlockedOnNetworkNanos;
  final MutableQuantiles[] sendDataPacketBlockedOnNetworkNanosQuantiles;
  @Metric MutableRate sendDataPacketTransferNanos;
  final MutableQuantiles[] sendDataPacketTransferNanosQuantiles;

  @Metric("Count of blocks in pending IBR")
  private MutableGaugeLong blocksInPendingIBR;
  @Metric("Count of blocks at receiving status in pending IBR")
  private MutableGaugeLong blocksReceivingInPendingIBR;
  @Metric("Count of blocks at received status in pending IBR")
  private MutableGaugeLong blocksReceivedInPendingIBR;
  @Metric("Count of blocks at deleted status in pending IBR")
  private MutableGaugeLong blocksDeletedInPendingIBR;
  @Metric("Count of erasure coding reconstruction tasks")
  MutableCounterLong ecReconstructionTasks;
  @Metric("Count of erasure coding failed reconstruction tasks")
  MutableCounterLong ecFailedReconstructionTasks;
  @Metric("Count of erasure coding invalidated reconstruction tasks")
  private MutableCounterLong ecInvalidReconstructionTasks;
  @Metric("Nanoseconds spent by decoding tasks")
  MutableCounterLong ecDecodingTimeNanos;
  @Metric("Bytes read by erasure coding worker")
  MutableCounterLong ecReconstructionBytesRead;
  @Metric("Bytes written by erasure coding worker")
  MutableCounterLong ecReconstructionBytesWritten;
  @Metric("Bytes remote read by erasure coding worker")
  MutableCounterLong ecReconstructionRemoteBytesRead;
  @Metric("Milliseconds spent on read by erasure coding worker")
  private MutableCounterLong ecReconstructionReadTimeMillis;
  @Metric("Milliseconds spent on decoding by erasure coding worker")
  private MutableCounterLong ecReconstructionDecodingTimeMillis;
  @Metric("Milliseconds spent on write by erasure coding worker")
  private MutableCounterLong ecReconstructionWriteTimeMillis;
  @Metric("Milliseconds spent on validating by erasure coding worker")
  private MutableCounterLong ecReconstructionValidateTimeMillis;
  @Metric("Sum of all BPServiceActors command queue length")
  private MutableCounterLong sumOfActorCommandQueueLength;
  @Metric("Num of processed commands of all BPServiceActors")
  private MutableCounterLong numProcessedCommands;
  @Metric("Rate of processed commands of all BPServiceActors")
  private MutableRate processedCommandsOp;
  @Metric("Number of blocks in IBRs that failed due to null storage")
  private MutableCounterLong nullStorageBlockReports;

  // FsDatasetImpl local file process metrics.
  @Metric private MutableRate createRbwOp;
  @Metric private MutableRate recoverRbwOp;
  @Metric private MutableRate convertTemporaryToRbwOp;
  @Metric private MutableRate createTemporaryOp;
  @Metric private MutableRate finalizeBlockOp;
  @Metric private MutableRate unfinalizeBlockOp;
  @Metric private MutableRate checkAndUpdateOp;
  @Metric private MutableRate updateReplicaUnderRecoveryOp;

  @Metric MutableCounterLong packetsReceived;
  @Metric MutableCounterLong packetsSlowWriteToMirror;
  @Metric MutableCounterLong packetsSlowWriteToDisk;
  @Metric MutableCounterLong packetsSlowWriteToOsCache;
  @Metric private MutableCounterLong slowFlushOrSyncCount;
  @Metric private MutableCounterLong slowAckToUpstreamCount;

  @Metric("Number of replaceBlock ops between" +
      " storage types on same host with local copy")
  private MutableCounterLong replaceBlockOpOnSameHost;
  @Metric("Number of replaceBlock ops between" +
      " storage types on same disk mount with same disk tiering feature")
  private MutableCounterLong replaceBlockOpOnSameMount;
  @Metric("Number of replaceBlock ops to another node")
  private MutableCounterLong replaceBlockOpToOtherHost;

  final MetricsRegistry registry = new MetricsRegistry("datanode");
  @Metric("Milliseconds spent on calling NN rpc")
  private MutableRatesWithAggregation
      nnRpcLatency = registry.newRatesWithAggregation("nnRpcLatency");

  final String name;
  JvmMetrics jvmMetrics = null;
  private DataNodeUsageReportUtil dnUsageReportUtil;

  public DataNodeMetrics(String name, String sessionId, int[] intervals,
      final JvmMetrics jvmMetrics) {
    this.name = name;
    this.jvmMetrics = jvmMetrics;    
    registry.tag(SessionId, sessionId);
    
    final int len = intervals.length;
    dnUsageReportUtil = new DataNodeUsageReportUtil();
    packetAckRoundTripTimeNanosQuantiles = new MutableQuantiles[len];
    flushNanosQuantiles = new MutableQuantiles[len];
    fsyncNanosQuantiles = new MutableQuantiles[len];
    sendDataPacketBlockedOnNetworkNanosQuantiles = new MutableQuantiles[len];
    sendDataPacketTransferNanosQuantiles = new MutableQuantiles[len];
    ramDiskBlocksEvictionWindowMsQuantiles = new MutableQuantiles[len];
    ramDiskBlocksLazyPersistWindowMsQuantiles = new MutableQuantiles[len];
    readTransferRateQuantiles = new MutableQuantiles[len];

    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      packetAckRoundTripTimeNanosQuantiles[i] = registry.newQuantiles(
          "packetAckRoundTripTimeNanos" + interval + "s",
          "Packet Ack RTT in ns", "ops", "latency", interval);
      flushNanosQuantiles[i] = registry.newQuantiles(
          "flushNanos" + interval + "s", 
          "Disk flush latency in ns", "ops", "latency", interval);
      fsyncNanosQuantiles[i] = registry.newQuantiles(
          "fsyncNanos" + interval + "s", "Disk fsync latency in ns", 
          "ops", "latency", interval);
      sendDataPacketBlockedOnNetworkNanosQuantiles[i] = registry.newQuantiles(
          "sendDataPacketBlockedOnNetworkNanos" + interval + "s", 
          "Time blocked on network while sending a packet in ns",
          "ops", "latency", interval);
      sendDataPacketTransferNanosQuantiles[i] = registry.newQuantiles(
          "sendDataPacketTransferNanos" + interval + "s", 
          "Time reading from disk and writing to network while sending " +
          "a packet in ns", "ops", "latency", interval);
      ramDiskBlocksEvictionWindowMsQuantiles[i] = registry.newQuantiles(
          "ramDiskBlocksEvictionWindows" + interval + "s",
          "Time between the RamDisk block write and eviction in ms",
          "ops", "latency", interval);
      ramDiskBlocksLazyPersistWindowMsQuantiles[i] = registry.newQuantiles(
          "ramDiskBlocksLazyPersistWindows" + interval + "s",
          "Time between the RamDisk block write and disk persist in ms",
          "ops", "latency", interval);
      readTransferRateQuantiles[i] = registry.newInverseQuantiles(
          "readTransferRate" + interval + "s",
          "Rate at which bytes are read from datanode calculated in bytes per second",
          "ops", "rate", interval);
    }
  }

  public static DataNodeMetrics create(Configuration conf, String dnName) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create("DataNode", sessionId, ms);
    String name = "DataNodeActivity-"+ (dnName.isEmpty()
        ? "UndefinedDataNodeName"+ ThreadLocalRandom.current().nextInt()
            : dnName.replace(':', '-'));

    // Percentile measurement is off by default, by watching no intervals
    int[] intervals = 
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    
    return ms.register(name, null, new DataNodeMetrics(name, sessionId,
        intervals, jm));
  }

  public String name() { return name; }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  public void addHeartbeat(long latency, String rpcMetricSuffix) {
    heartbeats.add(latency);
    if (rpcMetricSuffix != null) {
      nnRpcLatency.add("HeartbeatsFor" + rpcMetricSuffix, latency);
    }
  }

  public void addHeartbeatTotal(long latency, String rpcMetricSuffix) {
    heartbeatsTotal.add(latency);
    if (rpcMetricSuffix != null) {
      nnRpcLatency.add("HeartbeatsTotalFor" + rpcMetricSuffix, latency);
    }
  }

  public void addLifeline(long latency, String rpcMetricSuffix) {
    lifelines.add(latency);
    if (rpcMetricSuffix != null) {
      nnRpcLatency.add("LifelinesFor" + rpcMetricSuffix, latency);
    }
  }

  public void addBlockReport(long latency, String rpcMetricSuffix) {
    blockReports.add(latency);
    if (rpcMetricSuffix != null) {
      nnRpcLatency.add("BlockReportsFor" + rpcMetricSuffix, latency);
    }
  }

  public void addBlockReportCreateCost(long latency) {
    blockReportsCreateCostMills.add(latency);
  }

  public void addIncrementalBlockReport(long latency,
      String rpcMetricSuffix) {
    incrementalBlockReports.add(latency);
    if (rpcMetricSuffix != null) {
      nnRpcLatency.add("IncrementalBlockReportsFor" + rpcMetricSuffix, latency);
    }
  }

  public void addReadTransferRate(long readTransferRate) {
    this.readTransferRate.add(readTransferRate);
    for (MutableQuantiles q : readTransferRateQuantiles) {
      q.add(readTransferRate);
    }
  }

  public void addCacheReport(long latency) {
    cacheReports.add(latency);
  }

  public void incrBlocksReplicated() {
    blocksReplicated.incr();
  }

  public long getBlocksReplicated() {
    return blocksReplicated.value();
  }

  public void incrBlocksReplicatedViaHardlink() {
    blocksReplicatedViaHardlink.incr();
  }

  public long getBlocksReplicatedViaHardlink() {
    return blocksReplicatedViaHardlink.value();
  }

  public void incrBlocksWritten() {
    blocksWritten.incr();
  }

  public void incrBlocksRemoved(int delta) {
    blocksRemoved.incr(delta);
  }

  public long getBlocksRemoved() {
    return blocksRemoved.value();
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


  public void incrBlocksCached(int delta) {
    blocksCached.incr(delta);
  }

  public void incrBlocksUncached(int delta) {
    blocksUncached.incr(delta);
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

  public void addCopyBlockCrossNamespaceOp(long latency) {
    copyBlockCrossNamespaceOp.add(latency);
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

  public void incrFsyncCount() {
    fsyncCount.incr();
  }

  public void incrTotalWriteTime(long timeTaken) {
    totalWriteTime.incr(timeTaken);
  }

  public void incrTotalReadTime(long timeTaken) {
    totalReadTime.incr(timeTaken);
  }


  public void addPacketAckRoundTripTimeNanos(long latencyNanos) {
    packetAckRoundTripTimeNanos.add(latencyNanos);
    for (MutableQuantiles q : packetAckRoundTripTimeNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void addFlushNanos(long latencyNanos) {
    flushNanos.add(latencyNanos);
    for (MutableQuantiles q : flushNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void addFsyncNanos(long latencyNanos) {
    fsyncNanos.add(latencyNanos);
    for (MutableQuantiles q : fsyncNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public void incrWritesFromClient(boolean local, long size) {
    if(local) {
      writesFromLocalClient.incr();
    } else {
      writesFromRemoteClient.incr();
      remoteBytesWritten.incr(size);
    }
  }

  public void incrReadsFromClient(boolean local, long size) {

    if (local) {
      readsFromLocalClient.incr();
    } else {
      readsFromRemoteClient.incr();
      remoteBytesRead.incr(size);
    }
  }

  public void incrVolumeFailures(int size) {
    volumeFailures.incr(size);
  }

  public void incrSlowFlushOrSyncCount() {
    slowFlushOrSyncCount.incr();
  }

  public void incrSlowAckToUpstreamCount() {
    slowAckToUpstreamCount.incr();
  }

  public void incrDatanodeNetworkErrors() {
    datanodeNetworkErrors.incr();
  }

  /** Increment for getBlockLocalPathInfo calls */
  public void incrBlocksGetLocalPathInfo() {
    blocksGetLocalPathInfo.incr();
  }

  public void addSendDataPacketBlockedOnNetworkNanos(long latencyNanos) {
    sendDataPacketBlockedOnNetworkNanos.add(latencyNanos);
    for (MutableQuantiles q : sendDataPacketBlockedOnNetworkNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void addSendDataPacketTransferNanos(long latencyNanos) {
    sendDataPacketTransferNanos.add(latencyNanos);
    for (MutableQuantiles q : sendDataPacketTransferNanosQuantiles) {
      q.add(latencyNanos);
    }
  }

  public void incrRamDiskBlocksWrite() {
    ramDiskBlocksWrite.incr();
  }

  public void incrRamDiskBlocksWriteFallback() {
    ramDiskBlocksWriteFallback.incr();
  }

  public void addRamDiskBytesWrite(long bytes) {
    ramDiskBytesWrite.incr(bytes);
  }

  public void incrRamDiskBlocksReadHits() {
    ramDiskBlocksReadHits.incr();
  }

  public void incrRamDiskBlocksEvicted() {
    ramDiskBlocksEvicted.incr();
  }

  public void incrRamDiskBlocksEvictedWithoutRead() {
    ramDiskBlocksEvictedWithoutRead.incr();
  }

  public void addRamDiskBlocksEvictionWindowMs(long latencyMs) {
    ramDiskBlocksEvictionWindowMs.add(latencyMs);
    for (MutableQuantiles q : ramDiskBlocksEvictionWindowMsQuantiles) {
      q.add(latencyMs);
    }
  }

  public void incrRamDiskBlocksLazyPersisted() {
    ramDiskBlocksLazyPersisted.incr();
  }

  public void incrRamDiskBlocksDeletedBeforeLazyPersisted() {
    ramDiskBlocksDeletedBeforeLazyPersisted.incr();
  }

  public void incrRamDiskBytesLazyPersisted(long bytes) {
    ramDiskBytesLazyPersisted.incr(bytes);
  }

  public void addRamDiskBlocksLazyPersistWindowMs(long latencyMs) {
    ramDiskBlocksLazyPersistWindowMs.add(latencyMs);
    for (MutableQuantiles q : ramDiskBlocksLazyPersistWindowMsQuantiles) {
      q.add(latencyMs);
    }
  }

  /**
   * Resets blocks in pending IBR to zero.
   */
  public void resetBlocksInPendingIBR() {
    blocksInPendingIBR.set(0);
    blocksReceivingInPendingIBR.set(0);
    blocksReceivedInPendingIBR.set(0);
    blocksDeletedInPendingIBR.set(0);
  }

  public void incrBlocksInPendingIBR() {
    blocksInPendingIBR.incr();
  }

  public void incrBlocksReceivingInPendingIBR() {
    blocksReceivingInPendingIBR.incr();
  }

  public void incrBlocksReceivedInPendingIBR() {
    blocksReceivedInPendingIBR.incr();
  }

  public void incrBlocksDeletedInPendingIBR() {
    blocksDeletedInPendingIBR.incr();
  }

  public void incrECReconstructionTasks() {
    ecReconstructionTasks.incr();
  }

  public void incrECFailedReconstructionTasks() {
    ecFailedReconstructionTasks.incr();
  }

  public void incrECInvalidReconstructionTasks() {
    ecInvalidReconstructionTasks.incr();
  }

  public long getECInvalidReconstructionTasks() {
    return ecInvalidReconstructionTasks.value();
  }

  public void incrDataNodeActiveXceiversCount() {
    dataNodeActiveXceiversCount.incr();
  }

  public void decrDataNodeActiveXceiversCount() {
    dataNodeActiveXceiversCount.decr();
  }

  public void setDataNodeActiveXceiversCount(int value) {
    dataNodeActiveXceiversCount.set(value);
  }

  public int getDataNodeActiveXceiverCount() {
    return dataNodeActiveXceiversCount.value();
  }

  public void incrDataNodeReadActiveXceiversCount(){
    dataNodeReadActiveXceiversCount.incr();
  }

  public void decrDataNodeReadActiveXceiversCount(){
    dataNodeReadActiveXceiversCount.decr();
  }

  public void setDataNodeReadActiveXceiversCount(int value){
    dataNodeReadActiveXceiversCount.set(value);
  }

  public void incrDataNodeWriteActiveXceiversCount(){
    dataNodeWriteActiveXceiversCount.incr();
  }

  public void decrDataNodeWriteActiveXceiversCount(){
    dataNodeWriteActiveXceiversCount.decr();
  }

  public void setDataNodeWriteActiveXceiversCount(int value){
    dataNodeWriteActiveXceiversCount.set(value);
  }

  public void incrDataNodePacketResponderCount() {
    dataNodePacketResponderCount.incr();
  }

  public void decrDataNodePacketResponderCount() {
    dataNodePacketResponderCount.decr();
  }

  public void setDataNodePacketResponderCount(int value) {
    dataNodePacketResponderCount.set(value);
  }

  public int getDataNodePacketResponderCount() {
    return dataNodePacketResponderCount.value();
  }

  public void incrDataNodeBlockRecoveryWorkerCount() {
    dataNodeBlockRecoveryWorkerCount.incr();
  }

  public void decrDataNodeBlockRecoveryWorkerCount() {
    dataNodeBlockRecoveryWorkerCount.decr();
  }

  public void setDataNodeBlockRecoveryWorkerCount(int value) {
    dataNodeBlockRecoveryWorkerCount.set(value);
  }

  public int getDataNodeBlockRecoveryWorkerCount() {
    return dataNodeBlockRecoveryWorkerCount.value();
  }

  public void incrECDecodingTime(long decodingTimeNanos) {
    ecDecodingTimeNanos.incr(decodingTimeNanos);
  }

  public void incrECReconstructionBytesRead(long bytes) {
    ecReconstructionBytesRead.incr(bytes);
  }

  public void incrECReconstructionRemoteBytesRead(long bytes) {
    ecReconstructionRemoteBytesRead.incr(bytes);
  }

  public void incrECReconstructionBytesWritten(long bytes) {
    ecReconstructionBytesWritten.incr(bytes);
  }

  public void incrECReconstructionReadTime(long millis) {
    ecReconstructionReadTimeMillis.incr(millis);
  }

  public void incrECReconstructionWriteTime(long millis) {
    ecReconstructionWriteTimeMillis.incr(millis);
  }

  public void incrECReconstructionDecodingTime(long millis) {
    ecReconstructionDecodingTimeMillis.incr(millis);
  }

  public void incrECReconstructionValidateTime(long millis) {
    ecReconstructionValidateTimeMillis.incr(millis);
  }

  public DataNodeUsageReport getDNUsageReport(long timeSinceLastReport) {
    return dnUsageReportUtil.getUsageReport(bytesWritten.value(), bytesRead
            .value(), totalWriteTime.value(), totalReadTime.value(),
        blocksWritten.value(), blocksRead.value(), timeSinceLastReport);
  }

  public void incrActorCmdQueueLength(int delta) {
    sumOfActorCommandQueueLength.incr(delta);
  }

  public void incrNumProcessedCommands() {
    numProcessedCommands.incr();
  }

  /**
   * Add processedCommandsOp metrics.
   * @param latency milliseconds of process commands
   */
  public void addNumProcessedCommands(long latency) {
    processedCommandsOp.add(latency);
  }

  /**
   * Add addCreateRbwOp metrics.
   * @param latency milliseconds of create RBW file
   */
  public void addCreateRbwOp(long latency) {
    createRbwOp.add(latency);
  }

  /**
   * Add addRecoverRbwOp metrics.
   * @param latency milliseconds of recovery RBW file
   */
  public void addRecoverRbwOp(long latency) {
    recoverRbwOp.add(latency);
  }

  /**
   * Add addConvertTemporaryToRbwOp metrics.
   * @param latency milliseconds of convert temporary to RBW file
   */
  public void addConvertTemporaryToRbwOp(long latency) {
    convertTemporaryToRbwOp.add(latency);
  }

  /**
   * Add addCreateTemporaryOp metrics.
   * @param latency milliseconds of create temporary block file
   */
  public void addCreateTemporaryOp(long latency) {
    createTemporaryOp.add(latency);
  }

  /**
   * Add addFinalizeBlockOp metrics.
   * @param latency milliseconds of finalize block
   */
  public void addFinalizeBlockOp(long latency) {
    finalizeBlockOp.add(latency);
  }

  /**
   * Add addUnfinalizeBlockOp metrics.
   * @param latency milliseconds of un-finalize block file
   */
  public void addUnfinalizeBlockOp(long latency) {
    unfinalizeBlockOp.add(latency);
  }

  /**
   * Add addCheckAndUpdateOp metrics.
   * @param latency milliseconds of check and update block file
   */
  public void addCheckAndUpdateOp(long latency) {
    checkAndUpdateOp.add(latency);
  }

  /**
   * Add addUpdateReplicaUnderRecoveryOp metrics.
   * @param latency milliseconds of update and replica under recovery block file
   */
  public void addUpdateReplicaUnderRecoveryOp(long latency) {
    updateReplicaUnderRecoveryOp.add(latency);
  }

  public void incrPacketsReceived() {
    packetsReceived.incr();
  }

  public void incrPacketsSlowWriteToMirror() {
    packetsSlowWriteToMirror.incr();
  }

  public void incrPacketsSlowWriteToDisk() {
    packetsSlowWriteToDisk.incr();
  }

  public void incrPacketsSlowWriteToOsCache() {
    packetsSlowWriteToOsCache.incr();
  }

  public void incrReplaceBlockOpOnSameMount() {
    replaceBlockOpOnSameMount.incr();
  }

  public void incrReplaceBlockOpOnSameHost() {
    replaceBlockOpOnSameHost.incr();
  }

  public void incrReplaceBlockOpToOtherHost() {
    replaceBlockOpToOtherHost.incr();
  }

  public void incrNullStorageBlockReports() {
    nullStorageBlockReports.incr();
  }
}
