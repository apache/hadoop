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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * Manage Incremental Block Reports (IBRs).
 */
@InterfaceAudience.Private
class IncrementalBlockReportManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      IncrementalBlockReportManager.class);

  private static class PerStorageIBR {
    /** The blocks in this IBR. */
    final Map<Block, ReceivedDeletedBlockInfo> blocks = Maps.newHashMap();

    private DataNodeMetrics dnMetrics;
    PerStorageIBR(final DataNodeMetrics dnMetrics) {
      this.dnMetrics = dnMetrics;
    }

    /**
     * Remove the given block from this IBR
     * @return true if the block was removed; otherwise, return false.
     */
    ReceivedDeletedBlockInfo remove(Block block) {
      return blocks.remove(block);
    }

    /** @return all the blocks removed from this IBR. */
    ReceivedDeletedBlockInfo[] removeAll() {
      final int size = blocks.size();
      if (size == 0) {
        return null;
      }

      final ReceivedDeletedBlockInfo[] rdbis = blocks.values().toArray(
          new ReceivedDeletedBlockInfo[size]);
      blocks.clear();
      return rdbis;
    }

    /** Put the block to this IBR. */
    void put(ReceivedDeletedBlockInfo rdbi) {
      blocks.put(rdbi.getBlock(), rdbi);
      increaseBlocksCounter(rdbi);
    }

    private void increaseBlocksCounter(
        final ReceivedDeletedBlockInfo receivedDeletedBlockInfo) {
      switch (receivedDeletedBlockInfo.getStatus()) {
      case RECEIVING_BLOCK:
        dnMetrics.incrBlocksReceivingInPendingIBR();
        break;
      case RECEIVED_BLOCK:
        dnMetrics.incrBlocksReceivedInPendingIBR();
        break;
      case DELETED_BLOCK:
        dnMetrics.incrBlocksDeletedInPendingIBR();
        break;
      default:
        break;
      }
      dnMetrics.incrBlocksInPendingIBR();
    }

    /**
     * Put the all blocks to this IBR unless the block already exists.
     * @param rdbis list of blocks to add.
     * @return the number of missing blocks added.
     */
    int putMissing(ReceivedDeletedBlockInfo[] rdbis) {
      int count = 0;
      for (ReceivedDeletedBlockInfo rdbi : rdbis) {
        if (!blocks.containsKey(rdbi.getBlock())) {
          put(rdbi);
          count++;
        }
      }
      return count;
    }
  }

  /**
   * Between block reports (which happen on the order of once an hour) the
   * DN reports smaller incremental changes to its block list for each storage.
   * This map contains the pending changes not yet to be reported to the NN.
   */
  private final Map<DatanodeStorage, PerStorageIBR> pendingIBRs
      = Maps.newHashMap();

  /**
   * If this flag is set then an IBR will be sent immediately by the actor
   * thread without waiting for the IBR timer to elapse.
   */
  private volatile boolean readyToSend = false;

  /** The time interval between two IBRs. */
  private final long ibrInterval;

  /** The timestamp of the last IBR. */
  private volatile long lastIBR;
  private DataNodeMetrics dnMetrics;

  IncrementalBlockReportManager(
      final long ibrInterval,
      final DataNodeMetrics dnMetrics) {
    this.ibrInterval = ibrInterval;
    this.lastIBR = monotonicNow() - ibrInterval;
    this.dnMetrics = dnMetrics;
  }

  boolean sendImmediately() {
    return readyToSend && monotonicNow() - ibrInterval >= lastIBR;
  }

  synchronized void waitTillNextIBR(long waitTime) {
    if (waitTime > 0 && !sendImmediately()) {
      try {
        wait(ibrInterval > 0 && ibrInterval < waitTime? ibrInterval: waitTime);
      } catch (InterruptedException ie) {
        LOG.warn(getClass().getSimpleName() + " interrupted");
      }
    }
  }

  private synchronized StorageReceivedDeletedBlocks[] generateIBRs() {
    final List<StorageReceivedDeletedBlocks> reports
        = new ArrayList<>(pendingIBRs.size());
    for (Map.Entry<DatanodeStorage, PerStorageIBR> entry
        : pendingIBRs.entrySet()) {
      final PerStorageIBR perStorage = entry.getValue();

        // Send newly-received and deleted blockids to namenode
      final ReceivedDeletedBlockInfo[] rdbi = perStorage.removeAll();
      if (rdbi != null) {
        reports.add(new StorageReceivedDeletedBlocks(entry.getKey(), rdbi));
      }
    }

    /* set blocks to zero */
    this.dnMetrics.resetBlocksInPendingIBR();

    readyToSend = false;
    return reports.toArray(new StorageReceivedDeletedBlocks[reports.size()]);
  }

  private synchronized void putMissing(StorageReceivedDeletedBlocks[] reports) {
    for (StorageReceivedDeletedBlocks r : reports) {
      pendingIBRs.get(r.getStorage()).putMissing(r.getBlocks());
    }
    if (reports.length > 0) {
      readyToSend = true;
    }
  }

  /** Send IBRs to namenode. */
  void sendIBRs(DatanodeProtocol namenode, DatanodeRegistration registration,
      String bpid, String nnRpcLatencySuffix) throws IOException {
    // Generate a list of the pending reports for each storage under the lock
    final StorageReceivedDeletedBlocks[] reports = generateIBRs();
    if (reports.length == 0) {
      // Nothing new to report.
      return;
    }

    // Send incremental block reports to the Namenode outside the lock
    if (LOG.isDebugEnabled()) {
      LOG.debug("call blockReceivedAndDeleted: " + Arrays.toString(reports));
    }
    boolean success = false;
    final long startTime = monotonicNow();
    try {
      namenode.blockReceivedAndDeleted(registration, bpid, reports);
      success = true;
    } finally {

      if (success) {
        dnMetrics.addIncrementalBlockReport(monotonicNow() - startTime,
            nnRpcLatencySuffix);
        lastIBR = startTime;
      } else {
        // If we didn't succeed in sending the report, put all of the
        // blocks back onto our queue, but only in the case where we
        // didn't put something newer in the meantime.
        putMissing(reports);
        LOG.warn("Failed to call blockReceivedAndDeleted: {}, nnId: {}"
            + ", duration(ms): {}", Arrays.toString(reports),
            nnRpcLatencySuffix, monotonicNow() - startTime);
      }
    }
  }

  /** @return the pending IBR for the given {@code storage} */
  private PerStorageIBR getPerStorageIBR(DatanodeStorage storage) {
    PerStorageIBR perStorage = pendingIBRs.get(storage);
    if (perStorage == null) {
      // This is the first time we are adding incremental BR state for
      // this storage so create a new map. This is required once per
      // storage, per service actor.
      perStorage = new PerStorageIBR(dnMetrics);
      pendingIBRs.put(storage, perStorage);
    }
    return perStorage;
  }

  /**
   * Add a block for notification to NameNode.
   * If another entry exists for the same block it is removed.
   */
  @VisibleForTesting
  synchronized void addRDBI(ReceivedDeletedBlockInfo rdbi,
      DatanodeStorage storage) {
    // Make sure another entry for the same block is first removed.
    // There may only be one such entry.
    for (PerStorageIBR perStorage : pendingIBRs.values()) {
      if (perStorage.remove(rdbi.getBlock()) != null) {
        break;
      }
    }
    getPerStorageIBR(storage).put(rdbi);
  }

  synchronized void notifyNamenodeBlock(ReceivedDeletedBlockInfo rdbi,
      DatanodeStorage storage, boolean isOnTransientStorage) {
    addRDBI(rdbi, storage);

    final BlockStatus status = rdbi.getStatus();
    if (status == BlockStatus.RECEIVING_BLOCK) {
      // the report will be sent out in the next heartbeat.
      readyToSend = true;
    } else if (status == BlockStatus.RECEIVED_BLOCK) {
      // the report is sent right away.
      triggerIBR(isOnTransientStorage);
    }
  }

  synchronized void triggerIBR(boolean force) {
    readyToSend = true;
    if (force) {
      lastIBR = monotonicNow() - ibrInterval;
    }
    if (sendImmediately()) {
      notifyAll();
    }
  }

  @VisibleForTesting
  synchronized void triggerDeletionReportForTests() {
    triggerIBR(true);

    while (sendImmediately()) {
      try {
        wait(100);
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  void clearIBRs() {
    pendingIBRs.clear();
  }

  @VisibleForTesting
  int getPendingIBRSize() {
    return pendingIBRs.size();
  }
}