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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.util.Time.now;

import java.io.PrintWriter;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.Daemon;

/***************************************************
 * PendingReplicationBlocks does the bookkeeping of all
 * blocks that are getting replicated.
 *
 * It does the following:
 * 1)  record blocks that are getting replicated at this instant.
 * 2)  a coarse grain timer to track age of replication request
 * 3)  a thread that periodically identifies replication-requests
 *     that never made it.
 *
 ***************************************************/
class PendingReplicationBlocks {
  private static final Log LOG = BlockManager.LOG;

  private final Map<Block, PendingBlockInfo> pendingReplications;
  private final ArrayList<Block> timedOutItems;
  Daemon timerThread = null;
  private volatile boolean fsRunning = true;

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long timeout = 5 * 60 * 1000;
  private final static long DEFAULT_RECHECK_INTERVAL = 5 * 60 * 1000;

  PendingReplicationBlocks(long timeoutPeriod) {
    if ( timeoutPeriod > 0 ) {
      this.timeout = timeoutPeriod;
    }
    pendingReplications = new HashMap<Block, PendingBlockInfo>();
    timedOutItems = new ArrayList<Block>();
  }

  void start() {
    timerThread = new Daemon(new PendingReplicationMonitor());
    timerThread.start();
  }

  /**
   * Add a block to the list of pending Replications
   * @param block The corresponding block
   * @param targets The DataNodes where replicas of the block should be placed
   */
  void increment(Block block, DatanodeDescriptor[] targets) {
    synchronized (pendingReplications) {
      PendingBlockInfo found = pendingReplications.get(block);
      if (found == null) {
        pendingReplications.put(block, new PendingBlockInfo(targets));
      } else {
        found.incrementReplicas(targets);
        found.setTimeStamp();
      }
    }
  }

  /**
   * One replication request for this block has finished.
   * Decrement the number of pending replication requests
   * for this block.
   * 
   * @param The DataNode that finishes the replication
   */
  void decrement(Block block, DatanodeDescriptor dn) {
    synchronized (pendingReplications) {
      PendingBlockInfo found = pendingReplications.get(block);
      if (found != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Removing pending replication for " + block);
        }
        found.decrementReplicas(dn);
        if (found.getNumReplicas() <= 0) {
          pendingReplications.remove(block);
        }
      }
    }
  }

  /**
   * Remove the record about the given block from pendingReplications.
   * @param block The given block whose pending replication requests need to be
   *              removed
   */
  void remove(Block block) {
    synchronized (pendingReplications) {
      pendingReplications.remove(block);
    }
  }

  public void clear() {
    synchronized (pendingReplications) {
      pendingReplications.clear();
      timedOutItems.clear();
    }
  }

  /**
   * The total number of blocks that are undergoing replication
   */
  int size() {
    return pendingReplications.size();
  } 

  /**
   * How many copies of this block is pending replication?
   */
  int getNumReplicas(Block block) {
    synchronized (pendingReplications) {
      PendingBlockInfo found = pendingReplications.get(block);
      if (found != null) {
        return found.getNumReplicas();
      }
    }
    return 0;
  }

  /**
   * Returns a list of blocks that have timed out their 
   * replication requests. Returns null if no blocks have
   * timed out.
   */
  Block[] getTimedOutBlocks() {
    synchronized (timedOutItems) {
      if (timedOutItems.size() <= 0) {
        return null;
      }
      Block[] blockList = timedOutItems.toArray(
          new Block[timedOutItems.size()]);
      timedOutItems.clear();
      return blockList;
    }
  }

  /**
   * An object that contains information about a block that 
   * is being replicated. It records the timestamp when the 
   * system started replicating the most recent copy of this
   * block. It also records the list of Datanodes where the 
   * replication requests are in progress.
   */
  static class PendingBlockInfo {
    private long timeStamp;
    private final List<DatanodeDescriptor> targets;

    PendingBlockInfo(DatanodeDescriptor[] targets) {
      this.timeStamp = now();
      this.targets = targets == null ? new ArrayList<DatanodeDescriptor>()
          : new ArrayList<DatanodeDescriptor>(Arrays.asList(targets));
    }

    long getTimeStamp() {
      return timeStamp;
    }

    void setTimeStamp() {
      timeStamp = now();
    }

    void incrementReplicas(DatanodeDescriptor... newTargets) {
      if (newTargets != null) {
        for (DatanodeDescriptor dn : newTargets) {
          targets.add(dn);
        }
      }
    }

    void decrementReplicas(DatanodeDescriptor dn) {
      targets.remove(dn);
    }

    int getNumReplicas() {
      return targets.size();
    }
  }

  /*
   * A periodic thread that scans for blocks that never finished
   * their replication request.
   */
  class PendingReplicationMonitor implements Runnable {
    @Override
    public void run() {
      while (fsRunning) {
        long period = Math.min(DEFAULT_RECHECK_INTERVAL, timeout);
        try {
          pendingReplicationCheck();
          Thread.sleep(period);
        } catch (InterruptedException ie) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("PendingReplicationMonitor thread is interrupted.", ie);
          }
        }
      }
    }

    /**
     * Iterate through all items and detect timed-out items
     */
    void pendingReplicationCheck() {
      synchronized (pendingReplications) {
        Iterator<Map.Entry<Block, PendingBlockInfo>> iter =
                                    pendingReplications.entrySet().iterator();
        long now = now();
        if(LOG.isDebugEnabled()) {
          LOG.debug("PendingReplicationMonitor checking Q");
        }
        while (iter.hasNext()) {
          Map.Entry<Block, PendingBlockInfo> entry = iter.next();
          PendingBlockInfo pendingBlock = entry.getValue();
          if (now > pendingBlock.getTimeStamp() + timeout) {
            Block block = entry.getKey();
            synchronized (timedOutItems) {
              timedOutItems.add(block);
            }
            LOG.warn("PendingReplicationMonitor timed out " + block);
            iter.remove();
          }
        }
      }
    }
  }

  /*
   * Shuts down the pending replication monitor thread.
   * Waits for the thread to exit.
   */
  void stop() {
    fsRunning = false;
    if(timerThread == null) return;
    timerThread.interrupt();
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Iterate through all items and print them.
   */
  void metaSave(PrintWriter out) {
    synchronized (pendingReplications) {
      out.println("Metasave: Blocks being replicated: " +
                  pendingReplications.size());
      Iterator<Map.Entry<Block, PendingBlockInfo>> iter =
                                  pendingReplications.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<Block, PendingBlockInfo> entry = iter.next();
        PendingBlockInfo pendingBlock = entry.getValue();
        Block block = entry.getKey();
        out.println(block + 
                    " StartTime: " + new Time(pendingBlock.timeStamp) +
                    " NumReplicaInProgress: " + 
                    pendingBlock.getNumReplicas());
      }
    }
  }
}
