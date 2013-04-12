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

import java.io.IOException;
import java.util.TreeMap;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import com.google.common.annotations.VisibleForTesting;

/**
 * DataBlockScanner manages block scanning for all the block pools. For each
 * block pool a {@link BlockPoolSliceScanner} is created which runs in a separate
 * thread to scan the blocks for that block pool. When a {@link BPOfferService}
 * becomes alive or dies, blockPoolScannerMap in this class is updated.
 */
@InterfaceAudience.Private
public class DataBlockScanner implements Runnable {
  public static final Log LOG = LogFactory.getLog(DataBlockScanner.class);
  private final DataNode datanode;
  private final FsDatasetSpi<? extends FsVolumeSpi> dataset;
  private final Configuration conf;
  
  static final int SLEEP_PERIOD_MS = 5 * 1000;

  /**
   * Map to find the BlockPoolScanner for a given block pool id. This is updated
   * when a BPOfferService becomes alive or dies.
   */
  private final TreeMap<String, BlockPoolSliceScanner> blockPoolScannerMap = 
    new TreeMap<String, BlockPoolSliceScanner>();
  Thread blockScannerThread = null;
  
  DataBlockScanner(DataNode datanode,
      FsDatasetSpi<? extends FsVolumeSpi> dataset,
      Configuration conf) {
    this.datanode = datanode;
    this.dataset = dataset;
    this.conf = conf;
  }
  
  @Override
  public void run() {
    String currentBpId = "";
    boolean firstRun = true;
    while (datanode.shouldRun && !Thread.interrupted()) {
      //Sleep everytime except in the first iteration.
      if (!firstRun) {
        try {
          Thread.sleep(SLEEP_PERIOD_MS);
        } catch (InterruptedException ex) {
          // Interrupt itself again to set the interrupt status
          blockScannerThread.interrupt();
          continue;
        }
      } else {
        firstRun = false;
      }
      
      BlockPoolSliceScanner bpScanner = getNextBPScanner(currentBpId);
      if (bpScanner == null) {
        // Possible if thread is interrupted
        continue;
      }
      currentBpId = bpScanner.getBlockPoolId();
      // If BPOfferService for this pool is not alive, don't process it
      if (!datanode.isBPServiceAlive(currentBpId)) {
        LOG.warn("Block Pool " + currentBpId + " is not alive");
        // Remove in case BP service died abruptly without proper shutdown
        removeBlockPool(currentBpId);
        continue;
      }
      bpScanner.scanBlockPoolSlice();
    }

    // Call shutdown for each allocated BlockPoolSliceScanner.
    for (BlockPoolSliceScanner bpss: blockPoolScannerMap.values()) {
      bpss.shutdown();
    }
  }

  // Wait for at least one block pool to be up
  private void waitForInit() {
    while ((getBlockPoolSetSize() < datanode.getAllBpOs().length)
        || (getBlockPoolSetSize() < 1)) {
      try {
        Thread.sleep(SLEEP_PERIOD_MS);
      } catch (InterruptedException e) {
        blockScannerThread.interrupt();
        return;
      }
    }
  }
  
  /**
   * Find next block pool id to scan. There should be only one current
   * verification log file. Find which block pool contains the current
   * verification log file and that is used as the starting block pool id. If no
   * current files are found start with first block-pool in the blockPoolSet.
   * However, if more than one current files are found, the one with latest 
   * modification time is used to find the next block pool id.
   */
  private BlockPoolSliceScanner getNextBPScanner(String currentBpId) {
    
    String nextBpId = null;
    while ((nextBpId == null) && datanode.shouldRun
        && !blockScannerThread.isInterrupted()) {
      waitForInit();
      synchronized (this) {
        if (getBlockPoolSetSize() > 0) {          
          // Find nextBpId by the minimum of the last scan time
          long lastScanTime = 0;
          for (String bpid : blockPoolScannerMap.keySet()) {
            final long t = getBPScanner(bpid).getLastScanTime();
            if (t != 0L) {
              if (bpid == null || t < lastScanTime) {
                lastScanTime =  t;
                nextBpId = bpid;
              }
            }
          }
          
          // nextBpId can still be null if no current log is found,
          // find nextBpId sequentially.
          if (nextBpId == null) {
            nextBpId = blockPoolScannerMap.higherKey(currentBpId);
            if (nextBpId == null) {
              nextBpId = blockPoolScannerMap.firstKey();
            }
          }
          if (nextBpId != null) {
            return getBPScanner(nextBpId);
          }
        }
      }
      LOG.warn("No block pool is up, going to wait");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ex) {
        LOG.warn("Received exception: " + ex);
        blockScannerThread.interrupt();
        return null;
      }
    }
    return null;
  }

  private synchronized int getBlockPoolSetSize() {
    return blockPoolScannerMap.size();
  }
  
  @VisibleForTesting
  synchronized BlockPoolSliceScanner getBPScanner(String bpid) {
    return blockPoolScannerMap.get(bpid);
  }
  
  private synchronized String[] getBpIdList() {
    return blockPoolScannerMap.keySet().toArray(
        new String[blockPoolScannerMap.keySet().size()]);
  }
  
  public void addBlock(ExtendedBlock block) {
    BlockPoolSliceScanner bpScanner = getBPScanner(block.getBlockPoolId());
    if (bpScanner != null) {
      bpScanner.addBlock(block);
    } else {
      LOG.warn("No block pool scanner found for block pool id: "
          + block.getBlockPoolId());
    }
  }
  
  boolean isInitialized(String bpid) {
    return getBPScanner(bpid) != null;
  }

  public synchronized void printBlockReport(StringBuilder buffer,
      boolean summary) {
    String[] bpIdList = getBpIdList();
    if (bpIdList == null || bpIdList.length == 0) {
      buffer.append("Periodic block scanner is not yet initialized. "
          + "Please check back again after some time.");
      return;
    }
    for (String bpid : bpIdList) {
      BlockPoolSliceScanner bpScanner = getBPScanner(bpid);
      buffer.append("\n\nBlock report for block pool: "+bpid + "\n");
      bpScanner.printBlockReport(buffer, summary);
      buffer.append("\n");
    }
  }
  
  public void deleteBlock(String poolId, Block toDelete) {
    BlockPoolSliceScanner bpScanner = getBPScanner(poolId);
    if (bpScanner != null) {
      bpScanner.deleteBlock(toDelete);
    } else {
      LOG.warn("No block pool scanner found for block pool id: "
          + poolId);
    }
  }

  public void deleteBlocks(String poolId, Block[] toDelete) {
    BlockPoolSliceScanner bpScanner = getBPScanner(poolId);
    if (bpScanner != null) {
      bpScanner.deleteBlocks(toDelete);
    } else {
      LOG.warn("No block pool scanner found for block pool id: "
          + poolId);
    }
  }
  
  public void shutdown() {
    synchronized (this) {
      if (blockScannerThread != null) {
        blockScannerThread.interrupt();
      }
    }

    // We cannot join within the synchronized block, because it would create a
    // deadlock situation.  blockScannerThread calls other synchronized methods.
    if (blockScannerThread != null) {
      try {
        blockScannerThread.join();
      } catch (InterruptedException e) {
        // shutting down anyway
      }
    }
  }

  public synchronized void addBlockPool(String blockPoolId) {
    if (blockPoolScannerMap.get(blockPoolId) != null) {
      return;
    }
    BlockPoolSliceScanner bpScanner = new BlockPoolSliceScanner(blockPoolId,
        datanode, dataset, conf);
    blockPoolScannerMap.put(blockPoolId, bpScanner);
    LOG.info("Added bpid=" + blockPoolId + " to blockPoolScannerMap, new size="
        + blockPoolScannerMap.size());
  }
  
  public synchronized void removeBlockPool(String blockPoolId) {
    blockPoolScannerMap.remove(blockPoolId);
    LOG.info("Removed bpid="+blockPoolId+" from blockPoolScannerMap");
  }
  
  @VisibleForTesting
  long getBlocksScannedInLastRun(String bpid) throws IOException {
    BlockPoolSliceScanner bpScanner = getBPScanner(bpid);
    if (bpScanner == null) {
      throw new IOException("Block Pool: "+bpid+" is not running");
    } else {
      return bpScanner.getBlocksScannedInLastRun();
    }
  }

  @VisibleForTesting
  long getTotalScans(String bpid) throws IOException {
    BlockPoolSliceScanner bpScanner = getBPScanner(bpid);
    if (bpScanner == null) {
      throw new IOException("Block Pool: "+bpid+" is not running");
    } else {
      return bpScanner.getTotalScans();
    }
  }

  public void start() {
    blockScannerThread = new Thread(this);
    blockScannerThread.setDaemon(true);
    blockScannerThread.start();
  }
  
  @InterfaceAudience.Private
  public static class Servlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response) throws IOException {
      response.setContentType("text/plain");
      
      DataNode datanode = (DataNode) getServletContext().getAttribute("datanode");
      DataBlockScanner blockScanner = datanode.blockScanner;
      
      boolean summary = (request.getParameter("listblocks") == null);
      
      StringBuilder buffer = new StringBuilder(8*1024);
      if (blockScanner == null) {
        LOG.warn("Periodic block scanner is not running");
        buffer.append("Periodic block scanner is not running. " +
                      "Please check the datanode log if this is unexpected.");
      } else {
        blockScanner.printBlockReport(buffer, summary);
      }
      response.getWriter().write(buffer.toString()); // extra copy!
    }
  }

}
