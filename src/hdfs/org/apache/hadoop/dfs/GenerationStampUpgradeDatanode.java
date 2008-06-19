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
package org.apache.hadoop.dfs;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * This class associates a block generation stamp with with block. This
 * generation stamp is written to each metadata file. Please see
 * HADOOP-1700 for details.
 */
class GenerationStampUpgradeDatanode extends UpgradeObjectDatanode {

  public static final Log LOG = 
    LogFactory.getLog("org.apache.hadoop.dfs.GenerationStampUpgrade");

  DatanodeProtocol namenode;
  InetSocketAddress namenodeAddr;

  // stats
  private AtomicInteger blocksPreviouslyUpgraded = new AtomicInteger(0);
  private AtomicInteger blocksToUpgrade = new AtomicInteger(0);
  private AtomicInteger blocksUpgraded = new AtomicInteger(0);
  private AtomicInteger errors = new AtomicInteger(0);

  // process the upgrade using a pool of threads.
  static private final int poolSize = 4;

  // If no progress has occured during this time, print warnings message.
  static private final int LONG_TIMEOUT_MILLISEC = 1*60*1000; // 1 minute

  // This object is needed to indicate that namenode is not running upgrade.
  static UpgradeCommand noUpgradeOnNamenode = new UpgradeCommand();

  private List<UpgradeExecutor> completedList = new LinkedList<UpgradeExecutor>();

  /* This is set when the datanode misses the regular upgrade.
   * When this is set, it upgrades the block but stops heartbeating
   * to the namenode.
   */
  private AtomicBoolean offlineUpgrade = new AtomicBoolean(false);
  private AtomicBoolean upgradeCompleted = new AtomicBoolean(false);
  
  // Implement the common interfaces required by UpgradeObjectDatanode
  
  public int getVersion() {
    return GenerationStampUpgradeNamenode.PRE_GENERATIONSTAMP_LAYOUT_VERSION;
  }

  /*
   * Start upgrade if it not already running. It sends status to
   * namenode even if an upgrade is already in progress.
   */
  public synchronized UpgradeCommand startUpgrade() throws IOException {
    if (offlineUpgrade.get()) {
      doUpgrade();
    }
    return null; 
  }

  public String getDescription() {
    return "Block Generation Stamp Upgrade at Datanode";
  }

  public short getUpgradeStatus() {
    return (blocksToUpgrade.get() == blocksUpgraded.get()) ? 100 :
      (short) Math.floor(blocksUpgraded.get()*100.0/blocksToUpgrade.get());
  }

  public UpgradeCommand completeUpgrade() throws IOException {
    // return latest stats command.
    assert getUpgradeStatus() == 100;
    return new GenerationStampStatsUpgradeCommand(getUpgradeStatus(),
                                    getDatanode().dnRegistration,
                                    blocksPreviouslyUpgraded.get() + blocksUpgraded.get(),
                                    blocksToUpgrade.get()-blocksUpgraded.get(),
                                    errors.get(),
                                    GenerationStampUpgradeNamenode.PRE_GENERATIONSTAMP_LAYOUT_VERSION);
  }
  
  @Override
  boolean preUpgradeAction(NamespaceInfo nsInfo) throws IOException {
    int nsUpgradeVersion = nsInfo.getDistributedUpgradeVersion();
    if(nsUpgradeVersion >= getVersion()) {
      return false; // Normal upgrade.
    }
    
    LOG.info("\n  This Datanode has missed a cluster wide Block generation Stamp Upgrade." +
             "\n  Will perform an 'offline' upgrade of the blocks." +
             "\n  During this time, Datanode does not heartbeat.");
    
    
    // Namenode removes this node from the registered nodes
    try {
      getDatanode().namenode.errorReport(getDatanode().dnRegistration,
                                    DatanodeProtocol.NOTIFY, 
                                    "Performing an offline generation stamp " +
                                    "upgrade. " +
                                    "Will be back online once the ugprade " +
                                    "completes. Please see datanode logs.");
      
    } catch(IOException ignored) {
      LOG.info("\n  This Datanode was unable to send error report to namenode.");
    }
    offlineUpgrade.set(true);
    return true;
  }

  public GenerationStampUpgradeDatanode() {
    blocksPreviouslyUpgraded.set(0);
    blocksToUpgrade.set(0);
    blocksUpgraded.set(0);
    errors.set(0);
  }

  static File getPreGenerationMetaFile(File f) {
    return new File(f.getAbsolutePath() + FSDataset.METADATA_EXTENSION);
  }
  
  // This class is invoked by the worker thread to convert the
  // metafile into the new format
  //
  class UpgradeExecutor implements Runnable {
    Block block;
    Throwable throwable;
    
    UpgradeExecutor(Block b) {
      block = b;
    }

    public void run() {
      try {
        // do the real work here
        FSDataset dataset = (FSDataset) getDatanode().data;
        upgradeToCurVersion(dataset, block);
      } catch (Throwable t) {
        throwable = t;
      }
      synchronized (completedList) {
        completedList.add(this);
        completedList.notify();
      }
    }

    /**
     * Upgrades the metadata file to current version if required.
     * @param dataset
     * @param block
     */
    void upgradeToCurVersion(FSDataset dataset, Block block)
                                              throws IOException {
      File blockFile = dataset.getBlockFile(block);
      if (blockFile == null) {
        throw new IOException("Could find file for " + block);
      }

      File metadataFile = dataset.getMetaFile(block);
      File oldmetadataFile = getPreGenerationMetaFile(blockFile);

      if (metadataFile.exists() && oldmetadataFile.exists()) {
        //
        // If both file exists and are of the same size,
        // then delete the old one. If the sizes are not same then
        // leave both of them and consider the upgrade as successful.
        //
        if (metadataFile.length() == oldmetadataFile.length()) {
          if (!oldmetadataFile.delete()) {
            LOG.info("Unable to delete old metadata file " + oldmetadataFile);
          }
        }
      } else if (metadataFile.exists()) {
        //
        // Only the new file exists, nothing more to do.
        //
        return;
      } else if (oldmetadataFile.exists()) {
        //
        // The old file exists but the new one is missing. Rename
        // old one to new name.
        //
        if (!oldmetadataFile.renameTo(metadataFile)) {
          throw new IOException("Could find rename " +  oldmetadataFile +
                                " to " + metadataFile);
        }
      } else {
        throw new IOException("Could find any metadata file for " + block);
      }
    }
  }
  
  // This method iterates through all the blocks on a datanode and
  // do the upgrade.
  //
  void doUpgrade() throws IOException {
    
    if (upgradeCompleted.get()) {
      assert offlineUpgrade.get() : 
             ("Multiple calls to doUpgrade is expected only during " +
              "offline upgrade");
      return;
    }
    
    FSDataset dataset = (FSDataset) getDatanode().data;

    // Set up the retry policy so that each attempt waits for one minute.
    Configuration conf = new Configuration();
    // set rpc timeout to one minute.
    conf.set("ipc.client.timeout", "60000");

    RetryPolicy timeoutPolicy =
       RetryPolicies.retryUpToMaximumCountWithFixedSleep(
               LONG_TIMEOUT_MILLISEC/1000,
               1, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(SocketTimeoutException.class, timeoutPolicy);
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap =
                            new HashMap<String, RetryPolicy>();
    // do we need to set the policy for connection failures also?
    methodNameToPolicyMap.put("processUpgradeCommand", methodPolicy);

    LOG.info("Starting Block Generation Stamp Upgrade on datanode " +
             getDatanode());

    for (;;) {
      try {
        namenodeAddr = getDatanode().getNameNodeAddr();
        namenode = (DatanodeProtocol) RetryProxy.create(
                            DatanodeProtocol.class,
                            RPC.waitForProxy(DatanodeProtocol.class,
                                             DatanodeProtocol.versionID,
                                             namenodeAddr,
                                             conf),
                            methodNameToPolicyMap);
        break;
      } catch (IOException e) {
        LOG.warn("Generation Stamp Upgrade Exception " +
                 "while trying to connect to NameNode at " +
                 getDatanode().getNameNodeAddr().toString() + " : " +
                 StringUtils.stringifyException(e));
        try {
          Thread.sleep(10*1000);
        } catch (InterruptedException e1) {
          throw new IOException("Interrupted Sleep while creating RPC proxy." +
                                e1);
        }
      }
    }
    LOG.info("Block Generation Stamp Upgrade Datanode connected to " +
             "namenode at " + namenodeAddr);

    // Get a list of all the blocks :
    LinkedList<UpgradeExecutor> blockList = new LinkedList<UpgradeExecutor>();
    
    //Fill blockList with blocks to be upgraded.
    Block [] blockArr = dataset.getBlockReport();
    
    for (Block b : blockArr) {
      File blockFile = null;
      try {
        blockFile = dataset.getBlockFile(b);
      } catch (IOException e) {
        //The block might just be deleted. ignore it.
        LOG.warn("Could not find file location for " + b + 
                 ". It might already be deleted. Exception : " +
                 StringUtils.stringifyException(e));
        errors.getAndIncrement();
        continue;
      }
      if (!blockFile.exists()) {
        errors.getAndIncrement();
        LOG.error("could not find block file " + blockFile);
        continue;
      }
      File metaFile = dataset.getMetaFile(b);
      File oldMetaFile = getPreGenerationMetaFile(blockFile);
      if (metaFile.exists()) {
        blocksPreviouslyUpgraded.getAndIncrement();
        continue;
      }
      blocksToUpgrade.getAndIncrement();
      blockList.add(new UpgradeExecutor(b));
    }
    blockArr = null;
    int nLeft = blockList.size();
    
    LOG.info("Starting upgrade of " + blocksToUpgrade.get() + " blocks out of " +
             (blocksToUpgrade.get() + blocksPreviouslyUpgraded.get()));

    // Start the pool of upgrade workers
    ExecutorService pool = Executors.newFixedThreadPool(poolSize);
    for (Iterator<UpgradeExecutor> it = blockList.iterator(); it.hasNext();) {
      pool.submit(it.next());
    }

    // Inform the namenode
    sendStatus();
    
    // Report status to namenode every so many seconds:
    long now = System.currentTimeMillis();
    long statusReportIntervalMilliSec = 30*1000;
    long lastStatusReportTime = now;
    long lastUpdateTime = now;
    long lastWarnTime = now;
    
    // Now wait for the tasks to complete.
    //
    while (nLeft > 0) {
      synchronized (completedList) {
        if (completedList.size() <= 0) {
          try {
            completedList.wait(1000);
          } catch (InterruptedException ignored) {}
        }
        
        now = System.currentTimeMillis();
        
        if (completedList.size()> 0) {
          UpgradeExecutor exe = completedList.remove(0);
          nLeft--;
          if (exe.throwable != null) {
            errors.getAndIncrement();
            LOG.error("Got an exception during generation stamp upgrade of " +
                      exe.block + ": " + 
                      StringUtils.stringifyException(exe.throwable));
          }
          blocksUpgraded.getAndIncrement();
          lastUpdateTime = now;
        } else {
          if ((now - lastUpdateTime) >= LONG_TIMEOUT_MILLISEC &&
              (now - lastWarnTime) >= LONG_TIMEOUT_MILLISEC) {
            lastWarnTime = now;
            LOG.warn("No block was updated in last " +
                      (LONG_TIMEOUT_MILLISEC/(60*1000)) +
                      " minutes! will keep waiting... ");
          }  
        } 
      }
      
      if ((now-lastStatusReportTime) > statusReportIntervalMilliSec) {
        sendStatus();
        lastStatusReportTime = System.currentTimeMillis();
      }
    }

    pool.shutdown();
    upgradeCompleted.set(true);
    
    LOG.info("Completed Block Generation Stamp Upgrade. Total of " + 
             (blocksPreviouslyUpgraded.get() + blocksToUpgrade.get()) +
             " blocks : " + blocksPreviouslyUpgraded.get() + " blocks previously " +
             "upgraded, " + blocksUpgraded.get() + " blocks upgraded this time " +
             "with " + errors.get() + " errors.");       

    // now inform the name node about the completion.
    // What if there is no upgrade running on Namenode now?
    while (!sendStatus());
    
  }
  
  /** Sends current status and stats to namenode and logs it to local log*/ 
  boolean sendStatus() {
    LOG.info((offlineUpgrade.get() ? "Offline " : "") + 
              "Block Generation Stamp Upgrade : " + 
               getUpgradeStatus() + "% completed.");
    if (offlineUpgrade.get()) {
      return true;
    }
    
    GenerationStampStatsUpgradeCommand cmd = null;
    synchronized (this) {
      cmd = new GenerationStampStatsUpgradeCommand(getUpgradeStatus(),
                           getDatanode().dnRegistration,
                           blocksPreviouslyUpgraded.get() + blocksUpgraded.get(),
                           blocksToUpgrade.get()-blocksUpgraded.get(),
                           errors.get(),
                           GenerationStampUpgradeNamenode.PRE_GENERATIONSTAMP_LAYOUT_VERSION);
    }
    UpgradeCommand reply = sendCommand(namenodeAddr, namenode, cmd, 0);
    if (reply == null) {
      LOG.warn("Could not send status to Namenode. Namenode might be " +
               "over loaded or down.");
    }
    return reply != null;
  }


  // Sends a command to the namenode
  static UpgradeCommand sendCommand(InetSocketAddress namenodeAddr,
                                    DatanodeProtocol namenode,
                                    UpgradeCommand cmd, int retries) {
    for(int i=0; i<=retries || retries<0; i++) {
      try {
        UpgradeCommand reply = namenode.processUpgradeCommand(cmd);
        if (reply == null) {
          /* namenode might not be running upgrade or finished
           * an upgrade. We just return a static object */
          return noUpgradeOnNamenode;
        }
        return reply;
      } catch (IOException e) {
        // print the stack trace only for the last retry.
        LOG.warn("Exception to " + namenodeAddr +
                 " while sending command " + 
                 cmd.getAction() + ": " + e +
                 ((retries<0 || i>=retries)? "... will retry ..." :
                   ": " + StringUtils.stringifyException(e)));
      }
    }
    return null;
  }
}
