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
/**
 * Once an upgrade starts at the namenode , this class manages the upgrade 
 * process.
 */
class GenerationStampUpgradeNamenode extends UpgradeObjectNamenode {
  
  public static final Log LOG = 
    LogFactory.getLog("org.apache.hadoop.dfs.GenerationStampUpgradeNamenode");
  
  static final long inactivityExtension = 10*1000; // 10 seconds
  AtomicLong lastNodeCompletionTime = new AtomicLong(0);

  // The layout version before the generation stamp upgrade.
  static final int PRE_GENERATIONSTAMP_LAYOUT_VERSION = -13;

  static final int DN_CMD_STATS = 300;
  
  enum UpgradeStatus {
    INITIALIZED,
    STARTED,
    DATANODES_DONE,
    COMPLETED,
  }
  
  UpgradeStatus upgradeStatus = UpgradeStatus.INITIALIZED;
  
  class DnInfo { 
    short percentCompleted = 0;
    long blocksUpgraded = 0;
    long blocksRemaining = 0;
    long errors = 0;
    
    DnInfo(short pcCompleted) {
      percentCompleted = status;
    }
    DnInfo() {}
    
    void setStats(GenerationStampStatsUpgradeCommand cmd) {
      percentCompleted = cmd.getCurrentStatus();
      blocksUpgraded = cmd.blocksUpgraded;
      blocksRemaining = cmd.blocksRemaining;
      errors = cmd.errors;
    }
    
    boolean isDone() {
      return percentCompleted >= 100;
    }
  }
  
  /* We should track only the storageIDs and not DatanodeID, which
   * includes datanode name and storage id.
   */
  HashMap<DatanodeID, DnInfo> dnMap = new HashMap<DatanodeID, DnInfo>();
  HashMap<DatanodeID, DnInfo> unfinishedDnMap = 
                                      new HashMap<DatanodeID, DnInfo>();  

  Daemon monitorThread;
  double avgDatanodeCompletionPct = 0;
  boolean forceDnCompletion = false;
  
  //Upgrade object interface:
  
  public int getVersion() {
    return PRE_GENERATIONSTAMP_LAYOUT_VERSION;
  }

  public UpgradeCommand completeUpgrade() throws IOException {
    return null;
  }
 
  @Override
  public String getDescription() {
    return "Block Generation Stamp Upgrade at Namenode";
  }

  @Override
  public synchronized short getUpgradeStatus() {
    // Reserve 10% for deleting files.
    if (upgradeStatus == UpgradeStatus.COMPLETED) {
      return 100;
    }   
    return (short) avgDatanodeCompletionPct;
  }

  @Override
  public UpgradeCommand startUpgrade() throws IOException {
    
    assert monitorThread == null;
    lastNodeCompletionTime.set(System.currentTimeMillis());
    
    monitorThread = new Daemon(new UpgradeMonitor());
    monitorThread.start();    
    return super.startUpgrade();
  }
  
  @Override
  public synchronized void forceProceed() throws IOException {    
    if (forceDnCompletion) {
      LOG.warn("forceProceed is already set for this upgrade. It can take " +
               "a short while to take affect. Please wait.");
      return;
    }
    
    LOG.info("got forceProceed request for this upgrade. Datanodes upgrade " +
             "will be considered done. It can take a few seconds to take " +
             "effect.");
    forceDnCompletion = true;
  }

  @Override
  UpgradeCommand processUpgradeCommand(UpgradeCommand command) 
                                           throws IOException {
    switch (command.getAction()) {

    case GenerationStampUpgradeNamenode.DN_CMD_STATS :
      return handleStatsCmd(command);

     default:
       throw new IOException("Unknown Command for Generation Stamp Upgrade : " +
                             command.getAction());
    }
  }

  @Override
  public UpgradeStatusReport getUpgradeStatusReport(boolean details) 
                                                    throws IOException {

    /* If 'details' is true should we update block level status?
     * It could take multiple minutes
     * updateBlckLevelStats()?
     */
    
    String replyString = "";
    
    short status = 0;
    
    synchronized (this) {
     
      status = getUpgradeStatus();
     
      replyString = String.format(
      ((monitorThread == null) ? "\tUpgrade has not been started yet.\n" : "")+
      ((forceDnCompletion) ? "\tForce Proceed is ON\n" : "") +
      "\tLast Block Level Stats updated at : %tc\n" +
      "\tLast Block Level Stats : %s\n" +
      "\tBrief Datanode Status  : %s\n" +
      "%s",
      latestBlockLevelStats.updatedAt,
      latestBlockLevelStats.statusString("\n\t                         "), 
      printStatus("\n\t                         "), 
      ((status < 100 && upgradeStatus == UpgradeStatus.DATANODES_DONE) ?
      "\tNOTE: Upgrade at the Datanodes has finished. Deleteing \".crc\" " +
      "files\n\tcan take longer than status implies.\n" : "")
      );
      
      if (details) {
        // list all the known data nodes
        StringBuilder str = null;
        Iterator<DatanodeID> keys = dnMap.keySet().iterator();
        Iterator<DnInfo> values = dnMap.values().iterator();
        
        for(; keys.hasNext() && values.hasNext() ;) {
          DatanodeID dn = keys.next();
          DnInfo info = values.next();
          String dnStr = "\t\t" + dn.getName() + "\t : " + 
                         info.percentCompleted + " % \t" +
                         info.blocksUpgraded + " u \t" +
                         info.blocksRemaining + " r \t" +
                         info.errors + " e\n";
          if ( str == null ) {
            str = new StringBuilder(dnStr.length()*
                                    (dnMap.size() + (dnMap.size()+7)/8));
          }
          str.append(dnStr);
        }
        
        replyString += "\n\tDatanode Stats (total: " + dnMap.size() + "): " +
                       "pct Completion(%) blocks upgraded (u) " +
                       "blocks remaining (r) errors (e)\n\n" +
                       (( str == null ) ?
                        "\t\tThere are no known Datanodes\n" : str);
      }      
    }
    return new GenerationStampUpgradeStatusReport(
                   PRE_GENERATIONSTAMP_LAYOUT_VERSION,
                   status, replyString);
  }


  /**
   * The namenode process a periodic statistics message from the datanode.
   */
  private synchronized UpgradeCommand handleStatsCmd(UpgradeCommand cmd) {
    
    GenerationStampStatsUpgradeCommand stats = (GenerationStampStatsUpgradeCommand)cmd;
    
    DatanodeID dn = stats.datanodeId;
    DnInfo dnInfo = dnMap.get(dn);
    boolean alreadyCompleted = (dnInfo != null && dnInfo.isDone());
    
    if (dnInfo == null) {
      dnInfo = new DnInfo();
      dnMap.put(dn, dnInfo);
      LOG.info("Upgrade started/resumed at datanode " + dn.getName());  
    }
    
    dnInfo.setStats(stats);
    if (!dnInfo.isDone()) {
      unfinishedDnMap.put(dn, dnInfo);
    }
    
    if (dnInfo.isDone() && !alreadyCompleted) {
      LOG.info("upgrade completed on datanode " + dn.getName());      
      unfinishedDnMap.remove(dn);
      if (unfinishedDnMap.size() == 0) {
        lastNodeCompletionTime.set(System.currentTimeMillis());
      }
    }   
    
    //Should we send any more info?
    return new UpgradeCommand();
  }
  
  public GenerationStampUpgradeNamenode() {
  }
  
  // For now we will wait for all the nodes to complete upgrade.
  synchronized boolean isUpgradeDone() {
    return upgradeStatus == UpgradeStatus.COMPLETED;    
  }
  
  synchronized String printStatus(String spacing) {
    //NOTE: iterates on all the datanodes.
    
    // Calculate % completion on all the data nodes.
    long errors = 0;
    long totalCompletion = 0;
    for( Iterator<DnInfo> it = dnMap.values().iterator(); it.hasNext(); ) {
      DnInfo dnInfo = it.next();
      totalCompletion += dnInfo.percentCompleted;            
      errors += dnInfo.errors;
    }
    
    avgDatanodeCompletionPct = totalCompletion/(dnMap.size() + 1e-20);
    
    String msg = "Avg completion of all Datanodes: " +              
                 String.format("%.2f%%", avgDatanodeCompletionPct) +
                 " with " + errors + " errors. " +
                 ((unfinishedDnMap.size() > 0) ? spacing + 
                   unfinishedDnMap.size() + " out of " + dnMap.size() +
                   " nodes are not done." : "");
                 
    LOG.info("Generation Stamp Upgrade is " + (isUpgradeDone() ? 
             "complete. " : "still running. ") + spacing + msg);
    return msg;
  }
  
  private synchronized void setStatus(UpgradeStatus status) {
    upgradeStatus = status;
  }

  /* Checks if upgrade completed based on datanode's status and/or 
   * if all the blocks are upgraded.
   */
  private synchronized UpgradeStatus checkOverallCompletion() {
    
    if (upgradeStatus == UpgradeStatus.COMPLETED ||
        upgradeStatus == UpgradeStatus.DATANODES_DONE) {
      return upgradeStatus;
    }
    
    if (upgradeStatus != UpgradeStatus.DATANODES_DONE) {
      boolean datanodesDone =
        (dnMap.size() > 0 && unfinishedDnMap.size() == 0 &&
         ( System.currentTimeMillis() - lastNodeCompletionTime.get() ) > 
        inactivityExtension) || forceDnCompletion ;
                 
      if ( datanodesDone ) {
        LOG.info("Upgrade of DataNode blocks is complete. " +
                 ((forceDnCompletion) ? "(ForceDnCompletion is on.)" : ""));
        upgradeStatus = UpgradeStatus.DATANODES_DONE;
      }
    }
    
    if (upgradeStatus != UpgradeStatus.DATANODES_DONE &&
        latestBlockLevelStats.updatedAt > 0) {
      // check if last block report marked all
      if (latestBlockLevelStats.minimallyReplicatedBlocks == 0 &&
          latestBlockLevelStats.underReplicatedBlocks == 0) {
        
        LOG.info("Marking datanode upgrade complete since all the blocks are " +
                 "upgraded (even though some datanodes may not have " +
                 "reported completion. Block level stats :\n\t" +
                 latestBlockLevelStats.statusString("\n\t"));
        upgradeStatus = UpgradeStatus.DATANODES_DONE;
      }
    }
    
    return upgradeStatus;
  } 
    
  /**
   * This class monitors the upgrade progress and periodically prints 
   * status message to log.
   */
  class UpgradeMonitor implements Runnable {
    
    static final long statusReportIntervalMillis = 1*60*1000;
    static final long blockReportIntervalMillis = 5*60*1000;
    static final int sleepTimeSec = 5;
    
    public void run() {
      long lastReportTime = System.currentTimeMillis();
      long lastBlockReportTime = lastReportTime;
      
      while ( !isUpgradeDone() ) {
        UpgradeStatus status = checkOverallCompletion();
        
        if (status == UpgradeStatus.DATANODES_DONE) {
          setStatus(UpgradeStatus.COMPLETED);
        }
        
        long now = System.currentTimeMillis();
        
        
        if (now-lastBlockReportTime >= blockReportIntervalMillis) {
          updateBlockLevelStats();
          // Check if all the blocks have been upgraded.
          lastBlockReportTime = now;
        }
        
        if ((now - lastReportTime) >= statusReportIntervalMillis || 
            isUpgradeDone()) {
          printStatus("\n\t");
          lastReportTime = now;
        }

        if (isUpgradeDone()) {
          break;
        }
        
        try {
          Thread.sleep(sleepTimeSec*1000);
        } catch (InterruptedException e) {
          break;
        }
      }
      LOG.info("Leaving the Generation Stamp Upgrade Namenode monitor thread");
    }
  }
  
  private BlockLevelStats latestBlockLevelStats = new BlockLevelStats();
  // internal class to hold the stats.
  private static class BlockLevelStats {
    long fullyReplicatedBlocks = 0;
    long minimallyReplicatedBlocks = 0;
    long underReplicatedBlocks = 0; // includes unReplicatedBlocks
    long unReplicatedBlocks = 0; // zero replicas upgraded
    long errors;
    long updatedAt;
    
    String statusString(String spacing) {
      long totalBlocks = fullyReplicatedBlocks + 
                         minimallyReplicatedBlocks +
                         underReplicatedBlocks;
      double multiplier = 100/(totalBlocks + 1e-20);
      
      if (spacing.equals("")) {
        spacing = ", ";
      }
      
      return String.format(
                     "Total Blocks : %d" +
                     "%sFully Upgragraded : %.2f%%" +
                     "%sMinimally Upgraded : %.2f%%" +
                     "%sUnder Upgraded : %.2f%% (includes Un-upgraded blocks)" +
                     "%sUn-upgraded : %.2f%%" + 
                     "%sErrors : %d", totalBlocks, 
                     spacing, (fullyReplicatedBlocks * multiplier),
                     spacing, (minimallyReplicatedBlocks * multiplier),
                     spacing, (underReplicatedBlocks * multiplier),
                     spacing, (unReplicatedBlocks * multiplier),
                     spacing, errors);
    }
  }
  
  void updateBlockLevelStats(String path, BlockLevelStats stats) {
    DFSFileInfo[] fileArr = getFSNamesystem().dir.getListing(path);
    
    for (DFSFileInfo file:fileArr) {
      if (file.isDir()) {
        updateBlockLevelStats(file.getPath().toString(), stats);
      } else {
        // Get the all the blocks.
        LocatedBlocks blockLoc = null;
        try {
          blockLoc = getFSNamesystem().getBlockLocations(
              file.getPath().toString(), 0, file.getLen());
          int numBlocks = blockLoc.locatedBlockCount();
          for (int i=0; i<numBlocks; i++) {
            LocatedBlock loc = blockLoc.get(i);
            DatanodeInfo[] dnArr = loc.getLocations();
            int numUpgraded = 0;
            synchronized (this) {
              for (DatanodeInfo dn:dnArr) {
                DnInfo dnInfo = dnMap.get(dn);
                if (dnInfo != null && dnInfo.isDone()) {
                  numUpgraded++;
                }
              }
            }
            
            if (numUpgraded >= file.getReplication()) {
              stats.fullyReplicatedBlocks++;
            } else if (numUpgraded >= getFSNamesystem().getMinReplication()) {
              stats.minimallyReplicatedBlocks++;
            } else {
              stats.underReplicatedBlocks++;
            }
            if (numUpgraded == 0) {
              stats.unReplicatedBlocks++;
            }
          }
        } catch (IOException e) {
          LOG.error("BlockGenerationStampUpgrade: could not get block locations for " +
                    file.getPath().toString() + " : " +
                    StringUtils.stringifyException(e));
          stats.errors++;
        }
      }
    }
  }
  
  void updateBlockLevelStats() {
    /* This iterates over all the blocks and updates various 
     * counts.
     * Since iterating over all the blocks at once would be quite 
     * large operation under lock, we iterate over all the files
     * and update the counts for blocks that belong to a file.
     */
      
    LOG.info("Starting update of block level stats. " +
             "This could take a few minutes");
    BlockLevelStats stats = new BlockLevelStats();
    updateBlockLevelStats("/", stats);
    stats.updatedAt = System.currentTimeMillis();
    
    LOG.info("Block level stats:\n\t" + stats.statusString("\n\t"));
    synchronized (this) {
      latestBlockLevelStats = stats;
    }
  }
}

/**
 * A status report object for Generation Stamp Upgrades
 */
class GenerationStampUpgradeStatusReport extends UpgradeStatusReport {

  String extraText = "";

  public GenerationStampUpgradeStatusReport() {
  }

  public GenerationStampUpgradeStatusReport(int version, short status,
                                            String extraText) {
    super(version, status, false);
    this.extraText = extraText;
  }

  @Override
  public String getStatusText(boolean details) {
    return super.getStatusText(details) + "\n\n" + extraText;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    extraText = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, extraText);
  }
}

