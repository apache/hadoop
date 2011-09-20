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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Daemon;

import com.google.common.collect.Lists;

/**
 * The Checkpointer is responsible for supporting periodic checkpoints 
 * of the HDFS metadata.
 *
 * The Checkpointer is a daemon that periodically wakes up
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * 
 * The start of a checkpoint is triggered by one of the two factors:
 * (1) time or (2) the size of the edits file.
 */
class Checkpointer extends Daemon {
  public static final Log LOG = 
    LogFactory.getLog(Checkpointer.class.getName());

  private BackupNode backupNode;
  volatile boolean shouldRun;
  private long checkpointPeriod;    // in seconds
  // Transactions count to trigger the checkpoint
  private long checkpointTxnCount; 

  private String infoBindAddress;

  private BackupImage getFSImage() {
    return (BackupImage)backupNode.getFSImage();
  }

  private NamenodeProtocol getNamenode(){
    return backupNode.namenode;
  }

  /**
   * Create a connection to the primary namenode.
   */
  Checkpointer(Configuration conf, BackupNode bnNode)  throws IOException {
    this.backupNode = bnNode;
    try {
      initialize(conf);
    } catch(IOException e) {
      LOG.warn("Checkpointer got exception", e);
      shutdown();
      throw e;
    }
  }

  /**
   * Initialize checkpoint.
   */
  @SuppressWarnings("deprecation")
  private void initialize(Configuration conf) throws IOException {
    // Create connection to the namenode.
    shouldRun = true;

    // Initialize other scheduling parameters from the configuration
    checkpointPeriod = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    checkpointTxnCount = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 
                                  DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
    SecondaryNameNode.warnForDeprecatedConfigs(conf);

    // Pull out exact http address for posting url to avoid ip aliasing issues
    String fullInfoAddr = conf.get(DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, 
                                   DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT);
    infoBindAddress = fullInfoAddr.substring(0, fullInfoAddr.indexOf(":"));

    LOG.info("Checkpoint Period : " + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.info("Transactions count is  : " + checkpointTxnCount + ", to trigger checkpoint");
  }

  /**
   * Shut down the checkpointer.
   */
  void shutdown() {
    shouldRun = false;
    backupNode.stop();
  }

  //
  // The main work loop
  //
  public void run() {
    // Check the size of the edit log once every 5 minutes.
    long periodMSec = 5 * 60;   // 5 minutes
    if(checkpointPeriod < periodMSec) {
      periodMSec = checkpointPeriod;
    }
    periodMSec *= 1000;

    long lastCheckpointTime = 0;
    if (!backupNode.shouldCheckpointAtStartup()) {
      lastCheckpointTime = now();
    }
    while(shouldRun) {
      try {
        long now = now();
        boolean shouldCheckpoint = false;
        if(now >= lastCheckpointTime + periodMSec) {
          shouldCheckpoint = true;
        } else {
          long txns = countUncheckpointedTxns();
          if(txns >= checkpointTxnCount)
            shouldCheckpoint = true;
        }
        if(shouldCheckpoint) {
          doCheckpoint();
          lastCheckpointTime = now;
        }
      } catch(IOException e) {
        LOG.error("Exception in doCheckpoint: ", e);
      } catch(Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint: ", e);
        shutdown();
        break;
      }
      try {
        Thread.sleep(periodMSec);
      } catch(InterruptedException ie) {
        // do nothing
      }
    }
  }

  private long countUncheckpointedTxns() throws IOException {
    long curTxId = getNamenode().getTransactionID();
    long uncheckpointedTxns = curTxId -
      getFSImage().getStorage().getMostRecentCheckpointTxId();
    assert uncheckpointedTxns >= 0;
    return uncheckpointedTxns;
  }

  /**
   * Create a new checkpoint
   */
  void doCheckpoint() throws IOException {
    BackupImage bnImage = getFSImage();
    NNStorage bnStorage = bnImage.getStorage();

    long startTime = now();
    bnImage.freezeNamespaceAtNextRoll();
    
    NamenodeCommand cmd = 
      getNamenode().startCheckpoint(backupNode.getRegistration());
    CheckpointCommand cpCmd = null;
    switch(cmd.getAction()) {
      case NamenodeProtocol.ACT_SHUTDOWN:
        shutdown();
        throw new IOException("Name-node " + backupNode.nnRpcAddress
                                           + " requested shutdown.");
      case NamenodeProtocol.ACT_CHECKPOINT:
        cpCmd = (CheckpointCommand)cmd;
        break;
      default:
        throw new IOException("Unsupported NamenodeCommand: "+cmd.getAction());
    }

    bnImage.waitUntilNamespaceFrozen();
    
    CheckpointSignature sig = cpCmd.getSignature();

    // Make sure we're talking to the same NN!
    sig.validateStorageInfo(bnImage);

    long lastApplied = bnImage.getLastAppliedTxId();
    LOG.debug("Doing checkpoint. Last applied: " + lastApplied);
    RemoteEditLogManifest manifest =
      getNamenode().getEditLogManifest(bnImage.getLastAppliedTxId() + 1);

    if (!manifest.getLogs().isEmpty()) {
      RemoteEditLog firstRemoteLog = manifest.getLogs().get(0);
      // we don't have enough logs to roll forward using only logs. Need
      // to download and load the image.
      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
        LOG.info("Unable to roll forward using only logs. Downloading " +
            "image with txid " + sig.mostRecentCheckpointTxId);
        MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
            backupNode.nnHttpAddress, sig.mostRecentCheckpointTxId,
            bnStorage, true);
        bnImage.saveDigestAndRenameCheckpointImage(
            sig.mostRecentCheckpointTxId, downloadedHash);
        
        LOG.info("Loading image with txid " + sig.mostRecentCheckpointTxId);
        File file = bnStorage.findImageFile(sig.mostRecentCheckpointTxId);
        bnImage.reloadFromImageFile(file, backupNode.getNamesystem());
      }
      
      lastApplied = bnImage.getLastAppliedTxId();
      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
        throw new IOException("No logs to roll forward from " + lastApplied);
      }
  
      // get edits files
      for (RemoteEditLog log : manifest.getLogs()) {
        TransferFsImage.downloadEditsToStorage(
            backupNode.nnHttpAddress, log, bnStorage);
      }
  
      rollForwardByApplyingLogs(manifest, bnImage, backupNode.getNamesystem());
    }

    long txid = bnImage.getLastAppliedTxId();
    bnImage.saveFSImageInAllDirs(backupNode.getNamesystem(), txid);
    bnStorage.writeAll();

    if(cpCmd.needToReturnImage()) {
      TransferFsImage.uploadImageFromStorage(
          backupNode.nnHttpAddress, getImageListenAddress(),
          bnStorage, txid);
    }

    getNamenode().endCheckpoint(backupNode.getRegistration(), sig);

    if (backupNode.getRole() == NamenodeRole.BACKUP) {
      bnImage.convergeJournalSpool();
    }
    backupNode.setRegistration(); // keep registration up to date
    
    long imageSize = bnImage.getStorage().getFsImageName(txid).length();
    LOG.info("Checkpoint completed in "
        + (now() - startTime)/1000 + " seconds."
        + " New Image Size: " + imageSize);
  }

  private InetSocketAddress getImageListenAddress() {
    InetSocketAddress httpSocAddr = backupNode.getHttpAddress();
    int httpPort = httpSocAddr.getPort();
    return new InetSocketAddress(infoBindAddress, httpPort);
  }

  static void rollForwardByApplyingLogs(
      RemoteEditLogManifest manifest,
      FSImage dstImage,
      FSNamesystem dstNamesystem) throws IOException {
    NNStorage dstStorage = dstImage.getStorage();
  
    List<EditLogInputStream> editsStreams = Lists.newArrayList();    
    for (RemoteEditLog log : manifest.getLogs()) {
      File f = dstStorage.findFinalizedEditsFile(
          log.getStartTxId(), log.getEndTxId());
      if (log.getStartTxId() > dstImage.getLastAppliedTxId()) {
        editsStreams.add(new EditLogFileInputStream(f, log.getStartTxId(), 
                                                    log.getEndTxId()));
       }
    }
    LOG.info("Checkpointer about to load edits from " +
        editsStreams.size() + " stream(s).");
    dstImage.loadEdits(editsStreams, dstNamesystem);
  }
}
