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
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
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

  private final BackupNode backupNode;
  volatile boolean shouldRun;

  private String infoBindAddress;

  private CheckpointConf checkpointConf;
  private final Configuration conf;

  private BackupImage getFSImage() {
    return (BackupImage)backupNode.getFSImage();
  }

  private NamenodeProtocol getRemoteNamenodeProxy(){
    return backupNode.namenode;
  }

  /**
   * Create a connection to the primary namenode.
   */
  Checkpointer(Configuration conf, BackupNode bnNode)  throws IOException {
    this.conf = conf;
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
  private void initialize(Configuration conf) throws IOException {
    // Create connection to the namenode.
    shouldRun = true;

    // Initialize other scheduling parameters from the configuration
    checkpointConf = new CheckpointConf(conf);

    // Pull out exact http address for posting url to avoid ip aliasing issues
    String fullInfoAddr = conf.get(DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, 
                                   DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT);
    infoBindAddress = fullInfoAddr.substring(0, fullInfoAddr.indexOf(":"));

    LOG.info("Checkpoint Period : " +
             checkpointConf.getPeriod() + " secs " +
             "(" + checkpointConf.getPeriod()/60 + " min)");
    LOG.info("Transactions count is  : " +
             checkpointConf.getTxnCount() +
             ", to trigger checkpoint");
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
  @Override
  public void run() {
    // Check the size of the edit log once every 5 minutes.
    long periodMSec = 5 * 60;   // 5 minutes
    if(checkpointConf.getPeriod() < periodMSec) {
      periodMSec = checkpointConf.getPeriod();
    }
    periodMSec *= 1000;

    long lastCheckpointTime = 0;
    if (!backupNode.shouldCheckpointAtStartup()) {
      lastCheckpointTime = monotonicNow();
    }
    while(shouldRun) {
      try {
        long now = monotonicNow();
        boolean shouldCheckpoint = false;
        if(now >= lastCheckpointTime + periodMSec) {
          shouldCheckpoint = true;
        } else {
          long txns = countUncheckpointedTxns();
          if(txns >= checkpointConf.getTxnCount())
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
    long curTxId = getRemoteNamenodeProxy().getTransactionID();
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

    long startTime = monotonicNow();
    bnImage.freezeNamespaceAtNextRoll();
    
    NamenodeCommand cmd = 
      getRemoteNamenodeProxy().startCheckpoint(backupNode.getRegistration());
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
      getRemoteNamenodeProxy().getEditLogManifest(bnImage.getLastAppliedTxId() + 1);

    boolean needReloadImage = false;
    if (!manifest.getLogs().isEmpty()) {
      RemoteEditLog firstRemoteLog = manifest.getLogs().get(0);
      // we don't have enough logs to roll forward using only logs. Need
      // to download and load the image.
      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
        LOG.info("Unable to roll forward using only logs. Downloading " +
            "image with txid " + sig.mostRecentCheckpointTxId);
        MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
            backupNode.nnHttpAddress, sig.mostRecentCheckpointTxId, bnStorage,
            true);
        bnImage.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE,
            sig.mostRecentCheckpointTxId, downloadedHash);
        lastApplied = sig.mostRecentCheckpointTxId;
        needReloadImage = true;
      }

      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
        throw new IOException("No logs to roll forward from " + lastApplied);
      }
  
      // get edits files
      for (RemoteEditLog log : manifest.getLogs()) {
        TransferFsImage.downloadEditsToStorage(
            backupNode.nnHttpAddress, log, bnStorage);
      }

      if(needReloadImage) {
        LOG.info("Loading image with txid " + sig.mostRecentCheckpointTxId);
        File file = bnStorage.findImageFile(NameNodeFile.IMAGE,
            sig.mostRecentCheckpointTxId);
        bnImage.reloadFromImageFile(file, backupNode.getNamesystem());
      }
      rollForwardByApplyingLogs(manifest, bnImage, backupNode.getNamesystem());
    }
    
    long txid = bnImage.getLastAppliedTxId();
    
    backupNode.namesystem.writeLock();
    try {
      backupNode.namesystem.setImageLoaded();
      if(backupNode.namesystem.getBlocksTotal() > 0) {
        backupNode.namesystem.setBlockTotal();
      }
      bnImage.saveFSImageInAllDirs(backupNode.getNamesystem(), txid);
      bnStorage.writeAll();
    } finally {
      backupNode.namesystem.writeUnlock("doCheckpoint");
    }

    if(cpCmd.needToReturnImage()) {
      TransferFsImage.uploadImageFromStorage(backupNode.nnHttpAddress, conf,
          bnStorage, NameNodeFile.IMAGE, txid);
    }

    getRemoteNamenodeProxy().endCheckpoint(backupNode.getRegistration(), sig);

    if (backupNode.getRole() == NamenodeRole.BACKUP) {
      bnImage.convergeJournalSpool();
    }
    backupNode.setRegistration(); // keep registration up to date
    
    long imageSize = bnImage.getStorage().getFsImageName(txid).length();
    LOG.info("Checkpoint completed in "
        + (monotonicNow() - startTime)/1000 + " seconds."
        + " New Image Size: " + imageSize);
  }

  private URL getImageListenAddress() {
    InetSocketAddress httpSocAddr = backupNode.getHttpAddress();
    int httpPort = httpSocAddr.getPort();
    try {
      return new URL(DFSUtil.getHttpClientScheme(conf) + "://" + infoBindAddress + ":" + httpPort);
    } catch (MalformedURLException e) {
      // Unreachable
      throw new RuntimeException(e);
    }
  }

  static void rollForwardByApplyingLogs(
      RemoteEditLogManifest manifest,
      FSImage dstImage,
      FSNamesystem dstNamesystem) throws IOException {
    NNStorage dstStorage = dstImage.getStorage();
  
    List<EditLogInputStream> editsStreams = Lists.newArrayList();    
    for (RemoteEditLog log : manifest.getLogs()) {
      if (log.getEndTxId() > dstImage.getLastAppliedTxId()) {
        File f = dstStorage.findFinalizedEditsFile(
            log.getStartTxId(), log.getEndTxId());
        editsStreams.add(new EditLogFileInputStream(f, log.getStartTxId(), 
                                                    log.getEndTxId(), true));
      }
    }
    LOG.info("Checkpointer about to load edits from " +
        editsStreams.size() + " stream(s).");
    dstImage.loadEdits(editsStreams, dstNamesystem);
  }
}
