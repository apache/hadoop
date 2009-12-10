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

import java.io.IOException;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.FSImage.CheckpointStates;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.util.Daemon;

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
  private long checkpointPeriod;	// in seconds
  private long checkpointSize;    // size (in MB) of current Edit Log

  private BackupStorage getFSImage() {
    return (BackupStorage)backupNode.getFSImage();
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
    checkpointPeriod = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    checkpointSize = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_SIZE_KEY, 
                                  DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_SIZE_DEFAULT);

    HttpServer httpServer = backupNode.httpServer;
    httpServer.setAttribute("name.system.image", getFSImage());
    httpServer.setAttribute("name.conf", conf);
    httpServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);

    LOG.info("Checkpoint Period : " + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.info("Log Size Trigger  : " + checkpointSize + " bytes " +
             "(" + checkpointSize/1024 + " KB)");
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
    if(!backupNode.shouldCheckpointAtStartup())
      lastCheckpointTime = FSNamesystem.now();
    while(shouldRun) {
      try {
        long now = FSNamesystem.now();
        boolean shouldCheckpoint = false;
        if(now >= lastCheckpointTime + periodMSec) {
          shouldCheckpoint = true;
        } else {
          long size = getJournalSize();
          if(size >= checkpointSize)
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

  private long getJournalSize() throws IOException {
    // If BACKUP node has been loaded
    // get edits size from the local file. ACTIVE has the same.
    if(backupNode.isRole(NamenodeRole.BACKUP)
        && getFSImage().getEditLog().isOpen())
      return backupNode.journalSize();
    // Go to the ACTIVE node for its size
    return getNamenode().journalSize(backupNode.getRegistration());
  }

  /**
   * Download <code>fsimage</code> and <code>edits</code>
   * files from the remote name-node.
   */
  private void downloadCheckpoint(CheckpointSignature sig) throws IOException {
    // Retrieve image file
    String fileid = "getimage=1";
    Collection<File> list = getFSImage().getFiles(NameNodeFile.IMAGE,
        NameNodeDirType.IMAGE);
    File[] files = list.toArray(new File[list.size()]);
    assert files.length > 0 : "No checkpoint targets.";
    String nnHttpAddr = backupNode.nnHttpAddress;
    TransferFsImage.getFileClient(nnHttpAddr, fileid, files);
    LOG.info("Downloaded file " + files[0].getName() + " size " +
             files[0].length() + " bytes.");

    // Retrieve edits file
    fileid = "getedit=1";
    list = getFSImage().getFiles(NameNodeFile.EDITS, NameNodeDirType.EDITS);
    files = list.toArray(new File[list.size()]);
    assert files.length > 0 : "No checkpoint targets.";
    TransferFsImage.getFileClient(nnHttpAddr, fileid, files);
    LOG.info("Downloaded file " + files[0].getName() + " size " +
        files[0].length() + " bytes.");
  }

  /**
   * Copy the new image into remote name-node.
   */
  private void uploadCheckpoint(CheckpointSignature sig) throws IOException {
    InetSocketAddress httpSocAddr = backupNode.getHttpAddress();
    int httpPort = httpSocAddr.getPort();
    String fileid = "putimage=1&port=" + httpPort +
      "&machine=" +
      InetAddress.getLocalHost().getHostAddress() +
      "&token=" + sig.toString();
    LOG.info("Posted URL " + backupNode.nnHttpAddress + fileid);
    TransferFsImage.getFileClient(backupNode.nnHttpAddress, fileid, (File[])null);
  }

  /**
   * Create a new checkpoint
   */
  void doCheckpoint() throws IOException {
    long startTime = FSNamesystem.now();
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

    CheckpointSignature sig = cpCmd.getSignature();
    assert FSConstants.LAYOUT_VERSION == sig.getLayoutVersion() :
      "Signature should have current layout version. Expected: "
      + FSConstants.LAYOUT_VERSION + " actual "+ sig.getLayoutVersion();
    assert !backupNode.isRole(NamenodeRole.CHECKPOINT) ||
      cpCmd.isImageObsolete() : "checkpoint node should always download image.";
    backupNode.setCheckpointState(CheckpointStates.UPLOAD_START);
    if(cpCmd.isImageObsolete()) {
      // First reset storage on disk and memory state
      backupNode.resetNamespace();
      downloadCheckpoint(sig);
    }

    BackupStorage bnImage = getFSImage();
    bnImage.loadCheckpoint(sig);
    sig.validateStorageInfo(bnImage);
    bnImage.saveCheckpoint();

    if(cpCmd.needToReturnImage())
      uploadCheckpoint(sig);

    getNamenode().endCheckpoint(backupNode.getRegistration(), sig);

    bnImage.convergeJournalSpool();
    backupNode.setRegistration(); // keep registration up to date
    if(backupNode.isRole(NamenodeRole.CHECKPOINT))
        getFSImage().getEditLog().close();
    LOG.info("Checkpoint completed in "
        + (FSNamesystem.now() - startTime)/1000 + " seconds."
        +	" New Image Size: " + bnImage.getFsImageName().length());
  }
}
