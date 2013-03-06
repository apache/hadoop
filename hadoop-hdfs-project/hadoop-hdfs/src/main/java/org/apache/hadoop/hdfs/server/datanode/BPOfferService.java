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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A thread per namenode to perform:
 * <ul>
 * <li> Pre-registration handshake with namenode</li>
 * <li> Registration with namenode</li>
 * <li> Send periodic heartbeats to the namenode</li>
 * <li> Handle commands received from the namenode</li>
 * </ul>
 */
@InterfaceAudience.Private
class BPOfferService implements Runnable {
  static final Log LOG = DataNode.LOG;

  final InetSocketAddress nnAddr;
  
  /**
   * Information about the namespace that this service
   * is registering with. This is assigned after
   * the first phase of the handshake.
   */
  NamespaceInfo bpNSInfo;

  /**
   * The registration information for this block pool.
   * This is assigned after the second phase of the
   * handshake.
   */
  DatanodeRegistration bpRegistration;
  
  long lastBlockReport = 0;
  long lastDeletedReport = 0;

  boolean resetBlockReportTime = true;

  Thread bpThread;
  DatanodeProtocol bpNamenode;
  private long lastHeartbeat = 0;
  private volatile boolean initialized = false;
  private final LinkedList<ReceivedDeletedBlockInfo> receivedAndDeletedBlockList
      = new LinkedList<ReceivedDeletedBlockInfo>();
  private volatile int pendingReceivedRequests = 0;
  private volatile boolean shouldServiceRun = true;
  UpgradeManagerDatanode upgradeManager = null;
  private final DataNode dn;
  private final DNConf dnConf;

  BPOfferService(InetSocketAddress nnAddr, DataNode dn) {
    this.dn = dn;
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
  }

  /**
   * Run an immediate heartbeat from all actors. Used by tests.
   */
  @VisibleForTesting
  void triggerHeartbeatForTests() throws IOException {
    synchronized(receivedAndDeletedBlockList) {
      lastHeartbeat = 0;
      receivedAndDeletedBlockList.notifyAll();
      while (lastHeartbeat == 0) {
        try {
          receivedAndDeletedBlockList.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
  
  /**
   * returns true if BP thread has completed initialization of storage
   * and has registered with the corresponding namenode
   * @return true if initialized
   */
  public boolean isInitialized() {
    return initialized;
  }
  
  public boolean isAlive() {
    return shouldServiceRun && bpThread.isAlive();
  }
  
  public String getBlockPoolId() {
    if (bpNSInfo != null) {
      return bpNSInfo.getBlockPoolID();
    } else {
      LOG.warn("Block pool ID needed, but service not yet registered with NN",
          new Exception("trace"));
      return null;
    }
  }
  
  public NamespaceInfo getNamespaceInfo() {
    return bpNSInfo;
  }
  
  public String toString() {
    if (bpNSInfo == null) {
      // If we haven't yet connected to our NN, we don't yet know our
      // own block pool ID.
      // If _none_ of the block pools have connected yet, we don't even
      // know the storage ID of this DN.
      String storageId = dn.getStorageId();
      if (storageId == null || "".equals(storageId)) {
        storageId = "unknown";
      }
      return "Block pool <registering> (storage id " + storageId +
        ") connecting to " + nnAddr;
    } else {
      return "Block pool " + getBlockPoolId() +
        " (storage id " + dn.getStorageId() +
        ") registered with " + nnAddr;
    }
  }
  
  InetSocketAddress getNNSocketAddress() {
    return nnAddr;
  }

  /**
   * Used to inject a spy NN in the unit tests.
   */
  @VisibleForTesting
  void setNameNode(DatanodeProtocol dnProtocol) {
    bpNamenode = dnProtocol;
  }

  /**
   * Perform the first part of the handshake with the NameNode.
   * This calls <code>versionRequest</code> to determine the NN's
   * namespace and version info. It automatically retries until
   * the NN responds or the DN is shutting down.
   * 
   * @return the NamespaceInfo
   * @throws IncorrectVersionException if the remote NN does not match
   * this DN's version
   */
  NamespaceInfo retrieveNamespaceInfo() throws IncorrectVersionException {
    NamespaceInfo nsInfo = null;
    while (shouldRun()) {
      try {
        nsInfo = bpNamenode.versionRequest();
        LOG.debug(this + " received versionRequest response: " + nsInfo);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.warn("Problem connecting to server: " + nnAddr);
      } catch(IOException e ) {  // namenode is not available
        LOG.warn("Problem connecting to server: " + nnAddr);
      }
      
      // try again in a second
      sleepAndLogInterrupts(5000, "requesting version info from NN");
    }
    
    if (nsInfo != null) {
      checkNNVersion(nsInfo);        
    }
    return nsInfo;
  }

  private void checkNNVersion(NamespaceInfo nsInfo)
      throws IncorrectVersionException {
    // build and layout versions should match
    String nsBuildVer = nsInfo.getBuildVersion();
    String stBuildVer = Storage.getBuildVersion();
    if (!nsBuildVer.equals(stBuildVer)) {
      LOG.warn("Data-node and name-node Build versions must be the same. " +
        "Namenode build version: " + nsBuildVer + "Datanode " +
        "build version: " + stBuildVer);
      throw new IncorrectVersionException(nsBuildVer, "namenode", stBuildVer);
    }

    if (HdfsConstants.LAYOUT_VERSION != nsInfo.getLayoutVersion()) {
      LOG.warn("Data-node and name-node layout versions must be the same." +
        " Expected: "+ HdfsConstants.LAYOUT_VERSION +
        " actual "+ bpNSInfo.getLayoutVersion());
      throw new IncorrectVersionException(
          bpNSInfo.getLayoutVersion(), "namenode");
    }
  }

  private void connectToNNAndHandshake() throws IOException {
    
    // get NN proxy
    bpNamenode = dn.connectToNN(nnAddr);
    
    // First phase of the handshake with NN - get the namespace
    // info.
    bpNSInfo = retrieveNamespaceInfo();
    
    // Now that we know the namespace ID, etc, we can pass this to the DN.
    // The DN can now initialize its local storage if we are the
    // first BP to handshake, etc.
    dn.initBlockPool(this);
    
    // Second phase of the handshake with the NN.
    register();
  }
  
  /**
   * This methods  arranges for the data node to send the block report at 
   * the next heartbeat.
   */
  void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = System.currentTimeMillis()
      - ( dnConf.blockReportInterval - DFSUtil.getRandom().nextInt((int)(delay)));
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - dnConf.blockReportInterval;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }

  void reportBadBlocks(ExtendedBlock block) {
    DatanodeInfo[] dnArr = { new DatanodeInfo(bpRegistration) };
    LocatedBlock[] blocks = { new LocatedBlock(block, dnArr) }; 
    
    try {
      bpNamenode.reportBadBlocks(blocks);  
    } catch (IOException e){
      /* One common reason is that NameNode could be in safe mode.
       * Should we keep on retrying in that case?
       */
      LOG.warn("Failed to report bad block " + block + " to namenode : "
          + " Exception", e);
    }
    
  }
  
  /**
   * Report received blocks and delete hints to the Namenode
   * @throws IOException
   */
  private void reportReceivedDeletedBlocks() throws IOException {
     // check if there are newly received blocks
    ReceivedDeletedBlockInfo[] receivedAndDeletedBlockArray = null;
    int currentReceivedRequestsCounter;
    synchronized (receivedAndDeletedBlockList) {
      currentReceivedRequestsCounter = pendingReceivedRequests;
      int numBlocks = receivedAndDeletedBlockList.size();
      if (numBlocks > 0) {
        //
        // Send newly-received and deleted blockids to namenode
        //
        receivedAndDeletedBlockArray = receivedAndDeletedBlockList
            .toArray(new ReceivedDeletedBlockInfo[numBlocks]);
      }
    }
    if (receivedAndDeletedBlockArray != null) {
      bpNamenode.blockReceivedAndDeleted(bpRegistration, getBlockPoolId(),
          receivedAndDeletedBlockArray);
      synchronized (receivedAndDeletedBlockList) {
        for (int i = 0; i < receivedAndDeletedBlockArray.length; i++) {
          receivedAndDeletedBlockList.remove(receivedAndDeletedBlockArray[i]);
        }
        pendingReceivedRequests -= currentReceivedRequestsCounter;
      }
    }
  }



  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint) {
    if (block == null || delHint == null) {
      throw new IllegalArgumentException(block == null ? "Block is null"
          : "delHint is null");
    }

    if (!block.getBlockPoolId().equals(getBlockPoolId())) {
      LOG.warn("BlockPool mismatch " + block.getBlockPoolId() + " vs. "
          + getBlockPoolId());
      return;
    }

    synchronized (receivedAndDeletedBlockList) {
      receivedAndDeletedBlockList.add(new ReceivedDeletedBlockInfo(block
          .getLocalBlock(), delHint));
      pendingReceivedRequests++;
      receivedAndDeletedBlockList.notifyAll();
    }
  }

  void notifyNamenodeDeletedBlock(ExtendedBlock block) {
    if (block == null) {
      throw new IllegalArgumentException("Block is null");
    }

    if (!block.getBlockPoolId().equals(getBlockPoolId())) {
      LOG.warn("BlockPool mismatch " + block.getBlockPoolId() + " vs. "
          + getBlockPoolId());
      return;
    }

    synchronized (receivedAndDeletedBlockList) {
      receivedAndDeletedBlockList.add(new ReceivedDeletedBlockInfo(block
          .getLocalBlock(), ReceivedDeletedBlockInfo.TODELETE_HINT));
    }
  }

  /**
   * Report the list blocks to the Namenode
   * @throws IOException
   */
  DatanodeCommand blockReport() throws IOException {
    // send block report if timer has expired.
    DatanodeCommand cmd = null;
    long startTime = now();
    if (startTime - lastBlockReport > dnConf.blockReportInterval) {

      // Create block report
      long brCreateStartTime = now();
      BlockListAsLongs bReport = dn.data.getBlockReport(getBlockPoolId());

      // Send block report
      long brSendStartTime = now();
      cmd = bpNamenode.blockReport(bpRegistration, getBlockPoolId(), bReport
          .getBlockListAsLongs());

      // Log the block report processing stats from Datanode perspective
      long brSendCost = now() - brSendStartTime;
      long brCreateCost = brSendStartTime - brCreateStartTime;
      dn.metrics.addBlockReport(brSendCost);
      LOG.info("BlockReport of " + bReport.getNumberOfBlocks()
          + " blocks took " + brCreateCost + " msec to generate and "
          + brSendCost + " msecs for RPC and NN processing");

      // If we have sent the first block report, then wait a random
      // time before we start the periodic block reports.
      if (resetBlockReportTime) {
        lastBlockReport = startTime - DFSUtil.getRandom().nextInt((int)(dnConf.blockReportInterval));
        resetBlockReportTime = false;
      } else {
        /* say the last block report was at 8:20:14. The current report
         * should have started around 9:20:14 (default 1 hour interval).
         * If current time is :
         *   1) normal like 9:20:18, next report should be at 10:20:14
         *   2) unexpected like 11:35:43, next report should be at 12:20:14
         */
        lastBlockReport += (now() - lastBlockReport) /
        dnConf.blockReportInterval * dnConf.blockReportInterval;
      }
      LOG.info("sent block report, processed command:" + cmd);
    }
    return cmd;
  }
  
  
  DatanodeCommand [] sendHeartBeat() throws IOException {
    return bpNamenode.sendHeartbeat(bpRegistration,
        dn.data.getCapacity(),
        dn.data.getDfsUsed(),
        dn.data.getRemaining(),
        dn.data.getBlockPoolUsed(getBlockPoolId()),
        dn.xmitsInProgress.get(),
        dn.getXceiverCount(), dn.data.getNumFailedVolumes());
  }
  
  //This must be called only by blockPoolManager
  void start() {
    if ((bpThread != null) && (bpThread.isAlive())) {
      //Thread is started already
      return;
    }
    bpThread = new Thread(this, formatThreadName());
    bpThread.setDaemon(true); // needed for JUnit testing
    bpThread.start();
  }
  
  private String formatThreadName() {
    Collection<URI> dataDirs = DataNode.getStorageDirs(dn.getConf());
    return "DataNode: [" +
      StringUtils.uriToString(dataDirs.toArray(new URI[0])) + "] " +
      " heartbeating to " + nnAddr;
  }

  //This must be called only by blockPoolManager.
  void stop() {
    shouldServiceRun = false;
    if (bpThread != null) {
        bpThread.interrupt();
    }
  }
  
  //This must be called only by blockPoolManager
  void join() {
    try {
      if (bpThread != null) {
        bpThread.join();
      }
    } catch (InterruptedException ie) { }
  }
  
  //Cleanup method to be called by current thread before exiting.
  private synchronized void cleanUp() {
    
    if(upgradeManager != null)
      upgradeManager.shutdownUpgrade();
    shouldServiceRun = false;
    RPC.stopProxy(bpNamenode);
    dn.shutdownBlockPool(this);
  }

  /**
   * Main loop for each BP thread. Run until shutdown,
   * forever calling remote NameNode functions.
   */
  private void offerService() throws Exception {
    LOG.info("For namenode " + nnAddr + " using DELETEREPORT_INTERVAL of "
        + dnConf.deleteReportInterval + " msec " + " BLOCKREPORT_INTERVAL of "
        + dnConf.blockReportInterval + "msec" + " Initial delay: "
        + dnConf.initialBlockReportDelay + "msec" + "; heartBeatInterval="
        + dnConf.heartBeatInterval);

    //
    // Now loop for a long time....
    //
    while (shouldRun()) {
      try {
        long startTime = now();

        //
        // Every so often, send heartbeat or block-report
        //
        if (startTime - lastHeartbeat > dnConf.heartBeatInterval) {
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          lastHeartbeat = startTime;
          if (!dn.areHeartbeatsDisabledForTests()) {
            DatanodeCommand[] cmds = sendHeartBeat();
            dn.metrics.addHeartbeat(now() - startTime);

            long startProcessCommands = now();
            if (!processCommand(cmds))
              continue;
            long endProcessCommands = now();
            if (endProcessCommands - startProcessCommands > 2000) {
              LOG.info("Took " + (endProcessCommands - startProcessCommands) +
                  "ms to process " + cmds.length + " commands from NN");
            }
          }
        }

        if (pendingReceivedRequests > 0
            || (startTime - lastDeletedReport > dnConf.deleteReportInterval)) {
          reportReceivedDeletedBlocks();
          lastDeletedReport = startTime;
        }

        DatanodeCommand cmd = blockReport();
        processCommand(cmd);

        // Now safe to start scanning the block pool
        if (dn.blockScanner != null) {
          dn.blockScanner.addBlockPool(this.getBlockPoolId());
        }

        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        //
        long waitTime = dnConf.heartBeatInterval - 
        (System.currentTimeMillis() - lastHeartbeat);
        synchronized(receivedAndDeletedBlockList) {
          if (waitTime > 0 && pendingReceivedRequests == 0) {
            try {
              receivedAndDeletedBlockList.wait(waitTime);
            } catch (InterruptedException ie) {
              LOG.warn("BPOfferService for " + this + " interrupted");
            }
          }
        } // synchronized
      } catch(RemoteException re) {
        String reClass = re.getClassName();
        if (UnregisteredNodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn(this + " is shutting down", re);
          shouldServiceRun = false;
          return;
        }
        LOG.warn("RemoteException in offerService", re);
        try {
          long sleepTime = Math.min(1000, dnConf.heartBeatInterval);
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      } catch (IOException e) {
        LOG.warn("IOException in offerService", e);
      }
    } // while (shouldRun())
  } // offerService

  /**
   * Register one bp with the corresponding NameNode
   * <p>
   * The bpDatanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and 
   * 2) to receive a registrationID
   *  
   * issued by the namenode to recognize registered datanodes.
   * 
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   * @throws IOException
   */
  void register() throws IOException {
    Preconditions.checkState(bpNSInfo != null,
        "register() should be called after handshake()");
    
    // The handshake() phase loaded the block pool storage
    // off disk - so update the bpRegistration object from that info
    bpRegistration = dn.createBPRegistration(bpNSInfo);

    LOG.info(this + " beginning handshake with NN");

    while (shouldRun()) {
      try {
        // Use returned registration from namenode with updated machine name.
        bpRegistration = bpNamenode.registerDatanode(bpRegistration);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + nnAddr);
        sleepAndLogInterrupts(1000, "connecting to server");
      }
    }
    
    LOG.info("Block pool " + this + " successfully registered with NN");
    dn.bpRegistrationSucceeded(bpRegistration, getBlockPoolId());

    // random short delay - helps scatter the BR from all DNs
    scheduleBlockReport(dnConf.initialBlockReportDelay);
  }


  private void sleepAndLogInterrupts(int millis,
      String stateString) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      LOG.info("BPOfferService " + this +
          " interrupted while " + stateString);
    }
  }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   *
   * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
   * happen either at shutdown or due to refreshNamenodes.
   */
  @Override
  public void run() {
    LOG.info(this + " starting to offer service");

    try {
      // init stuff
      try {
        // setup storage
        connectToNNAndHandshake();
      } catch (IOException ioe) {
        // Initial handshake, storage recovery or registration failed
        // End BPOfferService thread
        LOG.fatal("Initialization failed for block pool " + this, ioe);
        return;
      }

      initialized = true; // bp is initialized;
      
      while (shouldRun()) {
        try {
          startDistributedUpgradeIfNeeded();
          offerService();
        } catch (Exception ex) {
          LOG.error("Exception in BPOfferService for " + this, ex);
          sleepAndLogInterrupts(5000, "offering service");
        }
      }
    } catch (Throwable ex) {
      LOG.warn("Unexpected exception in block pool " + this, ex);
    } finally {
      LOG.warn("Ending block pool service for: " + this);
      cleanUp();
    }
  }

  private boolean shouldRun() {
    return shouldServiceRun && dn.shouldRun();
  }

  /**
   * Process an array of datanode commands
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise. 
   */
  private boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        try {
          if (processCommand(cmd) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }

  /**
   * 
   * @param cmd
   * @return true if further processing may be required or false otherwise. 
   * @throws IOException
   */
  private boolean processCommand(DatanodeCommand cmd) throws IOException {
    if (cmd == null)
      return true;
    final BlockCommand bcmd = 
      cmd instanceof BlockCommand? (BlockCommand)cmd: null;

    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      // Send a copy of a block to another datanode
      dn.transferBlocks(bcmd.getBlockPoolId(), bcmd.getBlocks(), bcmd.getTargets());
      dn.metrics.incrBlocksReplicated(bcmd.getBlocks().length);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      //
      // Some local block(s) are obsolete and can be 
      // safely garbage-collected.
      //
      Block toDelete[] = bcmd.getBlocks();
      try {
        if (dn.blockScanner != null) {
          dn.blockScanner.deleteBlocks(bcmd.getBlockPoolId(), toDelete);
        }
        // using global fsdataset
        dn.data.invalidate(bcmd.getBlockPoolId(), toDelete);
      } catch(IOException e) {
        // Exceptions caught here are not expected to be disk-related.
        throw e;
      }
      dn.metrics.incrBlocksRemoved(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // shut down the data node
      shouldServiceRun = false;
      return false;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("DatanodeCommand action: DNA_REGISTER");
      if (shouldRun()) {
        // re-retrieve namespace info to make sure that, if the NN
        // was restarted, we still match its version (HDFS-2120)
        retrieveNamespaceInfo();
        // and re-register
        register();
      }
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      String bp = ((DatanodeCommand.Finalize) cmd).getBlockPoolId(); 
      assert getBlockPoolId().equals(bp) :
        "BP " + getBlockPoolId() + " received DNA_FINALIZE " +
        "for other block pool " + bp;

      dn.finalizeUpgradeForPool(bp);
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      processDistributedUpgradeCommand((UpgradeCommand)cmd);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      dn.recoverBlocks(((BlockRecoveryCommand)cmd).getRecoveringBlocks());
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
      if (dn.isBlockTokenEnabled) {
        dn.blockPoolTokenSecretManager.setKeys(getBlockPoolId(), 
            ((KeyUpdateCommand) cmd).getExportedKeys());
      }
      break;
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
      LOG.info("DatanodeCommand action: DNA_BALANCERBANDWIDTHUPDATE");
      long bandwidth =
                 ((BalancerBandwidthCommand) cmd).getBalancerBandwidthValue();
      if (bandwidth > 0) {
        DataXceiverServer dxcs =
                     (DataXceiverServer) dn.dataXceiverServer.getRunnable();
        dxcs.balanceThrottler.setBandwidth(bandwidth);
      }
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }
  
  private void processDistributedUpgradeCommand(UpgradeCommand comm)
  throws IOException {
    UpgradeManagerDatanode upgradeManager = getUpgradeManager();
    upgradeManager.processUpgradeCommand(comm);
  }

  synchronized UpgradeManagerDatanode getUpgradeManager() {
    if(upgradeManager == null)
      upgradeManager = 
        new UpgradeManagerDatanode(dn, getBlockPoolId());
    
    return upgradeManager;
  }
  
  /**
   * Start distributed upgrade if it should be initiated by the data-node.
   */
  private void startDistributedUpgradeIfNeeded() throws IOException {
    UpgradeManagerDatanode um = getUpgradeManager();
    
    if(!um.getUpgradeState())
      return;
    um.setUpgradeState(false, um.getUpgradeVersion());
    um.startUpgrade();
    return;
  }

  @VisibleForTesting
  DatanodeProtocol getBpNamenode() {
    return bpNamenode;
  }

  @VisibleForTesting
  void setBpNamenode(DatanodeProtocol bpNamenode) {
    this.bpNamenode = bpNamenode;
  }
}
