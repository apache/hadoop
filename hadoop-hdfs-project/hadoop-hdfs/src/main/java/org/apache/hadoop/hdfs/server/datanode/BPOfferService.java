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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * One instance per block-pool/namespace on the DN, which handles the
 * heartbeats to the active and standby NNs for that namespace.
 * This class manages an instance of {@link BPServiceActor} for each NN,
 * and delegates calls to both NNs. 
 * It also maintains the state about which of the NNs is considered active.
 */
@InterfaceAudience.Private
class BPOfferService {
  static final Log LOG = DataNode.LOG;

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
  volatile DatanodeRegistration bpRegistration;
  
  private final DataNode dn;

  /**
   * A reference to the BPServiceActor associated with the currently
   * ACTIVE NN. In the case that all NameNodes are in STANDBY mode,
   * this can be null. If non-null, this must always refer to a member
   * of the {@link #bpServices} list.
   */
  private BPServiceActor bpServiceToActive = null;
  
  /**
   * The list of all actors for namenodes in this nameservice, regardless
   * of their active or standby states.
   */
  private final List<BPServiceActor> bpServices =
    new CopyOnWriteArrayList<BPServiceActor>();

  /**
   * Each time we receive a heartbeat from a NN claiming to be ACTIVE,
   * we record that NN's most recent transaction ID here, so long as it
   * is more recent than the previous value. This allows us to detect
   * split-brain scenarios in which a prior NN is still asserting its
   * ACTIVE state but with a too-low transaction ID. See HDFS-2627
   * for details. 
   */
  private long lastActiveClaimTxId = -1;

  private final ReentrantReadWriteLock mReadWriteLock =
      new ReentrantReadWriteLock();
  private final Lock mReadLock  = mReadWriteLock.readLock();
  private final Lock mWriteLock = mReadWriteLock.writeLock();

  // utility methods to acquire and release read lock and write lock
  void readLock() {
    mReadLock.lock();
  }

  void readUnlock() {
    mReadLock.unlock();
  }

  void writeLock() {
    mWriteLock.lock();
  }

  void writeUnlock() {
    mWriteLock.unlock();
  }

  BPOfferService(List<InetSocketAddress> nnAddrs, DataNode dn) {
    Preconditions.checkArgument(!nnAddrs.isEmpty(),
        "Must pass at least one NN.");
    this.dn = dn;

    for (InetSocketAddress addr : nnAddrs) {
      this.bpServices.add(new BPServiceActor(addr, this));
    }
  }

  void refreshNNList(ArrayList<InetSocketAddress> addrs) throws IOException {
    Set<InetSocketAddress> oldAddrs = Sets.newHashSet();
    for (BPServiceActor actor : bpServices) {
      oldAddrs.add(actor.getNNSocketAddress());
    }
    Set<InetSocketAddress> newAddrs = Sets.newHashSet(addrs);
    
    if (!Sets.symmetricDifference(oldAddrs, newAddrs).isEmpty()) {
      // Keep things simple for now -- we can implement this at a later date.
      throw new IOException(
          "HA does not currently support adding a new standby to a running DN. " +
          "Please do a rolling restart of DNs to reconfigure the list of NNs.");
    }
  }

  /**
   * @return true if the service has registered with at least one NameNode.
   */
  boolean isInitialized() {
    return bpRegistration != null;
  }
  
  /**
   * @return true if there is at least one actor thread running which is
   * talking to a NameNode.
   */
  boolean isAlive() {
    for (BPServiceActor actor : bpServices) {
      if (actor.isAlive()) {
        return true;
      }
    }
    return false;
  }

  String getBlockPoolId() {
    readLock();
    try {
      if (bpNSInfo != null) {
        return bpNSInfo.getBlockPoolID();
      } else {
        LOG.warn("Block pool ID needed, but service not yet registered with NN",
            new Exception("trace"));
        return null;
      }
    } finally {
      readUnlock();
    }
  }

  boolean hasBlockPoolId() {
    return getNamespaceInfo() != null;
  }

  NamespaceInfo getNamespaceInfo() {
    readLock();
    try {
      return bpNSInfo;
    } finally {
      readUnlock();
    }
  }

  @Override
  public String toString() {
    readLock();
    try {
      if (bpNSInfo == null) {
        // If we haven't yet connected to our NN, we don't yet know our
        // own block pool ID.
        // If _none_ of the block pools have connected yet, we don't even
        // know the DatanodeID ID of this DN.
        String datanodeUuid = dn.getDatanodeUuid();

        if (datanodeUuid == null || datanodeUuid.isEmpty()) {
          datanodeUuid = "unassigned";
        }
        return "Block pool <registering> (Datanode Uuid " + datanodeUuid + ")";
      } else {
        return "Block pool " + getBlockPoolId() +
            " (Datanode Uuid " + dn.getDatanodeUuid() +
            ")";
      }
    } finally {
      readUnlock();
    }
  }
  
  void reportBadBlocks(ExtendedBlock block,
                       String storageUuid, StorageType storageType) {
    checkBlock(block);
    for (BPServiceActor actor : bpServices) {
      ReportBadBlockAction rbbAction = new ReportBadBlockAction
          (block, storageUuid, storageType);
      actor.bpThreadEnqueue(rbbAction);
    }
  }
  
  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeReceivedBlock(
      ExtendedBlock block, String delHint, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(
        block.getLocalBlock(),
        ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK,
        delHint);

    for (BPServiceActor actor : bpServices) {
      actor.notifyNamenodeBlock(bInfo, storageUuid, true);
    }
  }

  private void checkBlock(ExtendedBlock block) {
    Preconditions.checkArgument(block != null,
        "block is null");
    Preconditions.checkArgument(block.getBlockPoolId().equals(getBlockPoolId()),
        "block belongs to BP %s instead of BP %s",
        block.getBlockPoolId(), getBlockPoolId());
  }
  
  void notifyNamenodeDeletedBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(
       block.getLocalBlock(), BlockStatus.DELETED_BLOCK, null);
    
    for (BPServiceActor actor : bpServices) {
      actor.notifyNamenodeDeletedBlock(bInfo, storageUuid);
    }
  }
  
  void notifyNamenodeReceivingBlock(ExtendedBlock block, String storageUuid) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(
       block.getLocalBlock(), BlockStatus.RECEIVING_BLOCK, null);
    
    for (BPServiceActor actor : bpServices) {
      actor.notifyNamenodeBlock(bInfo, storageUuid, false);
    }
  }

  //This must be called only by blockPoolManager
  void start() {
    for (BPServiceActor actor : bpServices) {
      actor.start();
    }
  }
  
  //This must be called only by blockPoolManager.
  void stop() {
    for (BPServiceActor actor : bpServices) {
      actor.stop();
    }
  }
  
  //This must be called only by blockPoolManager
  void join() {
    for (BPServiceActor actor : bpServices) {
      actor.join();
    }
  }

  DataNode getDataNode() {
    return dn;
  }

  /**
   * Called by the BPServiceActors when they handshake to a NN.
   * If this is the first NN connection, this sets the namespace info
   * for this BPOfferService. If it's a connection to a new NN, it
   * verifies that this namespace matches (eg to prevent a misconfiguration
   * where a StandbyNode from a different cluster is specified)
   */
  void verifyAndSetNamespaceInfo(NamespaceInfo nsInfo) throws IOException {
    writeLock();
    try {
      if (this.bpNSInfo == null) {
        this.bpNSInfo = nsInfo;
        boolean success = false;

        // Now that we know the namespace ID, etc, we can pass this to the DN.
        // The DN can now initialize its local storage if we are the
        // first BP to handshake, etc.
        try {
          dn.initBlockPool(this);
          success = true;
        } finally {
          if (!success) {
            // The datanode failed to initialize the BP. We need to reset
            // the namespace info so that other BPService actors still have
            // a chance to set it, and re-initialize the datanode.
            this.bpNSInfo = null;
          }
        }
      } else {
        checkNSEquality(bpNSInfo.getBlockPoolID(), nsInfo.getBlockPoolID(),
            "Blockpool ID");
        checkNSEquality(bpNSInfo.getNamespaceID(), nsInfo.getNamespaceID(),
            "Namespace ID");
        checkNSEquality(bpNSInfo.getClusterID(), nsInfo.getClusterID(),
            "Cluster ID");
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * After one of the BPServiceActors registers successfully with the
   * NN, it calls this function to verify that the NN it connected to
   * is consistent with other NNs serving the block-pool.
   */
  void registrationSucceeded(BPServiceActor bpServiceActor,
      DatanodeRegistration reg) throws IOException {
    writeLock();
    try {
      if (bpRegistration != null) {
        checkNSEquality(bpRegistration.getStorageInfo().getNamespaceID(),
            reg.getStorageInfo().getNamespaceID(), "namespace ID");
        checkNSEquality(bpRegistration.getStorageInfo().getClusterID(),
            reg.getStorageInfo().getClusterID(), "cluster ID");
      }
      bpRegistration = reg;

      dn.bpRegistrationSucceeded(bpRegistration, getBlockPoolId());
      // Add the initial block token secret keys to the DN's secret manager.
      if (dn.isBlockTokenEnabled) {
        dn.blockPoolTokenSecretManager.addKeys(getBlockPoolId(),
            reg.getExportedKeys());
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Verify equality of two namespace-related fields, throwing
   * an exception if they are unequal.
   */
  private static void checkNSEquality(
      Object ourID, Object theirID,
      String idHelpText) throws IOException {
    if (!ourID.equals(theirID)) {
      throw new IOException(idHelpText + " mismatch: " +
          "previously connected to " + idHelpText + " " + ourID + 
          " but now connected to " + idHelpText + " " + theirID);
    }
  }

  DatanodeRegistration createRegistration() {
    writeLock();
    try {
      Preconditions.checkState(bpNSInfo != null,
          "getRegistration() can only be called after initial handshake");
      return dn.createBPRegistration(bpNSInfo);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Called when an actor shuts down. If this is the last actor
   * to shut down, shuts down the whole blockpool in the DN.
   */
  void shutdownActor(BPServiceActor actor) {
    writeLock();
    try {
      if (bpServiceToActive == actor) {
        bpServiceToActive = null;
      }

      bpServices.remove(actor);

      if (bpServices.isEmpty()) {
        dn.shutdownBlockPool(this);
      }
    } finally {
      writeUnlock();
    }
  }
  

  /**
   * Called by the DN to report an error to the NNs.
   */
  void trySendErrorReport(int errCode, String errMsg) {
    for (BPServiceActor actor : bpServices) {
      ErrorReportAction errorReportAction = new ErrorReportAction 
          (errCode, errMsg);
      actor.bpThreadEnqueue(errorReportAction);
    }
  }

  /**
   * Ask each of the actors to schedule a block report after
   * the specified delay.
   */
  void scheduleBlockReport(long delay) {
    for (BPServiceActor actor : bpServices) {
      actor.getScheduler().scheduleBlockReport(delay);
    }
  }

  /**
   * Ask each of the actors to report a bad block hosted on another DN.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block) {
    for (BPServiceActor actor : bpServices) {
      try {
        actor.reportRemoteBadBlock(dnInfo, block);
      } catch (IOException e) {
        LOG.warn("Couldn't report bad block " + block + " to " + actor,
            e);
      }
    }
  }

  /**
   * @return a proxy to the active NN, or null if the BPOS has not
   * acknowledged any NN as active yet.
   */
  DatanodeProtocolClientSideTranslatorPB getActiveNN() {
    readLock();
    try {
      if (bpServiceToActive != null) {
        return bpServiceToActive.bpNamenode;
      } else {
        return null;
      }
    } finally {
      readUnlock();
    }
  }

  @VisibleForTesting
  List<BPServiceActor> getBPServiceActors() {
    return Lists.newArrayList(bpServices);
  }
  
  /**
   * Signal the current rolling upgrade status as indicated by the NN.
   * @param rollingUpgradeStatus rolling upgrade status
   */
  void signalRollingUpgrade(RollingUpgradeStatus rollingUpgradeStatus)
      throws IOException {
    if (rollingUpgradeStatus == null) {
      return;
    }
    String bpid = getBlockPoolId();
    if (!rollingUpgradeStatus.isFinalized()) {
      dn.getFSDataset().enableTrash(bpid);
      dn.getFSDataset().setRollingUpgradeMarker(bpid);
    } else {
      dn.getFSDataset().clearTrash(bpid);
      dn.getFSDataset().clearRollingUpgradeMarker(bpid);
    }
  }

  /**
   * Update the BPOS's view of which NN is active, based on a heartbeat
   * response from one of the actors.
   * 
   * @param actor the actor which received the heartbeat
   * @param nnHaState the HA-related heartbeat contents
   */
  void updateActorStatesFromHeartbeat(
      BPServiceActor actor,
      NNHAStatusHeartbeat nnHaState) {
    writeLock();
    try {
      final long txid = nnHaState.getTxId();

      final boolean nnClaimsActive =
          nnHaState.getState() == HAServiceState.ACTIVE;
      final boolean bposThinksActive = bpServiceToActive == actor;
      final boolean isMoreRecentClaim = txid > lastActiveClaimTxId;

      if (nnClaimsActive && !bposThinksActive) {
        LOG.info("Namenode " + actor + " trying to claim ACTIVE state with " +
            "txid=" + txid);
        if (!isMoreRecentClaim) {
          // Split-brain scenario - an NN is trying to claim active
          // state when a different NN has already claimed it with a higher
          // txid.
          LOG.warn("NN " + actor + " tried to claim ACTIVE state at txid=" +
              txid + " but there was already a more recent claim at txid=" +
              lastActiveClaimTxId);
          return;
        } else {
          if (bpServiceToActive == null) {
            LOG.info("Acknowledging ACTIVE Namenode " + actor);
          } else {
            LOG.info("Namenode " + actor + " taking over ACTIVE state from " +
                bpServiceToActive + " at higher txid=" + txid);
          }
          bpServiceToActive = actor;
        }
      } else if (!nnClaimsActive && bposThinksActive) {
        LOG.info("Namenode " + actor + " relinquishing ACTIVE state with " +
            "txid=" + nnHaState.getTxId());
        bpServiceToActive = null;
      }

      if (bpServiceToActive == actor) {
        assert txid >= lastActiveClaimTxId;
        lastActiveClaimTxId = txid;
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * @return true if the given NN address is one of the NNs for this
   * block pool
   */
  boolean containsNN(InetSocketAddress addr) {
    for (BPServiceActor actor : bpServices) {
      if (actor.getNNSocketAddress().equals(addr)) {
        return true;
      }
    }
    return false;
  }
  
  @VisibleForTesting
  int countNameNodes() {
    return bpServices.size();
  }

  /**
   * Run an immediate block report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerBlockReportForTests() throws IOException {
    for (BPServiceActor actor : bpServices) {
      actor.triggerBlockReportForTests();
    }
  }

  /**
   * Run an immediate deletion report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerDeletionReportForTests() throws IOException {
    for (BPServiceActor actor : bpServices) {
      actor.triggerDeletionReportForTests();
    }
  }

  /**
   * Run an immediate heartbeat from all actors. Used by tests.
   */
  @VisibleForTesting
  void triggerHeartbeatForTests() throws IOException {
    for (BPServiceActor actor : bpServices) {
      actor.triggerHeartbeatForTests();
    }
  }

  boolean processCommandFromActor(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    assert bpServices.contains(actor);
    if (cmd == null) {
      return true;
    }
    /*
     * Datanode Registration can be done asynchronously here. No need to hold
     * the lock. for more info refer HDFS-5014
     */
    if (DatanodeProtocol.DNA_REGISTER == cmd.getAction()) {
      // namenode requested a registration - at start or if NN lost contact
      // Just logging the claiming state is OK here instead of checking the
      // actor state by obtaining the lock
      LOG.info("DatanodeCommand action : DNA_REGISTER from " + actor.nnAddr
          + " with " + actor.state + " state");
      actor.reRegister();
      return false;
    }
    writeLock();
    try {
      if (actor == bpServiceToActive) {
        return processCommandFromActive(cmd, actor);
      } else {
        return processCommandFromStandby(cmd, actor);
      }
    } finally {
      writeUnlock();
    }
  }

  private String blockIdArrayToString(long ids[]) {
    long maxNumberOfBlocksToLog = dn.getMaxNumberOfBlocksToLog();
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (int i = 0; i < ids.length; i++) {
      if (i >= maxNumberOfBlocksToLog) {
        bld.append("...");
        break;
      }
      bld.append(prefix).append(ids[i]);
      prefix = ", ";
    }
    return bld.toString();
  }

  /**
   * This method should handle all commands from Active namenode except
   * DNA_REGISTER which should be handled earlier itself.
   * 
   * @param cmd
   * @return true if further processing may be required or false otherwise. 
   * @throws IOException
   */
  private boolean processCommandFromActive(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    final BlockCommand bcmd = 
      cmd instanceof BlockCommand? (BlockCommand)cmd: null;
    final BlockIdCommand blockIdCmd = 
      cmd instanceof BlockIdCommand ? (BlockIdCommand)cmd: null;

    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      // Send a copy of a block to another datanode
      dn.transferBlocks(bcmd.getBlockPoolId(), bcmd.getBlocks(),
          bcmd.getTargets(), bcmd.getTargetStorageTypes());
      dn.metrics.incrBlocksReplicated(bcmd.getBlocks().length);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      //
      // Some local block(s) are obsolete and can be 
      // safely garbage-collected.
      //
      Block toDelete[] = bcmd.getBlocks();
      try {
        // using global fsdataset
        dn.getFSDataset().invalidate(bcmd.getBlockPoolId(), toDelete);
      } catch(IOException e) {
        // Exceptions caught here are not expected to be disk-related.
        throw e;
      }
      dn.metrics.incrBlocksRemoved(toDelete.length);
      break;
    case DatanodeProtocol.DNA_CACHE:
      LOG.info("DatanodeCommand action: DNA_CACHE for " +
        blockIdCmd.getBlockPoolId() + " of [" +
          blockIdArrayToString(blockIdCmd.getBlockIds()) + "]");
      dn.getFSDataset().cache(blockIdCmd.getBlockPoolId(), blockIdCmd.getBlockIds());
      break;
    case DatanodeProtocol.DNA_UNCACHE:
      LOG.info("DatanodeCommand action: DNA_UNCACHE for " +
        blockIdCmd.getBlockPoolId() + " of [" +
          blockIdArrayToString(blockIdCmd.getBlockIds()) + "]");
      dn.getFSDataset().uncache(blockIdCmd.getBlockPoolId(), blockIdCmd.getBlockIds());
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // TODO: DNA_SHUTDOWN appears to be unused - the NN never sends this command
      // See HDFS-2987.
      throw new UnsupportedOperationException("Received unimplemented DNA_SHUTDOWN");
    case DatanodeProtocol.DNA_FINALIZE:
      String bp = ((FinalizeCommand) cmd).getBlockPoolId();
      LOG.info("Got finalize command for block pool " + bp);
      assert getBlockPoolId().equals(bp) :
        "BP " + getBlockPoolId() + " received DNA_FINALIZE " +
        "for other block pool " + bp;

      dn.finalizeUpgradeForPool(bp);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      String who = "NameNode at " + actor.getNNSocketAddress();
      dn.recoverBlocks(who, ((BlockRecoveryCommand)cmd).getRecoveringBlocks());
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
      if (dn.isBlockTokenEnabled) {
        dn.blockPoolTokenSecretManager.addKeys(
            getBlockPoolId(), 
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
        LOG.info("Updating balance throttler bandwidth from "
            + dxcs.balanceThrottler.getBandwidth() + " bytes/s "
            + "to: " + bandwidth + " bytes/s.");
        dxcs.balanceThrottler.setBandwidth(bandwidth);
      }
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }
 
  /**
   * This method should handle commands from Standby namenode except
   * DNA_REGISTER which should be handled earlier itself.
   */
  private boolean processCommandFromStandby(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      LOG.info("DatanodeCommand action from standby: DNA_ACCESSKEYUPDATE");
      if (dn.isBlockTokenEnabled) {
        dn.blockPoolTokenSecretManager.addKeys(
            getBlockPoolId(), 
            ((KeyUpdateCommand) cmd).getExportedKeys());
      }
      break;
    case DatanodeProtocol.DNA_TRANSFER:
    case DatanodeProtocol.DNA_INVALIDATE:
    case DatanodeProtocol.DNA_SHUTDOWN:
    case DatanodeProtocol.DNA_FINALIZE:
    case DatanodeProtocol.DNA_RECOVERBLOCK:
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
    case DatanodeProtocol.DNA_CACHE:
    case DatanodeProtocol.DNA_UNCACHE:
      LOG.warn("Got a command from standby NN - ignoring command:" + cmd.getAction());
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  /*
   * Let the actor retry for initialization until all namenodes of cluster have
   * failed.
   */
  boolean shouldRetryInit() {
    if (hasBlockPoolId()) {
      // One of the namenode registered successfully. lets continue retry for
      // other.
      return true;
    }
    return isAlive();
  }

}
