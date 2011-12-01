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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.ipc.RPC;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
  DatanodeRegistration bpRegistration;
  
  UpgradeManagerDatanode upgradeManager = null;
  private final DataNode dn;

  private BPServiceActor bpServiceToActive;
  private List<BPServiceActor> bpServices =
    new CopyOnWriteArrayList<BPServiceActor>();

  BPOfferService(List<InetSocketAddress> nnAddrs, DataNode dn) {
    Preconditions.checkArgument(!nnAddrs.isEmpty(),
        "Must pass at least one NN.");
    this.dn = dn;

    for (InetSocketAddress addr : nnAddrs) {
      this.bpServices.add(new BPServiceActor(addr, this));
    }
    // TODO(HA): currently we just make the first one the initial
    // active. In reality it should start in an unknown state and then
    // as we figure out which is active, designate one as such.
    this.bpServiceToActive = this.bpServices.get(0);
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
   * returns true if BP thread has completed initialization of storage
   * and has registered with the corresponding namenode
   * @return true if initialized
   */
  boolean isInitialized() {
    // TODO(HA) is this right?
    return bpServiceToActive != null && bpServiceToActive.isInitialized();
  }
  
  boolean isAlive() {
    // TODO: should || all the bp actors probably?
    return bpServiceToActive != null &&
      bpServiceToActive.isAlive();
  }
  
  String getBlockPoolId() {
    if (bpNSInfo != null) {
      return bpNSInfo.getBlockPoolID();
    } else {
      LOG.warn("Block pool ID needed, but service not yet registered with NN",
          new Exception("trace"));
      return null;
    }
  }
  
  NamespaceInfo getNamespaceInfo() {
    return bpNSInfo;
  }
  
  @Override
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
        ")";
    } else {
      return "Block pool " + getBlockPoolId() +
        " (storage id " + dn.getStorageId() +
        ")";
    }
  }
  
  void reportBadBlocks(ExtendedBlock block) {
    checkBlock(block);
    for (BPServiceActor actor : bpServices) {
      actor.reportBadBlocks(block);
    }
  }
  
  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint) {
    checkBlock(block);
    checkDelHint(delHint);
    ReceivedDeletedBlockInfo bInfo = 
               new ReceivedDeletedBlockInfo(block.getLocalBlock(), delHint);
    for (BPServiceActor actor : bpServices) {
      actor.notifyNamenodeReceivedBlock(bInfo);
    }
  }

  private void checkBlock(ExtendedBlock block) {
    Preconditions.checkArgument(block != null,
        "block is null");
    Preconditions.checkArgument(block.getBlockPoolId().equals(getBlockPoolId()),
        "block belongs to BP %s instead of BP %s",
        block.getBlockPoolId(), getBlockPoolId());
  }
  
  private void checkDelHint(String delHint) {
    Preconditions.checkArgument(delHint != null,
        "delHint is null");
  }

  void notifyNamenodeDeletedBlock(ExtendedBlock block) {
    checkBlock(block);
    ReceivedDeletedBlockInfo bInfo = new ReceivedDeletedBlockInfo(block
          .getLocalBlock(), ReceivedDeletedBlockInfo.TODELETE_HINT);
    
    for (BPServiceActor actor : bpServices) {
      actor.notifyNamenodeDeletedBlock(bInfo);
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

  synchronized UpgradeManagerDatanode getUpgradeManager() {
    if(upgradeManager == null)
      upgradeManager = 
        new UpgradeManagerDatanode(dn, getBlockPoolId());
    
    return upgradeManager;
  }
  
  void processDistributedUpgradeCommand(UpgradeCommand comm)
  throws IOException {
    UpgradeManagerDatanode upgradeManager = getUpgradeManager();
    upgradeManager.processUpgradeCommand(comm);
  }

  /**
   * Start distributed upgrade if it should be initiated by the data-node.
   */
  synchronized void startDistributedUpgradeIfNeeded() throws IOException {
    UpgradeManagerDatanode um = getUpgradeManager();
    
    if(!um.getUpgradeState())
      return;
    um.setUpgradeState(false, um.getUpgradeVersion());
    um.startUpgrade();
    return;
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
    if (this.bpNSInfo == null) {
      this.bpNSInfo = nsInfo;
      
      // Now that we know the namespace ID, etc, we can pass this to the DN.
      // The DN can now initialize its local storage if we are the
      // first BP to handshake, etc.
      dn.initBlockPool(this);
      return;
    } else {
      checkNSEquality(bpNSInfo.getBlockPoolID(), nsInfo.getBlockPoolID(),
          "Blockpool ID");
      checkNSEquality(bpNSInfo.getNamespaceID(), nsInfo.getNamespaceID(),
          "Namespace ID");
      checkNSEquality(bpNSInfo.getClusterID(), nsInfo.getClusterID(),
          "Cluster ID");
    }
  }

  /**
   * After one of the BPServiceActors registers successfully with the
   * NN, it calls this function to verify that the NN it connected to
   * is consistent with other NNs serving the block-pool.
   */
  void registrationSucceeded(BPServiceActor bpServiceActor,
      DatanodeRegistration reg) throws IOException {
    if (bpRegistration != null) {
      checkNSEquality(bpRegistration.storageInfo.getNamespaceID(),
          reg.storageInfo.getNamespaceID(), "namespace ID");
      checkNSEquality(bpRegistration.storageInfo.getClusterID(),
          reg.storageInfo.getClusterID(), "cluster ID");
    } else {
      bpRegistration = reg;
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
    Preconditions.checkState(bpNSInfo != null,
        "getRegistration() can only be called after initial handshake");
    return dn.createBPRegistration(bpNSInfo);
  }

  /**
   * Called when an actor shuts down. If this is the last actor
   * to shut down, shuts down the whole blockpool in the DN.
   */
  void shutdownActor(BPServiceActor actor) {
    if (bpServiceToActive == actor) {
      bpServiceToActive = null;
    }

    bpServices.remove(actor);

    // TODO: synchronization should be a little better here
    if (bpServices.isEmpty()) {
      dn.shutdownBlockPool(this);
      
      if(upgradeManager != null)
        upgradeManager.shutdownUpgrade();
    }
  }

  @Deprecated
  InetSocketAddress getNNSocketAddress() {
    // TODO(HA) this doesn't make sense anymore
    return bpServiceToActive.getNNSocketAddress();
  }

  /**
   * Called by the DN to report an error to the NNs.
   */
  void trySendErrorReport(int errCode, String errMsg) {
    for (BPServiceActor actor : bpServices) {
      actor.trySendErrorReport(errCode, errMsg);
    }
  }

  /**
   * Ask each of the actors to schedule a block report after
   * the specified delay.
   */
  void scheduleBlockReport(long delay) {
    for (BPServiceActor actor : bpServices) {
      actor.scheduleBlockReport(delay);
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
   * TODO: this is still used in a few places where we need to sort out
   * what to do in HA!
   * @return a proxy to the active NN
   */
  @Deprecated
  DatanodeProtocol getActiveNN() {
    return bpServiceToActive.bpNamenode;
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

  boolean processCommandFromActor(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    assert bpServices.contains(actor);
    if (actor == bpServiceToActive) {
      return processCommandFromActive(cmd, actor);
    } else {
      return processCommandFromStandby(cmd, actor);
    }
  }

  /**
   * 
   * @param cmd
   * @return true if further processing may be required or false otherwise. 
   * @throws IOException
   */
  private boolean processCommandFromActive(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
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
        dn.checkDiskError();
        throw e;
      }
      dn.metrics.incrBlocksRemoved(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // TODO: DNA_SHUTDOWN appears to be unused - the NN never sends this command
      throw new UnsupportedOperationException("Received unimplemented DNA_SHUTDOWN");
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("DatanodeCommand action: DNA_REGISTER");
      actor.reRegister();
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      String bp = ((FinalizeCommand) cmd).getBlockPoolId(); 
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
        dn.blockPoolTokenSecretManager.setKeys(
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
        dxcs.balanceThrottler.setBandwidth(bandwidth);
      }
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }
 
  private boolean processCommandFromStandby(DatanodeCommand cmd,
      BPServiceActor actor) throws IOException {
    if (cmd == null)
      return true;
    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("DatanodeCommand action: DNA_REGISTER");
      actor.reRegister();
      return true;
    case DatanodeProtocol.DNA_TRANSFER:
    case DatanodeProtocol.DNA_INVALIDATE:
    case DatanodeProtocol.DNA_SHUTDOWN:
    case DatanodeProtocol.DNA_RECOVERBLOCK:
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
    case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
      LOG.warn("Got a command from standby NN - ignoring command:" + cmd.getAction());
      return true;   
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  /**
   * Connect to the NN at the given address. This is separated out for ease
   * of testing.
   */
  DatanodeProtocol connectToNN(InetSocketAddress nnAddr)
      throws IOException {
    return (DatanodeProtocol)RPC.waitForProxy(DatanodeProtocol.class,
        DatanodeProtocol.versionID, nnAddr, dn.getConf());
  }

}
