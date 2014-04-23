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
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.proto.JournalProtocolProtos.JournalProtocolService;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.JournalProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.protocol.FenceResponse;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.BlockingService;

/**
 * BackupNode.
 * <p>
 * Backup node can play two roles.
 * <ol>
 * <li>{@link NamenodeRole#CHECKPOINT} node periodically creates checkpoints, 
 * that is downloads image and edits from the active node, merges them, and
 * uploads the new image back to the active.</li>
 * <li>{@link NamenodeRole#BACKUP} node keeps its namespace in sync with the
 * active node, and periodically creates checkpoints by simply saving the
 * namespace image to local disk(s).</li>
 * </ol>
 */
@InterfaceAudience.Private
public class BackupNode extends NameNode {
  private static final String BN_ADDRESS_NAME_KEY = DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
  private static final String BN_ADDRESS_DEFAULT = DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_DEFAULT;
  private static final String BN_HTTP_ADDRESS_NAME_KEY = DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
  private static final String BN_HTTP_ADDRESS_DEFAULT = DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT;
  private static final String BN_SERVICE_RPC_ADDRESS_KEY = DFSConfigKeys.DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY;
  private static final float  BN_SAFEMODE_THRESHOLD_PCT_DEFAULT = 1.5f;
  private static final int    BN_SAFEMODE_EXTENSION_DEFAULT = Integer.MAX_VALUE;

  /** Name-node proxy */
  NamenodeProtocol namenode;
  /** Name-node RPC address */
  String nnRpcAddress;
  /** Name-node HTTP address */
  URL nnHttpAddress;
  /** Checkpoint manager */
  Checkpointer checkpointManager;
  
  BackupNode(Configuration conf, NamenodeRole role) throws IOException {
    super(conf, role);
  }

  /////////////////////////////////////////////////////
  // Common NameNode methods implementation for backup node.
  /////////////////////////////////////////////////////
  @Override // NameNode
  protected InetSocketAddress getRpcServerAddress(Configuration conf) {
    String addr = conf.get(BN_ADDRESS_NAME_KEY, BN_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr);
  }
  
  @Override
  protected InetSocketAddress getServiceRpcServerAddress(Configuration conf) {
    String addr = conf.get(BN_SERVICE_RPC_ADDRESS_KEY);
    if (addr == null || addr.isEmpty()) {
      return null;
    }
    return NetUtils.createSocketAddr(addr);
  }

  @Override // NameNode
  protected void setRpcServerAddress(Configuration conf,
      InetSocketAddress addr) {
    conf.set(BN_ADDRESS_NAME_KEY, NetUtils.getHostPortString(addr));
  }
  
  @Override // Namenode
  protected void setRpcServiceServerAddress(Configuration conf,
      InetSocketAddress addr) {
    conf.set(BN_SERVICE_RPC_ADDRESS_KEY, NetUtils.getHostPortString(addr));
  }

  @Override // NameNode
  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    assert getNameNodeAddress() != null : "rpcAddress should be calculated first";
    String addr = conf.get(BN_HTTP_ADDRESS_NAME_KEY, BN_HTTP_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr);
  }

  @Override // NameNode
  protected void loadNamesystem(Configuration conf) throws IOException {
    conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
                                BN_SAFEMODE_THRESHOLD_PCT_DEFAULT);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY,
                                BN_SAFEMODE_EXTENSION_DEFAULT);
    BackupImage bnImage = new BackupImage(conf);
    this.namesystem = new FSNamesystem(conf, bnImage);
    namesystem.dir.disableQuotaChecks();
    bnImage.setNamesystem(namesystem);
    bnImage.recoverCreateRead();
  }

  @Override // NameNode
  protected void initialize(Configuration conf) throws IOException {
    // Trash is disabled in BackupNameNode,
    // but should be turned back on if it ever becomes active.
    conf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 
                 CommonConfigurationKeys.FS_TRASH_INTERVAL_DEFAULT);
    NamespaceInfo nsInfo = handshake(conf);
    super.initialize(conf);
    namesystem.setBlockPoolId(nsInfo.getBlockPoolID());

    if (false == namesystem.isInSafeMode()) {
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    }

    // Backup node should never do lease recovery,
    // therefore lease hard limit should never expire.
    namesystem.leaseManager.setLeasePeriod(
        HdfsConstants.LEASE_SOFTLIMIT_PERIOD, Long.MAX_VALUE);

    // register with the active name-node 
    registerWith(nsInfo);
    // Checkpoint daemon should start after the rpc server started
    runCheckpointDaemon(conf);
    InetSocketAddress addr = getHttpAddress();
    if (addr != null) {
      conf.set(BN_HTTP_ADDRESS_NAME_KEY, NetUtils.getHostPortString(getHttpAddress()));
    }
  }

  @Override
  protected NameNodeRpcServer createRpcServer(Configuration conf)
      throws IOException {
    return new BackupNodeRpcServer(conf, this);
  }

  @Override // NameNode
  public void stop() {
    if(checkpointManager != null) {
      // Prevent from starting a new checkpoint.
      // Checkpoints that has already been started may proceed until 
      // the error reporting to the name-node is complete.
      // Checkpoint manager should not be interrupted yet because it will
      // close storage file channels and the checkpoint may fail with 
      // ClosedByInterruptException.
      checkpointManager.shouldRun = false;
    }
    if(namenode != null && getRegistration() != null) {
      // Exclude this node from the list of backup streams on the name-node
      try {
        namenode.errorReport(getRegistration(), NamenodeProtocol.FATAL,
            "Shutting down.");
      } catch(IOException e) {
        LOG.error("Failed to report to name-node.", e);
      }
    }
    // Stop the RPC client
    if (namenode != null) {
      RPC.stopProxy(namenode);
    }
    namenode = null;
    // Stop the checkpoint manager
    if(checkpointManager != null) {
      checkpointManager.interrupt();
      checkpointManager = null;
    }

    // Abort current log segment - otherwise the NN shutdown code
    // will close it gracefully, which is incorrect.
    getFSImage().getEditLog().abortCurrentLogSegment();

    // Stop name-node threads
    super.stop();
  }
  
  /* @Override */// NameNode
  public boolean setSafeMode(SafeModeAction action)
      throws IOException {
    throw new UnsupportedActionException("setSafeMode");
  }
  
  static class BackupNodeRpcServer extends NameNodeRpcServer implements
      JournalProtocol {
    private BackupNodeRpcServer(Configuration conf, BackupNode nn)
        throws IOException {
      super(conf, nn);
      JournalProtocolServerSideTranslatorPB journalProtocolTranslator = 
          new JournalProtocolServerSideTranslatorPB(this);
      BlockingService service = JournalProtocolService
          .newReflectiveBlockingService(journalProtocolTranslator);
      DFSUtil.addPBProtocol(conf, JournalProtocolPB.class, service,
          this.clientRpcServer);
    }
    
    /** 
     * Verifies a journal request
     */
    private void verifyJournalRequest(JournalInfo journalInfo)
        throws IOException {
      verifyLayoutVersion(journalInfo.getLayoutVersion());
      String errorMsg = null;
      int expectedNamespaceID = namesystem.getNamespaceInfo().getNamespaceID();
      if (journalInfo.getNamespaceId() != expectedNamespaceID) {
        errorMsg = "Invalid namespaceID in journal request - expected " + expectedNamespaceID
            + " actual " + journalInfo.getNamespaceId();
        LOG.warn(errorMsg);
        throw new UnregisteredNodeException(journalInfo);
      } 
      if (!journalInfo.getClusterId().equals(namesystem.getClusterId())) {
        errorMsg = "Invalid clusterId in journal request - expected "
            + journalInfo.getClusterId() + " actual " + namesystem.getClusterId();
        LOG.warn(errorMsg);
        throw new UnregisteredNodeException(journalInfo);
      }
    }

    /////////////////////////////////////////////////////
    // BackupNodeProtocol implementation for backup node.
    /////////////////////////////////////////////////////
    @Override
    public void startLogSegment(JournalInfo journalInfo, long epoch,
        long txid) throws IOException {
      namesystem.checkOperation(OperationCategory.JOURNAL);
      verifyJournalRequest(journalInfo);
      getBNImage().namenodeStartedLogSegment(txid);
    }
    
    @Override
    public void journal(JournalInfo journalInfo, long epoch, long firstTxId,
        int numTxns, byte[] records) throws IOException {
      namesystem.checkOperation(OperationCategory.JOURNAL);
      verifyJournalRequest(journalInfo);
      getBNImage().journal(firstTxId, numTxns, records);
    }

    private BackupImage getBNImage() {
      return (BackupImage)nn.getFSImage();
    }

    @Override
    public FenceResponse fence(JournalInfo journalInfo, long epoch,
        String fencerInfo) throws IOException {
      LOG.info("Fenced by " + fencerInfo + " with epoch " + epoch);
      throw new UnsupportedOperationException(
          "BackupNode does not support fence");
    }
  }
  
  //////////////////////////////////////////////////////
  

  boolean shouldCheckpointAtStartup() {
    FSImage fsImage = getFSImage();
    if(isRole(NamenodeRole.CHECKPOINT)) {
      assert fsImage.getStorage().getNumStorageDirs() > 0;
      return ! fsImage.getStorage().getStorageDir(0).getVersionFile().exists();
    }
    
    // BN always checkpoints on startup in order to get in sync with namespace
    return true;
  }

  private NamespaceInfo handshake(Configuration conf) throws IOException {
    // connect to name node
    InetSocketAddress nnAddress = NameNode.getServiceAddress(conf, true);
    this.namenode = NameNodeProxies.createNonHAProxy(conf, nnAddress,
        NamenodeProtocol.class, UserGroupInformation.getCurrentUser(),
        true).getProxy();
    this.nnRpcAddress = NetUtils.getHostPortString(nnAddress);
    this.nnHttpAddress = DFSUtil.getInfoServer(nnAddress, conf,
        DFSUtil.getHttpClientScheme(conf)).toURL();
    // get version and id info from the name-node
    NamespaceInfo nsInfo = null;
    while(!isStopRequested()) {
      try {
        nsInfo = handshake(namenode);
        break;
      } catch(SocketTimeoutException e) {  // name-node is busy
        LOG.info("Problem connecting to server: " + nnAddress);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          LOG.warn("Encountered exception ", e);
        }
      }
    }
    return nsInfo;
  }

  /**
   * Start a backup node daemon.
   */
  private void runCheckpointDaemon(Configuration conf) throws IOException {
    checkpointManager = new Checkpointer(conf, this);
    checkpointManager.start();
  }

  /**
   * Checkpoint.<br>
   * Tests may use it to initiate a checkpoint process.
   * @throws IOException
   */
  void doCheckpoint() throws IOException {
    checkpointManager.doCheckpoint();
  }

  /**
   * Register this backup node with the active name-node.
   * @param nsInfo namespace information
   * @throws IOException
   */
  private void registerWith(NamespaceInfo nsInfo) throws IOException {
    BackupImage bnImage = (BackupImage)getFSImage();
    NNStorage storage = bnImage.getStorage();
    // verify namespaceID
    if (storage.getNamespaceID() == 0) { // new backup storage
      storage.setStorageInfo(nsInfo);
      storage.setBlockPoolID(nsInfo.getBlockPoolID());
      storage.setClusterID(nsInfo.getClusterID());
    } else {
      nsInfo.validateStorage(storage);
    }
    bnImage.initEditLog(StartupOption.REGULAR);
    setRegistration();
    NamenodeRegistration nnReg = null;
    while(!isStopRequested()) {
      try {
        nnReg = namenode.registerSubordinateNamenode(getRegistration());
        break;
      } catch(SocketTimeoutException e) {  // name-node is busy
        LOG.info("Problem connecting to name-node: " + nnRpcAddress);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          LOG.warn("Encountered exception ", e);
        }
      }
    }

    String msg = null;
    if(nnReg == null) // consider as a rejection
      msg = "Registration rejected by " + nnRpcAddress;
    else if(!nnReg.isRole(NamenodeRole.NAMENODE)) {
      msg = "Name-node " + nnRpcAddress + " is not active";
    }
    if(msg != null) {
      msg += ". Shutting down.";
      LOG.error(msg);
      throw new IOException(msg); // stop the node
    }
    nnRpcAddress = nnReg.getAddress();
  }

  // TODO: move to a common with DataNode util class
  private static NamespaceInfo handshake(NamenodeProtocol namenode)
  throws IOException, SocketTimeoutException {
    NamespaceInfo nsInfo;
    nsInfo = namenode.versionRequest();  // throws SocketTimeoutException 
    String errorMsg = null;
    // verify build version
    if( ! nsInfo.getBuildVersion().equals( Storage.getBuildVersion())) {
      errorMsg = "Incompatible build versions: active name-node BV = " 
        + nsInfo.getBuildVersion() + "; backup node BV = "
        + Storage.getBuildVersion();
      LOG.fatal(errorMsg);
      throw new IOException(errorMsg);
    }
    assert HdfsConstants.NAMENODE_LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Active and backup node layout versions must be the same. Expected: "
      + HdfsConstants.NAMENODE_LAYOUT_VERSION + " actual "+ nsInfo.getLayoutVersion();
    return nsInfo;
  }

  @Override
  protected String getNameServiceId(Configuration conf) {
    return DFSUtil.getBackupNameServiceId(conf);
  }

  @Override
  protected HAState createHAState(StartupOption startOpt) {
    return new BackupState();
  }

  @Override // NameNode
  protected NameNodeHAContext createHAContext() {
    return new BNHAContext();
  }

  private class BNHAContext extends NameNodeHAContext {
    @Override // NameNodeHAContext
    public void checkOperation(OperationCategory op)
        throws StandbyException {
      if (op == OperationCategory.UNCHECKED ||
          op == OperationCategory.CHECKPOINT) {
        return;
      }
      if (OperationCategory.JOURNAL != op &&
          !(OperationCategory.READ == op && !isRole(NamenodeRole.CHECKPOINT))) {
        String msg = "Operation category " + op
            + " is not supported at " + getRole();
        throw new StandbyException(msg);
      }
    }

    @Override // NameNodeHAContext
    public void prepareToStopStandbyServices() throws ServiceFailedException {
    }

    /**
     * Start services for BackupNode.
     * <p>
     * The following services should be muted
     * (not run or not pass any control commands to DataNodes)
     * on BackupNode:
     * {@link LeaseManager.Monitor} protected by SafeMode.
     * {@link BlockManager.ReplicationMonitor} protected by SafeMode.
     * {@link HeartbeatManager.Monitor} protected by SafeMode.
     * {@link DecommissionManager.Monitor} need to prohibit refreshNodes().
     * {@link PendingReplicationBlocks.PendingReplicationMonitor} harmless,
     * because ReplicationMonitor is muted.
     */
    @Override
    public void startActiveServices() throws IOException {
      try {
        namesystem.startActiveServices();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void stopActiveServices() throws IOException {
      try {
        if (namesystem != null) {
          namesystem.stopActiveServices();
        }
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }
  }
}
