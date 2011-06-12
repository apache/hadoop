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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.FSImage.CheckpointStates;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;

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

  /** Name-node proxy */
  NamenodeProtocol namenode;
  /** Name-node RPC address */
  String nnRpcAddress;
  /** Name-node HTTP address */
  String nnHttpAddress;
  /** Checkpoint manager */
  Checkpointer checkpointManager;

  BackupNode(Configuration conf, NamenodeRole role) throws IOException {
    super(conf, role);
  }

  /////////////////////////////////////////////////////
  // Common NameNode methods implementation for backup node.
  /////////////////////////////////////////////////////
  @Override // NameNode
  protected InetSocketAddress getRpcServerAddress(Configuration conf) throws IOException {
    String addr = conf.get(BN_ADDRESS_NAME_KEY, BN_ADDRESS_DEFAULT);
    int port = NetUtils.createSocketAddr(addr).getPort();
    String hostName = DNS.getDefaultHost("default");
    return new InetSocketAddress(hostName, port);
  }

  @Override // NameNode
  protected void setRpcServerAddress(Configuration conf) {
    conf.set(BN_ADDRESS_NAME_KEY, getHostPortString(rpcAddress));
  }

  @Override // NameNode
  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    // It is necessary to resolve the hostname at this point in order
    // to ensure that the server address that is sent to the namenode
    // is correct.
    assert rpcAddress != null : "rpcAddress should be calculated first";
    String addr = conf.get(BN_HTTP_ADDRESS_NAME_KEY, BN_HTTP_ADDRESS_DEFAULT);
    int port = NetUtils.createSocketAddr(addr).getPort();
    String hostName = rpcAddress.getHostName();
    return new InetSocketAddress(hostName, port);
  }

  @Override // NameNode
  protected void setHttpServerAddress(Configuration conf){
    conf.set(BN_HTTP_ADDRESS_NAME_KEY, getHostPortString(httpAddress));
  }

  @Override // NameNode
  protected void loadNamesystem(Configuration conf) throws IOException {
    BackupStorage bnImage = new BackupStorage();
    this.namesystem = new FSNamesystem(conf, bnImage);
    bnImage.recoverCreateRead(FSNamesystem.getNamespaceDirs(conf),
                              FSNamesystem.getNamespaceEditsDirs(conf));
  }

  @Override // NameNode
  protected void initialize(Configuration conf) throws IOException {
    // Trash is disabled in BackupNameNode,
    // but should be turned back on if it ever becomes active.
    conf.setLong("fs.trash.interval", 0L);
    NamespaceInfo nsInfo = handshake(conf);
    super.initialize(conf);
    // Backup node should never do lease recovery,
    // therefore lease hard limit should never expire.
    namesystem.leaseManager.setLeasePeriod(
        FSConstants.LEASE_SOFTLIMIT_PERIOD, Long.MAX_VALUE);
    // register with the active name-node 
    registerWith(nsInfo);
    // Checkpoint daemon should start after the rpc server started
    runCheckpointDaemon(conf);
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
    RPC.stopProxy(namenode);
    namenode = null;
    // Stop the checkpoint manager
    if(checkpointManager != null) {
      checkpointManager.interrupt();
      checkpointManager = null;
    }
    // Stop name-node threads
    super.stop();
  }

  /////////////////////////////////////////////////////
  // NamenodeProtocol implementation for backup node.
  /////////////////////////////////////////////////////
  @Override // NamenodeProtocol
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException {
    throw new UnsupportedActionException("getBlocks");
  }

  // Only active name-node can register other nodes.
  @Override // NamenodeProtocol
  public NamenodeRegistration register(NamenodeRegistration registration
  ) throws IOException {
    throw new UnsupportedActionException("journal");
  }

  @Override // NamenodeProtocol
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
  throws IOException {
    throw new UnsupportedActionException("startCheckpoint");
  }

  @Override // NamenodeProtocol
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    throw new UnsupportedActionException("endCheckpoint");
  }

  @Override // NamenodeProtocol
  public void journal(NamenodeRegistration nnReg,
                      int jAction,
                      int length,
                      byte[] args) throws IOException {
    verifyRequest(nnReg);
    if(!nnRpcAddress.equals(nnReg.getAddress()))
      throw new IOException("Journal request from unexpected name-node: "
          + nnReg.getAddress() + " expecting " + nnRpcAddress);
    BackupStorage bnImage = (BackupStorage)getFSImage();
    switch(jAction) {
      case (int)JA_IS_ALIVE:
        return;
      case (int)JA_JOURNAL:
        bnImage.journal(length, args);
        return;
      case (int)JA_JSPOOL_START:
        bnImage.startJournalSpool(nnReg);
        return;
      case (int)JA_CHECKPOINT_TIME:
        bnImage.setCheckpointTime(length, args);
        setRegistration(); // keep registration up to date
        return;
      default:
        throw new IOException("Unexpected journal action: " + jAction);
    }
  }

  boolean shouldCheckpointAtStartup() {
    FSImage fsImage = getFSImage();
    if(isRole(NamenodeRole.CHECKPOINT)) {
      assert fsImage.getNumStorageDirs() > 0;
      return ! fsImage.getStorageDir(0).getVersionFile().exists();
    }
    if(namesystem == null || namesystem.dir == null || getFSImage() == null)
      return true;
    return fsImage.getEditLog().getNumEditStreams() == 0;
  }

  private NamespaceInfo handshake(Configuration conf) throws IOException {
    // connect to name node
    InetSocketAddress nnAddress = super.getRpcServerAddress(conf);
    this.namenode =
      (NamenodeProtocol) RPC.waitForProxy(NamenodeProtocol.class,
          NamenodeProtocol.versionID, nnAddress, conf);
    this.nnRpcAddress = getHostPortString(nnAddress);
    this.nnHttpAddress = getHostPortString(super.getHttpServerAddress(conf));
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
        } catch (InterruptedException ie) {}
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

  CheckpointStates getCheckpointState() {
    return getFSImage().getCheckpointState();
  }

  void setCheckpointState(CheckpointStates cs) {
    getFSImage().setCheckpointState(cs);
  }

  /**
   * Register this backup node with the active name-node.
   * @param nsInfo
   * @throws IOException
   */
  private void registerWith(NamespaceInfo nsInfo) throws IOException {
    BackupStorage bnImage = (BackupStorage)getFSImage();
    // verify namespaceID
    if(bnImage.getNamespaceID() == 0) // new backup storage
      bnImage.setStorageInfo(nsInfo);
    else if(bnImage.getNamespaceID() != nsInfo.getNamespaceID())
      throw new IOException("Incompatible namespaceIDs"
          + ": active node namespaceID = " + nsInfo.getNamespaceID() 
          + "; backup node namespaceID = " + bnImage.getNamespaceID());

    setRegistration();
    NamenodeRegistration nnReg = null;
    while(!isStopRequested()) {
      try {
        nnReg = namenode.register(getRegistration());
        break;
      } catch(SocketTimeoutException e) {  // name-node is busy
        LOG.info("Problem connecting to name-node: " + nnRpcAddress);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }

    String msg = null;
    if(nnReg == null) // consider as a rejection
      msg = "Registration rejected by " + nnRpcAddress;
    else if(!nnReg.isRole(NamenodeRole.ACTIVE)) {
      msg = "Name-node " + nnRpcAddress + " is not active";
    }
    if(msg != null) {
      msg += ". Shutting down.";
      LOG.error(msg);
      throw new IOException(msg); // stop the node
    }
    nnRpcAddress = nnReg.getAddress();
  }

  /**
   * Reset node namespace state in memory and in storage directories.
   * @throws IOException
   */
  void resetNamespace() throws IOException {
    ((BackupStorage)getFSImage()).reset();
  }

  /**
   * Get size of the local journal (edit log).
   * @return size of the current journal
   * @throws IOException
   */
  long journalSize() throws IOException {
    return namesystem.getEditLogSize();
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
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Active and backup node layout versions must be the same. Expected: "
      + FSConstants.LAYOUT_VERSION + " actual "+ nsInfo.getLayoutVersion();
    return nsInfo;
  }
}
